//! Deterministic ordering across simulator command, trade, and depth lanes.

use std::collections::VecDeque;
use std::fmt;

use super::core::latency::{LatencyBudget, rewound, shifted};
use crate::labelled_enum::labelled_enum;
use crate::time::TsUs;

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SimLane {
        Command = "command",
        Trade = "trade",
        Depth = "depth",
    }
    fn name;
    pub(super) const ALL;
}

impl fmt::Display for SimLane {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("{lane} lane buffer reached capacity {capacity}")]
pub struct LaneBufferFull {
    pub lane: SimLane,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimHorizon {
    latency: LatencyBudget,
    watermarks: [TsUs; SimLane::ALL.len()],
    observed: [bool; SimLane::ALL.len()],
}

impl SimHorizon {
    pub fn unseeded(latency: LatencyBudget) -> Self {
        Self {
            latency,
            watermarks: [TsUs::from_micros(i64::MIN); SimLane::ALL.len()],
            observed: [false; SimLane::ALL.len()],
        }
    }

    pub fn observe(&mut self, lane: SimLane, watermark: TsUs) {
        let held = &mut self.watermarks[lane as usize];
        *held = (*held).max(watermark);
        self.observed[lane as usize] = true;
    }

    pub fn watermark(&self, lane: SimLane) -> TsUs {
        self.watermarks[lane as usize]
    }

    pub fn has_all_watermarks(&self) -> bool {
        self.observed.iter().all(|observed| *observed)
    }

    pub fn safe_venue_horizon(&self) -> TsUs {
        let command = shifted(self.watermark(SimLane::Command), [self.latency.order_entry]);
        let market = |lane| rewound(self.watermark(lane), self.latency.max_market_data_delay);
        command
            .min(market(SimLane::Trade))
            .min(market(SimLane::Depth))
    }
}

#[derive(Debug, Clone)]
pub struct LaneBuffer<T> {
    lane: SimLane,
    capacity: usize,
    items: VecDeque<(TsUs, T)>,
    latest_ts_us: Option<TsUs>,
}

impl<T> LaneBuffer<T> {
    pub fn new(lane: SimLane, capacity: usize) -> Self {
        Self {
            lane,
            capacity,
            items: VecDeque::with_capacity(capacity),
            latest_ts_us: None,
        }
    }

    pub fn peek_ts_us(&self) -> Option<TsUs> {
        self.items.front().map(|(at, _)| *at)
    }

    /// # Errors
    /// [`LaneBufferFull`] when the fixed-capacity lane has no free slot.
    /// # Panics
    /// If timestamps move backwards.
    pub fn try_push(&mut self, at_ts_us: TsUs, item: T) -> Result<(), LaneBufferFull> {
        if self.items.len() == self.capacity {
            return Err(LaneBufferFull {
                lane: self.lane,
                capacity: self.capacity,
            });
        }
        if let Some(latest) = self.latest_ts_us {
            assert!(
                at_ts_us >= latest,
                "the {} lane delivered {}µs after {}µs — lane FIFO was broken",
                self.lane,
                at_ts_us.micros(),
                latest.micros()
            );
        }
        self.latest_ts_us = Some(at_ts_us);
        self.items.push_back((at_ts_us, item));
        Ok(())
    }

    pub fn next_due(&mut self, horizon: TsUs) -> Option<T> {
        match self.peek_ts_us() {
            Some(at) if at <= horizon => self.items.pop_front().map(|(_, item)| item),
            _ => None,
        }
    }
}
