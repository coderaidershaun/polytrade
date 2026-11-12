//! Kline candles: rolling closed-candle window + open-forming candle per interval.

use super::BACKING_MULTIPLE;
use crate::config::{CandlesSpec, KlineInterval};
use crate::hot::series::{Element, FastQueue};
use crate::ids::{Price, Qty};
use crate::msg::inbound::KlineEvent;
use crate::time::TsUs;

/// POD KlineEvent (minus instrument/transport stamps).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Candle {
    pub interval: KlineInterval,
    pub open_ts_us: TsUs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub base_volume: Qty,
    pub quote_volume: i64,
    pub trade_count: u32,
}

impl crate::hot::series::sealed::Sealed for Candle {}
impl Element for Candle {}

impl Candle {
    pub(super) fn from_event(event: &KlineEvent) -> Self {
        Self {
            interval: event.interval,
            open_ts_us: event.open_ts_us,
            open: event.open,
            high: event.high,
            low: event.low,
            close: event.close,
            base_volume: event.base_volume,
            quote_volume: event.quote_volume,
            trade_count: event.trade_count,
        }
    }
}

/// Rolling closed-candle series + open-forming candle.
#[derive(Debug, Clone, PartialEq)]
pub struct CandleSeries {
    pub closed: FastQueue<Candle>,
    pub open: Option<Candle>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct CandleBook {
    pub(super) series: Vec<(KlineInterval, CandleSeries)>,
}

impl CandleBook {
    pub(super) fn new(spec: &CandlesSpec, intervals: &[KlineInterval]) -> Self {
        Self {
            series: intervals
                .iter()
                .map(|&interval| {
                    let closed = FastQueue::new(spec.keep, BACKING_MULTIPLE);
                    (interval, CandleSeries { closed, open: None })
                })
                .collect(),
        }
    }

    pub(super) fn get(&self, interval: KlineInterval) -> Option<&CandleSeries> {
        self.series
            .iter()
            .find(|(each, _)| *each == interval)
            .map(|(_, series)| series)
    }

    pub(super) fn get_mut(&mut self, interval: KlineInterval) -> Option<&mut CandleSeries> {
        self.series
            .iter_mut()
            .find(|(each, _)| *each == interval)
            .map(|(_, series)| series)
    }

    pub(super) fn clear(&mut self) {
        for (_, series) in &mut self.series {
            series.closed.clear();
            series.open = None;
        }
    }
}
