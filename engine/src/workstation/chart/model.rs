//! One rolling five-minute mid series per instrument, bucketed by spin interval, with this engine's
//! fills marked on it. Prices stay the exact half-tick integers the ladder and the monitor show,
//! and a bucket's index comes from event time alone, so a spin with no trustworthy mid leaves a
//! hole rather than an invented sample.

use std::mem;

use crate::ids::{InstrumentId, Price, Side};
use crate::msg::ui::{UiBookSnapshot, UiEvent};
use crate::time::{DurationUs, TsUs};
use crate::workstation::dom_view::{snapshot_mid, tick_index};
use crate::workstation::history::{BoundedHistory, grow_to};

/// Five minutes, the length of one Polymarket up/down window. Buckets older than this leave the
/// chart even while the ring still holds them.
const WINDOW_US: u64 = 300_000_000;

/// Caps the ring so a fast spin cannot size it by accident. 3,000 holds the whole window at a 100
/// ms spin, which is more buckets than the widest plot has points. A 1 ms spin would otherwise ask
/// for 300,000.
const MAX_BUCKETS_PER_INSTRUMENT: usize = 3_000;

/// Fill markers allowed per bucket, which sizes the fill ring from the bucket ring. The recorder
/// strategy fills at most twice a spin, because it quotes once per side and stops quoting a side as
/// soon as that side fills. Four leaves room for a strategy that quotes more often.
const MAX_FILLS_PER_BUCKET: usize = 4;

/// A coarse spin leaves only a handful of buckets in the window, and four fills per bucket would
/// then hold far too few markers for minutes of trading.
const MIN_FILL_CAPACITY: usize = 256;

/// One spin interval of mid, in half-ticks: the first, highest, lowest and last mid banked inside
/// it. `has_gap_before` marks a bucket the line must not be drawn back from, because book snapshots
/// for its own interval were lost on the way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChartBucket {
    pub index: u64,
    pub open_half_ticks: i64,
    pub high_half_ticks: i64,
    pub low_half_ticks: i64,
    pub close_half_ticks: i64,
    pub has_gap_before: bool,
}

/// One real fill at its own price in half-ticks, never snapped to the bucket's mid, so a fill away
/// from the line shows as away from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChartFill {
    pub index: u64,
    pub half_ticks: i64,
    pub side: Side,
}

/// Whether book snapshots were lost between an instrument's previous snapshot and this one. The
/// loss marks the bucket whose interval it fell in, so the line splits there instead of drawing a
/// straight segment over data that never arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookContinuity {
    Continuous,
    GapBefore,
}

struct InstrumentChart {
    tick_size: Option<Price>,
    buckets: BoundedHistory<ChartBucket>,
    fills: BoundedHistory<ChartFill>,
    is_gap_pending: bool,
}

impl InstrumentChart {
    fn new(tick_size: Option<Price>, bucket_capacity: usize) -> Self {
        Self {
            tick_size,
            buckets: BoundedHistory::new(bucket_capacity),
            fills: BoundedHistory::new(fill_capacity(bucket_capacity)),
            is_gap_pending: false,
        }
    }

    fn bank(&mut self, index: u64, mid_half_ticks: i64) {
        if let Some(newest) = self.buckets.last_mut() {
            if index < newest.index {
                return;
            }
            if index == newest.index {
                newest.high_half_ticks = newest.high_half_ticks.max(mid_half_ticks);
                newest.low_half_ticks = newest.low_half_ticks.min(mid_half_ticks);
                newest.close_half_ticks = mid_half_ticks;
                newest.has_gap_before |= mem::take(&mut self.is_gap_pending);
                return;
            }
        }
        self.buckets.push(ChartBucket {
            index,
            open_half_ticks: mid_half_ticks,
            high_half_ticks: mid_half_ticks,
            low_half_ticks: mid_half_ticks,
            close_half_ticks: mid_half_ticks,
            has_gap_before: mem::take(&mut self.is_gap_pending),
        });
    }
}

pub struct ChartModel {
    charts: Vec<InstrumentChart>,
    spin_interval: DurationUs,
    bucket_capacity: usize,
}

impl ChartModel {
    pub fn with_capacity(instrument_count: usize, spin_interval: DurationUs) -> Self {
        let mut model = Self {
            charts: Vec::new(),
            spin_interval,
            bucket_capacity: 0,
        };
        model.configure(&vec![None; instrument_count], spin_interval);
        model
    }

    pub fn configure(&mut self, tick_sizes: &[Option<Price>], spin_interval: DurationUs) {
        self.spin_interval = spin_interval;
        self.bucket_capacity = bucket_capacity(spin_interval);
        self.charts = tick_sizes
            .iter()
            .map(|tick_size| InstrumentChart::new(*tick_size, self.bucket_capacity))
            .collect();
    }

    pub fn apply_book(&mut self, snapshot: &UiBookSnapshot, continuity: BookContinuity) {
        let slot = snapshot.instrument.0 as usize;
        self.ensure_instruments(slot + 1);
        if continuity == BookContinuity::GapBefore {
            self.charts[slot].is_gap_pending = true;
        }
        let Some(index) = bucket_index(snapshot.event_ts_us, self.spin_interval) else { return };
        let Some(tick) = self.charts[slot].tick_size else { return };
        let Some(mid_half_ticks) = snapshot_mid(snapshot, tick) else { return };
        self.charts[slot].bank(index, mid_half_ticks);
    }

    pub fn apply_event(&mut self, event: &UiEvent) {
        match *event {
            UiEvent::Fill {
                instrument,
                event_ts_us,
                side,
                price,
                ..
            } => self.bank_fill(instrument, event_ts_us, side, price),
            UiEvent::Rotation { instrument, .. } => self.clear(instrument),
            _ => {}
        }
    }

    pub fn buckets(&self, instrument: InstrumentId) -> impl Iterator<Item = &ChartBucket> {
        self.charts
            .get(instrument.0 as usize)
            .into_iter()
            .flat_map(|chart| chart.buckets.iter_oldest_first())
    }

    pub fn fills(&self, instrument: InstrumentId) -> impl Iterator<Item = &ChartFill> {
        self.charts
            .get(instrument.0 as usize)
            .into_iter()
            .flat_map(|chart| chart.fills.iter_oldest_first())
    }

    pub fn capacity(&self) -> usize {
        self.bucket_capacity
    }

    pub fn spin_interval(&self) -> DurationUs {
        self.spin_interval
    }

    fn bank_fill(&mut self, instrument: InstrumentId, at: TsUs, side: Side, price: Price) {
        let slot = instrument.0 as usize;
        self.ensure_instruments(slot + 1);
        let Some(index) = bucket_index(at, self.spin_interval) else { return };
        let Some(tick) = self.charts[slot].tick_size else { return };
        let Some(half_ticks) = tick_index(price, tick).and_then(|grid| grid.checked_mul(2)) else {
            return;
        };
        self.charts[slot].fills.push(ChartFill {
            index,
            half_ticks,
            side,
        });
    }

    fn clear(&mut self, instrument: InstrumentId) {
        let capacity = self.bucket_capacity;
        let Some(chart) = self.charts.get_mut(instrument.0 as usize) else { return };
        *chart = InstrumentChart::new(chart.tick_size, capacity);
    }

    fn ensure_instruments(&mut self, len: usize) {
        grow_to(&mut self.charts, len, || {
            InstrumentChart::new(None, self.bucket_capacity)
        });
    }
}

pub(crate) fn bucket_index(at: TsUs, spin_interval: DurationUs) -> Option<u64> {
    let micros = at.micros();
    let interval = u64::try_from(spin_interval.micros()).ok()?;
    if micros < 0 || interval == 0 {
        return None;
    }
    Some(micros as u64 / interval)
}

pub fn bucket_open_ts(index: u64, spin_interval: DurationUs) -> Option<TsUs> {
    let interval = u64::try_from(spin_interval.micros()).ok()?;
    if interval == 0 {
        return None;
    }
    i64::try_from(index.checked_mul(interval)?)
        .ok()
        .map(TsUs::from_micros)
}

pub(crate) fn bucket_capacity(spin_interval: DurationUs) -> usize {
    let interval = u64::try_from(spin_interval.micros()).unwrap_or(0);
    let buckets = WINDOW_US / interval.max(1);
    (buckets as usize).clamp(1, MAX_BUCKETS_PER_INSTRUMENT)
}

fn fill_capacity(bucket_capacity: usize) -> usize {
    bucket_capacity
        .saturating_mul(MAX_FILLS_PER_BUCKET)
        .max(MIN_FILL_CAPACITY)
}
