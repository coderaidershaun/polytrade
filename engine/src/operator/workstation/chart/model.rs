//! A rolling five-minute series of mids for each instrument, one bucket for each spin, with this
//! engine's fills on it. A spin with no mid it can trust leaves a hole, never an invented point.

use std::mem;

use crate::msg::ids::{InstrumentId, Price, Side};
use crate::msg::time::{DurationUs, VenueTs};
use crate::msg::ui::{UiBookSnapshot, UiEvent};
use crate::operator::workstation::dom_view::{snapshot_mid, tick_index};
use crate::operator::workstation::history::{BoundedHistory, grow_to};

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
    /// The newest rotation known, live or from history. A history fill before it belongs to the
    /// market that rotated away.
    rotated_ts_us: Option<VenueTs>,
}

impl InstrumentChart {
    fn new(tick_size: Option<Price>, bucket_capacity: usize) -> Self {
        Self {
            tick_size,
            buckets: BoundedHistory::new(bucket_capacity),
            fills: BoundedHistory::new(fill_capacity(bucket_capacity)),
            is_gap_pending: false,
            rotated_ts_us: None,
        }
    }

    fn is_before_rotation(&self, at: VenueTs) -> bool {
        self.rotated_ts_us.is_some_and(|rotated| at < rotated)
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
            UiEvent::Rotation {
                instrument,
                event_ts_us,
                ..
            } => self.clear(instrument, event_ts_us),
            _ => {}
        }
    }

    /// An event the engine recorded before this session came. It can arrive after live frames
    /// that are newer, so only its fills reach the chart, and a replayed rotation removes the
    /// older fills instead of clearing what the live frames drew.
    pub fn apply_history(&mut self, event: &UiEvent) {
        match *event {
            UiEvent::Fill {
                instrument,
                event_ts_us,
                side,
                price,
                ..
            } => {
                let slot = instrument.0 as usize;
                self.ensure_instruments(slot + 1);
                if !self.charts[slot].is_before_rotation(event_ts_us) {
                    self.bank_fill(instrument, event_ts_us, side, price);
                }
            }
            UiEvent::Rotation {
                instrument,
                event_ts_us,
                ..
            } => self.forget_fills_before(instrument, event_ts_us),
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

    fn bank_fill(&mut self, instrument: InstrumentId, at: VenueTs, side: Side, price: Price) {
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

    fn clear(&mut self, instrument: InstrumentId, rotated_ts_us: VenueTs) {
        let capacity = self.bucket_capacity;
        let Some(chart) = self.charts.get_mut(instrument.0 as usize) else { return };
        let newest = chart
            .rotated_ts_us
            .map_or(rotated_ts_us, |held| held.max(rotated_ts_us));
        *chart = InstrumentChart::new(chart.tick_size, capacity);
        chart.rotated_ts_us = Some(newest);
    }

    fn forget_fills_before(&mut self, instrument: InstrumentId, rotated_ts_us: VenueTs) {
        let slot = instrument.0 as usize;
        self.ensure_instruments(slot + 1);
        let chart = &mut self.charts[slot];
        if chart.is_before_rotation(rotated_ts_us) {
            return;
        }
        chart.rotated_ts_us = Some(rotated_ts_us);
        let Some(first_kept) = bucket_index(rotated_ts_us, self.spin_interval) else { return };
        let mut kept = BoundedHistory::new(fill_capacity(self.bucket_capacity));
        for fill in chart.fills.iter_oldest_first() {
            if fill.index >= first_kept {
                kept.push(*fill);
            }
        }
        chart.fills = kept;
    }

    fn ensure_instruments(&mut self, len: usize) {
        grow_to(&mut self.charts, len, || {
            InstrumentChart::new(None, self.bucket_capacity)
        });
    }
}

pub(crate) fn bucket_index(at: VenueTs, spin_interval: DurationUs) -> Option<u64> {
    let micros = at.micros();
    let interval = u64::try_from(spin_interval.micros()).ok()?;
    if micros < 0 || interval == 0 {
        return None;
    }
    Some(micros as u64 / interval)
}

pub fn bucket_open_ts(index: u64, spin_interval: DurationUs) -> Option<VenueTs> {
    let interval = u64::try_from(spin_interval.micros()).ok()?;
    if interval == 0 {
        return None;
    }
    i64::try_from(index.checked_mul(interval)?)
        .ok()
        .map(VenueTs::from_micros)
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
