//! One rolling five-minute exposure and PnL series per instrument, bucketed by the mid chart's own
//! spin arithmetic. The link carries these values as `f64`. They become exact quote amounts once,
//! here, so the mid chart's integer helpers serve this chart unchanged.

use super::model::{bucket_capacity, bucket_index};
use crate::ids::{InstrumentId, fixed_mantissa};
use crate::msg::ui::UiEvent;
use crate::time::{DurationUs, TsUs};
use crate::workstation::history::{BoundedHistory, grow_to};

/// The engine's absolute position at the end of a spin, never a change to add up, so a lost frame
/// costs resolution and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionBucket {
    pub index: u64,
    pub exposure_quote: i64,
    pub pnl_quote: i64,
}

pub struct PositionModel {
    series: Vec<BoundedHistory<PositionBucket>>,
    spin_interval: DurationUs,
    bucket_capacity: usize,
    rejected_frames: u64,
}

impl PositionModel {
    pub fn with_capacity(instrument_count: usize, spin_interval: DurationUs) -> Self {
        let mut model = Self {
            series: Vec::new(),
            spin_interval,
            bucket_capacity: 0,
            rejected_frames: 0,
        };
        model.configure(instrument_count, spin_interval);
        model
    }

    pub fn configure(&mut self, instrument_count: usize, spin_interval: DurationUs) {
        self.spin_interval = spin_interval;
        self.bucket_capacity = bucket_capacity(spin_interval);
        self.series = (0..instrument_count)
            .map(|_| BoundedHistory::new(self.bucket_capacity))
            .collect();
    }

    pub fn apply_event(&mut self, event: &UiEvent) {
        match *event {
            UiEvent::Position {
                instrument,
                event_ts_us,
                exposure_quote,
                pnl_quote,
                ..
            } => self.bank_position(instrument, event_ts_us, exposure_quote, pnl_quote),
            UiEvent::Rotation { instrument, .. } => self.clear(instrument),
            _ => {}
        }
    }

    pub fn buckets(&self, instrument: InstrumentId) -> impl Iterator<Item = &PositionBucket> {
        self.series
            .get(instrument.0 as usize)
            .into_iter()
            .flat_map(BoundedHistory::iter_oldest_first)
    }

    pub fn latest(&self, instrument: InstrumentId) -> Option<PositionBucket> {
        self.series
            .get(instrument.0 as usize)?
            .iter_newest_first()
            .next()
            .copied()
    }

    /// Buckets per instrument. It matches the mid chart's, so both charts share one window.
    pub fn capacity(&self) -> usize {
        self.bucket_capacity
    }

    /// Position frames an engine sent as infinite, NaN or too large to hold as a quote amount. The
    /// account band shows this count, because a chart that just goes flat would say nothing.
    pub fn rejected_frames(&self) -> u64 {
        self.rejected_frames
    }

    fn bank_position(
        &mut self,
        instrument: InstrumentId,
        at: TsUs,
        exposure_quote: f64,
        pnl_quote: f64,
    ) {
        let slot = instrument.0 as usize;
        self.ensure_instruments(slot + 1);
        let (Some(index), Some(exposure_quote), Some(pnl_quote)) = (
            bucket_index(at, self.spin_interval),
            fixed_mantissa(exposure_quote),
            fixed_mantissa(pnl_quote),
        ) else {
            self.rejected_frames += 1;
            return;
        };
        self.bank(
            slot,
            PositionBucket {
                index,
                exposure_quote,
                pnl_quote,
            },
        );
    }

    fn bank(&mut self, slot: usize, bucket: PositionBucket) {
        let series = &mut self.series[slot];
        if let Some(newest) = series.last_mut() {
            // A frame older than the newest bucket is dropped rather than rewriting history that is
            // already on screen.
            if bucket.index < newest.index {
                return;
            }
            if bucket.index == newest.index {
                *newest = bucket;
                return;
            }
        }
        series.push(bucket);
    }

    /// The engine sends no position after a rotation until a two-sided book marks the instrument
    /// again, so keeping the old series would show that silence as an unchanged position.
    fn clear(&mut self, instrument: InstrumentId) {
        let capacity = self.bucket_capacity;
        let Some(series) = self.series.get_mut(instrument.0 as usize) else { return };
        *series = BoundedHistory::new(capacity);
    }

    fn ensure_instruments(&mut self, len: usize) {
        grow_to(&mut self.series, len, || {
            BoundedHistory::new(self.bucket_capacity)
        });
    }
}
