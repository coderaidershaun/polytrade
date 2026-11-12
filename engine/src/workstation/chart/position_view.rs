//! The risk chart's projection: which series shows, its padded bounds and its visible buckets. It
//! has no window function on purpose. The mid chart banks on every two-sided book, but a position
//! banks once a spin and only while the engine holds a mark, so a window derived here would put one
//! pointer position on different buckets in the two stacked charts. Callers pass in the mid chart's
//! window.

use super::position_model::{PositionBucket, PositionModel};
use super::view::{ChartBounds, ChartDomain, pad};
use crate::ids::{FIXED_SCALE, InstrumentId};

/// One whole quote unit, so a flat series does not label its axis in steps of 1e-8.
const MIN_SPAN_QUOTE: i64 = FIXED_SCALE;

/// Bounds in quote amounts, a separate type from the mid chart's half-tick bounds so the two cannot
/// be swapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteBounds(ChartBounds);

impl QuoteBounds {
    pub fn as_chart_bounds(self) -> ChartBounds {
        self.0
    }
}

impl From<QuoteBounds> for ChartBounds {
    fn from(bounds: QuoteBounds) -> Self {
        bounds.as_chart_bounds()
    }
}

/// Which series the risk chart shows. Both read the same buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RiskSeries {
    #[default]
    Exposure,
    Pnl,
}

impl RiskSeries {
    /// Two, because both series are money. It lives on the type both readouts share, so the
    /// crosshair and the header cannot disagree.
    pub(crate) const READOUT_DECIMALS: usize = 2;

    pub fn value(self, bucket: &PositionBucket) -> i64 {
        match self {
            RiskSeries::Exposure => bucket.exposure_quote,
            RiskSeries::Pnl => bucket.pnl_quote,
        }
    }
}

/// The bounds always include zero, because the zero line is what tells long from short.
pub fn bounds(
    positions: &PositionModel,
    instrument: InstrumentId,
    series: RiskSeries,
    domain: ChartDomain,
) -> Option<QuoteBounds> {
    let mut values =
        visible_buckets(positions, instrument, domain).map(|bucket| series.value(bucket));
    let first = values.next()?;
    let (low, high) = values.fold((first.min(0), first.max(0)), |(low, high), value| {
        (low.min(value), high.max(value))
    });
    let (low, high) = widen_to_minimum_span(low, high);
    Some(QuoteBounds(pad(low, high)))
}

/// Buckets inside the window, oldest first. A missing index is a spin with no banked position, so
/// only consecutive buckets join up.
pub fn visible_buckets(
    positions: &PositionModel,
    instrument: InstrumentId,
    domain: ChartDomain,
) -> impl Iterator<Item = &PositionBucket> {
    positions
        .buckets(instrument)
        .filter(move |bucket| domain.contains(bucket.index))
}

fn widen_to_minimum_span(low: i64, high: i64) -> (i64, i64) {
    let span = high.saturating_sub(low);
    if span >= MIN_SPAN_QUOTE {
        return (low, high);
    }
    let deficit = MIN_SPAN_QUOTE - span;
    let below = deficit / 2;
    (
        low.saturating_sub(below),
        high.saturating_add(deficit - below),
    )
}
