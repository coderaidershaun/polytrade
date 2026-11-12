//! The mid chart's projection from buckets to plot geometry: the window, the padded bounds and the
//! fractions a painter scales to pixels. The maths stays in exact integers until the last step.

use super::model::{ChartBucket, ChartFill, ChartModel};
use crate::ids::InstrumentId;
use crate::workstation::format::{self, AxisTicks};

/// Air above and below the visible extremes, as a percentage of their span, so the line never runs
/// along the plot border.
const PADDING_PERCENT: i128 = 5;

/// The label spacing a value axis aims for, in font heights, and the tightest it stays readable at.
const LABEL_PITCH: f32 = 2.0;
const MIN_LABEL_PITCH: f32 = 1.2;

/// The axis generator always allows at least five labels, and five labels can span almost six
/// steps. A plot shorter than six legible pitches could therefore crowd them.
const MIN_LABELLED_PITCHES: f32 = 6.0;

/// The narrowest a candle may get, in points, and still read as a candle rather than a dot.
const MIN_CANDLE_PITCH: f64 = 4.0;

/// How the mid series is drawn. Both modes read the same buckets, so the toggle changes nothing but
/// the geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChartMode {
    #[default]
    Line,
    Candles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChartDomain {
    pub first: u64,
    pub last: u64,
}

impl ChartDomain {
    pub fn contains(self, index: u64) -> bool {
        index >= self.first && index <= self.last
    }

    pub fn width(self) -> u64 {
        self.last - self.first + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChartBounds {
    pub low: i64,
    pub high: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentPoint<'a> {
    pub bucket: &'a ChartBucket,
    pub is_run_start: bool,
}

/// How the visible buckets join into candles for a plot this wide, so a candle never gets thinner
/// than it can be read. Groups sit on whole multiples of the bucket index, which keeps a candle's
/// members the same as the window slides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandleGrouping {
    domain: ChartDomain,
    buckets_per_candle: u64,
}

/// One group of consecutive buckets drawn as one candle. `first` and `last` are the span it covers
/// on screen, clamped to the window, and need not be buckets the model holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candle {
    pub first: u64,
    pub last: u64,
    pub open_half_ticks: i64,
    pub high_half_ticks: i64,
    pub low_half_ticks: i64,
    pub close_half_ticks: i64,
    pub has_break_before: bool,
}

impl CandleGrouping {
    /// The fewest consecutive buckets per candle that keep each candle at least four points wide.
    pub fn for_plot(domain: ChartDomain, plot_width: f32) -> Self {
        let width = domain.width();
        let buckets_per_candle = if plot_width > 0.0 {
            let needed = (width as f64 * MIN_CANDLE_PITCH / f64::from(plot_width)).ceil();
            (needed as u64).clamp(1, width)
        } else {
            width
        };
        Self {
            domain,
            buckets_per_candle,
        }
    }

    pub fn buckets_per_candle(self) -> u64 {
        self.buckets_per_candle
    }

    /// The visible buckets as candles, oldest first. A group that holds no bucket gets no candle. A
    /// break precedes a candle where the line would have broken before its first bucket, or where a
    /// lost snapshot was revealed inside it. A spin missing inside a group is absorbed, and the
    /// window's first candle never breaks.
    pub fn candles(self, chart: &ChartModel, instrument: InstrumentId) -> Vec<Candle> {
        let per_candle = self.buckets_per_candle;
        let mut candles: Vec<Candle> = Vec::new();
        for point in segment_points(chart, instrument, self.domain) {
            let bucket = point.bucket;
            let group = bucket.index / per_candle;
            match candles.last_mut() {
                Some(candle) if candle.first / per_candle == group => candle.fold(bucket),
                _ => candles.push(Candle {
                    first: group.saturating_mul(per_candle).max(self.domain.first),
                    last: group
                        .saturating_mul(per_candle)
                        .saturating_add(per_candle - 1)
                        .min(self.domain.last),
                    open_half_ticks: bucket.open_half_ticks,
                    high_half_ticks: bucket.high_half_ticks,
                    low_half_ticks: bucket.low_half_ticks,
                    close_half_ticks: bucket.close_half_ticks,
                    has_break_before: point.is_run_start,
                }),
            }
        }
        if let Some(oldest) = candles.first_mut() {
            oldest.has_break_before = false;
        }
        candles
    }
}

impl Candle {
    fn fold(&mut self, bucket: &ChartBucket) {
        self.high_half_ticks = self.high_half_ticks.max(bucket.high_half_ticks);
        self.low_half_ticks = self.low_half_ticks.min(bucket.low_half_ticks);
        self.close_half_ticks = bucket.close_half_ticks;
        self.has_break_before |= bucket.has_gap_before;
    }
}

pub fn domain(chart: &ChartModel, instrument: InstrumentId) -> Option<ChartDomain> {
    let mut buckets = chart.buckets(instrument);
    let oldest = buckets.next()?.index;
    let newest = buckets.last().map_or(oldest, |bucket| bucket.index);
    Some(domain_from_range(oldest, newest, chart.capacity()))
}

pub(crate) fn domain_from_range(oldest: u64, newest: u64, capacity: usize) -> ChartDomain {
    debug_assert!(
        oldest <= newest,
        "a series spans oldest to newest, got {oldest} > {newest}"
    );
    let capacity = capacity.max(1) as u64;
    let filled = newest - oldest + 1 >= capacity;
    let first = if filled { newest + 1 - capacity } else { oldest };
    ChartDomain {
        first,
        last: first + capacity - 1,
    }
}

pub fn bounds(
    chart: &ChartModel,
    instrument: InstrumentId,
    domain: ChartDomain,
) -> Option<ChartBounds> {
    let mut low = i64::MAX;
    let mut high = i64::MIN;
    for bucket in visible_buckets(chart, instrument, domain) {
        low = low.min(bucket.low_half_ticks);
        high = high.max(bucket.high_half_ticks);
    }
    for fill in visible_fills(chart, instrument, domain) {
        low = low.min(fill.half_ticks);
        high = high.max(fill.half_ticks);
    }
    if low > high {
        return None;
    }
    Some(pad(low, high))
}

pub fn segment_points(
    chart: &ChartModel,
    instrument: InstrumentId,
    domain: ChartDomain,
) -> impl Iterator<Item = SegmentPoint<'_>> {
    let mut previous: Option<u64> = None;
    visible_buckets(chart, instrument, domain).map(move |bucket| {
        let follows = previous.is_some_and(|previous| bucket.index == previous + 1);
        previous = Some(bucket.index);
        SegmentPoint {
            bucket,
            is_run_start: !follows || bucket.has_gap_before,
        }
    })
}

fn visible_buckets(
    chart: &ChartModel,
    instrument: InstrumentId,
    domain: ChartDomain,
) -> impl Iterator<Item = &ChartBucket> {
    chart
        .buckets(instrument)
        .filter(move |bucket| domain.contains(bucket.index))
}

pub fn visible_fills(
    chart: &ChartModel,
    instrument: InstrumentId,
    domain: ChartDomain,
) -> impl Iterator<Item = &ChartFill> {
    chart
        .fills(instrument)
        .filter(move |fill| domain.contains(fill.index))
}

pub fn x_fraction(index: u64, domain: ChartDomain) -> f32 {
    let span = domain.last.saturating_sub(domain.first);
    if span == 0 {
        return 0.0;
    }
    let offset = index.saturating_sub(domain.first) as f64;
    (offset / span as f64).clamp(0.0, 1.0) as f32
}

pub fn bucket_at_fraction(fraction: f32, domain: ChartDomain) -> u64 {
    let span = domain.last.saturating_sub(domain.first);
    let offset = (f64::from(fraction).clamp(0.0, 1.0) * span as f64).round() as u64;
    domain.first + offset
}

pub fn y_fraction(value: i64, bounds: ChartBounds) -> f32 {
    let span = i128::from(bounds.high) - i128::from(bounds.low);
    if span <= 0 {
        return 0.5;
    }
    let offset = (i128::from(value) - i128::from(bounds.low)) as f64;
    (offset / span as f64).clamp(0.0, 1.0) as f32
}

/// The ticks to label on a value axis `plot_height` points tall, in a `font`-point face. Labels aim
/// to sit two font heights apart, and a plot shorter than six legible pitches gets none.
///
/// Above that height, five or fewer labels sit more than a sixth of the height apart, and more than
/// five appear only on a taller plot, where the ideal spacing keeps them apart. A fixed height
/// cut-off also stops the axis blinking on and off as live prices move.
pub fn value_ticks(bounds: ChartBounds, plot_height: f32, font: f32) -> AxisTicks {
    if plot_height < font * MIN_LABEL_PITCH * MIN_LABELLED_PITCHES {
        return format::axis_ticks(0, 0, 0);
    }
    let ceiling =
        format::legible_tick_ceiling(plot_height, font * LABEL_PITCH, font * MIN_LABEL_PITCH);
    format::axis_ticks(bounds.low, bounds.high, ceiling)
}

pub(crate) fn pad(low: i64, high: i64) -> ChartBounds {
    if low == high {
        return ChartBounds {
            low: low.saturating_sub(1),
            high: high.saturating_add(1),
        };
    }
    let span = i128::from(high) - i128::from(low);
    let padding = i64::try_from(span * PADDING_PERCENT / 100)
        .unwrap_or(i64::MAX)
        .max(1);
    ChartBounds {
        low: low.saturating_sub(padding),
        high: high.saturating_add(padding),
    }
}
