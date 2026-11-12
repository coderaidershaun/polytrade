//! Realised volatility: population stdev of consecutive returns, scaled /sec for cross-venue compare.

use crate::time::DurationUs;

/// `Log`: scale-free for prices. `Absolute`: for bounded 0..1 probability (log explodes at bounds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Returns {
    Log,
    Absolute,
}

/// Volatility /sec: pop stdev over consecutive returns, divided by sqrt(interval_secs).
/// Non-finite closes dropped; dropped close pairs neighbours into return spanning 2×interval.
/// Returns `None` on <2 usable closes or non-positive interval.
pub fn realised_vol_per_sec(
    closes: impl Iterator<Item = f64>,
    returns: Returns,
    interval: DurationUs,
) -> Option<f64> {
    let interval_secs = interval.to_secs();
    if interval_secs <= 0.0 {
        return None;
    }
    let mut count = 0usize;
    let mut sum = 0.0;
    let mut sum_squares = 0.0;
    for value in consecutive_returns(closes, returns) {
        count += 1;
        sum += value;
        sum_squares += value * value;
    }
    if count == 0 {
        return None;
    }
    let count = count as f64;
    let mean = sum / count;
    let variance = (sum_squares / count - mean * mean).max(0.0);
    Some(variance.sqrt() / interval_secs.sqrt())
}

/// Returns between consecutive usable closes. A non-finite or non-positive close is skipped, so the
/// closes either side of it pair into one return.
pub(super) fn consecutive_returns(
    closes: impl Iterator<Item = f64>,
    returns: Returns,
) -> impl Iterator<Item = f64> {
    let mut previous: Option<f64> = None;
    closes
        .filter(|close| close.is_finite() && *close > 0.0)
        .filter_map(move |close| {
            let value = previous.map(|previous| match returns {
                Returns::Log => (close / previous).ln(),
                Returns::Absolute => close - previous,
            });
            previous = Some(close);
            value
        })
}
