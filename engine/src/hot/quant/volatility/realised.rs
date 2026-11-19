//! Realised volatility per second, so that venues that sample at other intervals compare in one
//! unit.

use crate::msg::time::DurationUs;

/// `Log` for prices, because a log return has no scale. `Absolute` for a probability between 0 and
/// 1, where a log return explodes near the bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Returns {
    Log,
    Absolute,
}

/// The root mean square of the returns between consecutive closes, divided by the root of the
/// interval in seconds. No mean is subtracted, so a steady trend counts as volatility. A close that
/// is not finite or not positive drops out, and its neighbours pair into one return that spans two
/// intervals. `None` with fewer than two usable closes, or an interval that is not positive.
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
    let mut sum_squares = 0.0;
    for value in consecutive_returns(closes, returns) {
        count += 1;
        sum_squares += value * value;
    }
    if count == 0 {
        return None;
    }
    let variance = sum_squares / count as f64;
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
