//! VPIN: avg absolute classified imbalance / bucket volume over equal-notional bars. Directionless
//! by construction; signed-flow companion. Integer-sum, one f64 divide.

use crate::msg::time::TsUs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeBar {
    pub open_ts_us: TsUs,
    pub close_ts_us: TsUs,
    pub buy_notional: i64,
    pub sell_notional: i64,
    pub target: i64,
    /// Trades that ARRIVED while this bar was open, which partitions the tape across bars rather
    /// than measuring each bar's activity: a trade worth several targets books its one arrival to
    /// the bar it landed in, and the bars its notional pours through carry a full target and zero.
    pub trade_arrivals: u32,
}

/// Directionless toxicity + signed flow over completed buckets.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VpinEstimate {
    /// Σ|buy−sell| / Σtarget ∈ [0, 1]. Generalizes classic Σ|buy−sell| / (n·V) to per-bar targets.
    pub vpin: f64,
    /// Σ(buy−sell) / Σtarget ∈ [−1, 1]; positive = net aggressive buying.
    pub signed_flow: f64,
}

/// VPIN over last `buckets` completed bars. Volume-weighted denominator Σtarget reduces to n·V
/// when all targets match; weights by size on clock drift. Returns `None` on empty/short window or
/// non-positive Σtarget (unreachable for real bars). No clamp: ratio bounded by construction.
pub fn vpin(closed_bars: &[VolumeBar], buckets: usize) -> Option<VpinEstimate> {
    if buckets == 0 || closed_bars.len() < buckets {
        return None;
    }
    let window = &closed_bars[closed_bars.len() - buckets..];
    let mut abs_imbalance: i128 = 0;
    let mut signed_imbalance: i128 = 0;
    let mut total_target: i128 = 0;
    for bar in window {
        let imbalance = i128::from(bar.buy_notional) - i128::from(bar.sell_notional);
        abs_imbalance += imbalance.abs();
        signed_imbalance += imbalance;
        total_target += i128::from(bar.target);
    }
    if total_target <= 0 {
        return None;
    }
    let total_target = total_target as f64;
    Some(VpinEstimate {
        vpin: abs_imbalance as f64 / total_target,
        signed_flow: signed_imbalance as f64 / total_target,
    })
}
