//! Statistics of the top of a book: the touch, the imbalance of its depth, and how fast the mid
//! returns to the microprice. Each reads exact prices and sizes and gives an f64 statistic.

use crate::hot::quant::BPS;
use crate::msg::ids::Price;
use crate::msg::inbound::Level;
use crate::msg::time::LocalTs;

/// The best bid and the best ask of a book, each with its size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Touch {
    pub bid: Level,
    pub ask: Level,
}

impl Touch {
    /// The mid weighted by the size on the opposite side, so it leans toward the thinner side. A
    /// touch with no size has no weights and reads the mid.
    #[inline]
    pub fn microprice(&self) -> f64 {
        let bid_qty = self.bid.qty.to_f64();
        let ask_qty = self.ask.qty.to_f64();
        let total_qty = bid_qty + ask_qty;
        if total_qty <= 0.0 {
            return self.mid();
        }
        (self.bid.price.to_f64() * ask_qty + self.ask.price.to_f64() * bid_qty) / total_qty
    }

    #[inline]
    pub fn spread(&self) -> f64 {
        self.ask.price.to_f64() - self.bid.price.to_f64()
    }

    #[inline]
    pub fn mid(&self) -> f64 {
        mid(self.bid.price, self.ask.price)
    }
}

/// The levels of each side of a book, best first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BookSides<'a> {
    pub bids: &'a [Level],
    pub asks: &'a [Level],
}

impl BookSides<'_> {
    /// (bid size − ask size) / total size over the top `top_n` levels of each side, from −1 to 1. A
    /// book with no size reads 0.
    #[inline]
    pub fn imbalance(&self, top_n: usize) -> f64 {
        let bid_qty: f64 = self
            .bids
            .iter()
            .take(top_n)
            .map(|level| level.qty.to_f64())
            .sum();
        let ask_qty: f64 = self
            .asks
            .iter()
            .take(top_n)
            .map(|level| level.qty.to_f64())
            .sum();
        let total_qty = bid_qty + ask_qty;
        if total_qty <= 0.0 {
            return 0.0;
        }
        (bid_qty - ask_qty) / total_qty
    }

    /// The imbalance of the depth within `band`. Each side counts from its best level outward, up
    /// to and including the first level at or past the edge of the band, so the best level always
    /// counts, and so does one level just outside the band.
    #[inline]
    pub fn banded_imbalance(&self, band: PriceBand) -> f64 {
        let bid_qty = qty_to_edge(self.bids, |price| price <= band.low);
        let ask_qty = qty_to_edge(self.asks, |price| price >= band.high);
        let total_qty = bid_qty + ask_qty;
        if total_qty <= 0.0 {
            return 0.0;
        }
        (bid_qty - ask_qty) / total_qty
    }
}

/// The mid of two best prices, for a caller that keeps the prices of the touch without its sizes.
#[inline]
pub(crate) fn mid(best_bid: Price, best_ask: Price) -> f64 {
    (best_bid.to_f64() + best_ask.to_f64()) / 2.0
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriceBand {
    pub low: f64,
    pub high: f64,
}

impl PriceBand {
    #[inline]
    pub fn around(mid: f64, half_width_bps: f64) -> Self {
        let half_width = mid * half_width_bps / BPS;
        Self {
            low: mid - half_width,
            high: mid + half_width,
        }
    }
}

fn qty_to_edge(levels: &[Level], is_at_or_past_edge: impl Fn(f64) -> bool) -> f64 {
    let mut total = 0.0;
    for level in levels {
        total += level.qty.to_f64();
        if is_at_or_past_edge(level.price.to_f64()) {
            break;
        }
    }
    total
}

/// A deviation of the mid from the equilibrium below this share of the half spread is noise of the
/// tick grid. That share equals the imbalance of the touch, so the floor is an imbalance of 5%.
const MIN_DEVIATION_FRACTION: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResilienceSample {
    pub event_ts_us: LocalTs,
    pub mid: f64,
    /// The microprice of the same touch: the level that the mid relaxes toward.
    pub equilibrium: f64,
    pub half_spread: f64,
}

/// The rate at which the mid relaxes toward the equilibrium, as an Ornstein-Uhlenbeck process, in
/// 1/s. The last sample is the anchor of the next rate, so a rate after a gap averages over it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct OrderbookResilience {
    prev: Option<ResilienceSample>,
}

impl OrderbookResilience {
    pub fn new() -> Self {
        Self::default()
    }

    /// The rate R = −ln((M₁ − Π₀)/(M₀ − Π₀)) / Δt in 1/s, with M the mid and Π the equilibrium, 0
    /// the anchor and 1 this sample, and Δt the time between them. `None` while it warms up or
    /// where a gate skips the interval. It is never NaN. It is negative where the deviation grew,
    /// and −0.0 where the mid did not move, such as on a spin with no change of the book.
    pub fn on_sample(&mut self, sample: ResilienceSample) -> Option<f64> {
        // A sample with a value that is not finite is skipped and does not become the anchor, or
        // the next good sample would be lost too.
        if !(sample.mid.is_finite()
            && sample.equilibrium.is_finite()
            && sample.half_spread.is_finite())
        {
            return None;
        }
        // Each sample past this point becomes the next anchor, and the first one only anchors.
        let prev = self.prev.replace(sample)?;
        let dt = sample.event_ts_us.diff(prev.event_ts_us);
        if dt.micros() <= 0 {
            return None;
        }
        let x0 = prev.mid - prev.equilibrium;
        // A locked or crossed book has no positive half spread, and its ratio could be 0 / 0, so
        // the floor skips it too.
        if prev.half_spread <= 0.0 || x0.abs() <= MIN_DEVIATION_FRACTION * prev.half_spread {
            return None;
        }
        let ratio = (sample.mid - prev.equilibrium) / x0;
        // The mid reached or crossed the equilibrium, which the model cannot produce, and the
        // logarithm of a ratio that is not positive is not finite.
        if ratio <= 0.0 {
            return None;
        }
        Some(-ratio.ln() / dt.to_secs())
    }
}
