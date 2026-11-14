//! Microstructure primitives (pure functions, f64 derived statistics only).

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
    /// Size-weighted mid (weighted by opposite side's qty, leans to thinner side; falls back to mid if empty).
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
    /// Imbalance = (bid_qty - ask_qty) / total, ∈ [-1, 1] (0.0 if empty).
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

// Order-book resilience: OU rate of mid relaxation toward equilibrium (1/sec).
/// Deviation below this fraction of half-spread = discretisation noise (5% TOB imbalance minimum).
const MIN_DEVIATION_FRACTION: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResilienceSample {
    pub event_ts_us: LocalTs,
    pub mid: f64,
    pub equilibrium: f64,
    pub half_spread: f64,
}

/// Stateful mean-reversion rate calculator (stale anchor averages over gap).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct OrderbookResilience {
    prev: Option<ResilienceSample>,
}

impl OrderbookResilience {
    pub fn new() -> Self {
        Self::default()
    }

    /// OU rate R = −ln((M₁ − Π₀)/(M₀ − Π₀)) / Δt (1/sec) or None while warming up or gated.
    /// Never returns NaN (dust + sign gates exclude junk).
    pub fn on_sample(&mut self, sample: ResilienceSample) -> Option<f64> {
        // Junk skips without anchoring (else wastes next good sample too).
        if !(sample.mid.is_finite()
            && sample.equilibrium.is_finite()
            && sample.half_spread.is_finite())
        {
            return None;
        }
        // replace reanchors each call; `?` bails until prior sample (warm-up).
        let prev = self.prev.replace(sample)?;
        let dt = sample.event_ts_us.diff(prev.event_ts_us);
        if dt.micros() <= 0 {
            return None;
        }
        let x0 = prev.mid - prev.equilibrium;
        // Locked/crossed book: ratio 0/0 = NaN, dust floor catches.
        if prev.half_spread <= 0.0 || x0.abs() <= MIN_DEVIATION_FRACTION * prev.half_spread {
            return None;
        }
        let ratio = (sample.mid - prev.equilibrium) / x0;
        // Mid crossed equilibrium -> OU model violated; excludes ln(0)/ln(neg).
        if ratio <= 0.0 {
            return None;
        }
        Some(-ratio.ln() / dt.to_secs())
    }
}
