//! Microstructure primitives (pure functions, f64 derived statistics only).

mod orderbook_resilience;

pub use orderbook_resilience::{OrderbookResilience, ResilienceSample};

use crate::hot::quant::BPS;
use crate::ids::Price;
use crate::msg::inbound::Level;

/// Size-weighted mid (weighted by opposite side's qty, leans to thinner side; falls back to mid if empty).
#[inline]
pub fn microprice(best_bid: Level, best_ask: Level) -> f64 {
    let bid_qty = best_bid.qty.to_f64();
    let ask_qty = best_ask.qty.to_f64();
    let total_qty = bid_qty + ask_qty;
    if total_qty <= 0.0 {
        return mid(best_bid.price, best_ask.price);
    }
    (best_bid.price.to_f64() * ask_qty + best_ask.price.to_f64() * bid_qty) / total_qty
}

/// Equilibrium = microprice; Π = M + λ(d−s) where λ = spread/(2(d+s)).
/// Deliberately an alias, not a pass-through to delete: the OU resilience model is written in Π, so
/// the name carries the model's own vocabulary for whatever feeds [`ResilienceSample::equilibrium`].
#[inline]
pub fn orderbook_equilibrium(best_bid: Level, best_ask: Level) -> f64 {
    microprice(best_bid, best_ask)
}

#[inline]
pub fn spread(best_bid: Price, best_ask: Price) -> f64 {
    best_ask.to_f64() - best_bid.to_f64()
}

#[inline]
pub fn mid(best_bid: Price, best_ask: Price) -> f64 {
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

#[inline]
pub fn banded_imbalance(bids: &[Level], asks: &[Level], band: PriceBand) -> f64 {
    let bid_qty = qty_to_edge(bids, |price| price <= band.low);
    let ask_qty = qty_to_edge(asks, |price| price >= band.high);
    let total_qty = bid_qty + ask_qty;
    if total_qty <= 0.0 {
        return 0.0;
    }
    (bid_qty - ask_qty) / total_qty
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

/// Imbalance = (bid_qty - ask_qty) / total, ∈ [-1, 1] (0.0 if empty).
#[inline]
pub fn imbalance(bids: &[Level], asks: &[Level], top_n: usize) -> f64 {
    let bid_qty: f64 = bids
        .iter()
        .take(top_n)
        .map(|level| level.qty.to_f64())
        .sum();
    let ask_qty: f64 = asks
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
