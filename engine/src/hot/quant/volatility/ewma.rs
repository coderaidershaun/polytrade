//! A fast volatility that folds one microprice return at a time with RiskMetrics weights, so it
//! stores no window.

use std::num::NonZeroU32;

use crate::hot::digest::digest_fields;

/// Each return r moves the variance to λσ² + (1 − λ)r², with λ = 2^(−1/H) for a half-life of H
/// returns.
#[derive(Debug, Clone, PartialEq)]
pub struct EwmaVol {
    decay: f64,
    prev_price: Option<f64>,
    variance: Option<f64>,
}

digest_fields! { EwmaVol { decay, prev_price, variance } }

impl EwmaVol {
    pub fn new(halflife_events: NonZeroU32) -> Self {
        Self {
            decay: decay_for(halflife_events),
            prev_price: None,
            variance: None,
        }
    }

    /// Folds the log return from the previous microprice. A price that is not finite or not
    /// positive is skipped, because its return would turn the variance into NaN for good.
    pub fn on_microprice(&mut self, microprice: f64) {
        if !(microprice.is_finite() && microprice > 0.0) {
            return;
        }
        if let Some(prev) = self.prev_price {
            let log_return = (microprice / prev).ln();
            let squared = log_return * log_return;
            self.variance = Some(match self.variance {
                Some(previous) => self.decay * previous + (1.0 - self.decay) * squared,
                None => squared,
            });
        }
        self.prev_price = Some(microprice);
    }

    /// The standard deviation of one return between two folded microprices. `None` before the
    /// second price.
    #[inline]
    pub fn volatility_per_event(&self) -> Option<f64> {
        self.variance.map(f64::sqrt)
    }

    /// Forgets the previous price and keeps the variance, so the first price after a resync starts
    /// a new chain and no return spans the gap.
    pub fn reset_continuity(&mut self) {
        self.prev_price = None;
    }

    /// Forgets the previous price and the variance, and keeps the decay. A new market window starts
    /// here, because a variance carried across two windows mixes two distributions.
    pub fn reset(&mut self) {
        self.prev_price = None;
        self.variance = None;
    }
}

#[inline]
fn decay_for(halflife_events: NonZeroU32) -> f64 {
    (-std::f64::consts::LN_2 / f64::from(halflife_events.get())).exp()
}
