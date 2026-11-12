//! Guéant-Lehalle-Fernandez-Tapia optimal market-making quotes. Depths are in the caller's one
//! shared depth unit (ticks, bps, ...); only `QuoteCoefficients::quote` is tick-specific.

use crate::ids::Price;

/// Closed-form objective (fixes ξ, never tuned).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Objective {
    /// CARA utility of terminal MTM wealth (ξ = γ).
    CaraUtility,
    /// Terminal wealth minus quadratic inventory penalty (ξ = 0) at limits c1 = 1/k, c2 = √(γe/(2AΔk)).
    InventoryPenalty,
}

/// Maker's strategy choices (caller's depth unit); risk-budget decisions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GueantParams {
    gamma: f64,
    order_size: f64,
    objective: Objective,
}

impl GueantParams {
    /// gamma = γ̃ (inverse depth-unit-wealth risk aversion); order_size = Δ (inventory shock per fill).
    /// # Panics
    /// When gamma or order_size non-finite or <=0 (config bug, not market condition).
    pub fn new(gamma: f64, order_size: f64, objective: Objective) -> Self {
        assert!(
            gamma.is_finite() && gamma > 0.0,
            "gueant gamma must be finite and positive, got {gamma}"
        );
        assert!(
            order_size.is_finite() && order_size > 0.0,
            "gueant order_size must be finite and positive, got {order_size}"
        );
        Self {
            gamma,
            order_size,
            objective,
        }
    }

    /// Coefficients from live estimates (A, sigma, k same time basis and one shared depth unit;
    /// sigma absolute). Returns None if inputs invalid or closed form overflows.
    pub fn coefficients(
        &self,
        a_per_sec: f64,
        k_per_depth: f64,
        sigma_per_sqrt_sec: f64,
    ) -> Option<QuoteCoefficients> {
        let is_positive_finite = |value: f64| value.is_finite() && value > 0.0;
        if !(is_positive_finite(a_per_sec)
            && is_positive_finite(k_per_depth)
            && is_positive_finite(sigma_per_sqrt_sec))
        {
            return None;
        }
        let (c1, c2) = self.c1_c2(a_per_sec, k_per_depth);
        let half_spread = c1 + 0.5 * self.order_size * sigma_per_sqrt_sec * c2;
        let skew_per_inventory = sigma_per_sqrt_sec * c2;
        if !(half_spread.is_finite() && skew_per_inventory.is_finite()) {
            return None;
        }
        Some(QuoteCoefficients {
            half_spread,
            skew_per_inventory,
        })
    }

    fn c1_c2(&self, a_per_sec: f64, k_per_depth: f64) -> (f64, f64) {
        let denom = 2.0 * a_per_sec * self.order_size * k_per_depth;
        match self.objective {
            Objective::InventoryPenalty => {
                let c1 = 1.0 / k_per_depth;
                let c2 = (self.gamma * std::f64::consts::E / denom).sqrt();
                (c1, c2)
            }
            Objective::CaraUtility => {
                let xi_delta = self.gamma * self.order_size;
                let ratio = xi_delta / k_per_depth;
                // log1p preserves limit ξΔ/k->0: c1->1/k, c2 exponent->1 (Model B's e factor).
                let log1p_ratio = ratio.ln_1p();
                let c1 = log1p_ratio / xi_delta;
                let exponent = (k_per_depth / xi_delta + 1.0) * log1p_ratio;
                let c2 = (self.gamma / denom * exponent.exp()).sqrt();
                (c1, c2)
            }
        }
    }
}

/// Solved depth coefficients in the caller's depth unit: the half-spread h and the skew per unit of
/// inventory j.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteCoefficients {
    half_spread: f64,
    skew_per_inventory: f64,
}

impl QuoteCoefficients {
    /// h = c1 + (Δ/2)·σ·c2 (zero-inventory continuous half-spread).
    #[inline]
    pub fn half_spread(&self) -> f64 {
        self.half_spread
    }

    /// j = σ·c2 (reservation-centre shift per inventory unit).
    #[inline]
    pub fn skew_per_inventory(&self) -> f64 {
        self.skew_per_inventory
    }

    /// δᵇ(q) = h + j·q (continuous bid depth below fair).
    #[inline]
    pub fn bid_depth(&self, inventory: f64) -> f64 {
        self.half_spread + self.skew_per_inventory * inventory
    }

    /// δᵃ(q) = h − j·q (continuous ask depth above fair).
    #[inline]
    pub fn ask_depth(&self, inventory: f64) -> f64 {
        self.half_spread - self.skew_per_inventory * inventory
    }

    /// Depths -> post-only grid quote (tick indices + depths as diagnostics).
    /// # Panics
    /// When fair/inventory non-finite or tick <=0 (config bug).
    pub fn quote(&self, inputs: QuoteInputs) -> Quotes {
        assert!(
            inputs.fair.is_finite(),
            "gueant quote fair must be finite, got {}",
            inputs.fair
        );
        assert!(
            inputs.inventory.is_finite(),
            "gueant quote inventory must be finite, got {}",
            inputs.inventory
        );
        assert!(
            inputs.tick.0 > 0,
            "gueant quote tick must be positive, got {}",
            inputs.tick.0
        );
        let bid_depth = self.bid_depth(inputs.inventory);
        let ask_depth = self.ask_depth(inputs.inventory);
        let fair_ticks = inputs.fair / inputs.tick.to_f64();
        // Exact tick indices from mantissas (never float modulo on price).
        let best_bid_tick = inputs.best_bid.0.div_euclid(inputs.tick.0);
        let best_ask_tick = inputs.best_ask.0.div_euclid(inputs.tick.0);

        // Quantisation never more aggressive (bid floors, ask ceils).
        let model_bid_tick = (fair_ticks - bid_depth).floor() as i64;
        let model_ask_tick = (fair_ticks + ask_depth).ceil() as i64;

        // Post-only: improve touch, never cross resting opposite side.
        let mut bid_tick = model_bid_tick.min(best_ask_tick - 1);
        let mut ask_tick = model_ask_tick.max(best_bid_tick + 1);

        // On collision, keep inventory-reducing quote; push other +1 tick.
        if ask_tick <= bid_tick {
            if inputs.inventory < 0.0 {
                ask_tick = bid_tick + 1;
            } else {
                bid_tick = ask_tick - 1;
            }
        }

        Quotes {
            bid: Price(bid_tick * inputs.tick.0),
            ask: Price(ask_tick * inputs.tick.0),
            bid_tick,
            ask_tick,
            bid_depth,
            ask_depth,
        }
    }
}

/// Quote inputs: fair value, signed inventory, grid (zero origin assumed).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteInputs {
    /// Fair value (mid or microprice); tick index = fair / tick.
    pub fair: f64,
    /// Signed inventory (same units as params' order_size; q>0 is long).
    pub inventory: f64,
    pub best_bid: Price,
    pub best_ask: Price,
    /// Grid step (exact mantissa); tick index = price_mantissa / tick.
    pub tick: Price,
}

/// Produced quote pair (executable prices + tick indices + depths as diagnostics).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quotes {
    pub bid: Price,
    pub ask: Price,
    pub bid_tick: i64,
    pub ask_tick: i64,
    pub bid_depth: f64,
    pub ask_depth: f64,
}
