//! EWMA (RiskMetrics): streaming [`EwmaVol`], one log return folded in per microprice.

/// RiskMetrics: λ weights prior var, (1−λ) weights fresh squared return. λ = 2^(−1/H) for half-life H.
#[derive(Debug, Clone, PartialEq)]
pub struct EwmaVol {
    decay: f64,
    prev_price: Option<f64>,
    variance: Option<f64>,
}

impl EwmaVol {
    pub fn new(halflife_events: u32) -> Self {
        Self {
            decay: decay_for(halflife_events),
            prev_price: None,
            variance: None,
        }
    }

    /// Fold microprice via log-return. Non-finite/non-positive prices skipped (prevent NaN corruption).
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

    #[inline]
    pub fn volatility(&self) -> Option<f64> {
        self.variance.map(f64::sqrt)
    }

    /// Drop prev price for fresh return chain (variance kept). Resident survives resync.
    pub fn reset_continuity(&mut self) {
        self.prev_price = None;
    }

    /// Full reset (variance included, decay kept). Window rotation: variance across distros would lie.
    pub fn reset(&mut self) {
        self.prev_price = None;
        self.variance = None;
    }
}

#[inline]
fn decay_for(halflife_events: u32) -> f64 {
    (-std::f64::consts::LN_2 / f64::from(halflife_events)).exp()
}
