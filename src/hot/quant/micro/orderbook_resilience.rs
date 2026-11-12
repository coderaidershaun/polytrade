//! Order-book resilience: OU rate of mid relaxation toward equilibrium (1/sec).

use crate::time::TsUs;

/// Deviation below this fraction of half-spread = discretisation noise (5% TOB imbalance minimum).
const MIN_DEVIATION_FRACTION: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResilienceSample {
    pub event_ts_us: TsUs,
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
