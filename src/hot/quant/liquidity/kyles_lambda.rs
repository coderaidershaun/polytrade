//! Kyle's lambda — rolling OLS slope of mid-price change on signed order flow (depth/price-impact).
//! One observation per bar (bar clock + flow sum from feeder). λ = price per flow unit (feeder's units).

use crate::hot::series::FastQueue;
use crate::ids::Price;

const BACKING_MULTIPLE: usize = 2;

/// Rolling window + data gates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KylesLambdaSpec {
    pub window: usize,
    pub min_observations: usize,
    /// Var(Q) floor in squared flow units (fence denominator off zero).
    pub min_flow_variance: f64,
    /// Min fraction of bars per flow sign (>0.5 impossible).
    pub min_sign_fraction: f64,
}

impl Default for KylesLambdaSpec {
    fn default() -> Self {
        Self {
            window: 100,
            min_observations: 30,
            // A numerical epsilon, not an economic floor: real per-instrument floors are config.
            min_flow_variance: 1e-12,
            min_sign_fraction: 0.2,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KylesLambda {
    min_observations: usize,
    min_flow_variance: f64,
    min_sign_fraction: f64,
    tick: f64,
    // Two queues (Element can't be pairs); push/clear keep in sync.
    flows: FastQueue<f64>,
    mid_changes: FastQueue<f64>,
}

impl KylesLambda {
    /// # Panics
    /// When spec is invalid: min_observations <2 or >window, non-positive variance floor,
    /// sign fraction outside [0,0.5], or non-positive tick. Config bug, not market condition.
    pub fn new(spec: KylesLambdaSpec, tick: Price) -> Self {
        assert!(
            spec.min_observations >= 2,
            "kyles lambda min_observations must be at least 2, got {}",
            spec.min_observations
        );
        assert!(
            spec.min_observations <= spec.window,
            "kyles lambda min_observations {} exceeds window {}",
            spec.min_observations,
            spec.window
        );
        // Interlock with sign gate: zero-flow window never reaches division.
        assert!(
            spec.min_flow_variance.is_finite() && spec.min_flow_variance > 0.0,
            "kyles lambda min_flow_variance must be finite and positive, got {}",
            spec.min_flow_variance
        );
        assert!(
            (0.0..=0.5).contains(&spec.min_sign_fraction),
            "kyles lambda min_sign_fraction must be in 0.0..=0.5, got {}",
            spec.min_sign_fraction
        );
        assert!(
            tick.0 > 0,
            "kyles lambda tick must be positive, got {}",
            tick.0
        );
        Self {
            min_observations: spec.min_observations,
            min_flow_variance: spec.min_flow_variance,
            min_sign_fraction: spec.min_sign_fraction,
            tick: tick.to_f64(),
            flows: FastQueue::new(spec.window, BACKING_MULTIPLE),
            mid_changes: FastQueue::new(spec.window, BACKING_MULTIPLE),
        }
    }

    /// One bar (closed); flow units must be consistent.
    /// # Panics
    /// When either is non-finite (upstream bug, not market condition).
    #[inline]
    pub fn push(&mut self, flow: f64, mid_change: f64) {
        assert!(
            flow.is_finite(),
            "kyles lambda flow must be finite, got {flow}"
        );
        assert!(
            mid_change.is_finite(),
            "kyles lambda mid_change must be finite, got {mid_change}"
        );
        self.flows.push(flow);
        self.mid_changes.push(mid_change);
    }

    /// OLS slope or None; negative λ reported (informative).
    pub fn fit(&self) -> Option<KyleEstimate> {
        let observations = self.flows.len();
        if observations < self.min_observations {
            return None;
        }

        let flows = self.flows.as_slice();
        let mid_changes = self.mid_changes.as_slice();
        let count = observations as f64;
        // Two-pass off slices (eviction drifts).
        let mean_flow = flows.iter().sum::<f64>() / count;
        let mean_mid_change = mid_changes.iter().sum::<f64>() / count;

        let mut centred_flow_squares = 0.0;
        let mut cross_products = 0.0;
        let mut positive_bars = 0usize;
        let mut negative_bars = 0usize;
        for (&flow, &mid_change) in flows.iter().zip(mid_changes) {
            let centred_flow = flow - mean_flow;
            centred_flow_squares += centred_flow * centred_flow;
            cross_products += centred_flow * (mid_change - mean_mid_change);
            // Zero-flow bars anchor intercept.
            positive_bars += usize::from(flow > 0.0);
            negative_bars += usize::from(flow < 0.0);
        }

        let positive_fraction = positive_bars as f64 / count;
        let negative_fraction = negative_bars as f64 / count;
        if positive_fraction.min(negative_fraction) < self.min_sign_fraction {
            return None;
        }

        // Extreme finite bars overflow centred sums -> unusable, gate applied.
        let flow_variance = centred_flow_squares / count;
        if !(flow_variance.is_finite() && flow_variance >= self.min_flow_variance) {
            return None;
        }

        let lambda = cross_products / centred_flow_squares;
        let lambda_tick = lambda / self.tick;
        let intercept = mean_mid_change - lambda * mean_flow;
        // Cross sum can overflow -> gate result.
        if !(lambda.is_finite() && lambda_tick.is_finite() && intercept.is_finite()) {
            return None;
        }
        // Subnormal λ_tick inverts to infinity -> refuse (noise, not depth).
        let one_tick_flow = Some(1.0 / lambda_tick).filter(|flow| flow.is_finite() && *flow > 0.0);

        Some(KyleEstimate {
            lambda,
            lambda_tick,
            one_tick_flow,
            intercept,
            observations,
            flow_variance,
            positive_fraction,
            negative_fraction,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.flows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.flows.is_empty()
    }

    pub fn clear(&mut self) {
        self.flows.clear();
        self.mid_changes.clear();
    }
}

/// Slope + diagnostics (naked column not auditable).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KyleEstimate {
    /// Price units per flow unit; reported even when negative.
    pub lambda: f64,
    pub lambda_tick: f64,
    /// Flow units per tick = 1/λ_tick (None if λ_tick ≤0 or subnormal, to avoid nonsense depth).
    pub one_tick_flow: Option<f64>,
    /// α̂ — free intercept (drift + timestamp bias captured here, not in λ).
    pub intercept: f64,
    pub observations: usize,
    pub flow_variance: f64,
    pub positive_fraction: f64,
    pub negative_fraction: f64,
}
