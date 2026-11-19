//! Kyle's lambda: the rolling least-squares slope of the change of the mid on signed order flow,
//! with one observation for each bar, as the price impact of one unit of flow.

use crate::hot::series::FastQueue;

const BACKING_MULTIPLE: usize = 2;

/// The size of the rolling window, and the gates that refuse a window that cannot carry a slope.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KylesLambdaSpec {
    pub window: usize,
    pub min_observations: usize,
    /// The smallest variance of the flow that the fit accepts, in squared units of flow. It keeps
    /// the denominator of the slope away from zero.
    pub min_flow_variance: f64,
    /// The smallest share of the bars that each sign of flow must hold. A share above 0.5 could
    /// never be met.
    pub min_sign_fraction: f64,
}

impl Default for KylesLambdaSpec {
    fn default() -> Self {
        Self {
            window: 100,
            min_observations: 30,
            // A numerical epsilon, not an economic floor. Each instrument sets its real floor in
            // its config.
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
    // Two queues, because a queue cannot hold pairs. push and clear keep them in step.
    flows: FastQueue<f64>,
    mid_changes: FastQueue<f64>,
}

impl KylesLambda {
    /// # Panics
    /// When the spec is invalid: `min_observations` below 2 or above the window, a variance floor
    /// that is not finite and positive, or a sign fraction outside 0 to 0.5. That is a bug of the
    /// config, not a state of the market.
    pub fn new(spec: KylesLambdaSpec) -> Self {
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
        // Together with the sign gate, this keeps a window of zero flow from reaching the division.
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
        Self {
            min_observations: spec.min_observations,
            min_flow_variance: spec.min_flow_variance,
            min_sign_fraction: spec.min_sign_fraction,
            flows: FastQueue::new(spec.window, BACKING_MULTIPLE),
            mid_changes: FastQueue::new(spec.window, BACKING_MULTIPLE),
        }
    }

    /// Stores the flow and the mid change of one closed bar. Each bar must use the same unit of
    /// flow.
    ///
    /// # Panics
    /// When either value is not finite: a bug upstream, not a state of the market.
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

    /// The least-squares slope of the window, or `None` where a gate refuses the window. A negative
    /// slope is reported, because it carries information.
    pub fn fit(&self) -> Option<KyleEstimate> {
        let observations = self.flows.len();
        if observations < self.min_observations {
            return None;
        }

        let flows = self.flows.as_slice();
        let mid_changes = self.mid_changes.as_slice();
        let count = observations as f64;
        // Two passes over the window, because a running sum would drift as bars leave it.
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
            // A bar with zero flow counts toward neither sign, and still anchors the intercept.
            positive_bars += usize::from(flow > 0.0);
            negative_bars += usize::from(flow < 0.0);
        }

        let positive_fraction = positive_bars as f64 / count;
        let negative_fraction = negative_bars as f64 / count;
        if positive_fraction.min(negative_fraction) < self.min_sign_fraction {
            return None;
        }

        // Extreme but finite bars can overflow the centred sums, so this gate also refuses a sum
        // that is not finite.
        let flow_variance = centred_flow_squares / count;
        if !(flow_variance.is_finite() && flow_variance >= self.min_flow_variance) {
            return None;
        }

        let lambda = cross_products / centred_flow_squares;
        let intercept = mean_mid_change - lambda * mean_flow;
        // The cross products can overflow too, so the result is checked.
        if !(lambda.is_finite() && intercept.is_finite()) {
            return None;
        }

        Some(KyleEstimate {
            lambda,
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

/// The slope with its diagnostics, because a slope alone cannot be checked.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KyleEstimate {
    /// Price units per unit of flow. Reported even when negative.
    pub lambda: f64,
    /// The free intercept: drift and any bias of the timestamps land here, not in the slope.
    pub intercept: f64,
    pub observations: usize,
    pub flow_variance: f64,
    pub positive_fraction: f64,
    pub negative_fraction: f64,
}

impl KyleEstimate {
    /// The slope read in steps of `scale`, a price step such as one tick or one basis point of the
    /// mid. `None` where that reading is not a finite number.
    pub fn per_unit(&self, scale: f64) -> Option<PerUnitLambda> {
        let lambda = self.lambda / scale;
        if !lambda.is_finite() {
            return None;
        }
        // A slope that is not positive gives no depth, and neither does a slope so faint that its
        // inverse is too large for a float.
        let one_unit_flow = Some(1.0 / lambda).filter(|flow| flow.is_finite() && *flow > 0.0);
        Some(PerUnitLambda {
            lambda,
            one_unit_flow,
        })
    }
}

/// Kyle's lambda in steps of a chosen price unit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PerUnitLambda {
    /// Price steps per unit of flow.
    pub lambda: f64,
    /// The flow that moves the price by one step: the inverse of the slope.
    pub one_unit_flow: Option<f64>,
}
