//! Maximum-likelihood fit of the exponential kernel. The likelihood has a closed form, so one
//! objective call is a single pass over the window, and each refit starts from the last optimum.

use super::{HawkesEvents, HawkesParams};
use crate::hot::quant::MIN_RATE;
use crate::hot::quant::optimise::NelderMead;
use crate::time::TsUs;

const PARAMS: usize = 3;
const SIMPLEX: usize = PARAMS + 1;

const LOG_BOUNDS: [(f64, f64); PARAMS] = [(-25.0, 10.0), (-25.0, 10.0), (-12.0, 8.0)];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HawkesEstimate {
    pub mu: f64,
    pub alpha: f64,
    pub beta: f64,
    pub branching_ratio: f64,
    pub log_likelihood: f64,
    pub events: usize,
    pub converged: bool,
    pub iterations: usize,
    /// Not a fresh solve: the window could not support one, so these are the previous fit's
    /// numbers re-dated.
    pub is_stale: bool,
}

impl HawkesEstimate {
    pub fn params(&self) -> HawkesParams {
        HawkesParams::new(self.mu, self.alpha, self.beta)
    }

    #[inline]
    pub fn is_stationary(&self) -> bool {
        self.branching_ratio < 1.0
    }
}

/// An accidental move forks the warm-start cache, so keep the type non-`Copy`.
#[derive(Debug, Clone, PartialEq)]
pub struct HawkesMle {
    min_events: usize,
    cached: Option<[f64; PARAMS]>,
    last: Option<HawkesEstimate>,
}

impl HawkesMle {
    /// # Panics
    /// When `min_events < 2`, because a fit needs at least one gap between events.
    pub fn new(min_events: usize) -> Self {
        assert!(
            min_events >= 2,
            "hawkes mle needs at least two events, got {min_events}"
        );
        Self {
            min_events,
            cached: None,
            last: None,
        }
    }

    pub fn fit(&mut self, events: &HawkesEvents, now: TsUs) -> Option<HawkesEstimate> {
        let Some((times, end)) = fit_window(events, now, self.min_events) else {
            return self.last.map(|last| HawkesEstimate {
                is_stale: true,
                ..last
            });
        };
        let optimum = NelderMead::new(
            self.cached.unwrap_or_else(|| cold_seed(times, end)),
            LOG_BOUNDS,
        )
        .minimize::<SIMPLEX>(|params| exp_nll(params, times, end));

        let [mu, alpha, beta] = optimum.x.map(f64::exp);
        let estimate = HawkesEstimate {
            mu,
            alpha,
            beta,
            branching_ratio: alpha / beta,
            log_likelihood: -optimum.value,
            events: times.len(),
            converged: optimum.converged,
            iterations: optimum.iterations,
            is_stale: false,
        };
        self.cached = Some(optimum.x);
        self.last = Some(estimate);
        Some(estimate)
    }
}

fn fit_window(events: &HawkesEvents, now: TsUs, min_events: usize) -> Option<(&[f64], f64)> {
    let times = events.times_secs();
    if times.len() < min_events {
        return None;
    }
    let end = events.end_secs(now)?;
    if end - times[0] <= 0.0 {
        return None;
    }
    Some((times, end))
}

fn cold_seed(times: &[f64], end: f64) -> [f64; PARAMS] {
    let count = times.len() as f64;
    let mean_gap = (times[times.len() - 1] - times[0]) / (count - 1.0);
    let decay = mean_gap.recip();
    [
        bounded((0.5 * count / (end - times[0])).ln(), LOG_BOUNDS[0]),
        bounded((0.5 * decay).ln(), LOG_BOUNDS[1]),
        bounded(decay.ln(), LOG_BOUNDS[2]),
    ]
}

fn exp_nll(params: &[f64; PARAMS], times: &[f64], end: f64) -> f64 {
    let [mu, alpha, beta] = params.map(f64::exp);
    let mut excitation = 0.0;
    let mut previous = times[0];
    let mut log_sum = 0.0;
    for (index, &time) in times.iter().enumerate() {
        if index > 0 {
            excitation = (-beta * (time - previous)).exp() * (excitation + 1.0);
            previous = time;
        }
        log_sum += (mu + alpha * excitation).max(MIN_RATE).ln();
    }
    let tail = (-beta * (end - previous)).exp() * (excitation + 1.0);
    let compensator = mu * (end - times[0]) + (alpha / beta) * (times.len() as f64 - tail);
    penalised(compensator - log_sum)
}

fn bounded(value: f64, bound: (f64, f64)) -> f64 {
    if value.is_finite() {
        value.clamp(bound.0, bound.1)
    } else if value > 0.0 {
        bound.1
    } else {
        bound.0
    }
}

fn penalised(nll: f64) -> f64 {
    if nll.is_finite() { nll } else { f64::MAX }
}
