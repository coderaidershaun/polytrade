//! The exponential kernel and its live evaluator. An exponential kernel lets one decaying sum stand
//! in for the whole event history, so each event and each intensity query costs constant time.

use super::HawkesEvents;
use crate::time::TsUs;

/// Exponential kernel parameters, every rate per second.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HawkesParams {
    pub mu: f64,
    pub alpha: f64,
    pub beta: f64,
}

impl HawkesParams {
    /// # Panics
    /// Non-finite, mu <= 0, alpha < 0, or beta <= 0.
    pub fn new(mu: f64, alpha: f64, beta: f64) -> Self {
        assert!(
            mu.is_finite() && alpha.is_finite() && beta.is_finite(),
            "hawkes params must be finite, got mu={mu} alpha={alpha} beta={beta}"
        );
        assert!(mu > 0.0, "hawkes baseline must be positive, got {mu}");
        assert!(
            alpha >= 0.0,
            "hawkes jump must be non-negative, got {alpha}"
        );
        assert!(beta > 0.0, "hawkes decay must be positive, got {beta}");
        Self { mu, alpha, beta }
    }

    #[inline]
    pub fn branching_ratio(&self) -> f64 {
        self.alpha / self.beta
    }

    #[inline]
    pub fn half_life_secs(&self) -> f64 {
        core::f64::consts::LN_2 / self.beta
    }
}

/// Live intensity that keeps only the last stamp and one decayed excitation sum.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnivariateHawkes {
    params: HawkesParams,
    last_ts: Option<TsUs>,
    excitation: f64,
}

impl UnivariateHawkes {
    pub fn new(params: HawkesParams) -> Self {
        Self {
            params,
            last_ts: None,
            excitation: 0.0,
        }
    }

    pub fn set_params(&mut self, params: HawkesParams) {
        self.params = params;
    }

    /// Walks the whole window, so it runs once per refit and never once per event.
    pub fn reseed_from(&mut self, events: &HawkesEvents) {
        let Some(last_ts) = events.last_ts() else {
            self.clear();
            return;
        };
        let times = events.times_secs();
        let mut excitation = 0.0;
        let mut previous = times[0];
        for &time in times {
            excitation = (-self.params.beta * (time - previous)).exp() * excitation + 1.0;
            previous = time;
        }
        self.excitation = excitation;
        self.last_ts = Some(last_ts);
    }

    #[inline]
    pub fn on_event(&mut self, ts: TsUs) {
        self.excitation = match self.last_ts {
            Some(last) => {
                (-self.params.beta * ts.diff(last).to_secs().max(0.0)).exp() * self.excitation + 1.0
            }
            None => 1.0,
        };
        self.last_ts = Some(ts);
    }

    #[inline]
    pub fn intensity(&self, now: TsUs) -> f64 {
        let Some(last) = self.last_ts else {
            return self.params.mu;
        };
        let elapsed = now.diff(last).to_secs().max(0.0);
        self.params.mu + self.params.alpha * (-self.params.beta * elapsed).exp() * self.excitation
    }

    pub fn clear(&mut self) {
        self.last_ts = None;
        self.excitation = 0.0;
    }
}
