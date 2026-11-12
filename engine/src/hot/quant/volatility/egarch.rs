//! EGARCH(1,1): 4-param MLE fit (Nelder-Mead), allocation-free, warm-started.

use crate::hot::quant::optimise::NelderMead;
use crate::time::DurationUs;

use super::realised::{Returns, consecutive_returns};

/// `[omega, gamma, theta, beta]`: baseline log-variance, news-impact magnitude/asymmetry, persistence.
/// `ω/(1−β) = −14` seeds σ ≈ 1e-3 per 1m close.
const COLD_START: [f64; 4] = [-1.4, 0.1, -0.05, 0.9];

/// Per-param bounds: β < 1 keeps ω/(1−β) finite; γ ≥ 0 keeps impact magnitude; θ ∈ [−1, 1] sign.
const BOUNDS: [(f64, f64); 4] = [(-10.0, 10.0), (0.0, 1.0), (-1.0, 1.0), (0.5, 0.9999)];

const PARAMS: usize = 4;
const SIMPLEX: usize = PARAMS + 1;

/// Floor seed variance to avoid ln(0) in recursion.
const MIN_SEED_VARIANCE: f64 = 1e-12;

/// Conditional/unconditional vol/sec, fitted params [ω, γ, θ, β], diagnostics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EgarchEstimate {
    pub conditional_vol_per_sec: f64,
    pub unconditional_vol_per_sec: f64,
    pub omega: f64,
    pub gamma: f64,
    pub theta: f64,
    pub beta: f64,
    /// Sign-flipped objective (Gaussian constant omitted, compares within length).
    pub log_likelihood: f64,
    pub converged: bool,
    pub iterations: usize,
}

/// Persistent fit: caches params for warm-start, owns returns buffer. Rescales per-close σ to /sec.
#[derive(Debug, Clone, PartialEq)]
pub struct Egarch {
    params: Option<[f64; 4]>,
    interval: DurationUs,
    min_closes: usize,
    max_closes: usize,
    returns: Vec<f64>,
}

impl Egarch {
    /// # Panics
    /// Non-positive `interval`.
    pub fn new(interval: DurationUs, min_closes: usize, max_closes: usize) -> Self {
        assert!(
            interval.micros() > 0,
            "egarch close interval must be positive, got {}us",
            interval.micros()
        );
        Self {
            params: None,
            interval,
            min_closes,
            max_closes,
            returns: Vec::with_capacity(max_closes.saturating_sub(1)),
        }
    }

    /// Fits log returns via MLE, warm-started. Returns `None` below `min_closes` or <2 usable returns.
    /// Non-finite closes drop pairwise. Cache adopts best vertex; check `converged` for quality.
    /// # Panics
    /// Closes exceed `max_closes`.
    pub fn fit(&mut self, closes: &[f64]) -> Option<EgarchEstimate> {
        if closes.len() < self.min_closes {
            return None;
        }
        assert!(
            closes.len() <= self.max_closes,
            "egarch handed {} closes but sized for {}",
            closes.len(),
            self.max_closes
        );
        let seed_log_variance = self.fill_centred_returns(closes)?;
        let residuals = self.returns.as_slice();
        let optimum = NelderMead::new(self.params.unwrap_or(COLD_START), BOUNDS)
            .minimize::<SIMPLEX>(|params| {
                recursion(params, residuals, seed_log_variance).neg_log_likelihood
            });
        self.params = Some(optimum.x);
        let terminal = recursion(&optimum.x, residuals, seed_log_variance);
        let per_second_scale = self.interval.to_secs().sqrt().recip();
        let conditional_vol_per_sec = (0.5 * terminal.last_log_variance).exp() * per_second_scale;
        if !conditional_vol_per_sec.is_finite() {
            return None;
        }
        let [omega, gamma, theta, beta] = optimum.x;
        let unconditional = (0.5 * omega / (1.0 - beta)).exp() * per_second_scale;
        Some(EgarchEstimate {
            conditional_vol_per_sec,
            unconditional_vol_per_sec: if unconditional.is_finite() {
                unconditional
            } else {
                conditional_vol_per_sec
            },
            omega,
            gamma,
            theta,
            beta,
            log_likelihood: -terminal.neg_log_likelihood,
            converged: optimum.converged,
            iterations: optimum.iterations,
        })
    }

    /// Drop cached params for cold restart (keeps buffer). Prevents simplex seeding from stale fit.
    pub fn reset_warm_start(&mut self) {
        self.params = None;
    }

    /// Mean-centre log returns of usable closes. Returns seed log-variance (floored) or `None` if <2.
    fn fill_centred_returns(&mut self, closes: &[f64]) -> Option<f64> {
        self.returns.clear();
        let mut sum = 0.0;
        for log_return in consecutive_returns(closes.iter().copied(), Returns::Log) {
            sum += log_return;
            self.returns.push(log_return);
        }
        if self.returns.len() < 2 {
            return None;
        }
        let mean = sum / self.returns.len() as f64;
        let mut sum_squares = 0.0;
        for residual in &mut self.returns {
            *residual -= mean;
            sum_squares += *residual * *residual;
        }
        let variance = (sum_squares / self.returns.len() as f64).max(MIN_SEED_VARIANCE);
        Some(variance.ln())
    }
}

struct Recursion {
    neg_log_likelihood: f64,
    last_log_variance: f64,
}

/// Single EGARCH pass, scalar log-variance carry (allocation-free). Neg-log-likelihood + terminal var.
fn recursion(params: &[f64; 4], residuals: &[f64], seed_log_variance: f64) -> Recursion {
    let [omega, gamma, theta, beta] = *params;
    let expected_abs_z = std::f64::consts::FRAC_2_PI.sqrt();
    let mut log_variance = seed_log_variance;
    let mut neg_log_likelihood = 0.0;
    let mut previous = residuals[0];
    for &residual in &residuals[1..] {
        let standardised = previous / (0.5 * log_variance).exp();
        log_variance = omega
            + beta * log_variance
            + gamma * (standardised.abs() - expected_abs_z)
            + theta * standardised;
        neg_log_likelihood += 0.5 * (log_variance + residual * residual / log_variance.exp());
        previous = residual;
    }
    Recursion {
        neg_log_likelihood,
        last_log_variance: log_variance,
    }
}
