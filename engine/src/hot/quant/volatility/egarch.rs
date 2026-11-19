//! The long-horizon volatility of the quotes: an EGARCH(1,1) fit by maximum likelihood to the log
//! returns of closed candles. It forecasts the next close and refits from its last optimum.

use crate::hot::quant::optimise::NelderMead;
use crate::hot::quant::options::exp;
use crate::hot::series::FastQueue;
use crate::msg::time::DurationUs;

use super::realised::{Returns, consecutive_returns};

/// `[omega, gamma, theta, beta]`. With ω/(1 − β) = −14, the volatility of the first fit starts near
/// 1e-3 for each close of one minute.
const COLD_START: [f64; 4] = [-1.4, 0.1, -0.05, 0.9];

/// β stays below 1, so that ω/(1 − β) is finite. γ stays at 0 or above, because it scales the size
/// of a shock, and θ, which gives its sign, stays within ±1.
const BOUNDS: [(f64, f64); 4] = [(-10.0, 10.0), (0.0, 1.0), (-1.0, 1.0), (0.5, 0.9999)];

const PARAMS: usize = 4;
const SIMPLEX: usize = PARAMS + 1;

/// A window of equal closes has a variance of 0, and the recursion seeds from its logarithm.
const MIN_SEED_VARIANCE: f64 = 1e-12;

const WINDOW_BACKING_MULTIPLE: usize = 2;

/// One fit of the window, with each volatility per second.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EgarchEstimate {
    /// The volatility of the next close: one step of the recursion past the last return.
    pub forecast_vol_per_sec: f64,
    /// exp(½ · ω/(1 − β)), the level that the log variance returns to. It lies below the long-run
    /// standard deviation whenever the variance moves. `None` when it is not finite.
    pub long_run_log_vol_per_sec: Option<f64>,
    pub omega: f64,
    pub gamma: f64,
    pub theta: f64,
    pub beta: f64,
    /// The objective with its sign flipped and the Gaussian constant left out, so it compares
    /// only between windows of one length.
    pub log_likelihood: f64,
    pub converged: bool,
    pub iterations: usize,
}

/// The closes of one candle interval, the fit in force over them, and the optimum that the next
/// fit starts from.
#[derive(Debug, Clone, PartialEq)]
pub struct Egarch {
    params: Option<[f64; 4]>,
    interval: DurationUs,
    min_closes: usize,
    closes: FastQueue<f64>,
    returns: Vec<f64>,
    is_due: bool,
    estimate: Option<EgarchEstimate>,
}

impl Egarch {
    /// The window keeps the newest `max_closes` closes.
    ///
    /// # Panics
    /// A close interval that is not positive, or a window of 0 closes.
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
            closes: FastQueue::new(max_closes, WINDOW_BACKING_MULTIPLE),
            returns: Vec::with_capacity(max_closes.saturating_sub(1)),
            is_due: false,
            estimate: None,
        }
    }

    /// A full window evicts its oldest close, and the next [`Egarch::estimate`] refits.
    pub fn push_close(&mut self, close: f64) {
        self.closes.push(close);
        self.is_due = true;
    }

    /// Empties the window and drops the fit in force. The optimum that the next fit starts from
    /// stays: [`Egarch::reset_warm_start`] drops it.
    pub fn clear(&mut self) {
        self.closes.clear();
        self.is_due = false;
        self.estimate = None;
    }

    /// Whether a close arrived since the last fit, so that the next [`Egarch::estimate`] refits.
    #[inline]
    pub fn is_due(&self) -> bool {
        self.is_due
    }

    /// Refits when a close arrived since the last fit, then gives the fit in force. A read on each
    /// spin is cheap, because the simplex search runs only when a close arrived.
    pub fn estimate(&mut self) -> Option<EgarchEstimate> {
        if self.is_due {
            self.fit();
        }
        self.estimate
    }

    /// Fits the window now, and the result becomes the fit in force. The search starts from the
    /// last optimum. `None` below `min_closes`, with fewer than two usable returns, or when the
    /// likelihood or the forecast is not finite; such a fit leaves the optimum that the next one
    /// starts from as it was. A close that is not finite or not positive drops out, and its
    /// neighbours pair into one return.
    pub fn fit(&mut self) -> Option<EgarchEstimate> {
        self.is_due = false;
        self.estimate = self.solve();
        self.estimate
    }

    /// The next fit starts from the cold start, not from the last optimum.
    pub fn reset_warm_start(&mut self) {
        self.params = None;
    }

    fn solve(&mut self) -> Option<EgarchEstimate> {
        let closes = self.closes.as_slice();
        if closes.len() < self.min_closes {
            return None;
        }
        let seed_log_variance = fill_centred_returns(closes, &mut self.returns)?;
        let residuals = self.returns.as_slice();
        let optimum = NelderMead::new(self.params.unwrap_or(COLD_START), BOUNDS)
            .minimize::<SIMPLEX>(|params| {
                recursion(params, residuals, seed_log_variance).neg_log_likelihood
            });
        let terminal = recursion(&optimum.x, residuals, seed_log_variance);
        let last_residual = residuals[residuals.len() - 1];
        let standardised = last_residual / exp(0.5 * terminal.last_log_variance);
        let forecast_log_variance =
            next_log_variance(&optimum.x, terminal.last_log_variance, standardised);
        let per_second_scale = self.interval.to_secs().sqrt().recip();
        let forecast_vol_per_sec = exp(0.5 * forecast_log_variance) * per_second_scale;
        let log_likelihood = -terminal.neg_log_likelihood;
        if !(forecast_vol_per_sec.is_finite() && log_likelihood.is_finite()) {
            return None;
        }
        self.params = Some(optimum.x);
        let [omega, gamma, theta, beta] = optimum.x;
        let long_run = exp(0.5 * omega / (1.0 - beta)) * per_second_scale;
        Some(EgarchEstimate {
            forecast_vol_per_sec,
            long_run_log_vol_per_sec: long_run.is_finite().then_some(long_run),
            omega,
            gamma,
            theta,
            beta,
            log_likelihood,
            converged: optimum.converged,
            iterations: optimum.iterations,
        })
    }
}

/// Fills `returns` with the log returns of the usable closes less their mean, and gives the log
/// of their mean square, floored, as the seed of the recursion. `None` with fewer than two returns.
fn fill_centred_returns(closes: &[f64], returns: &mut Vec<f64>) -> Option<f64> {
    returns.clear();
    let mut sum = 0.0;
    for log_return in consecutive_returns(closes.iter().copied(), Returns::Log) {
        sum += log_return;
        returns.push(log_return);
    }
    if returns.len() < 2 {
        return None;
    }
    let mean = sum / returns.len() as f64;
    let mut sum_squares = 0.0;
    for residual in returns.iter_mut() {
        *residual -= mean;
        sum_squares += *residual * *residual;
    }
    let variance = (sum_squares / returns.len() as f64).max(MIN_SEED_VARIANCE);
    Some(variance.ln())
}

struct Recursion {
    neg_log_likelihood: f64,
    last_log_variance: f64,
}

/// One pass over the residuals with the log variance carried as one number, so it allocates
/// nothing. Each step reads the residual before it. It gives the objective and the log variance
/// of the last residual.
fn recursion(params: &[f64; 4], residuals: &[f64], seed_log_variance: f64) -> Recursion {
    let mut log_variance = seed_log_variance;
    let mut neg_log_likelihood = 0.0;
    let mut previous = residuals[0];
    for &residual in &residuals[1..] {
        let standardised = previous / (0.5 * log_variance).exp();
        log_variance = next_log_variance(params, log_variance, standardised);
        neg_log_likelihood += 0.5 * (log_variance + residual * residual / log_variance.exp());
        previous = residual;
    }
    Recursion {
        neg_log_likelihood,
        last_log_variance: log_variance,
    }
}

/// One step of the log variance, from the standardised residual before it.
#[inline]
fn next_log_variance(params: &[f64; 4], log_variance: f64, standardised: f64) -> f64 {
    let [omega, gamma, theta, beta] = *params;
    let expected_abs_z = std::f64::consts::FRAC_2_PI.sqrt();
    omega
        + beta * log_variance
        + gamma * (standardised.abs() - expected_abs_z)
        + theta * standardised
}
