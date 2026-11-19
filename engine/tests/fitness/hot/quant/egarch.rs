//! EGARCH(1,1) fit on a synthetic price path with a known optimum. The fitter's whole job is to
//! turn per-close volatility into per-second volatility; a persistence pinned at a bound, or a
//! rescaling off by the interval, produces a σ that is plausible-looking and wrong in every
//! research row it feeds.

use super::Lcg;
use engine::hot::quant::volatility::Egarch;
use engine::msg::time::DurationUs;

/// Synthetic price path: log returns follow exact EGARCH(1,1) recursion with real optimum.
fn synthetic_price_path(count: usize, params: [f64; 4], seed: u64) -> Vec<f64> {
    let [omega, gamma, theta, beta] = params;
    let expected_abs_z = std::f64::consts::FRAC_2_PI.sqrt();
    let mut rng = Lcg(seed);
    let mut log_variance = omega / (1.0 - beta);
    let mut price = 100_000.0;
    let mut closes = Vec::with_capacity(count);
    for _ in 0..count {
        let z = rng.next_normal();
        let sigma = (0.5 * log_variance).exp();
        price *= (sigma * z).exp();
        closes.push(price);
        log_variance = omega + beta * log_variance + gamma * (z.abs() - expected_abs_z) + theta * z;
    }
    closes
}

#[test]
fn fit_on_a_price_path_recovers_persistence_in_per_second_units() {
    let closes = synthetic_price_path(1500, [-0.7, 0.2, -0.1, 0.95], 0x1234_5678);
    let mut egarch = Egarch::new(DurationUs::from_secs(60), 300, 1500);
    for &close in &closes {
        egarch.push_close(close);
    }

    let cold = egarch.fit().expect("1500 closes exceeds floor");
    assert!(cold.converged, "cold fit hit max_iter");
    assert!(
        cold.beta > 0.9 && cold.beta < 0.9999,
        "persistence {} not interior",
        cold.beta
    );

    let long_run = cold
        .long_run_log_vol_per_sec
        .expect("an interior persistence gives a finite long-run level");
    for vol in [cold.forecast_vol_per_sec, long_run] {
        assert!(
            (1e-5..1e-3).contains(&vol),
            "per-second σ {vol} outside return scale"
        );
    }

    let warm = egarch.fit().expect("refit");
    assert!(
        warm.iterations <= cold.iterations,
        "warm refit {} exceeded cold {}",
        warm.iterations,
        cold.iterations
    );
}

/// The quote reads the volatility of the next close. The variance of the last return, given the
/// returns before it, has not seen that return, so the forecast takes one more step with it.
#[test]
fn forecast_is_one_recursion_step_past_the_last_return() {
    let closes = synthetic_price_path(1500, [-0.7, 0.2, -0.1, 0.95], 0x1234_5678);
    let mut egarch = Egarch::new(DurationUs::from_secs(60), 300, 1500);
    for &close in &closes {
        egarch.push_close(close);
    }
    let estimate = egarch.estimate().expect("1500 closes exceeds floor");

    // The recursion of the fit, under its fitted params. The residuals are the log returns less
    // their mean, the seed is the log of their mean square, and each step reads the residual
    // before it, scaled by the volatility of that residual.
    let returns: Vec<f64> = closes
        .windows(2)
        .map(|pair| (pair[1] / pair[0]).ln())
        .collect();
    let mut sum = 0.0;
    for &log_return in &returns {
        sum += log_return;
    }
    let mean = sum / returns.len() as f64;
    let residuals: Vec<f64> = returns.iter().map(|log_return| log_return - mean).collect();
    let mut sum_squares = 0.0;
    for &residual in &residuals {
        sum_squares += residual * residual;
    }
    let seed = (sum_squares / residuals.len() as f64).max(1e-12).ln();
    let expected_abs_z = std::f64::consts::FRAC_2_PI.sqrt();
    let step = |log_variance: f64, residual: f64| {
        let z = residual / (0.5 * log_variance).exp();
        estimate.omega
            + estimate.beta * log_variance
            + estimate.gamma * (z.abs() - expected_abs_z)
            + estimate.theta * z
    };
    let mut log_variance_of_last = seed;
    for pair in residuals.windows(2) {
        log_variance_of_last = step(log_variance_of_last, pair[0]);
    }
    let last = residuals[residuals.len() - 1];
    let forecast = (0.5 * step(log_variance_of_last, last)).exp() / 60.0f64.sqrt();
    let last_return_vol = (0.5 * log_variance_of_last).exp() / 60.0f64.sqrt();
    assert!(
        (estimate.forecast_vol_per_sec / forecast - 1.0).abs() < 1e-12,
        "forecast {} is not one step past the last return, {forecast}; the last return has {last_return_vol}",
        estimate.forecast_vol_per_sec
    );

    // A close of 1e300 right after 1e-300 overflows the quotient of its log return to +inf. Then
    // the mean return is +inf, and each residual is -inf or NaN, so no params give a finite
    // likelihood. That fit must give no estimate and keep the optimum that the next fit starts
    // from, as a twin that never saw the window shows.
    let mut poisoned = Egarch::new(DurationUs::from_secs(60), 300, 1500);
    let mut twin = Egarch::new(DurationUs::from_secs(60), 300, 1500);
    for fit in [&mut poisoned, &mut twin] {
        for &close in &closes {
            fit.push_close(close);
        }
        fit.fit().expect("cold fit of the path");
    }
    poisoned.push_close(1e-300);
    poisoned.push_close(1e300);
    assert_eq!(
        poisoned.fit(),
        None,
        "a window with no finite likelihood gave an estimate"
    );
    for fit in [&mut poisoned, &mut twin] {
        fit.clear();
        for &close in &closes {
            fit.push_close(close);
        }
    }
    assert_eq!(
        poisoned.fit(),
        twin.fit(),
        "the fit with no finite estimate changed the optimum that the next fit starts from"
    );
}
