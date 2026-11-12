//! EGARCH(1,1) fit on a synthetic price path with a known optimum. The fitter's whole job is to
//! turn per-close volatility into per-second volatility; a persistence pinned at a bound, or a
//! rescaling off by the interval, produces a σ that is plausible-looking and wrong in every
//! research row it feeds.

use super::Lcg;
use polysim::hot::quant::volatility::Egarch;
use polysim::time::DurationUs;

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

    let cold = egarch.fit(&closes).expect("1500 closes exceeds floor");
    assert!(cold.converged, "cold fit hit max_iter");
    assert!(
        cold.beta > 0.9 && cold.beta < 0.9999,
        "persistence {} not interior",
        cold.beta
    );

    for vol in [cold.conditional_vol_per_sec, cold.unconditional_vol_per_sec] {
        assert!(
            (1e-5..1e-3).contains(&vol),
            "per-second σ {vol} outside return scale"
        );
    }

    let warm = egarch.fit(&closes).expect("refit");
    assert!(
        warm.iterations <= cold.iterations,
        "warm refit {} exceeded cold {}",
        warm.iterations,
        cold.iterations
    );
}
