//! Simplex MLE fitter over a simulated path with known truth. A fitter that quietly returns its
//! seed, or a warm start that silently re-does the cold search, costs nothing at run time and
//! everything in the research columns downstream.

use super::{ExpSimulation, START, exponential_fixture, window};
use engine::hot::quant::hawkes::{HawkesMle, HawkesParams};
use engine::msg::time::DurationUs;

#[test]
fn fit_recovers_exponential_params_and_warm_starts() {
    // Warm refit reduces iterations via seeded simplex.
    let (truth, events, now) = exponential_fixture();
    let mut fitter = HawkesMle::new(64);
    let cold = fitter.fit(&events, now).expect("cold fit");
    assert!(cold.converged, "cold fit hit max_iter");
    for (fitted, expected, name) in [
        (cold.mu, truth.mu_per_sec(), "mu"),
        (cold.alpha, truth.alpha_per_sec(), "alpha"),
        (cold.beta, truth.beta_per_sec(), "beta"),
    ] {
        assert!(
            (fitted / expected - 1.0).abs() < 0.25,
            "{name} {fitted} missed {expected}"
        );
    }
    assert!(cold.is_stationary() && (cold.branching_ratio - 0.4).abs() < 0.15);

    let warm = fitter.fit(&events, now).expect("warm refit");
    assert!(warm.iterations <= cold.iterations);
    assert!(!warm.is_stale);

    let thin = window(&[START, START + DurationUs::from_secs(1)]);
    assert!(fitter.fit(&thin, now).expect("stale reissue").is_stale);

    // The likelihood that a fit reports equals its definition, summed here term by term at the
    // returned params: a wrong compensator would still pass the 25% bounds above.
    let horizon = DurationUs::from_secs(300);
    let stamps = ExpSimulation {
        params: truth,
        start_ts: START,
        horizon,
        seed: 0x5EED_0002,
        max_events: 1_000,
    }
    .run();
    let short = fitter
        .fit(&window(&stamps), START + horizon)
        .expect("short fit");
    let times: Vec<f64> = stamps
        .iter()
        .map(|stamp| stamp.diff(stamps[0]).to_secs())
        .collect();
    let end = (START + horizon).diff(stamps[0]).to_secs();
    let direct = direct_log_likelihood(&times, end, short.params());
    assert!(
        (short.log_likelihood - direct).abs() < 1e-9,
        "reported {} vs direct {direct}",
        short.log_likelihood
    );
}

/// Σ ln λ(t_i) − ∫λ from the first event to `end`, with λ(t) = μ + α Σ_{j<i} e^{−β(t − t_j)}. Each
/// kernel term integrates to (α/β)(1 − e^{−β(end − t_j)}).
fn direct_log_likelihood(times: &[f64], end: f64, params: HawkesParams) -> f64 {
    let (mu, alpha, beta) = (
        params.mu_per_sec(),
        params.alpha_per_sec(),
        params.beta_per_sec(),
    );
    let log_sum: f64 = times
        .iter()
        .enumerate()
        .map(|(index, &time)| {
            let excitation: f64 = times[..index]
                .iter()
                .map(|&earlier| (-beta * (time - earlier)).exp())
                .sum();
            (mu + alpha * excitation).ln()
        })
        .sum();
    let kernel_mass: f64 = times
        .iter()
        .map(|&time| (alpha / beta) * (1.0 - (-beta * (end - time)).exp()))
        .sum();
    log_sum - (mu * (end - times[0]) + kernel_mass)
}
