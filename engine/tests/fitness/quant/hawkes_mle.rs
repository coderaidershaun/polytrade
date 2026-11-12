//! Simplex MLE fitter over a simulated path with known truth. A fitter that quietly returns its
//! seed, or a warm start that silently re-does the cold search, costs nothing at run time and
//! everything in the research columns downstream.

use super::{START, exponential_fixture, window};
use polysim::hot::quant::hawkes::HawkesMle;
use polysim::time::DurationUs;

#[test]
fn fit_recovers_exponential_params_and_warm_starts() {
    // Warm refit reduces iterations via seeded simplex.
    let (truth, events, now) = exponential_fixture();
    let mut fitter = HawkesMle::new(64);
    let cold = fitter.fit(&events, now).expect("cold fit");
    assert!(cold.converged, "cold fit hit max_iter");
    for (fitted, expected, name) in [
        (cold.mu, truth.mu, "mu"),
        (cold.alpha, truth.alpha, "alpha"),
        (cold.beta, truth.beta, "beta"),
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
}
