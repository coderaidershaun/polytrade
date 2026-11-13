//! Simplex MLE fitter and the side that paces it, over a simulated path with known truth. A fitter
//! that quietly returns its seed, or a warm start that silently re-does the cold search, costs
//! nothing at run time and everything in the research columns downstream.

use super::{ExpSimulation, START, exponential_fixture, exponential_path, window};
use engine::hot::quant::hawkes::{
    HawkesChoice, HawkesChoiceError, HawkesFit, HawkesMle, HawkesParams, HawkesSide,
};
use engine::msg::time::{DurationUs, VenueTs};

#[test]
fn fit_recovers_exponential_params_and_warm_starts() {
    // Warm refit reduces iterations via seeded simplex.
    let (truth, events, now) = exponential_fixture();
    let mut fitter = HawkesMle::new(HawkesChoice::new(32_768, 64, 1).expect("a valid choice"));
    let cold = fitter
        .fit(&events, now)
        .and_then(HawkesFit::fresh)
        .expect("cold fit");
    assert!(cold.converged, "cold fit hit max_iter");
    for (fitted, expected, name) in [
        (cold.params.mu_per_sec(), truth.mu_per_sec(), "mu"),
        (cold.params.alpha_per_sec(), truth.alpha_per_sec(), "alpha"),
        (cold.params.beta_per_sec(), truth.beta_per_sec(), "beta"),
    ] {
        assert!(
            (fitted / expected - 1.0).abs() < 0.25,
            "{name} {fitted} missed {expected}"
        );
    }
    assert!(cold.params.is_stationary() && (cold.params.branching_ratio() - 0.4).abs() < 0.15);

    let warm = fitter.fit(&events, now).expect("warm refit");
    let warm = warm.fresh().expect("a fresh warm refit");
    assert!(warm.iterations <= cold.iterations);

    let thin = window(&[START, START + DurationUs::from_secs(1)]);
    assert!(matches!(
        fitter.fit(&thin, now).expect("stale reissue"),
        HawkesFit::Stale(_)
    ));

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
        .and_then(HawkesFit::fresh)
        .expect("short fit");
    let times: Vec<f64> = stamps
        .iter()
        .map(|stamp| stamp.diff(stamps[0]).to_secs())
        .collect();
    let end = (START + horizon).diff(stamps[0]).to_secs();
    let direct = direct_log_likelihood(&times, end, short.params);
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

#[test]
fn each_bad_hawkes_choice_is_refused_by_name() {
    use HawkesChoiceError::{EmptyWindow, FloorAboveWindow, NoRefitCadence, TooFewEvents};
    let refusals = [
        (HawkesChoice::new(0, 2, 1), EmptyWindow, "max_events"),
        (
            HawkesChoice::new(100, 1, 1),
            TooFewEvents { min_events: 1 },
            "min_events",
        ),
        (
            HawkesChoice::new(100, 120, 1),
            FloorAboveWindow {
                min_events: 120,
                max_events: 100,
            },
            "min_events = 120 exceeds max_events = 100",
        ),
        (
            HawkesChoice::new(100, 2, 0),
            NoRefitCadence,
            "refit_arrivals",
        ),
        (HawkesChoice::new(0, 1, 0), EmptyWindow, "max_events"),
    ];
    for (choice, refusal, field) in refusals {
        assert_eq!(choice, Err(refusal));
        assert!(
            refusal.to_string().contains(field),
            "{refusal} names no {field}"
        );
    }

    for (max_events, min_events, refit_arrivals) in [(2, 2, 1), (1024, 100, 128)] {
        assert!(
            HawkesChoice::new(max_events, min_events, refit_arrivals).is_ok(),
            "({max_events}, {min_events}, {refit_arrivals}) refused"
        );
    }
}

#[test]
fn intensity_after_a_refit_equals_the_direct_sum() {
    let (_, path, _) = exponential_path();
    let (max_events, min_events, refit_arrivals) = (1024, 100, 128);
    let mut side = HawkesSide::new(
        HawkesChoice::new(max_events, min_events, refit_arrivals).expect("a valid choice"),
    );
    let first_fit = min_events - 1;
    let refit = first_fit + refit_arrivals;
    assert!(
        refit < max_events,
        "the window must hold each arrival up to the refit"
    );
    assert!(
        path[..=refit].windows(2).all(|pair| pair[0] < pair[1]),
        "an arrival with the stamp of the one before it would be dropped"
    );

    let mut fits = Vec::new();
    for (index, &stamp) in path[..=refit].iter().enumerate() {
        side.on_arrival(VenueTs::from_micros(stamp.micros()), stamp);
        if let Some(estimate) = side.estimate(stamp) {
            fits.push((index, estimate));
        }
    }
    let (first_index, first) = fits[0];
    let (refit_index, refitted) = fits[fits.len() - 1];
    assert_eq!(
        (first_index, refit_index),
        (first_fit, refit),
        "the first fit and the refit come at the floor and after the cadence"
    );
    assert_ne!(refitted.params, first.params, "the refit gives new params");

    let now = path[refit];
    let params = refitted.params;
    let direct = params.mu_per_sec()
        + params.alpha_per_sec()
            * path[..=refit]
                .iter()
                .map(|&time| (-params.beta_per_sec() * now.diff(time).to_secs()).exp())
                .sum::<f64>();
    let live = side.intensity(now).expect("a fresh fit seeds the live sum");
    assert!(
        ((live - direct) / direct).abs() < 1e-12,
        "live {live} vs direct {direct}"
    );
}
