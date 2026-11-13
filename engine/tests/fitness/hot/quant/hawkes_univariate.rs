//! Univariate exponential Hawkes: the O(1) recursion must equal the direct kernel sum it replaces,
//! and the closed-form readouts must match hand arithmetic. The recursion is the only intensity the
//! engine ever computes, so a drift from the definition is invisible at run time.

use super::ts;
use engine::hot::quant::hawkes::{HawkesEvents, HawkesParams, UnivariateHawkes};

#[test]
fn recursion_matches_the_direct_kernel_sum_and_relaxes_to_baseline() {
    let params = HawkesParams::new(0.4, 1.2, 2.5);
    let times = [0.0, 0.3, 0.35, 1.9, 4.2];
    let mut hawkes = UnivariateHawkes::new(params);
    let mut events = HawkesEvents::new(16);
    for &secs in &times {
        hawkes.on_event(ts(secs));
        events.push(ts(secs));
    }

    let now = 5.0;
    let direct = params.mu_per_sec()
        + params.alpha_per_sec()
            * times
                .iter()
                .map(|time| (-params.beta_per_sec() * (now - time)).exp())
                .sum::<f64>();
    let recursive = hawkes.intensity(ts(now));
    assert!(
        (recursive - direct).abs() < 1e-12,
        "recursion {recursive} vs direct {direct}"
    );

    assert!((hawkes.intensity(ts(65.0)) - params.mu_per_sec()).abs() < 1e-9);

    let mut reseeded = UnivariateHawkes::new(params);
    reseeded.reseed_from(&events);
    assert!((reseeded.intensity(ts(now)) - direct).abs() < 1e-12);
}

#[test]
fn known_worked_example_reads_back_from_the_accessors() {
    let params = HawkesParams::new(2.0, 1.5, 4.0);
    assert!((params.branching_ratio() - 0.375).abs() < 1e-12);
    assert!((params.half_life_secs() - 0.1733).abs() < 1e-4);
}

#[test]
fn a_late_stamp_reads_the_intensity_of_a_reseed() {
    let params = HawkesParams::new(0.5, 0.8, 2.0);
    let mut live = UnivariateHawkes::new(params);
    let mut events = HawkesEvents::new(16);
    for secs in [10.0, 10.5, 10.2] {
        live.on_event(ts(secs));
        events.push(ts(secs));
    }
    assert_eq!(events.out_of_order_count(), 1, "the window clamps 10.2 s");

    let now = ts(11.0);
    let mut reseeded = UnivariateHawkes::new(params);
    reseeded.reseed_from(&events);
    let reseed = reseeded.intensity(now);
    let late = live.intensity(now);
    assert!(
        ((late - reseed) / reseed).abs() < 1e-12,
        "live {late} vs reseed {reseed}"
    );

    // The window holds the late event at 10.5 s, so its kernel decays once, from there.
    let direct = params.mu_per_sec()
        + params.alpha_per_sec()
            * [10.0, 10.5, 10.5]
                .iter()
                .map(|time| (-params.beta_per_sec() * (11.0 - time)).exp())
                .sum::<f64>();
    assert!(
        ((late - direct) / direct).abs() < 1e-12,
        "live {late} vs direct {direct}"
    );
}
