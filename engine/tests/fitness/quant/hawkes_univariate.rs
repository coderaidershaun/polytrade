//! Univariate exponential Hawkes: the O(1) recursion must equal the direct kernel sum it replaces,
//! and the closed-form readouts must match hand arithmetic. The recursion is the only intensity the
//! engine ever computes, so a drift from the definition is invisible at run time.

use super::ts;
use polysim::hot::quant::hawkes::{HawkesEvents, HawkesParams, UnivariateHawkes};

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
    let direct = params.mu
        + params.alpha
            * times
                .iter()
                .map(|time| (-params.beta * (now - time)).exp())
                .sum::<f64>();
    let recursive = hawkes.intensity(ts(now));
    assert!(
        (recursive - direct).abs() < 1e-12,
        "recursion {recursive} vs direct {direct}"
    );

    assert!((hawkes.intensity(ts(65.0)) - params.mu).abs() < 1e-9);

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
