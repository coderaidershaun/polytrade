//! The rate at which the mid returns to its equilibrium. Each gate keeps a NaN or a negative rate
//! out of the research tape when the book is locked, crossed, or barely moves.

use engine::hot::quant::micro::{OrderbookResilience, ResilienceSample};
use engine::msg::time::LocalTs;

fn sample(ts_us: i64, mid: f64, equilibrium: f64, half_spread: f64) -> ResilienceSample {
    ResilienceSample {
        event_ts_us: LocalTs::from_micros(ts_us),
        mid,
        equilibrium,
        half_spread,
    }
}

#[test]
fn exponential_relaxation_recovers_the_rate() {
    let equilibrium = 100.0;
    let kappa: f64 = 2.0;
    let dt_secs: f64 = 1.0;
    let mut resilience = OrderbookResilience::new();

    assert!(
        resilience
            .on_sample(sample(0, equilibrium + 0.4, equilibrium, 1.0))
            .is_none()
    );
    let decayed = equilibrium + 0.4 * (-kappa * dt_secs).exp();
    let rate = resilience
        .on_sample(sample(1_000_000, decayed, equilibrium, 1.0))
        .expect("second sample yields a rate");
    assert!((rate - kappa).abs() < 1e-9, "recovered rate {rate}");
}

#[test]
fn locked_or_crossed_book_skips() {
    // Crossed book: ratio 0/0 = NaN.
    let mut resilience = OrderbookResilience::new();
    assert!(
        resilience
            .on_sample(sample(0, 100.0, 100.0, -0.5))
            .is_none()
    );
    assert!(
        resilience
            .on_sample(sample(1_000_000, 100.0, 100.0, -0.5))
            .is_none()
    );

    // Crossed book with deviation: interval rejected.
    let mut resilience = OrderbookResilience::new();
    assert!(
        resilience
            .on_sample(sample(0, 100.1, 100.0, -0.5))
            .is_none()
    );
    assert!(
        resilience
            .on_sample(sample(1_000_000, 100.05, 100.0, -0.5))
            .is_none()
    );
}

/// Each gate skips an interval that the model cannot score, so that no NaN, infinity or meaningless
/// rate reaches the research tape. A gate that let one through would fail no run.
#[test]
fn each_gate_skips_an_interval_it_cannot_score() {
    let equilibrium = 100.0;
    let kappa: f64 = 2.0;
    let decayed = equilibrium + 0.4 * (-kappa).exp();

    // A sample with a value that is not finite is skipped and does not replace the anchor: the
    // rate after it is measured over the full second from the first sample.
    let mut resilience = OrderbookResilience::new();
    assert!(
        resilience
            .on_sample(sample(0, 100.4, equilibrium, 1.0))
            .is_none()
    );
    for junk in [
        sample(500_000, f64::NAN, equilibrium, 1.0),
        sample(500_000, 100.2, f64::INFINITY, 1.0),
        sample(500_000, 100.2, equilibrium, f64::NAN),
    ] {
        assert!(resilience.on_sample(junk).is_none(), "{junk:?} was scored");
    }
    let rate = resilience
        .on_sample(sample(1_000_000, decayed, equilibrium, 1.0))
        .expect("the anchor survived the skipped samples");
    assert!(
        (rate - kappa).abs() < 1e-9,
        "rate {rate} after skipped samples"
    );

    // No time passed, or time ran back: no rate.
    let mut resilience = OrderbookResilience::new();
    assert!(
        resilience
            .on_sample(sample(1_000_000, 100.4, equilibrium, 1.0))
            .is_none()
    );
    assert!(
        resilience
            .on_sample(sample(1_000_000, 100.2, equilibrium, 1.0))
            .is_none()
    );
    assert!(
        resilience
            .on_sample(sample(500_000, 100.1, equilibrium, 1.0))
            .is_none()
    );

    // Dust floor: a deviation of exactly 5% of the half spread is noise of the tick grid and is
    // skipped. One float step above it is scored. The equilibrium is 0, so each deviation is
    // exact.
    let floor = 0.05;
    let mut resilience = OrderbookResilience::new();
    assert!(resilience.on_sample(sample(0, floor, 0.0, 1.0)).is_none());
    assert!(
        resilience
            .on_sample(sample(1_000_000, 0.04, 0.0, 1.0))
            .is_none()
    );
    let above_floor = f64::from_bits(floor.to_bits() + 1);
    let mut resilience = OrderbookResilience::new();
    assert!(
        resilience
            .on_sample(sample(0, above_floor, 0.0, 1.0))
            .is_none()
    );
    let rate = resilience
        .on_sample(sample(1_000_000, above_floor * (-kappa).exp(), 0.0, 1.0))
        .expect("a deviation above the dust floor is scored");
    assert!(
        (rate - kappa).abs() < 1e-9,
        "rate {rate} just above the dust floor"
    );

    // The mid reached or crossed the equilibrium: the model holds no such path, so no rate.
    for (reached, label) in [(equilibrium, "reached"), (99.9, "crossed")] {
        let mut resilience = OrderbookResilience::new();
        assert!(
            resilience
                .on_sample(sample(0, 100.4, equilibrium, 1.0))
                .is_none()
        );
        assert!(
            resilience
                .on_sample(sample(1_000_000, reached, equilibrium, 1.0))
                .is_none(),
            "the mid {label} the equilibrium and was scored"
        );
    }
}
