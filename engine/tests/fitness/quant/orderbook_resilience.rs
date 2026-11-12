//! Order-book resilience: the OU relaxation rate of mid toward equilibrium. Every gate here exists
//! to keep a NaN or a nonsense negative rate out of the research tape when the book is locked,
//! crossed, or the deviation is discretisation dust.

use polysim::hot::quant::micro::{OrderbookResilience, ResilienceSample};
use polysim::time::TsUs;

fn sample(ts_us: i64, mid: f64, equilibrium: f64, half_spread: f64) -> ResilienceSample {
    ResilienceSample {
        event_ts_us: TsUs::from_micros(ts_us),
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
