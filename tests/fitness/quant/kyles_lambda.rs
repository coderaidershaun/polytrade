//! Kyle's lambda regression: the slope a research column reports as price impact. A silently wrong
//! slope, or an infinity leaking out of a degenerate window, poisons every downstream sizing model
//! without ever failing a run.

use super::Lcg;
use polysim::hot::quant::liquidity::{KylesLambda, KylesLambdaSpec};
use polysim::ids::Price;

const TICK: Price = Price(1_000_000); // $0.01 at the 1e-8 fixed-point scale.

fn approx(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol
}

fn spec(window: usize, min_observations: usize) -> KylesLambdaSpec {
    KylesLambdaSpec {
        window,
        min_observations,
        ..KylesLambdaSpec::default()
    }
}

fn estimator_over(spec: KylesLambdaSpec, bars: &[(f64, f64)]) -> KylesLambda {
    let mut estimator = KylesLambda::new(spec, TICK);
    for &(flow, mid_change) in bars {
        estimator.push(flow, mid_change);
    }
    estimator
}

#[test]
fn slope_recovery_matches_a_known_worked_example() {
    // λ = 0.0002 price/contract, τ = 0.01 -> λ_tick = 0.02 ticks/contract, Q_1tick = 50 contracts.
    const LAMBDA: f64 = 0.0002;
    const ALPHA: f64 = 5e-5;

    let mut lcg = Lcg(0xC0FF_EE01);
    let bars: Vec<(f64, f64)> = (0..200)
        .map(|_| {
            let flow = (lcg.unit() - 0.5) * 200.0;
            let noise = (lcg.unit() - 0.5) * 2e-4;
            (flow, ALPHA + LAMBDA * flow + noise)
        })
        .collect();

    let estimate = estimator_over(spec(200, 30), &bars)
        .fit()
        .expect("well-conditioned window");
    assert!(
        approx(estimate.lambda, LAMBDA, 0.05 * LAMBDA),
        "lambda {}",
        estimate.lambda
    );
    assert!(
        approx(estimate.lambda_tick, 0.02, 1e-3),
        "lambda_tick {}",
        estimate.lambda_tick
    );
    let one_tick_flow = estimate.one_tick_flow.expect("positive slope inverts");
    assert!(
        approx(one_tick_flow, 50.0, 2.5),
        "one_tick_flow {one_tick_flow}"
    );
    assert!(
        approx(estimate.intercept, ALPHA, 2e-5),
        "intercept {}",
        estimate.intercept
    );

    // Mirrored input (same flow, negated response) gives a negated lambda, and the inversion is
    // refused.
    let mirrored: Vec<(f64, f64)> = bars
        .iter()
        .map(|&(flow, mid_change)| (flow, -mid_change))
        .collect();
    let falling = estimator_over(spec(200, 30), &mirrored)
        .fit()
        .expect("mirrored window");
    assert!(
        approx(falling.lambda, -estimate.lambda, 1e-9),
        "lambda {}",
        falling.lambda
    );
    assert_eq!(
        falling.one_tick_flow, None,
        "a negative slope must never invert"
    );
}

#[test]
fn overflowed_sums_and_subnormal_slopes_never_leak_infinity() {
    // Flow magnitudes overflow the centred sums, so the window is garbage and no estimate is given.
    let overflowing: Vec<(f64, f64)> = [1e160, -1e160, 1e160, -1e160]
        .iter()
        .map(|&flow| (flow, 1e-162 * flow))
        .collect();
    assert!(
        estimator_over(spec(64, 2), &overflowing).fit().is_none(),
        "an overflowed window produced an estimate"
    );

    // A subnormal positive λ is reportable, but its reciprocal overflows, so the inversion is
    // refused.
    let faint = [
        (10.0, 5e-311),
        (-10.0, -5e-311),
        (10.0, 5e-311),
        (-10.0, -5e-311),
    ];
    let estimate = estimator_over(spec(64, 2), &faint)
        .fit()
        .expect("a faint but conditioned window still fits");
    assert!(estimate.lambda > 0.0, "lambda {}", estimate.lambda);
    assert_eq!(
        estimate.one_tick_flow, None,
        "a subnormal slope inverted to infinite depth"
    );
}
