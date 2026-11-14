//! The Kyle's lambda slope that a research column reports as price impact: a wrong slope, or an
//! infinity from a degenerate window, fails no run.

use super::Lcg;
use engine::hot::quant::liquidity::{KylesLambda, KylesLambdaSpec};
use engine::msg::ids::Price;

const TICK: Price = Price::from_mantissa(1_000_000); // $0.01 at the 1e-8 fixed-point scale.

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
    let mut estimator = KylesLambda::new(spec);
    for &(flow, mid_change) in bars {
        estimator.push(flow, mid_change);
    }
    estimator
}

// λ = 0.0002 price/contract, τ = 0.01 -> λ_tick = 0.02 ticks/contract, Q_1tick = 50 contracts.
const LAMBDA: f64 = 0.0002;
const ALPHA: f64 = 5e-5;

/// Bars around the line ALPHA + slope · flow, with two-sided flow and light noise.
fn impact_bars(seed: u64, count: usize, slope: f64) -> Vec<(f64, f64)> {
    let mut lcg = Lcg(seed);
    (0..count)
        .map(|_| {
            let flow = (lcg.unit() - 0.5) * 200.0;
            let noise = (lcg.unit() - 0.5) * 2e-4;
            (flow, ALPHA + slope * flow + noise)
        })
        .collect()
}

#[test]
fn slope_recovery_matches_a_known_worked_example() {
    let bars = impact_bars(0xC0FF_EE01, 200, LAMBDA);

    let estimate = estimator_over(spec(200, 30), &bars)
        .fit()
        .expect("well-conditioned window");
    assert!(
        approx(estimate.lambda, LAMBDA, 0.05 * LAMBDA),
        "lambda {}",
        estimate.lambda
    );
    let in_ticks = estimate
        .per_unit(TICK.to_f64())
        .expect("a slope of 0.0002 reads in ticks");
    assert!(
        approx(in_ticks.lambda, 0.02, 1e-3),
        "lambda in ticks {}",
        in_ticks.lambda
    );
    let one_tick_flow = in_ticks.one_unit_flow.expect("positive slope inverts");
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
        falling
            .per_unit(TICK.to_f64())
            .expect("a falling slope reads in ticks")
            .one_unit_flow,
        None,
        "a negative slope must never invert"
    );

    // Only the last `window` bars fit: a full window of the opposite, doubled slope is pushed out
    // by a window of the worked example, which then fits as if it were alone.
    let second = impact_bars(0xBCDE, 64, LAMBDA);
    let mut evicted = estimator_over(spec(64, 30), &impact_bars(0xABCD, 64, -2.0 * LAMBDA));
    for &(flow, mid_change) in &second {
        evicted.push(flow, mid_change);
    }
    let evicted = evicted
        .fit()
        .expect("the second window is well conditioned");
    assert!(
        approx(evicted.lambda, LAMBDA, 0.05 * LAMBDA),
        "lambda {} after the first window was pushed out",
        evicted.lambda
    );
    assert_eq!(
        Some(evicted),
        estimator_over(spec(64, 30), &second).fit(),
        "a bar pushed out of the window still weighs in the fit"
    );
}

/// The data gates refuse a window that cannot carry a slope. Each is placed exactly: one bar, one
/// seller or one float step short of its edge it refuses, and at its edge it admits.
#[test]
fn each_data_gate_refuses_below_its_edge_and_admits_at_it() {
    // Count: four bars refuse a floor of five, and the fifth admits.
    let line: Vec<(f64, f64)> = [1.0, -1.0, 2.0, -2.0, 3.0]
        .iter()
        .map(|&flow| (flow, 0.1 * flow))
        .collect();
    assert_eq!(
        estimator_over(spec(64, 5), &line[..4]).fit(),
        None,
        "four bars passed a floor of five observations"
    );
    let counted = estimator_over(spec(64, 5), &line)
        .fit()
        .expect("five bars meet a floor of five observations");
    assert_eq!(counted.observations, 5);
    assert!(
        approx(counted.lambda, 0.1, 1e-12),
        "lambda {}",
        counted.lambda
    );

    // Sign balance: under a floor of 0.2, one seller in ten bars refuses and two admit.
    let with_sellers = |sellers: usize| -> Vec<(f64, f64)> {
        (0..10)
            .map(|index| {
                let size = 1.0 + index as f64;
                let flow = if index < sellers { -size } else { size };
                (flow, 0.1 * flow)
            })
            .collect()
    };
    let balanced_spec = KylesLambdaSpec {
        min_sign_fraction: 0.2,
        ..spec(64, 2)
    };
    assert_eq!(
        estimator_over(balanced_spec, &with_sellers(1)).fit(),
        None,
        "one seller in ten bars passed a sign floor of 0.2"
    );
    let balanced = estimator_over(balanced_spec, &with_sellers(2))
        .fit()
        .expect("two sellers in ten bars meet a sign floor of 0.2");
    assert_eq!(balanced.negative_fraction, 0.2);

    // Variance floor: a flow of ±0.5 has a variance of exactly 0.25, four centred squares of 0.25
    // over four bars. A floor of 0.25 admits it, and the next float above the floor refuses it.
    let halves = [(0.5, 0.25), (-0.5, -0.5), (0.5, 0.75), (-0.5, 0.0)];
    let floored = estimator_over(
        KylesLambdaSpec {
            min_flow_variance: 0.25,
            ..spec(64, 2)
        },
        &halves,
    )
    .fit()
    .expect("a flow variance at its floor is admitted");
    assert_eq!(floored.flow_variance, 0.25);
    let above_floor = KylesLambdaSpec {
        min_flow_variance: f64::from_bits(0.25f64.to_bits() + 1),
        ..spec(64, 2)
    };
    assert_eq!(
        estimator_over(above_floor, &halves).fit(),
        None,
        "a flow variance below its floor passed"
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
        estimate
            .per_unit(TICK.to_f64())
            .expect("a faint slope reads in ticks")
            .one_unit_flow,
        None,
        "a subnormal slope inverted to infinite depth"
    );

    // A steep but finite slope still fits. Its reading in ticks would pass the largest float, so
    // only that reading is refused. The two points (1, d) and (-1, -d) give the exact slope
    // 2d / 2 = d, and d = 1e307 over a tick of 0.01 is 1e309.
    let steep = [(1.0, 1e307), (-1.0, -1e307)];
    let estimate = estimator_over(spec(64, 2), &steep)
        .fit()
        .expect("a finite slope fits, however steep");
    assert_eq!(estimate.lambda, 1e307);
    assert_eq!(estimate.intercept, 0.0);
    assert_eq!(
        estimate.per_unit(TICK.to_f64()),
        None,
        "a reading in ticks past the largest float was given"
    );
}
