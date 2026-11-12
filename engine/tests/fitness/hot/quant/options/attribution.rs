//! A step that the greeks cannot explain hides a wrong greek, and the balance of gamma against
//! theta is what the pay of a hedged option rests on.

use engine::hot::quant::options::{
    Black76, Bumps, ExplainGreeks, GreekExplain, HedgedStep, OptionRight, Shift, greek_explain,
    second_order_by_bump,
};

use OptionRight::{Call, Put};

const VOLATILITY: f64 = 0.5;

fn reference_market() -> Black76 {
    Black76 {
        forward: 100_000.0,
        strike: 100_000.0,
        years: 30.0 / 365.0,
        discount: 1.0,
    }
}

/// The greek explain of one option of the reference market over one hour, with the move of the
/// test, and the change of its value by full revaluation.
fn explain_and_revaluation(strike: f64, right: OptionRight) -> (GreekExplain, f64) {
    let option = Black76 {
        strike,
        ..reference_market()
    };
    let bumps = Bumps {
        near_move: 100.0,
        far_move: 200.0,
        volatility: 0.001,
        day: 0.00001,
    };
    let shift = Shift {
        underlying: 100.0,
        volatility: 0.001,
        years: 1.0 / 8760.0,
    };
    let greeks = option.greeks(right, VOLATILITY).unwrap();
    let second = second_order_by_bump(
        |state| option.shifted_price(right, VOLATILITY, state),
        bumps,
    )
    .unwrap();
    let explain_greeks = ExplainGreeks {
        delta: greeks.delta,
        gamma: greeks.gamma,
        vega_per_point: greeks.vega_per_point,
        theta_per_day: greeks.theta_per_day,
        vanna_per_point: second.vanna_per_point,
        volga_per_point_squared: second.volga_per_point_squared,
    };
    let explain = greek_explain(explain_greeks, shift).unwrap();
    let revaluation = option.shifted_price(right, VOLATILITY, shift).unwrap()
        - option.price(right, VOLATILITY).unwrap();
    (explain, revaluation)
}

#[test]
fn gamma_theta_balances_at_the_break_even_move() {
    let price = reference_market().forward;
    let greeks = reference_market().greeks(Call, VOLATILITY).unwrap();
    let step = |step_return| HedgedStep {
        gamma: greeks.gamma,
        price,
        step_return,
        volatility: VOLATILITY,
        years: 1.0 / 365.0,
    };

    let break_even = VOLATILITY / 365.0f64.sqrt();
    let balanced = step(break_even).gamma_theta().unwrap();
    assert!(
        balanced.abs() <= 1e-9,
        "result at the break-even move {balanced}"
    );
    let move_in_usd = price * break_even;
    let gamma_pay = 0.5 * greeks.gamma * move_in_usd * move_in_usd;
    assert!((gamma_pay - 95.07).abs() <= 0.005, "gamma pay {gamma_pay}");
    assert!(
        (-greeks.theta_per_day - 95.07).abs() <= 0.005,
        "theta per day {}",
        greeks.theta_per_day
    );

    for (step_return, expected) in [(0.01, -81.186796), (-0.03, 29.850883)] {
        let result = step(step_return).gamma_theta().unwrap();
        assert!(
            (result - expected).abs() <= 1e-5,
            "step return {step_return} gives {result}, not {expected}"
        );
    }
}

#[test]
fn greek_explain_reconciles_with_full_revaluation() {
    for (strike, right) in [(100_000.0, Call), (110_000.0, Call), (90_000.0, Put)] {
        let (explain, revaluation) = explain_and_revaluation(strike, right);
        let gross = explain.delta.abs()
            + explain.gamma.abs()
            + explain.theta.abs()
            + explain.vega.abs()
            + explain.cross.abs();
        let total = explain.total().unwrap();
        assert!(
            (total - revaluation).abs() <= 0.002 * gross,
            "strike {strike}: explain {total} against revaluation {revaluation}"
        );
    }

    let (explain, _) = explain_and_revaluation(100_000.0, Call);
    let lines = [
        ("delta", explain.delta, 52.857),
        ("gamma", explain.gamma, 0.139),
        ("theta", explain.theta, -3.961),
        ("vega", explain.vega, 11.408),
        ("cross", explain.cross, 0.006),
    ];
    for (name, value, expected) in lines {
        assert!(
            (value - expected).abs() <= 0.0005,
            "{name} line {value} against {expected}"
        );
    }
}
