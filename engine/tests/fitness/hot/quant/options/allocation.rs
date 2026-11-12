//! The options maths runs on the hot thread, where an allocation in steady state breaks the
//! promise of a fixed cost for each message.

use std::hint::black_box;

use engine::hot::quant::options::{
    Black76, Bumps, CoinParity, ExplainGreeks, GreekExplain, HedgedStep, NoTradeBand, OptionRight,
    OptionsError, Pillar, Shift, account_deltas, forward_volatility, greek_explain, greeks_by_bump,
    largest_rise, lowest_butterfly, second_order_by_bump, volatility_between,
};

const ROUNDS: usize = 10_000;

fn is_ok<T>(result: Result<T, OptionsError>) -> usize {
    usize::from(black_box(result).is_ok())
}

#[test]
fn options_maths_allocates_nothing() {
    let option = Black76 {
        forward: 100_000.0,
        strike: 100_000.0,
        years: 30.0 / 365.0,
        discount: 1.0,
    };
    let expired = Black76 {
        years: 0.0,
        ..option
    };
    let parity = CoinParity {
        call: 0.0571,
        put: 0.0571,
        strike: 100_000.0,
        discount: 1.0,
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
    let near = Pillar {
        years: 90.0 / 365.0,
        volatility: 0.17,
    };
    let far = Pillar {
        years: 180.0 / 365.0,
        volatility: 0.155,
    };
    let falling = Pillar {
        volatility: 0.11,
        ..far
    };
    let strikes = [90_000.0, 100_000.0, 110_000.0];
    let calls = [11_827.693354, 5_713.767509, 2_275.433781];
    let raised = [11_827.693354, 7_713.767509, 2_275.433781];
    let before = [5_713.77, 2_275.43];
    let after = [5_600.00, 2_300.00];
    let band = NoTradeBand {
        cost: 0.0002,
        price: 100_000.0,
        gamma: 0.00278,
        risk_aversion: 0.00001,
    };
    let step = HedgedStep {
        gamma: 2.775941975323e-05,
        price: 100_000.0,
        step_return: 0.01,
        volatility: 0.5,
        years: 1.0 / 365.0,
    };
    let explain_greeks = ExplainGreeks {
        delta: 0.5286,
        gamma: 2.78e-5,
        vega_per_point: 114.08,
        theta_per_day: -95.07,
        vanna_per_point: 0.00057,
        volga_per_point_squared: -0.012,
    };
    let lines = GreekExplain {
        delta: 52.857,
        gamma: 0.139,
        theta: -3.961,
        vega: 11.408,
        cross: 0.006,
    };
    let refused = |_: Shift| -> Result<f64, OptionsError> {
        Err(OptionsError::NotFinite { output: "closure" })
    };

    let mut successes = 0;
    let allocations_before = crate::alloc_count();
    for _ in 0..ROUNDS {
        let volatility = black_box(0.5);
        let value = |state| option.shifted_price(OptionRight::Call, volatility, state);
        successes += is_ok(option.price(OptionRight::Call, volatility));
        successes += is_ok(option.coin_price(OptionRight::Put, volatility));
        successes += is_ok(option.greeks(OptionRight::Call, volatility));
        successes += is_ok(expired.greeks(OptionRight::Call, volatility));
        successes += is_ok(option.coin_delta(OptionRight::Put, volatility));
        successes += is_ok(option.digital_from_call_spread(1.0, volatility, volatility));
        successes += is_ok(parity.forward());
        successes += is_ok(option.implied_volatility(OptionRight::Call, 5_713.77));
        successes += is_ok(option.implied_volatility(OptionRight::Call, 100_000.0));
        successes += is_ok(greeks_by_bump(value, bumps));
        successes += is_ok(greeks_by_bump(refused, bumps));
        successes += is_ok(second_order_by_bump(value, bumps));
        successes += is_ok(account_deltas(|spot| Ok(50_000.0 / spot), 100_000.0, 1.0));
        successes += is_ok(option.shifted_price(OptionRight::Put, volatility, shift));
        successes += is_ok(forward_volatility(near, far));
        successes += is_ok(forward_volatility(near, falling));
        successes += is_ok(volatility_between(near, far, 135.0 / 365.0));
        successes += is_ok(lowest_butterfly(&strikes, &calls, 0.0));
        successes += is_ok(lowest_butterfly(&strikes, &raised, 0.0));
        successes += is_ok(largest_rise(&before, &after, 1.0));
        successes += is_ok(band.half_width());
        successes += is_ok(step.gamma_theta());
        successes += is_ok(greek_explain(explain_greeks, shift));
        successes += is_ok(lines.total());
    }
    let allocations_after = crate::alloc_count();

    assert_eq!(
        allocations_after, allocations_before,
        "the options maths allocated in {ROUNDS} rounds"
    );
    assert_eq!(
        successes,
        ROUNDS * 18,
        "each round gives 18 results and 6 refusals"
    );
}
