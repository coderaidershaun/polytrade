//! A bump rule that drifts from the closed forms, or that mixes the up side of a move with the down
//! side, hedges a book against the wrong move, and nothing complains.

use std::f64::consts::PI;

use engine::hot::quant::options::{
    AccountDeltas, Black76, Bumps, OptionRight, OptionsError, SecondOrderGreeks, Shift,
    account_deltas, greeks_by_bump, second_order_by_bump,
};

use OptionRight::{Call, Put};

const VOLATILITY: f64 = 0.5;

/// Strike and right of the five options, each in the reference market.
const FIVE_OPTIONS: [(f64, OptionRight); 5] = [
    (100_000.0, Call),
    (110_000.0, Call),
    (90_000.0, Put),
    (125_000.0, Call),
    (80_000.0, Put),
];

const SMALL_BUMPS: Bumps = Bumps {
    near_move: 100.0,
    far_move: 200.0,
    volatility: 0.001,
    day: 0.00001,
};

fn reference_market() -> Black76 {
    Black76 {
        forward: 100_000.0,
        strike: 100_000.0,
        years: 30.0 / 365.0,
        discount: 1.0,
    }
}

fn relative_gap(value: f64, expected: f64) -> f64 {
    ((value - expected) / expected).abs()
}

fn assert_within(name: &str, value: f64, expected: f64, relative: f64) {
    let gap = relative_gap(value, expected);
    assert!(
        gap <= relative,
        "{name} {value} is {gap:e} away from {expected}"
    );
}

/// The seven second-order greeks in closed form, for discount 1, in the units of
/// `SecondOrderGreeks`: charm, color and vega decay for one day of 365.
fn closed_second_order(option: Black76, volatility: f64) -> SecondOrderGreeks {
    let Black76 {
        forward,
        strike,
        years,
        ..
    } = option;
    let deviation = volatility * years.sqrt();
    let d1 = ((forward / strike).ln() + 0.5 * deviation * deviation) / deviation;
    let d2 = d1 - deviation;
    let density = (-0.5 * d1 * d1).exp() / (2.0 * PI).sqrt();
    let gamma = density / (forward * deviation);
    let vega = forward * density * years.sqrt();
    SecondOrderGreeks {
        vanna_per_point: -density * d2 / volatility / 100.0,
        volga_per_point_squared: vega * d1 * d2 / volatility / 1e4,
        charm_per_day: density * d2 / (2.0 * years) / 365.0,
        speed: -gamma / forward * (1.0 + d1 / deviation),
        color_per_day: gamma * (1.0 - d1 * d2) / (2.0 * years) / 365.0,
        zomma_per_point: gamma * (d1 * d2 - 1.0) / volatility / 100.0,
        vega_decay_per_day: -vega / 100.0 * (1.0 + d1 * d2) / (2.0 * years) / 365.0,
    }
}

/// Carry, discount rate and elasticity of a call by central differences of its price, each for a
/// rate or a move of 1.
fn central_differences(option: Black76) -> (f64, f64, f64) {
    let step = 1e-5;
    let Black76 {
        forward,
        years,
        discount,
        ..
    } = option;
    let at_forward = |forward| {
        let moved = Black76 { forward, ..option };
        moved.price(Call, VOLATILITY).unwrap()
    };
    let at_discount = |discount| {
        let moved = Black76 { discount, ..option };
        moved.price(Call, VOLATILITY).unwrap()
    };
    let carry = (at_forward(forward * (1.0 + step * years))
        - at_forward(forward * (1.0 - step * years)))
        / (2.0 * step);
    let discount_rate = (at_discount(discount * (1.0 - step * years))
        - at_discount(discount * (1.0 + step * years)))
        / (2.0 * step);
    let price = option.price(Call, VOLATILITY).unwrap();
    let elasticity = (at_forward(forward * (1.0 + step)) - at_forward(forward * (1.0 - step)))
        / (2.0 * step * price);
    (carry, discount_rate, elasticity)
}

#[test]
fn closed_form_greeks_agree_with_bumps() {
    for (strike, right) in FIVE_OPTIONS {
        let option = Black76 {
            strike,
            ..reference_market()
        };
        let closed = option.greeks(right, VOLATILITY).unwrap();
        let bumped = greeks_by_bump(
            |shift| option.shifted_price(right, VOLATILITY, shift),
            SMALL_BUMPS,
        )
        .unwrap();
        let mean_gamma = 0.5 * (bumped.up_gamma + bumped.down_gamma);
        let theta_per_day = bumped.theta_per_day / (SMALL_BUMPS.day * 365.0);
        assert_within("delta", bumped.delta, closed.delta, 1e-3);
        assert_within("mean gamma", mean_gamma, closed.gamma, 1e-3);
        assert_within("up gamma", bumped.up_gamma, closed.gamma, 5e-2);
        assert_within("down gamma", bumped.down_gamma, closed.gamma, 5e-2);
        assert_within(
            "vega_per_point",
            bumped.vega_per_point,
            closed.vega_per_point,
            1e-3,
        );
        assert_within("theta_per_day", theta_per_day, closed.theta_per_day, 1e-3);
    }

    let discounted = Black76 {
        discount: 0.99,
        ..reference_market()
    };
    let greeks = discounted.greeks(Call, VOLATILITY).unwrap();
    let Black76 { forward, years, .. } = discounted;
    let (carry, discount_rate, elasticity) = central_differences(discounted);
    assert_within("carry", carry, greeks.delta * forward * years, 1e-6);
    assert_within("discount rate", discount_rate, -years * greeks.price, 1e-9);
    assert_within(
        "elasticity",
        elasticity,
        greeks.delta * forward / greeks.price,
        1e-6,
    );

    let (carry, discount_rate, _) = central_differences(reference_market());
    let carry_for_one_percent = carry * 0.01;
    let discount_rate_for_one_percent = discount_rate * 0.01;
    assert!(
        (carry_for_one_percent - 43.0).abs() <= 0.5,
        "carry for 1% {carry_for_one_percent}"
    );
    assert!(
        (discount_rate_for_one_percent + 4.7).abs() <= 0.05,
        "discount rate for 1% {discount_rate_for_one_percent}"
    );
}

#[test]
fn second_order_bumps_agree_with_closed_forms() {
    let per_day = SMALL_BUMPS.day * 365.0;
    for (strike, right) in FIVE_OPTIONS {
        let option = Black76 {
            strike,
            ..reference_market()
        };
        let closed = closed_second_order(option, VOLATILITY);
        let bumped = second_order_by_bump(
            |shift| option.shifted_price(right, VOLATILITY, shift),
            SMALL_BUMPS,
        )
        .unwrap();
        let rows = [
            ("vanna", bumped.vanna_per_point, closed.vanna_per_point),
            (
                "volga",
                bumped.volga_per_point_squared,
                closed.volga_per_point_squared,
            ),
            (
                "charm",
                bumped.charm_per_day / per_day,
                closed.charm_per_day,
            ),
            ("speed", bumped.speed, closed.speed),
            (
                "color",
                bumped.color_per_day / per_day,
                closed.color_per_day,
            ),
            ("zomma", bumped.zomma_per_point, closed.zomma_per_point),
            (
                "vega decay",
                bumped.vega_decay_per_day / per_day,
                closed.vega_decay_per_day,
            ),
        ];
        for (name, value, expected) in rows {
            assert_within(name, value, expected, 1e-3);
        }
    }

    let call = Black76 {
        strike: 110_000.0,
        ..reference_market()
    };
    let bumped = second_order_by_bump(
        |shift| call.shifted_price(Call, VOLATILITY, shift),
        SMALL_BUMPS,
    )
    .unwrap();
    let manual = [
        ("vanna", bumped.vanna_per_point, 0.0049, 0.00005),
        ("volga", bumped.volga_per_point_squared, 0.84, 0.005),
        ("charm", bumped.charm_per_day / per_day, -0.0041, 0.00005),
    ];
    for (name, value, expected, tolerance) in manual {
        assert!(
            (value - expected).abs() <= tolerance,
            "{name} {value} against the manual's {expected}"
        );
    }
}

#[test]
fn up_gamma_and_down_gamma_keep_their_signs_apart() {
    let long_call = Black76 {
        strike: 110_000.0,
        ..reference_market()
    };
    let short_put = Black76 {
        strike: 90_000.0,
        ..reference_market()
    };
    let risk_reversal = |shift: Shift| -> Result<f64, OptionsError> {
        let call = long_call.shifted_price(Call, VOLATILITY, shift)?;
        let put = short_put.shifted_price(Put, VOLATILITY, shift)?;
        Ok(call - put)
    };

    let wide = Bumps {
        near_move: 2500.0,
        far_move: 5000.0,
        volatility: 0.05,
        day: 1.0 / 365.0,
    };
    let greeks = greeks_by_bump(risk_reversal, wide).unwrap();
    assert_within("up gamma", greeks.up_gamma, 7.960007401e-06, 1e-8);
    assert_within("down gamma", greeks.down_gamma, -2.202631278e-06, 1e-8);
    assert_within("delta", greeks.delta, 0.488583629, 1e-8);

    let forward = reference_market().forward;
    let hour_and_day = Bumps {
        near_move: forward * VOLATILITY / (365.0f64 * 24.0).sqrt(),
        far_move: forward * VOLATILITY / 365.0f64.sqrt(),
        ..wide
    };
    let greeks = greeks_by_bump(risk_reversal, hour_and_day).unwrap();
    assert_within("up gamma", greeks.up_gamma, 5.328603944e-06, 1e-8);
    assert_within("down gamma", greeks.down_gamma, 1.006339505e-06, 1e-8);
}

#[test]
fn account_deltas_reproduce_both_currencies() {
    let spot = 100_000.0;
    let bump = 1.0;
    let assert_pair = |name: &str, deltas: AccountDeltas, coin: f64, usd: f64| {
        assert!(
            (deltas.coin - coin).abs() <= 1e-8 && (deltas.usd - usd).abs() <= 1e-8,
            "{name}: {deltas:?} against coin {coin}, usd {usd}"
        );
    };

    let coin_cash = account_deltas(|_| Ok(2.0), spot, bump).unwrap();
    assert_pair("coin cash", coin_cash, 0.0, 2.0);
    let stablecoin_cash = account_deltas(|spot| Ok(50_000.0 / spot), spot, bump).unwrap();
    assert_pair("stablecoin cash", stablecoin_cash, -0.5, 0.0);
    let linear_future =
        account_deltas(|spot| Ok(3.0 * (1.0 - 90_000.0 / spot)), spot, bump).unwrap();
    assert_pair("linear future", linear_future, 2.7, 3.0);
    let inverse_future = account_deltas(
        |spot| Ok(200_000.0 * (1.0 / 80_000.0 - 1.0 / spot)),
        spot,
        bump,
    )
    .unwrap();
    assert_pair("inverse future", inverse_future, 2.0, 2.5);

    let reference = reference_market();
    let call_in_coin = |spot: f64| -> Result<f64, OptionsError> {
        let moved = Black76 {
            forward: spot,
            ..reference
        };
        Ok(moved.price(Call, VOLATILITY)? / spot)
    };
    let call = account_deltas(call_in_coin, spot, bump).unwrap();
    let coin_delta = reference.coin_delta(Call, VOLATILITY).unwrap();
    let delta = reference.greeks(Call, VOLATILITY).unwrap().delta;
    assert_pair("reference call", call, coin_delta, delta);
}
