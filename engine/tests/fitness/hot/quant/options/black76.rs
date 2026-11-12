//! Black-76 pins. A price or greek that drifts from its identities or its reference numbers still
//! looks like a plausible number, so every risk check and hedge would act on it without complaint.

use engine::hot::quant::options::{Black76, CoinParity, OptionRight};
use proptest::prelude::*;

use OptionRight::{Call, Put};

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

fn intrinsic_value(option: Black76, right: OptionRight) -> f64 {
    let Black76 {
        forward,
        strike,
        discount,
        ..
    } = option;
    match right {
        Call if forward > strike => discount * (forward - strike),
        Put if strike > forward => discount * (strike - forward),
        _ => 0.0,
    }
}

fn right() -> impl Strategy<Value = OptionRight> {
    prop_oneof![Just(Call), Just(Put)]
}

/// Forward 0.001 to 1e6. Deviation `volatility × √years` 0.01 to 3, with years 0.0001 to 5.
/// `ln(forward / strike)` within four deviations. Discount 0.5 to 1.
fn market() -> impl Strategy<Value = (Black76, f64)> {
    (
        -3.0f64..6.0,
        0.0001f64..5.0,
        0.01f64..3.0,
        -4.0f64..4.0,
        0.5f64..1.0,
    )
        .prop_map(|(forward_power, years, deviation, moneyness, discount)| {
            let forward = 10f64.powf(forward_power);
            let strike = forward * (-moneyness * deviation).exp();
            let option = Black76 {
                forward,
                strike,
                years,
                discount,
            };
            (option, deviation / years.sqrt())
        })
}

/// Forward, strike, years, volatility, discount, right, price.
// The digits stay as the references print them, so that each row can be matched to its source by eye.
#[allow(clippy::excessive_precision)]
const SIXTY_DIGIT_PRICES: [(f64, f64, f64, f64, f64, OptionRight, f64); 9] = [
    (100000.0, 100000.0, 0.25, 0.5, 1.0, Call, 9947.6449660225786),
    (100000.0, 150000.0, 0.25, 0.5, 1.0, Call, 671.86257620080932),
    (100000.0, 250000.0, 0.25, 0.5, 1.0, Call, 1.1754412575559254),
    (100000.0, 60000.0, 0.25, 0.5, 1.0, Put, 145.39605105062667),
    (
        100000.0,
        30000.0,
        0.25,
        0.5,
        1.0,
        Put,
        0.0019194917647164102,
    ),
    (
        100000.0,
        101000.0,
        0.0025,
        0.4,
        1.0,
        Call,
        399.09830409690726,
    ),
    (3000.0, 2500.0, 1.0, 0.8, 0.95, Put, 594.41232342226945),
    (3000.0, 9000.0, 2.0, 1.5, 0.9, Call, 1443.6820789955837),
    (
        0.00001,
        0.000012,
        0.5,
        0.6,
        1.0,
        Call,
        0.0000010129352470963252,
    ),
];

/// Forward, strike, years, volatility, discount, coin delta of the call.
// The digits stay as the references print them, so that each row can be matched to its source by eye.
#[allow(clippy::excessive_precision)]
const SIXTY_DIGIT_COIN_DELTAS: [(f64, f64, f64, f64, f64, f64); 3] = [
    (100000.0, 150000.0, 0.25, 0.5, 1.0, 0.060496163064018300),
    (100000.0, 60000.0, 0.25, 0.5, 1.0, 0.58347820012526280),
    (3000.0, 9000.0, 2.0, 1.5, 0.9, 0.15449276881781376),
];

/// Strike and right in the reference market, then delta, coin price and coin delta as the manual
/// prints them.
const COIN_DELTA_ROWS: [(f64, OptionRight, [f64; 3]); 4] = [
    (100_000.0, Call, [0.529, 0.0571, 0.471]),
    (110_000.0, Call, [0.277, 0.0228, 0.254]),
    (90_000.0, Put, [-0.210, 0.0183, -0.228]),
    (60_000.0, Call, [1.000, 0.4000, 0.600]),
];

proptest! {
    #[test]
    fn parity_holds_in_usd_and_in_coin((option, volatility) in market()) {
        let Black76 { forward, strike, discount, .. } = option;
        let call = option.price(Call, volatility)?;
        let put = option.price(Put, volatility)?;
        let usd_gap = call - put - discount * (forward - strike);
        prop_assert!(usd_gap.abs() <= 1e-12 * discount * (forward + strike), "usd gap {usd_gap}");

        let call_coin = option.coin_price(Call, volatility)?;
        let put_coin = option.coin_price(Put, volatility)?;
        let strike_in_forwards = strike / forward;
        let coin_gap = call_coin - put_coin - discount * (1.0 - strike_in_forwards);
        prop_assert!(
            coin_gap.abs() <= 1e-12 * discount * (1.0 + strike_in_forwards),
            "coin gap {coin_gap}"
        );

        let parity = CoinParity { call: call_coin, put: put_coin, strike, discount };
        let parity_forward = parity.forward()?;
        prop_assert!(
            relative_gap(parity_forward, forward) <= 1e-9,
            "forward {parity_forward} from parity against {forward}"
        );
    }

    #[test]
    fn call_equals_the_flipped_put((option, volatility) in market()) {
        let flipped = Black76 { forward: 1.0 / option.forward, strike: 1.0 / option.strike, ..option };
        let call = option.price(Call, volatility)?;
        let flipped_put = option.forward * option.strike * flipped.price(Put, volatility)?;
        prop_assert!(relative_gap(flipped_put, call) <= 1e-9, "call {call}, flipped put {flipped_put}");
    }

    #[test]
    fn price_stays_in_bounds_and_rises_with_volatility(
        (option, volatility) in market(),
        right in right(),
        rise in 0.0f64..1.0,
    ) {
        let Black76 { forward, strike, years, discount } = option;
        let slack = 1e-12 * discount * (forward + strike);
        let intrinsic = intrinsic_value(option, right);
        let ceiling = match right {
            Call => discount * forward,
            Put => discount * strike,
        };
        let price = option.price(right, volatility)?;
        prop_assert!(price >= intrinsic - slack, "price {price} below intrinsic value {intrinsic}");
        prop_assert!(price <= ceiling + slack, "price {price} above its ceiling {ceiling}");

        let deviation = volatility * years.sqrt();
        let higher_volatility = (deviation + rise * (3.0 - deviation)) / years.sqrt();
        let higher_price = option.price(right, higher_volatility)?;
        prop_assert!(
            higher_price >= price - slack,
            "price {higher_price} at volatility {higher_volatility} below {price} at {volatility}"
        );

        prop_assert_eq!(option.price(right, 0.0)?, intrinsic);
        prop_assert_eq!(Black76 { years: 0.0, ..option }.price(right, volatility)?, intrinsic);
    }

    #[test]
    fn gamma_vega_theta_are_one_quantity((option, volatility) in market()) {
        let Black76 { forward, years, discount, .. } = option;
        let call = option.greeks(Call, volatility)?;
        let put = option.greeks(Put, volatility)?;
        let cash_gamma = forward * forward * call.gamma;
        prop_assert!(relative_gap(call.vega_per_point * 100.0, volatility * years * cash_gamma) <= 1e-12);
        prop_assert!(
            relative_gap(call.theta_per_day * 365.0, -0.5 * volatility * volatility * cash_gamma) <= 1e-12
        );
        prop_assert!(relative_gap(put.gamma, call.gamma) <= 1e-12);
        prop_assert!(relative_gap(put.vega_per_point, call.vega_per_point) <= 1e-12);
        prop_assert!(relative_gap(put.theta_per_day, call.theta_per_day) <= 1e-12);
        prop_assert!(relative_gap(call.delta - put.delta, discount) <= 1e-12);
    }
}

#[test]
fn reference_option_matches_the_manual() {
    let option = reference_market();
    let greeks = option.greeks(Call, 0.5).unwrap();
    let rows = [
        ("price", greeks.price, 5713.77, 0.005),
        ("delta", greeks.delta, 0.5286, 0.00005),
        ("vega_per_point", greeks.vega_per_point, 114.08, 0.005),
        ("theta_per_day", greeks.theta_per_day, -95.07, 0.005),
        (
            "coin_price",
            option.coin_price(Call, 0.5).unwrap(),
            0.0571,
            0.00005,
        ),
        (
            "gamma for a move of 1%",
            greeks.gamma * option.forward / 100.0,
            0.0278,
            0.00005,
        ),
        (
            "coin_delta",
            option.coin_delta(Call, 0.5).unwrap(),
            0.471,
            0.0005,
        ),
    ];
    for (name, value, expected, tolerance) in rows {
        assert!(
            (value - expected).abs() <= tolerance,
            "{name} {value} against the manual's {expected}"
        );
    }
}

#[test]
fn coin_delta_is_delta_less_coin_price() {
    for (strike, right, [delta, coin_price, coin_delta]) in COIN_DELTA_ROWS {
        let option = Black76 {
            strike,
            ..reference_market()
        };
        let greeks = option.greeks(right, 0.5).unwrap();
        let built = [
            (greeks.delta, delta, 0.0005),
            (option.coin_price(right, 0.5).unwrap(), coin_price, 0.00005),
            (option.coin_delta(right, 0.5).unwrap(), coin_delta, 0.0005),
        ];
        for (value, expected, tolerance) in built {
            assert!(
                (value - expected).abs() <= tolerance,
                "strike {strike}: {value} against the manual's {expected}"
            );
        }
    }

    proptest!(|((option, volatility) in market(), right in right())| {
        let greeks = option.greeks(right, volatility)?;
        let coin_delta = option.coin_delta(right, volatility)?;
        let gap = coin_delta - (greeks.delta - greeks.price / option.forward);
        let scale = option.discount * (1.0 + option.strike / option.forward);
        prop_assert!(gap.abs() <= 1e-12 * scale, "gap {gap}");
    });
}

#[test]
fn prices_match_references_of_sixty_digits() {
    for (forward, strike, years, volatility, discount, right, expected) in SIXTY_DIGIT_PRICES {
        let option = Black76 {
            forward,
            strike,
            years,
            discount,
        };
        let price = option.price(right, volatility).unwrap();
        let gap = relative_gap(price, expected);
        assert!(
            gap <= 1e-11,
            "price {price} at strike {strike} is {gap:e} away from {expected}"
        );
    }
    for (forward, strike, years, volatility, discount, expected) in SIXTY_DIGIT_COIN_DELTAS {
        let option = Black76 {
            forward,
            strike,
            years,
            discount,
        };
        let coin_delta = option.coin_delta(Call, volatility).unwrap();
        let gap = relative_gap(coin_delta, expected);
        assert!(
            gap <= 1e-11,
            "coin delta {coin_delta} at strike {strike} is {gap:e} away from {expected}"
        );
    }
}

#[test]
fn tight_call_spread_gives_the_strike_derivative() {
    let option = reference_market();
    let manual = [
        (0.5, 0.5, 0.471),
        (0.500003, 0.499997, 0.506),
        (0.499997, 0.500003, 0.437),
    ];
    for (low_volatility, high_volatility, expected) in manual {
        let digital = option
            .digital_from_call_spread(1.0, low_volatility, high_volatility)
            .unwrap();
        assert!(
            (digital - expected).abs() <= 0.0005,
            "digital {digital} against the manual's {expected}"
        );
    }

    let flat_digital = option.coin_delta(Call, 0.5).unwrap() * option.forward / option.strike;
    let vega_per_unit = option.greeks(Call, 0.5).unwrap().vega_per_point * 100.0;
    proptest!(|(slope in -1e-5f64..1e-5)| {
        let digital = option.digital_from_call_spread(1.0, 0.5 - slope, 0.5 + slope)?;
        let derivative = flat_digital - vega_per_unit * slope;
        prop_assert!((digital - derivative).abs() <= 1e-6, "digital {digital}, derivative {derivative}");
    });
}
