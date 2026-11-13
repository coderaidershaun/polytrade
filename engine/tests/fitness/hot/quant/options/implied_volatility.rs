//! A volatility that misses its price gives each mark, greek and hedge built on it a quiet error,
//! and a price with no root must say so with the bounds it missed.

use engine::hot::quant::options::{Black76, OptionRight, OptionsError};
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

fn right() -> impl Strategy<Value = OptionRight> {
    prop_oneof![Just(Call), Just(Put)]
}

/// Forward 0.001 to 1e6. Deviation `volatility × √years` 0.005 to 3. `ln(forward / strike)`
/// within three deviations. Discount 0.5 to 1. Years start at 0.001, so that each volatility stays
/// inside the bounds of the solve, 0.0001 to 100.
fn market() -> impl Strategy<Value = (Black76, f64)> {
    (
        -3.0f64..6.0,
        0.001f64..5.0,
        0.005f64..3.0,
        -3.0f64..3.0,
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

fn no_root_bounds(result: Result<f64, OptionsError>) -> (f64, f64, f64) {
    match result {
        Err(OptionsError::NoRoot { price, low, high }) => (price, low, high),
        other => panic!("expected no root, got {other:?}"),
    }
}

#[test]
fn price_and_implied_volatility_round_trip() {
    let reference = reference_market()
        .implied_volatility(Call, 5713.77)
        .unwrap();
    assert!(
        (reference - 0.5).abs() <= 1e-6,
        "reference call gives {reference}"
    );

    // Forward and discount from 6% of continuous interest over 70 days.
    let seventy_days = Black76 {
        forward: 98.830712169,
        strike: 100.0,
        years: 70.0 / 365.0,
        discount: 0.988559101,
    };
    let seventy_day_call = seventy_days.implied_volatility(Call, 5.00).unwrap();
    assert!(
        (seventy_day_call - 0.3240).abs() <= 0.00005,
        "call at 5.00 gives {seventy_day_call}"
    );

    let in_the_money = Black76 {
        forward: 110_000.0,
        strike: 100_000.0,
        ..reference_market()
    };
    let from_call = in_the_money.implied_volatility(Call, 12_000.0).unwrap();
    let from_put = in_the_money.implied_volatility(Put, 2_000.0).unwrap();
    assert!(
        relative_gap(from_call, from_put) <= 1e-12,
        "call gives {from_call}, put gives {from_put}"
    );
    for solved in [from_call, from_put] {
        assert!(
            (solved - 0.470895086).abs() <= 1e-8,
            "parity pair gives {solved}"
        );
    }

    proptest!(|((option, volatility) in market(), right in right())| {
        let price = option.price(right, volatility)?;
        let solved = option.implied_volatility(right, price)?;
        prop_assert!(
            relative_gap(solved, volatility) <= 1e-9,
            "price {price} of volatility {volatility} solves to {solved}"
        );
    });
}

#[test]
fn price_without_a_root_names_its_bounds() {
    let in_the_money = Black76 {
        forward: 105_000.0,
        ..reference_market()
    };
    let (price, low, high) = no_root_bounds(in_the_money.implied_volatility(Call, 4_000.0));
    assert_eq!(price, 4_000.0);
    assert!(relative_gap(low, 5_000.0) <= 1e-9, "low bound {low}");
    assert!(relative_gap(high, 105_000.0) <= 1e-9, "high bound {high}");

    let (price, low, high) = no_root_bounds(reference_market().implied_volatility(Call, 100_000.0));
    assert_eq!(price, 100_000.0);
    assert!(relative_gap(low, 1.143731970) <= 1e-9, "low bound {low}");
    assert!(relative_gap(high, 100_000.0) <= 1e-9, "high bound {high}");

    for price in [0.0, -1.0] {
        assert_eq!(
            reference_market().implied_volatility(Call, price),
            Err(OptionsError::OutsideDomain {
                input: "price",
                value: price
            })
        );
    }
    let expired = Black76 {
        years: 0.0,
        ..reference_market()
    };
    assert_eq!(
        expired.implied_volatility(Call, 5713.77),
        Err(OptionsError::OutsideDomain {
            input: "years",
            value: 0.0
        })
    );
}
