//! Pins of the checks of a set of marks. A check that passes an arbitrage lets quotes and hedges
//! rest on marks that pay out for free, and one that refuses sound marks stops all quoting.

use engine::hot::quant::options::{
    Black76, OptionRight, OptionsError, Pillar, forward_volatility, largest_rise, lowest_butterfly,
    volatility_between,
};
use proptest::prelude::*;

fn relative_gap(value: f64, expected: f64) -> f64 {
    ((value - expected) / expected).abs()
}

fn right() -> impl Strategy<Value = OptionRight> {
    prop_oneof![Just(OptionRight::Call), Just(OptionRight::Put)]
}

/// Strikes that rise strictly within four deviations of the forward, with the price of each for
/// one right at one volatility. Forward 0.001 to 1e6, deviation `volatility × √years` 0.01 to 3,
/// years 0.0001 to 5, discount 0.5 to 1, and 3 to 12 strikes.
fn sound_slice() -> impl Strategy<Value = (f64, Vec<f64>, Vec<f64>)> {
    (
        -3.0f64..6.0,
        0.0001f64..5.0,
        0.01f64..3.0,
        0.5f64..1.0,
        right(),
        prop::collection::btree_set(0u32..=800, 3..=12),
    )
        .prop_map(
            |(forward_power, years, deviation, discount, right, steps)| {
                let forward = 10f64.powf(forward_power);
                let volatility = deviation / years.sqrt();
                let strikes: Vec<f64> = steps
                    .iter()
                    .map(|step| forward * ((f64::from(*step) / 100.0 - 4.0) * deviation).exp())
                    .collect();
                let prices = strikes
                    .iter()
                    .map(|strike| {
                        let option = Black76 {
                            forward,
                            strike: *strike,
                            years,
                            discount,
                        };
                        option.price(right, volatility).unwrap()
                    })
                    .collect();
                (forward, strikes, prices)
            },
        )
}

#[test]
fn forward_volatility_matches_the_manual_and_refuses_falling_variance() {
    let near = Pillar {
        years: 90.0 / 365.0,
        volatility: 0.17,
    };
    let far = Pillar {
        years: 180.0 / 365.0,
        volatility: 0.155,
    };
    let forward = forward_volatility(near, far).unwrap();
    assert!(
        (forward - 0.1384).abs() <= 0.00005,
        "forward volatility {forward}"
    );
    for (days, expected) in [(90.0, 0.17), (135.0, 0.16015617378), (180.0, 0.155)] {
        let between = volatility_between(near, far, days / 365.0).unwrap();
        assert!(
            relative_gap(between, expected) <= 1e-9,
            "volatility at {days} days {between} against {expected}"
        );
    }

    let falling = Pillar {
        volatility: 0.11,
        ..far
    };
    match forward_volatility(near, falling) {
        Err(OptionsError::CalendarArbitrage { near, far }) => {
            assert!(relative_gap(near, 0.007126027397) <= 1e-9, "near {near}");
            assert!(relative_gap(far, 0.005967123288) <= 1e-9, "far {far}");
        }
        other => panic!("expected a calendar arbitrage, got {other:?}"),
    }

    // The far expiry is at least 1.01 times the near one, so that the difference of the two total
    // variances keeps its digits.
    proptest!(|(
        years in 0.001f64..5.0,
        ratio in 1.01f64..11.0,
        volatility in 0.01f64..3.0,
        position in 0.0f64..0.999,
    )| {
        let near = Pillar { years, volatility };
        let far = Pillar { years: years * ratio, volatility };
        let forward = forward_volatility(near, far)?;
        prop_assert!(relative_gap(forward, volatility) <= 1e-12, "forward volatility {forward}");
        let target = near.years + position * (far.years - near.years);
        let between = volatility_between(near, far, target)?;
        prop_assert!(relative_gap(between, volatility) <= 1e-12, "volatility between {between}");
    });
}

#[test]
fn sound_slices_pass_and_each_planted_violation_is_named() {
    let equal_strikes = [90_000.0, 100_000.0, 110_000.0];
    let calls = [11_827.693354, 5_713.767509, 2_275.433781];
    let butterfly = lowest_butterfly(&equal_strikes, &calls, 0.0).unwrap();
    assert!(
        (butterfly - 1_337.796059).abs() <= 1e-5,
        "butterfly {butterfly}"
    );

    let unequal_strikes = [90_000.0, 100_000.0, 125_000.0];
    let unequal_calls = [11_827.693354, 5_713.767509, 411.463875];
    let butterfly = lowest_butterfly(&unequal_strikes, &unequal_calls, 0.0).unwrap();
    assert!(
        (butterfly - 2_852.145994).abs() <= 1e-5,
        "butterfly {butterfly}"
    );

    let raised = [calls[0], calls[1] + 2_000.0, calls[2]];
    match lowest_butterfly(&equal_strikes, &raised, 0.0) {
        Err(OptionsError::NegativeButterfly { strike, value }) => {
            assert_eq!(strike, 100_000.0);
            assert!((value + 662.203941).abs() <= 1e-5, "butterfly {value}");
        }
        other => panic!("expected a negative butterfly, got {other:?}"),
    }

    let before = [5_713.77, 2_275.43];
    match largest_rise(&before, &[5_600.00, 2_300.00], 1.0) {
        Err(OptionsError::PerverseResponse { index, rise }) => {
            assert_eq!(index, 1);
            assert!((rise - 24.57).abs() <= 1e-5, "rise {rise}");
        }
        other => panic!("expected a perverse response, got {other:?}"),
    }
    let fallen = largest_rise(&before, &[5_600.00, 2_275.43], 1.0);
    assert!(fallen.is_ok(), "values at or below before give {fallen:?}");

    proptest!(|((forward, strikes, prices) in sound_slice())| {
        let butterfly = lowest_butterfly(&strikes, &prices, 1e-9 * forward);
        prop_assert!(butterfly.is_ok(), "sound slice gives {butterfly:?}");
    });
}
