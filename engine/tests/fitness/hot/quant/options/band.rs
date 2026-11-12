//! No-trade band pins. A band that is too wide leaves a book unhedged, and one that is too narrow
//! pays the cost of a trade on each small move.

use engine::hot::quant::options::NoTradeBand;
use proptest::prelude::*;

const WORKED_EXAMPLE: NoTradeBand = NoTradeBand {
    cost: 0.0002,
    price: 100_000.0,
    gamma: 0.00278,
    risk_aversion: 0.00001,
};

fn relative_gap(value: f64, expected: f64) -> f64 {
    ((value - expected) / expected).abs()
}

/// Cost 1e-5 to 1e-2, price 1e-3 to 1e6, gamma 1e-8 to 1 in size with either sign, risk aversion
/// 1e-8 to 1e-2, each spread evenly in its logarithm. The quantity under the cube root then spans
/// many powers of 8, so its mantissa covers 1 to 8, and it stays far from the ends of the float
/// range.
fn band() -> impl Strategy<Value = NoTradeBand> {
    (
        -5.0f64..-2.0,
        -3.0f64..6.0,
        -8.0f64..0.0,
        prop_oneof![Just(1.0), Just(-1.0)],
        -8.0f64..-2.0,
    )
        .prop_map(
            |(cost_power, price_power, gamma_power, sign, risk_power)| NoTradeBand {
                cost: 10f64.powf(cost_power),
                price: 10f64.powf(price_power),
                gamma: sign * 10f64.powf(gamma_power),
                risk_aversion: 10f64.powf(risk_power),
            },
        )
}

#[test]
fn band_matches_the_worked_example_and_scales_as_the_formula() {
    proptest!(|(band in band())| {
        let NoTradeBand { cost, price, gamma, risk_aversion } = band;
        let half_width = band.half_width()?;
        let cube = half_width * half_width * half_width;
        let radicand = 3.0 * cost * price * (gamma * gamma) / (2.0 * risk_aversion);
        prop_assert!(
            relative_gap(cube, radicand) <= 1e-14,
            "half width {half_width} cubes to {cube}, not {radicand}"
        );
    });

    let half_width = WORKED_EXAMPLE.half_width().unwrap();
    assert!(
        (half_width - 2.85).abs() <= 0.005,
        "worked example gives {half_width}"
    );

    proptest!(|(band in band())| {
        let half_width = band.half_width()?;
        let scaled = [
            ("gamma times 8", NoTradeBand { gamma: band.gamma * 8.0, ..band }, 4.0),
            ("cost times 8", NoTradeBand { cost: band.cost * 8.0, ..band }, 2.0),
            (
                "risk aversion times 8",
                NoTradeBand { risk_aversion: band.risk_aversion * 8.0, ..band },
                0.5,
            ),
            ("gamma of the other sign", NoTradeBand { gamma: -band.gamma, ..band }, 1.0),
        ];
        for (name, changed, factor) in scaled {
            let changed_width = changed.half_width()?;
            prop_assert!(
                relative_gap(changed_width, factor * half_width) <= 1e-12,
                "{name}: {changed_width} against {factor} times {half_width}"
            );
        }
        prop_assert_eq!(NoTradeBand { gamma: 0.0, ..band }.half_width()?, 0.0);
        prop_assert_eq!(NoTradeBand { cost: 0.0, ..band }.half_width()?, 0.0);
    });
}
