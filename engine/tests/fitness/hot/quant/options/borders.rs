//! Border pins of the options maths. A NaN or an infinity that enters or leaves it reaches a risk
//! check, a hedge or a tape row as a number that each comparison lets through without a word.

use std::cell::Cell;
use std::fmt::Debug;

use engine::hot::quant::options::{
    AccountDeltas, Black76, BumpGreeks, Bumps, CoinParity, ExplainGreeks, GreekExplain, Greeks,
    HedgedStep, NoTradeBand, OptionRight, OptionsError, Pillar, SecondOrderGreeks, Shift,
    account_deltas, forward_volatility, greek_explain, greeks_by_bump, largest_rise,
    lowest_butterfly, second_order_by_bump, volatility_between,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use OptionRight::{Call, Put};

const NOT_FINITE: [f64; 3] = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];

/// The fields of `Black76` with a value outside the domain of a price.
const MARKET: [(&str, Option<f64>); 4] = [
    ("forward", Some(0.0)),
    ("strike", Some(0.0)),
    ("years", Some(-1.0)),
    ("discount", Some(0.0)),
];

/// The fields of `Black76` with a value outside the domain of a greek: no time left.
const MARKET_BEFORE_EXPIRY: [(&str, Option<f64>); 4] = [
    ("forward", Some(0.0)),
    ("strike", Some(0.0)),
    ("years", Some(0.0)),
    ("discount", Some(0.0)),
];

const BUMPS: Bumps = Bumps {
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

fn with_market_field(field: &str, value: f64) -> Black76 {
    let mut option = reference_market();
    match field {
        "forward" => option.forward = value,
        "strike" => option.strike = value,
        "years" => option.years = value,
        "discount" => option.discount = value,
        _ => {}
    }
    option
}

/// `value` when `field` is `name`, else `base`.
fn pick(field: &str, name: &str, value: f64, base: f64) -> f64 {
    if field == name { value } else { base }
}

fn inputs(
    market: &[(&'static str, Option<f64>)],
    more: &[(&'static str, Option<f64>)],
) -> Vec<(&'static str, Option<f64>)> {
    [market, more].concat()
}

/// Sets each input in turn to NaN, to each infinity, and to its value outside the domain if it has
/// one. Each time the function must refuse with the name of that input.
fn assert_each_input_is_named<T: Debug>(
    function: &str,
    run: impl Fn(&str, f64) -> Result<T, OptionsError>,
    inputs: &[(&str, Option<f64>)],
) {
    let base = run("none", 0.0);
    assert!(base.is_ok(), "{function}: the base inputs give {base:?}");
    for &(input, outside) in inputs {
        for value in NOT_FINITE.into_iter().chain(outside) {
            assert_named(function, input, run(input, value));
        }
    }
}

fn assert_named<T: Debug>(function: &str, input: &str, result: Result<T, OptionsError>) {
    let is_named = matches!(
        result,
        Err(OptionsError::OutsideDomain { input: named, .. }) if named == input
    );
    assert!(
        is_named,
        "{function}: expected {input} to be named, got {result:?}"
    );
}

fn assert_not_finite<T: Debug>(function: &str, result: Result<T, OptionsError>) {
    let is_not_finite = matches!(result, Err(OptionsError::NotFinite { .. }));
    assert!(
        is_not_finite,
        "{function}: expected NotFinite, got {result:?}"
    );
}

/// The numbers that a result carries out of the options maths.
trait Carried {
    fn carried(&self) -> Vec<f64>;
}

impl Carried for f64 {
    fn carried(&self) -> Vec<f64> {
        vec![*self]
    }
}

impl Carried for Greeks {
    fn carried(&self) -> Vec<f64> {
        vec![
            self.price,
            self.delta,
            self.gamma,
            self.vega_per_point,
            self.theta_per_day,
        ]
    }
}

impl Carried for BumpGreeks {
    fn carried(&self) -> Vec<f64> {
        vec![
            self.delta,
            self.delta_up,
            self.delta_down,
            self.up_gamma,
            self.down_gamma,
            self.vega_per_point,
            self.theta_per_day,
        ]
    }
}

impl Carried for SecondOrderGreeks {
    fn carried(&self) -> Vec<f64> {
        vec![
            self.vanna_per_point,
            self.volga_per_point_squared,
            self.charm_per_day,
            self.speed,
            self.color_per_day,
            self.zomma_per_point,
            self.vega_decay_per_day,
        ]
    }
}

impl Carried for AccountDeltas {
    fn carried(&self) -> Vec<f64> {
        vec![self.usd, self.coin]
    }
}

impl Carried for GreekExplain {
    fn carried(&self) -> Vec<f64> {
        vec![self.delta, self.gamma, self.theta, self.vega, self.cross]
    }
}

/// A result is a number with each part finite, or a refusal. A refusal that carries numbers the
/// function computed carries finite ones. `OutsideDomain` carries the input as the caller gave it.
fn assert_finite_or_refused<T: Carried + Debug>(
    function: &str,
    result: Result<T, OptionsError>,
) -> Result<(), TestCaseError> {
    let carried = match &result {
        Ok(value) => value.carried(),
        Err(OptionsError::NoRoot { price, low, high }) => vec![*price, *low, *high],
        Err(OptionsError::CalendarArbitrage { near, far }) => vec![*near, *far],
        Err(OptionsError::NegativeButterfly { strike, value }) => vec![*strike, *value],
        Err(OptionsError::PerverseResponse { rise, .. }) => vec![*rise],
        Err(OptionsError::OutsideDomain { .. } | OptionsError::NotFinite { .. }) => Vec::new(),
    };
    prop_assert!(
        carried.iter().all(|number| number.is_finite()),
        "{function} gives {result:?}"
    );
    Ok(())
}

fn magnitude() -> impl Strategy<Value = f64> {
    (-300.0f64..300.0).prop_map(|power| 10f64.powf(power))
}

fn signed() -> impl Strategy<Value = f64> {
    (magnitude(), prop_oneof![Just(1.0), Just(-1.0)]).prop_map(|(size, sign)| sign * size)
}

#[test]
fn no_number_that_is_not_finite_crosses_a_border() {
    each_input_outside_its_domain_is_named();
    each_function_refuses_a_result_that_overflows();
    an_error_of_the_closure_comes_back_from_the_first_shift_that_fails();

    proptest!(|(size in prop::array::uniform8(magnitude()), any in prop::array::uniform4(signed()))| {
        let option = Black76 { forward: size[0], strike: size[1], years: size[2], discount: size[3] };
        let volatility = size[4];
        assert_finite_or_refused("price", option.price(Call, volatility))?;
        assert_finite_or_refused("coin_price", option.coin_price(Put, volatility))?;
        assert_finite_or_refused("greeks", option.greeks(Call, volatility))?;
        assert_finite_or_refused("coin_delta", option.coin_delta(Put, volatility))?;
        assert_finite_or_refused(
            "digital_from_call_spread",
            option.digital_from_call_spread(size[5], size[6], size[7]),
        )?;
        let parity = CoinParity { call: size[4], put: size[5], strike: size[1], discount: size[3] };
        assert_finite_or_refused("forward", parity.forward())?;
        assert_finite_or_refused("implied_volatility", option.implied_volatility(Call, size[5]))?;

        let bumps = Bumps {
            near_move: size[5],
            far_move: size[5] + size[6],
            volatility: size[6],
            day: size[7],
        };
        let value = |shift| option.shifted_price(Call, volatility, shift);
        assert_finite_or_refused("greeks_by_bump", greeks_by_bump(value, bumps))?;
        assert_finite_or_refused("second_order_by_bump", second_order_by_bump(value, bumps))?;
        let equity = |spot| Ok(any[0] / spot);
        assert_finite_or_refused("account_deltas", account_deltas(equity, size[0], 0.5 * size[0]))?;
        let shift = Shift { underlying: any[1], volatility: any[2], years: size[7] };
        assert_finite_or_refused("shifted_price", option.shifted_price(Put, volatility, shift))?;

        let near = Pillar { years: size[2], volatility: size[4] };
        let far = Pillar { years: size[2] + size[7], volatility: size[6] };
        assert_finite_or_refused("forward_volatility", forward_volatility(near, far))?;
        let between = size[2] + 0.5 * size[7];
        assert_finite_or_refused("volatility_between", volatility_between(near, far, between))?;
        let strikes = [size[0], size[0] + size[1], size[0] + size[1] + size[2]];
        let prices = [size[3], size[4], size[5]];
        assert_finite_or_refused("lowest_butterfly", lowest_butterfly(&strikes, &prices, size[6]))?;
        let rise = largest_rise(&[any[0], any[1]], &[any[2], any[3]], size[7]);
        assert_finite_or_refused("largest_rise", rise)?;

        let band = NoTradeBand { cost: size[0], price: size[1], gamma: any[0], risk_aversion: size[2] };
        assert_finite_or_refused("half_width", band.half_width())?;
        let step = HedgedStep {
            gamma: any[0],
            price: size[1],
            step_return: any[1],
            volatility: size[2],
            years: size[3],
        };
        assert_finite_or_refused("gamma_theta", step.gamma_theta())?;
        let explain_greeks = ExplainGreeks {
            delta: any[0],
            gamma: any[1],
            vega_per_point: any[2],
            theta_per_day: any[3],
            vanna_per_point: any[0],
            volga_per_point_squared: any[1],
        };
        assert_finite_or_refused("greek_explain", greek_explain(explain_greeks, shift))?;
        let lines = GreekExplain { delta: any[0], gamma: any[1], theta: any[2], vega: any[3], cross: size[0] };
        assert_finite_or_refused("total", lines.total())?;
    });
}

fn each_input_outside_its_domain_is_named() {
    let volatility = [("volatility", Some(-0.1))];
    let positive_volatility = [("volatility", Some(0.0))];
    assert_each_input_is_named(
        "price",
        |field, value| {
            with_market_field(field, value).price(Call, pick(field, "volatility", value, 0.5))
        },
        &inputs(&MARKET, &volatility),
    );
    assert_each_input_is_named(
        "coin_price",
        |field, value| {
            with_market_field(field, value).coin_price(Put, pick(field, "volatility", value, 0.5))
        },
        &inputs(&MARKET, &volatility),
    );
    assert_each_input_is_named(
        "greeks",
        |field, value| {
            with_market_field(field, value).greeks(Call, pick(field, "volatility", value, 0.5))
        },
        &inputs(&MARKET_BEFORE_EXPIRY, &positive_volatility),
    );
    assert_each_input_is_named(
        "coin_delta",
        |field, value| {
            with_market_field(field, value).coin_delta(Put, pick(field, "volatility", value, 0.5))
        },
        &inputs(&MARKET_BEFORE_EXPIRY, &positive_volatility),
    );
    assert_each_input_is_named(
        "digital_from_call_spread",
        |field, value| {
            with_market_field(field, value).digital_from_call_spread(
                pick(field, "half_width", value, 1.0),
                pick(field, "low_volatility", value, 0.5),
                pick(field, "high_volatility", value, 0.5),
            )
        },
        &inputs(
            &MARKET,
            &[
                ("half_width", Some(0.0)),
                ("half_width", Some(100_000.0)),
                ("low_volatility", Some(-0.1)),
                ("high_volatility", Some(-0.1)),
            ],
        ),
    );
    assert_named(
        "digital_from_call_spread",
        "high_volatility",
        reference_market().digital_from_call_spread(1.0, 0.0, 2.0),
    );
    assert_each_input_is_named(
        "forward of coin parity",
        |field, value| {
            CoinParity {
                call: pick(field, "call", value, 0.0571),
                put: pick(field, "put", value, 0.0571),
                strike: pick(field, "strike", value, 100_000.0),
                discount: pick(field, "discount", value, 1.0),
            }
            .forward()
        },
        &[
            ("call", Some(-0.1)),
            ("put", Some(-0.1)),
            ("strike", Some(0.0)),
            ("discount", Some(0.0)),
        ],
    );
    let no_forward = CoinParity {
        call: 1.0,
        put: 0.0,
        strike: 100_000.0,
        discount: 1.0,
    };
    assert_named("forward of coin parity", "call", no_forward.forward());
    assert_each_input_is_named(
        "implied_volatility",
        |field, value| {
            with_market_field(field, value)
                .implied_volatility(Call, pick(field, "price", value, 5_713.77))
        },
        &inputs(&MARKET_BEFORE_EXPIRY, &[("price", Some(0.0))]),
    );

    let bump_inputs = [
        ("bumps.near_move", Some(0.0)),
        ("bumps.far_move", Some(50.0)),
        ("bumps.volatility", Some(0.0)),
        ("bumps.day", Some(0.0)),
    ];
    let bumps_with = |field: &str, value: f64| Bumps {
        near_move: pick(field, "bumps.near_move", value, BUMPS.near_move),
        far_move: pick(field, "bumps.far_move", value, BUMPS.far_move),
        volatility: pick(field, "bumps.volatility", value, BUMPS.volatility),
        day: pick(field, "bumps.day", value, BUMPS.day),
    };
    let value = |shift| reference_market().shifted_price(Call, 0.5, shift);
    assert_each_input_is_named(
        "greeks_by_bump",
        |field, number| greeks_by_bump(value, bumps_with(field, number)),
        &bump_inputs,
    );
    assert_each_input_is_named(
        "second_order_by_bump",
        |field, number| second_order_by_bump(value, bumps_with(field, number)),
        &bump_inputs,
    );
    assert_each_input_is_named(
        "account_deltas",
        |field, value| {
            account_deltas(
                |spot| Ok(50_000.0 / spot),
                pick(field, "spot", value, 100_000.0),
                pick(field, "bump", value, 1.0),
            )
        },
        &[
            ("spot", Some(0.0)),
            ("bump", Some(0.0)),
            ("bump", Some(100_000.0)),
        ],
    );
    assert_each_input_is_named(
        "shifted_price",
        |field, value| {
            let shift = Shift {
                underlying: pick(field, "shift.underlying", value, 100.0),
                volatility: pick(field, "shift.volatility", value, 0.001),
                years: pick(field, "shift.years", value, 0.001),
            };
            with_market_field(field, value).shifted_price(
                Call,
                pick(field, "volatility", value, 0.5),
                shift,
            )
        },
        &inputs(
            &MARKET,
            &[
                ("volatility", Some(-0.1)),
                ("shift.underlying", None),
                ("shift.volatility", None),
                ("shift.years", Some(-1.0)),
            ],
        ),
    );
    let below_zero_forward = Shift {
        underlying: -200_000.0,
        ..Shift::default()
    };
    let below_zero_volatility = Shift {
        volatility: -1.0,
        ..Shift::default()
    };
    let option = reference_market();
    assert_named(
        "shifted_price",
        "forward",
        option.shifted_price(Call, 0.5, below_zero_forward),
    );
    assert_named(
        "shifted_price",
        "volatility",
        option.shifted_price(Call, 0.5, below_zero_volatility),
    );

    let pillars = |field: &str, value: f64| {
        let near = Pillar {
            years: pick(field, "near.years", value, 0.25),
            volatility: pick(field, "near.volatility", value, 0.17),
        };
        let far = Pillar {
            years: pick(field, "far.years", value, 0.5),
            volatility: pick(field, "far.volatility", value, 0.155),
        };
        (near, far)
    };
    assert_each_input_is_named(
        "forward_volatility",
        |field, value| {
            let (near, far) = pillars(field, value);
            forward_volatility(near, far)
        },
        &[
            ("near.years", Some(-1.0)),
            ("near.volatility", Some(-0.1)),
            ("far.years", Some(0.25)),
            ("far.volatility", Some(-0.1)),
        ],
    );
    assert_each_input_is_named(
        "volatility_between",
        |field, value| {
            let (near, far) = pillars(field, value);
            volatility_between(near, far, pick(field, "years", value, 0.4))
        },
        &[
            ("near.years", Some(0.0)),
            ("near.volatility", Some(-0.1)),
            ("far.years", Some(0.25)),
            ("far.volatility", Some(-0.1)),
            ("years", Some(0.1)),
            ("years", Some(0.6)),
        ],
    );

    assert_each_input_is_named(
        "lowest_butterfly",
        |field, value| {
            let strikes = [
                90_000.0,
                pick(field, "strikes", value, 100_000.0),
                110_000.0,
            ];
            let prices = [11_827.69, pick(field, "prices", value, 5_713.77), 2_275.43];
            lowest_butterfly(&strikes, &prices, pick(field, "slack", value, 0.0))
        },
        &[
            ("strikes", Some(0.0)),
            ("strikes", Some(80_000.0)),
            ("prices", Some(-1.0)),
            ("slack", Some(-1.0)),
        ],
    );
    assert_named(
        "lowest_butterfly",
        "strikes",
        lowest_butterfly(&[90_000.0, 100_000.0], &[11_827.69, 5_713.77], 0.0),
    );
    assert_named(
        "lowest_butterfly",
        "prices",
        lowest_butterfly(
            &[90_000.0, 100_000.0, 110_000.0],
            &[11_827.69, 5_713.77],
            0.0,
        ),
    );
    assert_each_input_is_named(
        "largest_rise",
        |field, value| {
            let before = [5_713.77, pick(field, "before", value, 2_275.43)];
            let after = [5_600.0, pick(field, "after", value, 2_275.0)];
            largest_rise(&before, &after, pick(field, "slack", value, 1.0))
        },
        &[("before", None), ("after", None), ("slack", Some(-1.0))],
    );
    assert_named("largest_rise", "before", largest_rise(&[], &[], 1.0));
    assert_named(
        "largest_rise",
        "after",
        largest_rise(&[5_713.77, 2_275.43], &[5_600.0], 1.0),
    );

    assert_each_input_is_named(
        "half_width",
        |field, value| {
            NoTradeBand {
                cost: pick(field, "cost", value, 0.0002),
                price: pick(field, "price", value, 100_000.0),
                gamma: pick(field, "gamma", value, 0.00278),
                risk_aversion: pick(field, "risk_aversion", value, 0.00001),
            }
            .half_width()
        },
        &[
            ("cost", Some(-0.1)),
            ("price", Some(0.0)),
            ("gamma", None),
            ("risk_aversion", Some(0.0)),
        ],
    );
    assert_each_input_is_named(
        "gamma_theta",
        |field, value| {
            HedgedStep {
                gamma: pick(field, "gamma", value, 2.78e-5),
                price: pick(field, "price", value, 100_000.0),
                step_return: pick(field, "step_return", value, 0.01),
                volatility: pick(field, "volatility", value, 0.5),
                years: pick(field, "years", value, 1.0 / 365.0),
            }
            .gamma_theta()
        },
        &[
            ("gamma", None),
            ("price", Some(0.0)),
            ("step_return", None),
            ("volatility", Some(-0.1)),
            ("years", Some(-1.0)),
        ],
    );
    assert_each_input_is_named(
        "greek_explain",
        |field, value| {
            let greeks = ExplainGreeks {
                delta: pick(field, "greeks.delta", value, 0.5286),
                gamma: pick(field, "greeks.gamma", value, 2.78e-5),
                vega_per_point: pick(field, "greeks.vega_per_point", value, 114.08),
                theta_per_day: pick(field, "greeks.theta_per_day", value, -95.07),
                vanna_per_point: pick(field, "greeks.vanna_per_point", value, 0.00057),
                volga_per_point_squared: pick(
                    field,
                    "greeks.volga_per_point_squared",
                    value,
                    -0.012,
                ),
            };
            let shift = Shift {
                underlying: pick(field, "shift.underlying", value, 100.0),
                volatility: pick(field, "shift.volatility", value, 0.001),
                years: pick(field, "shift.years", value, 1.0 / 8760.0),
            };
            greek_explain(greeks, shift)
        },
        &[
            ("greeks.delta", None),
            ("greeks.gamma", None),
            ("greeks.vega_per_point", None),
            ("greeks.theta_per_day", None),
            ("greeks.vanna_per_point", None),
            ("greeks.volga_per_point_squared", None),
            ("shift.underlying", None),
            ("shift.volatility", None),
            ("shift.years", Some(-1.0)),
        ],
    );
    assert_each_input_is_named(
        "total",
        |field, value| {
            GreekExplain {
                delta: pick(field, "delta", value, 52.857),
                gamma: pick(field, "gamma", value, 0.139),
                theta: pick(field, "theta", value, -3.961),
                vega: pick(field, "vega", value, 11.408),
                cross: pick(field, "cross", value, 0.006),
            }
            .total()
        },
        &[
            ("delta", None),
            ("gamma", None),
            ("theta", None),
            ("vega", None),
            ("cross", None),
        ],
    );
}

fn each_function_refuses_a_result_that_overflows() {
    let huge = Black76 {
        forward: 1e300,
        strike: 1e300,
        years: 1.0,
        discount: 1e300,
    };
    let tiny_forward = Black76 {
        forward: 1e-300,
        ..huge
    };
    assert_not_finite("price", huge.price(Call, 0.5));
    assert_not_finite("coin_price", huge.coin_price(Call, 0.5));
    assert_not_finite("greeks", huge.greeks(Call, 0.5));
    assert_not_finite("coin_delta", tiny_forward.coin_delta(Call, 0.5));
    assert_not_finite(
        "digital_from_call_spread",
        huge.digital_from_call_spread(1.0, 0.5, 0.5),
    );
    let thin_parity = CoinParity {
        call: 1.0 - 1e-10,
        put: 0.0,
        strike: 1e300,
        discount: 1.0,
    };
    assert_not_finite("forward of coin parity", thin_parity.forward());
    assert_not_finite("implied_volatility", huge.implied_volatility(Call, 1.0));
    let steep = |shift: Shift| Ok(shift.underlying * 1e306);
    assert_not_finite("greeks_by_bump", greeks_by_bump(steep, BUMPS));
    assert_not_finite("second_order_by_bump", second_order_by_bump(steep, BUMPS));
    assert_not_finite(
        "account_deltas",
        account_deltas(|spot| Ok(spot * 1e300), 1e10, 1.0),
    );
    assert_not_finite(
        "shifted_price",
        huge.shifted_price(Call, 0.5, Shift::default()),
    );
    let near = Pillar {
        years: 1e200,
        volatility: 1e200,
    };
    let far = Pillar {
        years: 2e200,
        volatility: 1e200,
    };
    assert_not_finite("forward_volatility", forward_volatility(near, far));
    assert_not_finite("volatility_between", volatility_between(near, far, 1.5e200));
    assert_not_finite(
        "lowest_butterfly",
        lowest_butterfly(&[1.0, 2.0, 1e300], &[1e308, 1e308, 1e308], 0.0),
    );
    assert_not_finite("largest_rise", largest_rise(&[-1e308], &[1e308], 0.0));
    let wide_band = NoTradeBand {
        cost: 1e300,
        price: 1e300,
        gamma: 1.0,
        risk_aversion: 1.0,
    };
    assert_not_finite("half_width", wide_band.half_width());
    let wild_step = HedgedStep {
        gamma: 1e300,
        price: 1e300,
        step_return: 1.0,
        volatility: 0.0,
        years: 0.0,
    };
    assert_not_finite("gamma_theta", wild_step.gamma_theta());
    let wild_greeks = ExplainGreeks {
        delta: 1e300,
        gamma: 0.0,
        vega_per_point: 0.0,
        theta_per_day: 0.0,
        vanna_per_point: 0.0,
        volga_per_point_squared: 0.0,
    };
    let wild_move = Shift {
        underlying: 1e300,
        ..Shift::default()
    };
    assert_not_finite("greek_explain", greek_explain(wild_greeks, wild_move));
    let wild_lines = GreekExplain {
        delta: 1e308,
        gamma: 1e308,
        theta: 0.0,
        vega: 0.0,
        cross: 0.0,
    };
    assert_not_finite("total", wild_lines.total());
}

fn an_error_of_the_closure_comes_back_from_the_first_shift_that_fails() {
    let option = reference_market();
    for (function, call_count) in [("greeks_by_bump", 8), ("second_order_by_bump", 32)] {
        for failing_call in 1..=call_count {
            let calls = Cell::new(0);
            let refusal = OptionsError::OutsideDomain {
                input: "closure",
                value: f64::from(failing_call),
            };
            let value = |shift: Shift| {
                calls.set(calls.get() + 1);
                if calls.get() == failing_call {
                    return Err(refusal);
                }
                option.shifted_price(Call, 0.5, shift)
            };
            let error = if function == "greeks_by_bump" {
                greeks_by_bump(value, BUMPS).err()
            } else {
                second_order_by_bump(value, BUMPS).err()
            };
            assert_eq!(error, Some(refusal), "{function}, call {failing_call}");
            assert_eq!(
                calls.get(),
                failing_call,
                "{function} went on after a refusal"
            );
        }
    }
    for failing_call in 1..=3 {
        let calls = Cell::new(0);
        let refusal = OptionsError::OutsideDomain {
            input: "closure",
            value: f64::from(failing_call),
        };
        let equity = |spot: f64| {
            calls.set(calls.get() + 1);
            if calls.get() == failing_call {
                return Err(refusal);
            }
            Ok(50_000.0 / spot)
        };
        let error = account_deltas(equity, 100_000.0, 1.0).err();
        assert_eq!(error, Some(refusal), "account_deltas, call {failing_call}");
        assert_eq!(
            calls.get(),
            failing_call,
            "account_deltas went on after a refusal"
        );
    }
}
