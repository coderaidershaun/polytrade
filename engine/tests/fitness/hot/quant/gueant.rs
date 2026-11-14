//! The quoting model's closed-form terms against a worked example, and its rounding onto the grid,
//! which stops a computed depth from becoming an order that crosses the book.

use engine::hot::quant::pricing::{GueantParams, Objective, QuoteCoefficients, QuoteInputs};
use engine::msg::ids::Price;

const TICK: Price = Price::from_mantissa(1_000_000); // $0.01 at the 1e-8 fixed-point scale.

fn approx(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol
}

const ORDER_SIZE: f64 = 1.0;
const SIGMA: f64 = 2.0;

// Δ=1, q=3, A=5, k̃=0.5, σ̃=2, γ̃=0.0736 -> c1=2, c2=0.2, h=2.2, j=0.4.
fn worked_example() -> QuoteCoefficients {
    GueantParams::new(0.0736, ORDER_SIZE, Objective::InventoryPenalty)
        .coefficients(5.0, 0.5, SIGMA)
        .expect("valid estimates")
}

fn inputs(fair_ticks: f64, inventory: f64, best_bid_tick: i64, best_ask_tick: i64) -> QuoteInputs {
    QuoteInputs {
        fair: fair_ticks * TICK.to_f64(),
        inventory,
        best_bid: Price::from_mantissa(best_bid_tick * TICK.mantissa()),
        best_ask: Price::from_mantissa(best_ask_tick * TICK.mantissa()),
        tick: TICK,
    }
}

#[test]
fn model_b_matches_a_known_worked_example() {
    // -> δᵇ=3.4, δᵃ=1.0 at q=3.
    let c = worked_example();
    // c1 and c2 are not exposed, so they are read back from h = c1 + ½·Δ·σ̃·c2 and j = σ̃·c2.
    let c1 = c.half_spread() - 0.5 * ORDER_SIZE * c.skew_per_inventory();
    let c2 = c.skew_per_inventory() / SIGMA;
    assert!(approx(c1, 2.0, 1e-9), "c1 {c1}");
    assert!(approx(c2, 0.2, 2e-3), "c2 {c2}");
    assert!(approx(c.half_spread(), 2.2, 2e-3), "h {}", c.half_spread());
    assert!(
        approx(c.skew_per_inventory(), 0.4, 4e-3),
        "j {}",
        c.skew_per_inventory()
    );
    assert!(
        approx(c.bid_depth(3.0), 3.4, 1e-2),
        "bid_depth {}",
        c.bid_depth(3.0)
    );
    assert!(
        approx(c.ask_depth(3.0), 1.0, 1e-2),
        "ask_depth {}",
        c.ask_depth(3.0)
    );
}

#[test]
fn rounding_is_outward_and_never_crosses_the_book() {
    // Symmetric depth 2.2, fair mid-book -> bid floors, ask ceils, both outside book.
    let clean = worked_example()
        .quote(inputs(10_050.0, 0.0, 10_049, 10_051))
        .expect("finite inputs");
    assert_eq!(clean.bid_tick, 10_047, "bid floors: floor(10050 - 2.2)");
    assert_eq!(clean.ask_tick, 10_053, "ask ceils: ceil(10050 + 2.2)");
    assert_eq!(
        clean.bid,
        Some(Price::from_mantissa(10_047 * TICK.mantissa()))
    );
    assert_eq!(
        clean.ask,
        Some(Price::from_mantissa(10_053 * TICK.mantissa()))
    );

    // Long inventory -> ask_depth negative (liquidation skew); crossing ask clamps to best_bid+1.
    let long = worked_example()
        .quote(inputs(10_050.0, 10.0, 10_049, 10_051))
        .expect("finite inputs");
    assert_eq!(
        long.ask_tick, 10_050,
        "post-only lifts the crossing ask to best_bid + 1"
    );
    assert_eq!(
        long.bid_tick, 10_043,
        "defensive bid floors to floor(10050 - 6.2)"
    );
    assert!(long.ask_tick > long.bid_tick);

    // When both depths are below half the spacing of floats at fair, both sides snap onto fair and
    // collide. The side that sheds inventory stays, and the other moves one tick out: a long book
    // keeps its ask, a short book keeps its bid. A quarter tick keeps fair on the grid exactly.
    let quarter = Price::from_mantissa(25_000_000);
    let tiny = GueantParams::new(1e-30, ORDER_SIZE, Objective::InventoryPenalty)
        .coefficients(1.0, 1e14, 1.0)
        .expect("valid estimates");
    let spacing = 10_050f64.next_up() - 10_050.0;
    assert!(
        tiny.bid_depth(1.0).abs() < spacing / 2.0 && tiny.ask_depth(-1.0).abs() < spacing / 2.0
    );
    let at_fair = |inventory: f64| QuoteInputs {
        fair: 10_050.0 * quarter.to_f64(),
        inventory,
        best_bid: Price::from_mantissa(10_049 * quarter.mantissa()),
        best_ask: Price::from_mantissa(10_051 * quarter.mantissa()),
        tick: quarter,
    };
    let long = tiny.quote(at_fair(1.0)).expect("finite inputs");
    assert_eq!(
        (long.bid_tick, long.ask_tick),
        (10_049, 10_050),
        "a long book keeps its ask and moves its bid out"
    );
    let short = tiny.quote(at_fair(-1.0)).expect("finite inputs");
    assert_eq!(
        (short.bid_tick, short.ask_tick),
        (10_050, 10_051),
        "a short book keeps its bid and moves its ask out"
    );
}

/// Model A (ξ = γ) by its closed form at one point, its limit Model B as ξΔ/k goes to 0, and each
/// rate that the closed form cannot take. A wrong branch or a wrong factor of Δ moves each quote of
/// a strategy that picks Model A, and nothing else reads it.
#[test]
fn model_a_matches_its_closed_form_and_each_bad_rate_is_refused() {
    let (sigma, order_size) = (2.0, 2.0);
    // As r = ξΔ/k goes to 0, Model A tends to Model B (wiki-gueant.txt:49-52). By hand, for
    // 0 < r < 1: r − r²/2 ≤ ln(1 + r) ≤ r, so c1 of Model A is within r/2 of 1/k, relative, and c2
    // is within e^(r/2) − 1 < r of the c2 of Model B.
    let (k, a, r) = (1.0, 5.0, 1e-6);
    let gamma = r * k / order_size;
    let limit = GueantParams::new(gamma, order_size, Objective::CaraUtility)
        .coefficients(a, k, sigma)
        .expect("valid estimates");
    let model_b = GueantParams::new(gamma, order_size, Objective::InventoryPenalty)
        .coefficients(a, k, sigma)
        .expect("valid estimates");
    let c1_of = |c: &QuoteCoefficients| c.half_spread() - 0.5 * order_size * c.skew_per_inventory();
    let c2_of = |c: &QuoteCoefficients| c.skew_per_inventory() / sigma;
    assert!(
        (c1_of(&limit) / c1_of(&model_b) - 1.0).abs() <= r / 2.0,
        "c1 {} against {}",
        c1_of(&limit),
        c1_of(&model_b)
    );
    assert!(
        (c2_of(&limit) / c2_of(&model_b) - 1.0).abs() <= r,
        "c2 {} against {}",
        c2_of(&limit),
        c2_of(&model_b)
    );

    // c1 = ln(1 + ξΔ/k)/(ξΔ) and c2 = √(γ/(2AΔk)·(1 + ξΔ/k)^(k/(ξΔ) + 1)) (wiki-gueant.txt:41-48).
    // With γ = ξ = 0.25, Δ = 2, k = 0.5 and A = 0.5, ξΔ/k = 1, so by hand c1 = ln 2/0.5 = 2·ln 2
    // and c2 = √(0.25/(2·0.5·2·0.5)·2^2) = √1 = 1.
    let model_a = GueantParams::new(0.25, order_size, Objective::CaraUtility)
        .coefficients(0.5, 0.5, sigma)
        .expect("valid estimates");
    let c1 = model_a.half_spread() - 0.5 * order_size * model_a.skew_per_inventory();
    let c2 = model_a.skew_per_inventory() / sigma;
    assert!(approx(c1, 2.0 * std::f64::consts::LN_2, 1e-12), "c1 {c1}");
    assert!(approx(c2, 1.0, 1e-12), "c2 {c2}");

    // Each rate must be finite and positive, and the coefficients finite. 1e-300 · 1e-300 is below
    // the smallest float, so 2·A·Δ·k is 0 there and c2 is infinite.
    assert_eq!(1e-300 * 1e-300, 0.0);
    for objective in [Objective::InventoryPenalty, Objective::CaraUtility] {
        let params = GueantParams::new(0.5, order_size, objective);
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            for (a, k, sigma) in [(bad, 0.5, 2.0), (5.0, bad, 2.0), (5.0, 0.5, bad)] {
                assert!(
                    params.coefficients(a, k, sigma).is_none(),
                    "{objective:?}: A {a}, k {k}, sigma {sigma} gave coefficients"
                );
            }
        }
        assert!(
            params.coefficients(1e-300, 1e-300, 2.0).is_none(),
            "{objective:?}: an infinite c2 gave coefficients"
        );
    }
}

/// A quote reads market and ledger values, so a fair or an inventory that is not finite, a grid tick
/// that is not positive, a side whose tick is not positive, and a price past the range of a mantissa
/// each give no quote for that side, never a panic and never a price at or below zero.
#[test]
fn quote_refuses_values_that_are_not_finite_and_ticks_that_are_not_positive() {
    let c = worked_example();
    let base = inputs(10_050.0, 0.0, 10_049, 10_051);
    for (case, refused) in [
        (
            "fair NaN",
            QuoteInputs {
                fair: f64::NAN,
                ..base
            },
        ),
        (
            "fair +inf",
            QuoteInputs {
                fair: f64::INFINITY,
                ..base
            },
        ),
        (
            "fair -inf",
            QuoteInputs {
                fair: f64::NEG_INFINITY,
                ..base
            },
        ),
        (
            "inventory NaN",
            QuoteInputs {
                inventory: f64::NAN,
                ..base
            },
        ),
        (
            "inventory +inf",
            QuoteInputs {
                inventory: f64::INFINITY,
                ..base
            },
        ),
        (
            "tick 0",
            QuoteInputs {
                tick: Price::from_mantissa(0),
                ..base
            },
        ),
        (
            "tick -1",
            QuoteInputs {
                tick: Price::from_mantissa(-1),
                ..base
            },
        ),
    ] {
        assert_eq!(c.quote(refused), None, "{case}: quoted");
    }

    // Fair 2 ticks up with a depth of 2.2 on each side, by hand: the bid floors to tick
    // floor(2 − 2.2) = −1, so it has no price, and the ask ceils to ceil(2 + 2.2) = 5.
    let low = c.quote(inputs(2.0, 0.0, 1, 3)).expect("finite inputs");
    assert_eq!(low.bid, None, "a bid at or below zero was quoted");
    assert_eq!(low.ask, Some(Price::from_mantissa(5 * TICK.mantissa())));

    // Fair 1e13 ticks up: the ask ceils to tick 1e13 + 3, whose price passes the range of a
    // mantissa, so it has no price; the bid stays behind the best ask, at tick 2.
    assert!(i64::MAX / TICK.mantissa() < 10_000_000_000_003);
    let high = c.quote(inputs(1e13, 0.0, 1, 3)).expect("finite inputs");
    assert_eq!(
        high.ask, None,
        "an ask past the range of a mantissa was quoted"
    );
    assert_eq!(high.bid, Some(Price::from_mantissa(2 * TICK.mantissa())));
}
