//! Guéant–Lehalle–Fernandez-Tapia quoting. Two things must never drift: the closed-form
//! coefficients against a hand-worked example, and the grid rounding, which is what stands between
//! a computed depth and an order that crosses the book. A quote rounded the wrong way is not an
//! error anywhere — it is a marketable order the venue happily fills.

use polysim::hot::quant::pricing::{GueantParams, Objective, QuoteCoefficients, QuoteInputs};
use polysim::ids::Price;

const TICK: Price = Price(1_000_000); // $0.01 at the 1e-8 fixed-point scale.

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
        best_bid: Price(best_bid_tick * TICK.0),
        best_ask: Price(best_ask_tick * TICK.0),
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
    let clean = worked_example().quote(inputs(10_050.0, 0.0, 10_049, 10_051));
    assert_eq!(clean.bid_tick, 10_047, "bid floors: floor(10050 - 2.2)");
    assert_eq!(clean.ask_tick, 10_053, "ask ceils: ceil(10050 + 2.2)");
    assert_eq!(clean.bid, Price(10_047 * TICK.0));
    assert_eq!(clean.ask, Price(10_053 * TICK.0));

    // Long inventory -> ask_depth negative (liquidation skew); crossing ask clamps to best_bid+1.
    let long = worked_example().quote(inputs(10_050.0, 10.0, 10_049, 10_051));
    assert_eq!(
        long.ask_tick, 10_050,
        "post-only lifts the crossing ask to best_bid + 1"
    );
    assert_eq!(
        long.bid_tick, 10_043,
        "defensive bid floors to floor(10050 - 6.2)"
    );
    assert!(long.ask_tick > long.bid_tick);
}
