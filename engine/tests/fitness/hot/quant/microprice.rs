//! The book equilibrium. `orderbook_equilibrium` is a public alias of `microprice`, and nothing in
//! the engine calls it, so these pins are the only thing that keeps the alias the same number as
//! `microprice` and as the longhand size-weighted formula.

use crate::engine_support::level;
use engine::hot::quant::micro::{microprice, mid, orderbook_equilibrium, spread};

#[test]
fn equilibrium_is_the_microprice() {
    const SCALE: i64 = 100_000_000;
    let best_bid = level(100 * SCALE, 3 * SCALE);
    let best_ask = level(101 * SCALE, SCALE);

    let equilibrium = orderbook_equilibrium(best_bid, best_ask);
    assert_eq!(equilibrium, microprice(best_bid, best_ask));

    let d = best_bid.qty.to_f64();
    let s = best_ask.qty.to_f64();
    let lambda = spread(best_bid.price, best_ask.price) / (2.0 * (d + s));
    let longhand = mid(best_bid.price, best_ask.price) + lambda * (d - s);
    assert!(
        (equilibrium - longhand).abs() < 1e-12,
        "equilibrium {equilibrium} vs longhand {longhand}"
    );

    // With no size on either side the weights are undefined, so the microprice falls back to the
    // mid, (100 + 101) / 2 = 100.5, and never reads 0 / 0.
    let sizeless = microprice(level(100 * SCALE, 0), level(101 * SCALE, 0));
    assert_eq!(sizeless, 100.5, "a touch with no size gave {sizeless}");
}
