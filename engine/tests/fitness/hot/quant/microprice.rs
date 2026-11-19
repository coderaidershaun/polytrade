//! The microprice of the touch is the equilibrium that the resilience model relaxes toward. These
//! pins hold it to the longhand size-weighted formula and to its fallback for a touch with no size.

use crate::engine_support::level;
use engine::hot::quant::micro::Touch;

#[test]
fn equilibrium_is_the_microprice() {
    const SCALE: i64 = 100_000_000;
    let touch = Touch {
        bid: level(100 * SCALE, 3 * SCALE),
        ask: level(101 * SCALE, SCALE),
    };

    let equilibrium = touch.microprice();

    let d = touch.bid.qty.to_f64();
    let s = touch.ask.qty.to_f64();
    let lambda = touch.spread() / (2.0 * (d + s));
    let longhand = touch.mid() + lambda * (d - s);
    assert!(
        (equilibrium - longhand).abs() < 1e-12,
        "equilibrium {equilibrium} vs longhand {longhand}"
    );

    // With no size on either side the weights are undefined, so the microprice falls back to the
    // mid, (100 + 101) / 2 = 100.5, and never reads 0 / 0.
    let sizeless = Touch {
        bid: level(100 * SCALE, 0),
        ask: level(101 * SCALE, 0),
    }
    .microprice();
    assert_eq!(sizeless, 100.5, "a touch with no size gave {sizeless}");
}
