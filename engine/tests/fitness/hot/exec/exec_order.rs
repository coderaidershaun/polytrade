//! A passive quote never snaps into aggression. The failure is silent by construction: an inverted
//! snap still compiles, still makes a legal order, and each layer after it agrees with it.

use engine::msg::ids::{Price, Side};
use proptest::prelude::*;

proptest! {
    /// FITNESS: snapping is passive on both sides, at every price and grid. A buy that snapped UP or
    /// a sell that snapped DOWN would cross a spread the strategy meant to rest inside — and it
    /// would look like a working system, because the order is legal and every layer agrees.
    #[test]
    fn snapping_never_manufactures_aggression(
        mantissa in -1_000_000_000_000i64..1_000_000_000_000,
        increment in 1i64..1_000_000,
    ) {
        let raw = Price::from_mantissa(mantissa);
        let bid = Side::Buy.snap_passive(raw, increment);
        let ask = Side::Sell.snap_passive(raw, increment);

        prop_assert!(bid.mantissa() <= raw.mantissa(), "a buy snapped UP, from {} to {}", raw.mantissa(), bid.mantissa());
        prop_assert!(ask.mantissa() >= raw.mantissa(), "a sell snapped DOWN, from {} to {}", raw.mantissa(), ask.mantissa());
        prop_assert_eq!(bid.mantissa().rem_euclid(increment), 0, "a buy left the grid");
        prop_assert_eq!(ask.mantissa().rem_euclid(increment), 0, "a sell left the grid");
        // Never further than one increment: rounding past a whole tick is a different price, not a
        // snap, and on a wide grid it would silently reprice the quote.
        prop_assert!(raw.mantissa() - bid.mantissa() < increment);
        prop_assert!(ask.mantissa() - raw.mantissa() < increment);
    }
}
