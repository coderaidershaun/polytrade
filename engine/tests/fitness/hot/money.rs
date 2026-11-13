//! Money on the order path and in the ledger: a notional that is exact or panics, and quote money
//! that no code can take for a base quantity.

use std::panic::{AssertUnwindSafe, catch_unwind};

use engine::msg::ids::{FIXED_SCALE, Notional, Price, Qty, Side, Spend};
use proptest::prelude::*;

const ONE: i64 = FIXED_SCALE;

/// The exact product of two mantissas, truncated toward zero, or `None` when it leaves i64.
fn exact_notional(price: i64, qty: i64) -> Option<Notional> {
    let product = i128::from(price) * i128::from(qty) / i128::from(FIXED_SCALE);
    i64::try_from(product).ok().map(Notional::from_mantissa)
}

fn assert_notional_is_exact_or_panics_with_values(price: i64, qty: i64) {
    let notional = catch_unwind(AssertUnwindSafe(|| {
        Price::from_mantissa(price).notional(Qty::from_mantissa(qty))
    }));
    match exact_notional(price, qty) {
        Some(expected) => assert_eq!(notional.ok(), Some(expected), "price {price} qty {qty}"),
        None => {
            let message = notional.expect_err("a notional outside i64 must panic");
            let message = message
                .downcast_ref::<String>()
                .expect("the panic names its values");
            assert!(
                message.contains(&format!("price {price} qty {qty}")),
                "{message}"
            );
        }
    }
}

/// A wrapped or rounded notional books the wrong cash and the wrong exposure, with no error. Each
/// notional is the exact product, truncated toward zero, and one that does not fit panics with the
/// price and the quantity that made it.
#[test]
fn notional_is_exact_and_overflow_panics_with_values() {
    let notional = |price, qty| Price::from_mantissa(price).notional(Qty::from_mantissa(qty));
    assert_eq!(
        notional(3 * ONE / 2, 2 * ONE),
        Notional::from_mantissa(3 * ONE)
    );
    assert_eq!(notional(1, 1), Notional::ZERO);
    assert_eq!(notional(-1, 3 * ONE / 2), Notional::from_mantissa(-1));
    assert_notional_is_exact_or_panics_with_values(i64::MAX, 2 * ONE);
    assert_notional_is_exact_or_panics_with_values(i64::MIN, 2 * ONE);

    notional_equals_the_exact_product();
}

proptest! {
    fn notional_equals_the_exact_product(
        price in prop_oneof![any::<i64>(), -1_000_000 * ONE..1_000_000 * ONE],
        qty in prop_oneof![any::<i64>(), -1_000_000 * ONE..1_000_000 * ONE],
    ) {
        assert_notional_is_exact_or_panics_with_values(price, qty);
    }
}

/// A swap of base and quote money compiles silently while both are a bare i64, and then a quantity
/// is booked as cash. A buy reserves quote money and a sell reserves base quantity, and the
/// reservation says which.
#[test]
fn no_conversion_between_base_and_quote_money() {
    assert_lacks!(Notional: From<Qty>);
    assert_lacks!(Qty: From<Notional>);
    assert_lacks!(Notional: From<Price>);
    assert_lacks!(Notional: From<i64>);
    assert_lacks!(Qty: From<i64>);
    assert_lacks!(Price: From<i64>);
    assert_lacks!(Notional: std::ops::Add<Qty>);
    assert_lacks!(Qty: std::ops::Add<Notional>);
    assert_lacks!(Notional: std::ops::Sub<Qty>);
    assert_lacks!(Qty: std::ops::Sub<Notional>);
    assert_lacks!(Notional: PartialEq<Qty>);
    assert_lacks!(Qty: PartialEq<Notional>);
    assert_lacks!(Notional: PartialOrd<Qty>);
    assert_lacks!(Qty: PartialOrd<Notional>);

    let price = Price::from_mantissa(3 * ONE / 2);
    let qty = Qty::from_mantissa(2 * ONE);
    assert_eq!(
        Side::Buy.spend(price, qty),
        Spend::Quote(Notional::from_mantissa(3 * ONE))
    );
    assert_eq!(Side::Sell.spend(price, qty), Spend::Base(qty));
}
