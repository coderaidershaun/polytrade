//! The shipped recorder's declaration seam: what it RECORDS as its quote and what it actually asks
//! the venue for must be the same number.
//!
//! The recorder snaps its Guéant level to the NEAREST tick and then clamps it outside the
//! minimum-distance band, while the engine snaps a bid down and an ask up. The two agree only
//! because the strategy's price is already on the venue's grid, from the same `tick_size` — and
//! nothing about that is enforced by a type. Break it and `gueant_bid_price` describes an order
//! that was never placed, which is a research dataset quietly documenting a different strategy from
//! the one that traded. The size is the same story one layer down: `order_notional` is in QUOTE
//! units and only becomes a base quantity through `qty_at`, so a units slip there prices right and
//! trades wrong.
//!
//! This drives the REAL `MicroRecorder` against the real engine, because both halves of the
//! agreement are only present when the real strategy meets the real reconciler.

use polysim::ids::{FIXED_SCALE, Price, Side};

use crate::engine_support::{ONE, pop};
use crate::recorder_fixture::{
    STEP_US, STEPS, column_value, drain_places, make_ready, recorded_row, recorder_engine,
    seed_book, spin_columns, step_messages,
};

const BASE: i64 = 100 * ONE;

/// Coarse enough that the engine's step snap actually bites: `order_notional / price` lands on a
/// quantity this does not divide, so a test that ignored the snap would read a different number.
const STEP: i64 = ONE / 1_000;

/// The shipped default, in QUOTE units.
const ORDER_NOTIONAL: i64 = 10 * ONE;

/// FITNESS: the recorded quote price and the placed order price are the same number, and the placed
/// size is `order_notional` converted at that price and snapped down onto the venue step.
#[test]
fn the_recorded_quote_is_the_order_that_gets_placed() {
    let instruments = [recorded_row(STEP)];
    let (mut engine, mut persist, mut commands) = recorder_engine(&instruments);
    make_ready(&mut engine, 0);
    seed_book(&mut engine, BASE, 0);

    let mut placed = Vec::new();
    let mut columns = Vec::new();
    for index in 0..STEPS {
        let when = index * STEP_US;
        for message in step_messages(BASE, index, when) {
            engine.dispatch(pop(0, 0), &message);
        }
        let spun = spin_columns(&mut persist);
        let issued = drain_places(&mut commands);
        if !issued.is_empty() {
            placed = issued;
            columns = spun;
            break;
        }
    }

    assert_eq!(
        placed.len(),
        2,
        "the recorder declares both sides every spin, so its first quoting spin places two orders \
         — got {placed:?}"
    );
    for order in &placed {
        let column = match order.side {
            Side::Buy => "gueant_bid_price",
            Side::Sell => "gueant_ask_price",
        };
        let recorded = column_value(&columns, column).unwrap_or_else(|| {
            panic!("{column} was not recorded on the spin that placed {order:?}")
        });
        assert_eq!(
            order.price,
            Price((recorded * FIXED_SCALE as f64).round() as i64),
            "{column} recorded {recorded}, but the order went out at {}: the research column now \
             describes a price nobody quoted",
            order.price.to_f64(),
        );
        let wanted =
            i128::from(ORDER_NOTIONAL) * i128::from(FIXED_SCALE) / i128::from(order.price.0);
        assert_eq!(
            i128::from(order.qty.0),
            wanted - wanted.rem_euclid(i128::from(STEP)),
            "order_notional {} at {} is {wanted} base, which snaps down to the {STEP} step — the \
             order asked for {} instead",
            ORDER_NOTIONAL,
            order.price.to_f64(),
            order.qty.0,
        );
    }
}
