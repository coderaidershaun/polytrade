//! FITNESS: what the Polymarket preflight stamps on a row is what every execution gate downstream
//! believes about the venue, and an unstamped field is not a missing number — it is a gate that
//! silently passes.
//!
//! The four stamps here each close one of those. A `min_qty` of zero admits an order below the
//! venue's floor. A `max_num_orders` of zero sizes the edge's order mirror at nothing, and the FIRST
//! placement of the run dies on it. An absent amend budget lets the reconciler try to shrink an
//! order on a venue with no amend endpoint at all. An absent price ceiling lets an aggressive price
//! walk past parity on a market whose prices ARE probabilities.
//!
//! `scale_preflight` pins the tick and the step this same pass writes; the split is deliberate —
//! that file is about parsing a venue's grid, this one is about arming execution against it.

use polysim::adapters::polymarket::preflight::stamp_poly_scales;
use polysim::config::VenueMarket;
use polysim::hot::exec::MAX_QUOTE_LEVELS;
use polysim::ids::{FIXED_SCALE, Price, Qty};
use polysim::registry::InstrumentRow;

use crate::scale_preflight::{build_poly_registry, poly_market};

/// The venue's ordinary grid: a 0.01 tick and a five-share minimum, both exact at the 1e-8 scale.
const TICK: Price = Price(FIXED_SCALE / 100);
const MIN_ORDER_SIZE: Qty = Qty(5 * FIXED_SCALE);

fn stamped_rows() -> Vec<InstrumentRow> {
    let mut registry = build_poly_registry();
    stamp_poly_scales(&mut registry, &poly_market(TICK, MIN_ORDER_SIZE));
    registry
        .instruments()
        .iter()
        .filter(|row| matches!(row.market, VenueMarket::Polymarket(_)))
        .cloned()
        .collect()
}

#[test]
fn the_size_floor_and_the_size_step_are_stamped_separately() {
    for row in stamped_rows() {
        assert_eq!(
            row.min_qty,
            Some(MIN_ORDER_SIZE),
            "the venue's orderMinSize is the floor an order must clear"
        );
        assert_eq!(
            row.lot_size,
            Some(Qty(FIXED_SCALE / 100)),
            "and sizes quantise to the venue's two-decimal share step, not to the floor"
        );
        assert!(
            row.lot_size < row.min_qty,
            "a step at or above the floor is the collapsed pair this test exists to keep apart"
        );
        assert_eq!(
            row.min_notional, None,
            "polymarket floors an order by shares and by nothing else — a zero stamped here would \
             read as a floor the venue never set"
        );
    }
}

#[test]
fn the_order_ceiling_and_a_zero_amend_budget_are_both_stamped() {
    for row in stamped_rows() {
        let max_num_orders = row.max_num_orders.expect(
            "an unstamped ceiling sizes the edge's mirror at zero and the first place dies",
        );
        assert_eq!(max_num_orders, 4 * MAX_QUOTE_LEVELS as u32);
        assert!(
            max_num_orders >= 2 * MAX_QUOTE_LEVELS as u32,
            "the ceiling has to hold a full ladder on BOTH sides or the startup capacity check \
             refuses every configuration"
        );
        assert_eq!(
            row.max_num_order_amends,
            Some(0),
            "no amend endpoint exists; absent would read as an unpublished filter"
        );
    }
}

#[test]
fn the_price_ceiling_is_one_tick_below_parity() {
    for row in stamped_rows() {
        assert_eq!(row.max_price, Some(Price(FIXED_SCALE - TICK.0)));
        assert_eq!(
            row.tick_size,
            Some(TICK),
            "the ceiling is derived from the tick, so a fixture that drifted one would hide the other"
        );
    }

    // The endgame tick moves the ceiling with it: near resolution the venue quotes at 0.001 and the
    // top price becomes 0.999, so a constant would refuse legal prices exactly when a position most
    // needs closing.
    let mut registry = build_poly_registry();
    let endgame_tick = Price(FIXED_SCALE / 1_000);
    stamp_poly_scales(&mut registry, &poly_market(endgame_tick, MIN_ORDER_SIZE));
    for row in registry.instruments() {
        assert_eq!(row.max_price, Some(Price(FIXED_SCALE - endgame_tick.0)));
    }
}
