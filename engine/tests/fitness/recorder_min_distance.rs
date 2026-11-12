//! The shipped recorder's quote must sit at least the minimum distance from mid, and the floor's
//! clamp must be what puts it there: a tape that solves a depth above the floor would pass without
//! ever exercising the clamp.

use polysim::ids::Side;

use crate::engine_support::{ONE, pop};
use crate::micro_strategy::models::MIN_QUOTE_DISTANCE_BPS;
use crate::recorder_fixture::{
    STEP_US, STEPS, TICK, column_value, drain_places, make_ready, recorded_row, recorder_engine,
    seed_book, spin_columns, step_messages,
};

const BASE: i64 = 10_000 * ONE;
const STEP: i64 = ONE / 100_000;

#[test]
fn a_quote_is_never_tighter_than_the_minimum_distance() {
    let instruments = [recorded_row(STEP)];
    let (mut engine, mut persist, mut commands) = recorder_engine(&instruments);
    make_ready(&mut engine, 0);
    seed_book(&mut engine, BASE, 0);

    let mut bid_checked = false;
    let mut ask_checked = false;
    for index in 0..STEPS {
        let when = index * STEP_US;
        for message in step_messages(BASE, index, when) {
            engine.dispatch(pop(0, 0), &message);
        }
        let columns = spin_columns(&mut persist);
        let issued = drain_places(&mut commands);
        if issued.is_empty() {
            continue;
        }
        let mid = column_value(&columns, "mid").expect("mid was not recorded on a quoting spin");
        let tick_bps = TICK as f64 / mid * 1e4;
        for order in &issued {
            let half_spread_column = match order.side {
                Side::Buy => "gueant_bid_half_spread_bps",
                Side::Sell => "gueant_ask_half_spread_bps",
            };
            let half_spread = column_value(&columns, half_spread_column).unwrap_or_else(|| {
                panic!("{half_spread_column} was not recorded on the spin that placed {order:?}")
            });
            assert!(
                half_spread < MIN_QUOTE_DISTANCE_BPS,
                "the fixture must solve a depth below the floor so the clamp is what places the \
                 quote — got h = {half_spread} bps; quieten the tape, never widen the pin"
            );
            let distance_bps = (order.price.to_f64() - mid).abs() / mid * 1e4;
            assert!(
                distance_bps >= MIN_QUOTE_DISTANCE_BPS - 1e-9,
                "{:?} order at {} sits {distance_bps} bps from mid {mid}, tighter than the \
                 {MIN_QUOTE_DISTANCE_BPS} bps floor",
                order.side,
                order.price.to_f64(),
            );
            assert!(
                distance_bps <= MIN_QUOTE_DISTANCE_BPS + tick_bps + 1e-9,
                "{:?} order at {} sits {distance_bps} bps from mid {mid} — the clamp lands on the \
                 first grid tick past the floor, never further",
                order.side,
                order.price.to_f64(),
            );
            match order.side {
                Side::Buy => bid_checked = true,
                Side::Sell => ask_checked = true,
            }
        }
        if bid_checked && ask_checked {
            break;
        }
    }
    assert!(
        bid_checked && ask_checked,
        "both sides must place within the fixture: bid {bid_checked}, ask {ask_checked}"
    );
}
