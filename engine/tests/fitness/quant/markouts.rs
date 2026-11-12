//! Markout fill model: which prints fill our resting quote, and what the mid does afterwards. A
//! wrong fill gate is silent — it produces a full research column of markouts for fills that never
//! happened, and no run ever errors.

use polysim::hot::quant::toxicity::{
    ForwardHorizon, MarkoutFill, MarkoutSide, MarkoutSpec, MarkoutTracker,
};
use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::TradeEvent;
use polysim::time::DurationUs;

use crate::engine_support::ts as at;

fn tracker(spin_us: i64) -> MarkoutTracker {
    MarkoutTracker::new(MarkoutSpec {
        spin_interval: DurationUs::from_micros(spin_us),
        max_mids_per_sec: 10,
    })
}

fn secs(count: i64) -> i64 {
    count * 1_000_000
}

fn price(units: f64) -> Price {
    Price((units * FIXED_SCALE as f64).round() as i64)
}

fn qty(units: f64) -> Qty {
    Qty((units * FIXED_SCALE as f64).round() as i64)
}

// Clocks disagree deliberately: fills key off received_ts_us only (trips boundary if wrong).
fn print(side: Side, units: f64, when: i64) -> TradeEvent {
    TradeEvent {
        instrument: InstrumentId(0),
        price: price(units),
        qty: Qty(FIXED_SCALE),
        side,
        exchange_ts_us: at(when - 7),
        exchange_sent_ts_us: None,
        received_ts_us: at(when),
        queued_ts_us: at(when + 3),
    }
}

fn sized_print(side: Side, units: f64, when: i64, qty: Qty) -> TradeEvent {
    TradeEvent {
        qty,
        ..print(side, units, when)
    }
}

// Decimal mids != exact binaries -> bps off by few ULPs vs hand-computed.
fn assert_bps(realised: Option<f64>, expected: f64) {
    let realised = realised.expect("horizon has realised a markout");
    assert!(
        (realised - expected).abs() < 1e-9,
        "markout {realised} bps, expected {expected}"
    );
}

// Arm at empty level: queue gate out, test at-or-through + one-fill-per-placement.
#[test]
fn unqueued_quote_fills_at_or_through_once_per_placement() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty(0));
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty(0));

    // Buying aggressor never reaches bid, however far it prints.
    markouts.on_trade(&print(Side::Buy, 99.0, 0));
    assert_eq!(markouts.bid().fill_count(), 0);
    assert_eq!(markouts.bid().armed_quote(), Some(price(100.0)));

    // Equality counts -> print exactly at level fills alone (tested before deeper prints).
    markouts.on_trade(&print(Side::Sell, 100.0, 0));
    assert_eq!(markouts.bid().fill_count(), 1);
    assert_eq!(markouts.bid().armed_quote(), None);

    // Next print through level -> finds side disarmed.
    markouts.on_trade(&print(Side::Sell, 99.0, secs(1)));
    assert_eq!(markouts.bid().fill_count(), 1);

    // Fresh placement to make fillable again; through counts as at.
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty(0));
    markouts.on_trade(&print(Side::Sell, 99.0, secs(2)));
    assert_eq!(markouts.bid().fill_count(), 2);

    // Ask mirrors: seller never lifts, buyer exactly at does.
    markouts.on_trade(&print(Side::Sell, 102.0, secs(2)));
    assert_eq!(markouts.ask().fill_count(), 0);
    markouts.on_trade(&print(Side::Buy, 101.0, secs(3)));
    assert_eq!(markouts.ask().fill_count(), 1);
    assert_eq!(markouts.ask().armed_quote(), None);

    // Through ask from above must fill (equality prints alone can't pin >=).
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty(0));
    markouts.on_trade(&print(Side::Buy, 102.0, secs(4)));
    assert_eq!(markouts.ask().fill_count(), 2);

    // Disarm != placement: unquoted side takes level away, prints find nothing to fill.
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty(0));
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty(0));
    markouts.disarm(MarkoutSide::Bid);
    markouts.disarm(MarkoutSide::Ask);
    markouts.on_trade(&print(Side::Sell, 99.0, secs(5)));
    markouts.on_trade(&print(Side::Buy, 102.0, secs(5)));
    assert_eq!(markouts.bid().fill_count(), 2);
    assert_eq!(markouts.ask().fill_count(), 2);
    assert_eq!(markouts.bid().armed_quote(), None);
    assert_eq!(markouts.ask().armed_quote(), None);
}

#[test]
fn forward_markouts_are_side_signed_and_ripen_per_horizon() {
    // ALL must stay in discriminant order: lanes are built by iterating it and read back by
    // discriminant, so a reordered ALL routes a horizon to another horizon's lane. Pinned by the
    // ripening times below rather than by reading the private index, and it must stay that way —
    // a horizon reading the wrong lane ripens at the wrong second.
    let mut markouts = tracker(secs(1));
    markouts.on_mid(at(0), 100.0);
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty(0));
    markouts.arm(MarkoutSide::Ask, price(100.0), Qty(0));
    markouts.on_trade(&print(Side::Sell, 100.0, 0));
    markouts.on_trade(&print(Side::Buy, 100.0, 0));

    markouts.on_mid(at(secs(1) - 1), 100.01);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs1).len(), 0);

    // Mid +1c above 100.00 fill -> +1 bps bought bid, -1 bps sold ask.
    markouts.on_mid(at(secs(1)), 100.01);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs1).last(), 1.0);
    assert_bps(markouts.ask().forward(ForwardHorizon::Secs1).last(), -1.0);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs3).len(), 0);

    // Deeper horizon realises on its elapse vs mid at that time.
    markouts.on_mid(at(secs(3)), 99.98);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs3).last(), -2.0);
    assert_bps(markouts.ask().forward(ForwardHorizon::Secs3).last(), 2.0);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs5).len(), 0);

    // Print sweep fills at OUR armed 100.00 (not 99.90 on tape) -> markout = +1 bps (not +110).
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty(0));
    markouts.on_trade(&print(Side::Sell, 99.9, secs(10)));
    markouts.on_mid(at(secs(11)), 100.01);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs1).last(), 1.0);
}

#[test]
fn on_trade_reports_the_fill_it_records() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty(0));
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty(0));

    // Print reaching no armed level -> reports nothing.
    assert_eq!(markouts.on_trade(&print(Side::Buy, 99.0, 0)), None);

    // Seller through bid fills at OUR armed 100.00, stamped at receipt, routed to bid.
    assert_eq!(
        markouts.on_trade(&print(Side::Sell, 99.5, secs(1))),
        Some(MarkoutFill {
            side: MarkoutSide::Bid,
            price: price(100.0),
            fill_ts_us: at(secs(1)),
        })
    );
    // Side now disarmed -> next print reports nothing.
    assert_eq!(markouts.on_trade(&print(Side::Sell, 99.0, secs(2))), None);

    // Ask mirrors: buyer lifting fills at OUR armed 101.00, routed to ask.
    assert_eq!(
        markouts.on_trade(&print(Side::Buy, 101.0, secs(3))),
        Some(MarkoutFill {
            side: MarkoutSide::Ask,
            price: price(101.0),
            fill_ts_us: at(secs(3)),
        })
    );
}

#[test]
fn queued_last_fills_after_ninety_percent_of_level_eaten() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(10.0));

    // Prints at level eat queue. 2 of 10 in -> 8 ahead, nothing ours traded.
    markouts.on_trade(&sized_print(Side::Sell, 100.0, 0, qty(2.0)));
    assert_eq!(markouts.bid().fill_count(), 0);
    assert_eq!(markouts.bid().armed_quote(), Some(price(100.0)));

    // Buying aggressor never reaches bid -> eats no queue.
    markouts.on_trade(&sized_print(Side::Buy, 100.0, 0, qty(7.0)));
    assert_eq!(markouts.bid().fill_count(), 0);

    // 8.9 of 10 eaten -> tenth short of threshold (last print before it).
    markouts.on_trade(&sized_print(Side::Sell, 100.0, secs(1), qty(6.9)));
    assert_eq!(markouts.bid().fill_count(), 0);

    // Landing exactly 9/10 fills (gate is >=, boundary to us).
    markouts.on_trade(&sized_print(Side::Sell, 100.0, secs(2), qty(0.1)));
    assert_eq!(markouts.bid().fill_count(), 1);
    assert_eq!(markouts.bid().armed_quote(), None);

    // Print > whole queue -> clears threshold alone.
    markouts.arm(MarkoutSide::Ask, price(101.0), qty(10.0));
    markouts.on_trade(&sized_print(Side::Buy, 101.0, secs(3), qty(50.0)));
    assert_eq!(markouts.ask().fill_count(), 1);

    // Print THROUGH level swept everything -> queue moot, smallest print fills.
    markouts.arm(MarkoutSide::Ask, price(101.0), qty(1000.0));
    markouts.on_trade(&sized_print(Side::Buy, 101.5, secs(4), qty(0.001)));
    assert_eq!(markouts.ask().fill_count(), 2);
}

#[test]
fn rearm_keeps_queue_only_while_armed_at_same_price() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(10.0));
    markouts.on_trade(&sized_print(Side::Sell, 100.0, 0, qty(8.0)));
    assert_eq!(markouts.bid().fill_count(), 0);

    // Re-arm same level (now 500); unbroken streak keeps earned place (8 eaten + 1 = 9/10 of 10).
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(500.0));
    markouts.on_trade(&sized_print(Side::Sell, 100.0, secs(1), qty(1.0)));
    assert_eq!(markouts.bid().fill_count(), 1);

    // Fill disarmed -> re-arm same price joins back of new queue (9 of 500 nowhere near).
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(500.0));
    markouts.on_trade(&sized_print(Side::Sell, 100.0, secs(2), qty(9.0)));
    assert_eq!(markouts.bid().fill_count(), 1);

    // Moving level = fresh join (nothing eaten at 100 buys anything at 99).
    markouts.arm(MarkoutSide::Bid, price(99.0), qty(10.0));
    markouts.on_trade(&sized_print(Side::Sell, 99.0, secs(3), qty(1.0)));
    assert_eq!(markouts.bid().fill_count(), 1);
    markouts.on_trade(&sized_print(Side::Sell, 99.0, secs(4), qty(8.0)));
    assert_eq!(markouts.bid().fill_count(), 2);
}
