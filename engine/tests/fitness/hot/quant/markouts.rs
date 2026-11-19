//! The markout fill model: which prints fill our resting quote, and how the mid moves before and
//! after. A wrong fill rule fills a research column with markouts of fills that never happened.

use engine::hot::quant::toxicity::{
    ForwardHorizon, MarkoutFill, MarkoutSide, MarkoutSpec, MarkoutTracker, ReverseHorizon,
};
use engine::msg::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::TradeEvent;
use engine::msg::time::DurationUs;

use crate::engine_support::{ts as at, venue_ts};

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
    Price::from_mantissa((units * FIXED_SCALE as f64).round() as i64)
}

fn qty(units: f64) -> Qty {
    Qty::from_mantissa((units * FIXED_SCALE as f64).round() as i64)
}

// The venue stamp sits 7 microseconds before the receipt on purpose. A fill keys off the received
// stamp only, and a fill that read the venue stamp would cross a horizon boundary here.
fn print(side: Side, units: f64, when: i64) -> TradeEvent {
    TradeEvent {
        instrument: InstrumentId(0),
        price: price(units),
        qty: Qty::from_mantissa(FIXED_SCALE),
        side,
        exchange_ts_us: venue_ts(when - 7),
        exchange_sent_ts_us: None,
    }
}

fn sized_print(side: Side, units: f64, when: i64, qty: Qty) -> TradeEvent {
    TradeEvent {
        qty,
        ..print(side, units, when)
    }
}

// Decimal mids are not exact in binary, so a markout differs from its hand value in its last bits.
fn assert_bps(realised: Option<f64>, expected: f64) {
    let realised = realised.expect("horizon has realised a markout");
    assert!(
        (realised - expected).abs() < 1e-9,
        "markout {realised} bps, expected {expected}"
    );
}

// Each quote arms at an empty level, so no queue holds a fill back. This tests the rule of a print
// at or through the level, and one fill for each placement.
#[test]
fn unqueued_quote_fills_at_or_through_once_per_placement() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));

    // A buying aggressor never reaches the bid, however far it prints.
    markouts.on_trade(&print(Side::Buy, 99.0, 0), at(0));
    assert_eq!(markouts.bid().fill_count(), 0);
    assert_eq!(markouts.bid().armed_quote(), Some(price(100.0)));

    // A print exactly at the level fills on its own. It comes before any deeper print.
    markouts.on_trade(&print(Side::Sell, 100.0, 0), at(0));
    assert_eq!(markouts.bid().fill_count(), 1);
    assert_eq!(markouts.bid().armed_quote(), None);

    // The next print through the level finds the side disarmed.
    markouts.on_trade(&print(Side::Sell, 99.0, secs(1)), at(secs(1)));
    assert_eq!(markouts.bid().fill_count(), 1);

    // A fresh placement makes the side fillable again, and a print through the level fills it.
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.on_trade(&print(Side::Sell, 99.0, secs(2)), at(secs(2)));
    assert_eq!(markouts.bid().fill_count(), 2);

    // The ask mirrors it: a seller never lifts it, and a buyer exactly at its level does.
    markouts.on_trade(&print(Side::Sell, 102.0, secs(2)), at(secs(2)));
    assert_eq!(markouts.ask().fill_count(), 0);
    markouts.on_trade(&print(Side::Buy, 101.0, secs(3)), at(secs(3)));
    assert_eq!(markouts.ask().fill_count(), 1);
    assert_eq!(markouts.ask().armed_quote(), None);

    // A print through the ask from above must fill too: prints exactly at the level could not show
    // that a print beyond it fills.
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));
    markouts.on_trade(&print(Side::Buy, 102.0, secs(4)), at(secs(4)));
    assert_eq!(markouts.ask().fill_count(), 2);

    // A disarm is not a placement: a side that we do not quote takes its level away, so prints find
    // nothing to fill.
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));
    markouts.disarm(MarkoutSide::Bid);
    markouts.disarm(MarkoutSide::Ask);
    markouts.on_trade(&print(Side::Sell, 99.0, secs(5)), at(secs(5)));
    markouts.on_trade(&print(Side::Buy, 102.0, secs(5)), at(secs(5)));
    assert_eq!(markouts.bid().fill_count(), 2);
    assert_eq!(markouts.ask().fill_count(), 2);
    assert_eq!(markouts.bid().armed_quote(), None);
    assert_eq!(markouts.ask().armed_quote(), None);
}

#[test]
fn forward_markouts_are_side_signed_and_ripen_per_horizon() {
    // ALL must list the horizons in the order of their discriminants: the lanes are built by
    // walking it and read back by discriminant, so a reordered ALL routes a horizon to the lane of
    // another. The build checks that order. The ripening times below pin it from outside, not
    // through the private index, and they must keep doing so: a horizon that reads the wrong lane
    // ripens at the wrong second.
    let mut markouts = tracker(secs(1));
    markouts.on_mid(at(0), 100.0);
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.arm(MarkoutSide::Ask, price(100.0), Qty::from_mantissa(0));
    markouts.on_trade(&print(Side::Sell, 100.0, 0), at(0));
    markouts.on_trade(&print(Side::Buy, 100.0, 0), at(0));

    markouts.on_mid(at(secs(1) - 1), 100.01);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs1).len(), 0);

    // A mid 1 cent above the fill at 100.00 is +1 bps for the bought bid and -1 bps for the sold
    // ask.
    markouts.on_mid(at(secs(1)), 100.01);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs1).last(), 1.0);
    assert_bps(markouts.ask().forward(ForwardHorizon::Secs1).last(), -1.0);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs3).len(), 0);

    // A deeper horizon realises when it elapses, against the mid at that time.
    markouts.on_mid(at(secs(3)), 99.98);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs3).last(), -2.0);
    assert_bps(markouts.ask().forward(ForwardHorizon::Secs3).last(), 2.0);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs5).len(), 0);

    // A sweep fills at our armed 100.00, not at the 99.90 of the print, so the markout is +1 bps,
    // not +110.
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.on_trade(&print(Side::Sell, 99.9, secs(10)), at(secs(10)));
    markouts.on_mid(at(secs(11)), 100.01);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs1).last(), 1.0);

    // A fill realises at the first mid at or after each horizon when that mid comes at most two
    // spins late. One microsecond more is a feed gap: the fill is dropped and counted.
    let mut gap = tracker(secs(1));
    gap.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    gap.on_trade(&print(Side::Sell, 100.0, 0), at(0));
    gap.on_mid(at(secs(3)), 100.01);
    assert_bps(gap.bid().forward(ForwardHorizon::Secs1).last(), 1.0);
    assert_bps(gap.bid().forward(ForwardHorizon::Secs3).last(), 1.0);
    assert_eq!(gap.bid().stale_maturation_count(), 0);
    gap.on_mid(at(secs(7) + 1), 100.02);
    assert_eq!(gap.bid().forward(ForwardHorizon::Secs5).len(), 0);
    assert_eq!(gap.bid().stale_maturation_count(), 1);
    // The dropped fill left its 5 s lane, and its 10 s lane still realises: +2 bps on 100.00.
    gap.on_mid(at(secs(12)), 100.02);
    assert_eq!(gap.bid().forward(ForwardHorizon::Secs5).len(), 0);
    assert_bps(gap.bid().forward(ForwardHorizon::Secs10).last(), 2.0);

    // At a 1 s spin the 1 s lane holds 1 + 2 fills in flight, and the 3 s lane 3 + 2. A fourth fill
    // before any mid is refused by the 1 s lane alone, and counted once.
    let mut burst = tracker(secs(1));
    for when in 0..4 {
        burst.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));
        burst.on_trade(&print(Side::Buy, 101.0, when), at(when));
        assert_eq!(burst.ask().pending_overflow_count(), u64::from(when == 3));
    }
    assert_eq!(burst.ask().fill_count(), 4);
    burst.on_mid(at(secs(1) + 3), 101.0);
    assert_eq!(burst.ask().forward(ForwardHorizon::Secs1).len(), 3);
    burst.on_mid(at(secs(3) + 3), 101.0);
    assert_eq!(burst.ask().forward(ForwardHorizon::Secs3).len(), 4);

    // Both resets keep the lifetime counters.
    gap.reset_continuity();
    burst.reset_continuity();
    assert_eq!(gap.bid().stale_maturation_count(), 1);
    assert_eq!(burst.ask().pending_overflow_count(), 1);
    gap.clear();
    burst.clear();
    assert_eq!(gap.bid().stale_maturation_count(), 1);
    assert_eq!(burst.ask().pending_overflow_count(), 1);
}

#[test]
fn reverse_markouts_read_the_mid_path_and_resets_keep_what_was_realised() {
    let mut markouts = tracker(secs(1));
    markouts.on_mid(at(0), 99.0);
    markouts.on_mid(at(secs(1) - 100_000), 100.0);
    markouts.on_mid(at(secs(1) + 50_000), 101.0);
    markouts.on_mid(at(secs(2)), 100.01);
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.arm(MarkoutSide::Ask, price(100.0), Qty::from_mantissa(0));
    markouts.on_trade(&print(Side::Sell, 100.0, secs(2)), at(secs(2)));
    markouts.on_trade(&print(Side::Buy, 100.0, secs(2)), at(secs(2)));

    // One second back from the fills at 2 s is 1 s. The newest mid at or before it is 100.00 at
    // 0.9 s, not the nearer 101.00 at 1.05 s, so the mid rose 1 bps into the fills: +1 bps for the
    // bought bid, -1 bps for the sold ask.
    assert_bps(markouts.bid().reverse(ReverseHorizon::Secs1).last(), 1.0);
    assert_bps(markouts.ask().reverse(ReverseHorizon::Secs1).last(), -1.0);
    // Five seconds back is before the first mid: no sample, and one gap for each side.
    assert_eq!(markouts.bid().reverse(ReverseHorizon::Secs5).len(), 0);
    assert_eq!(markouts.bid().reverse_gap_count(), 1);
    assert_eq!(markouts.ask().reverse_gap_count(), 1);

    // A resync drops the quotes, the fills in flight and the mid path, and keeps what was realised.
    markouts.on_mid(at(secs(3)), 100.01);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs1).last(), 1.0);
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.reset_continuity();
    assert_eq!(markouts.bid().armed_quote(), None);
    assert_eq!(markouts.ask().armed_quote(), None);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs1).len(), 1);
    assert_eq!(markouts.bid().reverse(ReverseHorizon::Secs1).len(), 1);
    assert_eq!(markouts.bid().fill_count(), 1);
    // A fill right after the resync finds no mid to read back to: one gap for each horizon.
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.on_trade(
        &print(Side::Sell, 100.0, secs(3) + 500_000),
        at(secs(3) + 500_000),
    );
    assert_eq!(markouts.bid().reverse(ReverseHorizon::Secs1).len(), 1);
    assert_eq!(markouts.bid().reverse_gap_count(), 3);
    // At 5 s the fills from 2 s would realise their 3 s markouts, but the resync dropped them. The
    // fill from 3.5 s realises its 1 s markout: +2 bps on 100.00.
    markouts.on_mid(at(secs(5)), 100.02);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs3).len(), 0);
    assert_eq!(markouts.ask().forward(ForwardHorizon::Secs3).len(), 0);
    assert_eq!(markouts.bid().forward(ForwardHorizon::Secs1).len(), 2);
    assert_bps(markouts.bid().forward(ForwardHorizon::Secs1).last(), 2.0);

    // A clear wipes the realised series too. The lifetime counters stay.
    markouts.clear();
    for side in [markouts.bid(), markouts.ask()] {
        for horizon in ForwardHorizon::ALL {
            assert_eq!(side.forward(horizon).len(), 0);
        }
        for horizon in ReverseHorizon::ALL {
            assert_eq!(side.reverse(horizon).len(), 0);
        }
    }
    assert_eq!(markouts.bid().fill_count(), 2);
    assert_eq!(markouts.bid().reverse_gap_count(), 3);
    assert_eq!(markouts.ask().fill_count(), 1);
    assert_eq!(markouts.ask().reverse_gap_count(), 1);

    // A ring sized for one mid a second holds six mids. Fed two a second, it reaches back 2.5 s, so
    // the 5 s lookback finds nothing although mids were fed from 0 s on.
    let mut short = MarkoutTracker::new(MarkoutSpec {
        spin_interval: DurationUs::from_micros(secs(1)),
        max_mids_per_sec: 1,
    });
    for half in 0..=12 {
        let mid = if half == 12 { 100.01 } else { 100.0 };
        short.on_mid(at(half * 500_000), mid);
    }
    short.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    short.on_trade(&print(Side::Sell, 100.0, secs(6)), at(secs(6)));
    assert_bps(short.bid().reverse(ReverseHorizon::Secs1).last(), 1.0);
    assert_eq!(short.bid().reverse(ReverseHorizon::Secs5).len(), 0);
    assert_eq!(short.bid().reverse_gap_count(), 1);
}

#[test]
fn on_trade_reports_the_fill_it_records() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));

    // A print that reaches no armed level reports nothing.
    assert_eq!(markouts.on_trade(&print(Side::Buy, 99.0, 0), at(0)), None);

    // A seller through the bid fills it at our armed 100.00, stamped at receipt.
    assert_eq!(
        markouts.on_trade(&print(Side::Sell, 99.5, secs(1)), at(secs(1))),
        Some(MarkoutFill {
            side: MarkoutSide::Bid,
            price: price(100.0),
            fill_ts_us: at(secs(1)),
        })
    );
    // The side is disarmed now, so the next print reports nothing.
    assert_eq!(
        markouts.on_trade(&print(Side::Sell, 99.0, secs(2)), at(secs(2))),
        None
    );

    // The ask mirrors it: a buyer that lifts it fills at our armed 101.00.
    assert_eq!(
        markouts.on_trade(&print(Side::Buy, 101.0, secs(3)), at(secs(3))),
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

    // Prints at the level eat the queue. After 2 of 10, 8 are still ahead of us.
    markouts.on_trade(&sized_print(Side::Sell, 100.0, 0, qty(2.0)), at(0));
    assert_eq!(markouts.bid().fill_count(), 0);
    assert_eq!(markouts.bid().armed_quote(), Some(price(100.0)));

    // A buying aggressor never reaches the bid, so it eats none of the queue.
    markouts.on_trade(&sized_print(Side::Buy, 100.0, 0, qty(7.0)), at(0));
    assert_eq!(markouts.bid().fill_count(), 0);

    // With 8.9 of 10 eaten, the quote is a tenth short of its threshold.
    markouts.on_trade(
        &sized_print(Side::Sell, 100.0, secs(1), qty(6.9)),
        at(secs(1)),
    );
    assert_eq!(markouts.bid().fill_count(), 0);

    // Exactly 9 of 10 fills: the gate is >=, so the boundary goes to us.
    markouts.on_trade(
        &sized_print(Side::Sell, 100.0, secs(2), qty(0.1)),
        at(secs(2)),
    );
    assert_eq!(markouts.bid().fill_count(), 1);
    assert_eq!(markouts.bid().armed_quote(), None);

    // A print larger than the whole queue clears the threshold on its own.
    markouts.arm(MarkoutSide::Ask, price(101.0), qty(10.0));
    markouts.on_trade(
        &sized_print(Side::Buy, 101.0, secs(3), qty(50.0)),
        at(secs(3)),
    );
    assert_eq!(markouts.ask().fill_count(), 1);

    // A print through the level swept each order there, so the queue does not count and the
    // smallest print fills.
    markouts.arm(MarkoutSide::Ask, price(101.0), qty(1000.0));
    markouts.on_trade(
        &sized_print(Side::Buy, 101.5, secs(4), qty(0.001)),
        at(secs(4)),
    );
    assert_eq!(markouts.ask().fill_count(), 2);
}

#[test]
fn rearm_keeps_queue_only_while_armed_at_same_price() {
    let mut markouts = tracker(secs(1));
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(10.0));
    markouts.on_trade(&sized_print(Side::Sell, 100.0, 0, qty(8.0)), at(0));
    assert_eq!(markouts.bid().fill_count(), 0);

    // A re-arm at the same level, now showing 500, keeps the place earned while it stayed armed: 8
    // eaten and 1 more are 9 of the 10 ahead at the first arm.
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(500.0));
    markouts.on_trade(
        &sized_print(Side::Sell, 100.0, secs(1), qty(1.0)),
        at(secs(1)),
    );
    assert_eq!(markouts.bid().fill_count(), 1);

    // The fill disarmed the side, so a re-arm at the same price joins the back of the new queue,
    // where 9 of 500 is far short.
    markouts.arm(MarkoutSide::Bid, price(100.0), qty(500.0));
    markouts.on_trade(
        &sized_print(Side::Sell, 100.0, secs(2), qty(9.0)),
        at(secs(2)),
    );
    assert_eq!(markouts.bid().fill_count(), 1);

    // A new level is a fresh join: nothing eaten at 100 counts at 99.
    markouts.arm(MarkoutSide::Bid, price(99.0), qty(10.0));
    markouts.on_trade(
        &sized_print(Side::Sell, 99.0, secs(3), qty(1.0)),
        at(secs(3)),
    );
    assert_eq!(markouts.bid().fill_count(), 1);
    markouts.on_trade(
        &sized_print(Side::Sell, 99.0, secs(4), qty(8.0)),
        at(secs(4)),
    );
    assert_eq!(markouts.bid().fill_count(), 2);
}

#[test]
fn a_late_mid_is_raised_to_the_last_stamp() {
    // Mids at 10 s and 11 s, then one stamped 10.5 s. It is held at 11 s, after the mids before it,
    // so the ring stays in time order in each build.
    let mut late = tracker(secs(1));
    late.on_mid(at(secs(10)), 100.00);
    late.on_mid(at(secs(11)), 100.01);
    late.on_mid(at(secs(10) + 500_000), 100.02);
    let mut raised = tracker(secs(1));
    raised.on_mid(at(secs(10)), 100.00);
    raised.on_mid(at(secs(11)), 100.01);
    raised.on_mid(at(secs(11)), 100.02);
    assert_eq!(late, raised, "the late mid holds the stamp 11 s");

    // The sample at or before 11 s is the newest mid at 11 s. A bid fill at 12 s reads back to it,
    // and the latest mid is that same sample, so the mid did not move into the fill: 0 bps. The
    // older mid at 11 s would give (100.02 - 100.01) / 100.01, about 1 bps.
    late.arm(MarkoutSide::Bid, price(100.0), Qty::from_mantissa(0));
    late.on_trade(&print(Side::Sell, 100.0, secs(12)), at(secs(12)));
    assert_eq!(late.bid().reverse(ReverseHorizon::Secs1).last(), Some(0.0));

    // A resync keeps the clamp: after it, a mid stamped before the last fed stamp is still raised.
    let mut resynced = tracker(secs(1));
    resynced.on_mid(at(secs(11)), 100.01);
    resynced.reset_continuity();
    resynced.on_mid(at(secs(10)), 100.00);
    let mut held = tracker(secs(1));
    held.on_mid(at(secs(11)), 100.01);
    held.reset_continuity();
    held.on_mid(at(secs(11)), 100.00);
    assert_eq!(resynced, held, "a resync keeps the last fed stamp");

    // A clear starts the clock again: after it, the same feed gives the state of a new tracker.
    let mut cleared = tracker(secs(1));
    cleared.on_mid(at(secs(11)), 100.01);
    cleared.clear();
    cleared.on_mid(at(secs(10)), 100.00);
    let mut fresh = tracker(secs(1));
    fresh.on_mid(at(secs(10)), 100.00);
    assert_eq!(cleared, fresh, "a clear forgets the last fed stamp");
}

#[test]
fn arm_refuses_a_price_that_is_not_positive() {
    // A price at or below zero is no level, and the forward markout divides by the armed price.
    let mut markouts = tracker(secs(1));
    markouts.on_mid(at(0), 100.0);
    markouts.arm(
        MarkoutSide::Bid,
        Price::from_mantissa(0),
        Qty::from_mantissa(0),
    );
    markouts.arm(
        MarkoutSide::Ask,
        Price::from_mantissa(-1),
        Qty::from_mantissa(0),
    );
    assert_eq!(markouts.bid().armed_quote(), None);
    assert_eq!(markouts.ask().armed_quote(), None);
    // A buyer at any price is through an ask at -1, so an armed ask would fill here.
    markouts.on_trade(&print(Side::Buy, 100.0, 0), at(0));
    assert_eq!(markouts.ask().fill_count(), 0);

    // The refusal also withdraws the quote that the side held, because that level is no longer
    // shown, and a print must not fill it.
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));
    markouts.arm(
        MarkoutSide::Ask,
        Price::from_mantissa(0),
        Qty::from_mantissa(0),
    );
    assert_eq!(markouts.ask().armed_quote(), None);
    markouts.on_trade(&print(Side::Buy, 101.0, secs(1)), at(secs(1)));
    assert_eq!(markouts.ask().fill_count(), 0);

    // A positive price still arms the side after the refusals.
    markouts.arm(MarkoutSide::Ask, price(101.0), Qty::from_mantissa(0));
    assert_eq!(markouts.ask().armed_quote(), Some(price(101.0)));
}
