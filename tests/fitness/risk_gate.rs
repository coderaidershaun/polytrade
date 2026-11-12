//! Risk-gate fitness: the exposure ceiling must never trap a position it cannot unwind.
//!
//! A gate that withdrew both sides would deadlock: nothing quoting means nothing fills, and nothing
//! filling means the position never comes down. Exposure is marked to market, so a price move alone
//! can carry it past the ceiling with no fill at all, and only a test that moves the mark apart from
//! the fills can see that. The engine's own ceiling and its consecutive-reject kill switch live here
//! too, because they are the controls a strategy bug cannot bypass.

use polysim::config::RecordedTables;
use polysim::exposure::InstrumentExposure;
use polysim::hot::dispatch::HotEngine;
use polysim::hot::exec::{
    ClientIdLayout, ExecHalt, ExposureCheck, HaltReason, QuotePermission, assess_exposure,
    side_base,
};
use polysim::ids::{Price, Qty, Side};
use polysim::msg::exec::{ExecCommand, ExecEvent, ExecKind, RejectClass, VenueOrderStatus};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::ui::UiEvent;
use proptest::prelude::*;
use rtrb::Consumer;

use crate::engine_support::{ALL_TABLES, ONE, exec_event, pop};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, QuotingEngine, QuotingSetup, TwoSidedQuoter,
    built_quoting_engine, drain_commands, exec_settings, has_halted, is_placing, make_ready,
    quoting_engine, reseat_book, row_with_ceiling, spin_at,
};
use crate::micro_strategy::models::{RiskBudget, signed};

proptest! {
    /// FITNESS: whatever the exposure, at least one side is always quotable. This is the deadlock
    /// stated directly rather than through one scenario — a gate that can withdraw both sides at
    /// once has no way back to flat, and no fill can arrive to prove it wrong.
    #[test]
    fn some_side_is_always_quotable(
        exposure_units in -500i64..=500,
        // The startup assert in `InstrumentState::new` refuses a run where Δ exceeds the ceiling, so
        // the ranges are disjoint rather than filtered — a rejected case would be one the engine
        // never boots into.
        order_units in 1i64..=50,
        max_units in 50i64..=200,
    ) {
        let budget = RiskBudget {
            order_notional: order_units * ONE,
            max_exposure_quote: max_units * ONE,
        };
        let exposure = exposure_units * ONE;
        prop_assert!(
            !budget.would_breach(exposure, Side::Buy) || !budget.would_breach(exposure, Side::Sell),
            "both sides withdrawn at exposure {} with Δ {} and ceiling {} — the position is trapped",
            exposure,
            budget.order_notional,
            budget.max_exposure_quote,
        );
    }

    /// FITNESS: a side that strictly shrinks the position is never withdrawn, at any exposure. The
    /// ceiling exists to stop the position growing, so it must never be what stops it shrinking.
    #[test]
    fn a_side_that_shrinks_the_position_is_never_withdrawn(
        exposure_units in -500i64..=500,
        order_units in 1i64..=50,
        max_units in 50i64..=200,
    ) {
        let budget = RiskBudget {
            order_notional: order_units * ONE,
            max_exposure_quote: max_units * ONE,
        };
        let exposure = exposure_units * ONE;
        for side in [Side::Buy, Side::Sell] {
            let projected = exposure + signed(side, budget.order_notional);
            if projected.abs() < exposure.abs() {
                prop_assert!(
                    !budget.would_breach(exposure, side),
                    "{:?} takes exposure from {} to {} — strictly smaller, yet withdrawn",
                    side,
                    exposure,
                    projected,
                );
            }
        }
    }
}

/// The mid of `BID` and `ASK`, which is what the ledger marks against.
const MARK: i64 = 100 * ONE;

fn ceiling_check(side: Side, position_base: i64, has_mark: bool) -> ExposureCheck {
    ExposureCheck {
        exposure_quote: if has_mark { Price(MARK).notional(Qty(position_base)) } else { 0 },
        position_base: Qty(position_base),
        has_mark,
        side,
        price: Price(match side {
            Side::Buy => BID,
            Side::Sell => ASK,
        }),
        qty: QUOTE_QTY,
        ceiling_quote: CEILING,
    }
}

/// FITNESS: `assess_exposure` verdicts at the ceiling match a named table. From over the ceiling the
/// engine withdraws the side that would ADD to the position and keeps the side that would reduce it —
/// the reducing side admitted by construction rather than by arithmetic, so no rounding or sign error
/// can reach it — and an UNMARKED position is not a flat one: the ledger's mark is `None` until the
/// first two-sided committed book, exposure reads 0 in that window too, and only `has_mark` tells the
/// two zeroes apart, so a restarted engine holding unvalued inventory must refuse to grow it rather
/// than read itself as flat.
#[test]
fn assess_exposure_ceiling_verdicts_match_named_cases() {
    struct Case {
        name: &'static str,
        side: Side,
        position_base: i64,
        has_mark: bool,
        exposure_override: Option<i64>,
        expected: QuotePermission,
    }
    let cases = [
        Case {
            name: "long_at_the_ceiling_buy_is_withdrawn",
            side: Side::Buy,
            position_base: ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::ReducingOnly {
                reducing: Side::Sell,
            },
        },
        Case {
            name: "long_at_the_ceiling_sell_is_the_way_back",
            side: Side::Sell,
            position_base: ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        // Twice the ceiling — the regime this file was written for, since a price move alone can
        // carry exposure there with no fill at all. The unwinding side is not merely permitted, it
        // is unobjectionable: a gate answering "restricted, but not on this side" from deep over the
        // budget is reasoning about the position when it was asked about the way out of it.
        Case {
            name: "twice_the_ceiling_the_unwinding_side_is_unobjectionable",
            side: Side::Sell,
            position_base: 2 * ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        Case {
            name: "short_at_the_ceiling_sell_is_withdrawn",
            side: Side::Sell,
            position_base: -ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::ReducingOnly {
                reducing: Side::Buy,
            },
        },
        Case {
            name: "short_at_the_ceiling_buy_is_the_way_back",
            side: Side::Buy,
            position_base: -ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        Case {
            name: "an_unmarked_long_refuses_the_growing_side",
            side: Side::Buy,
            position_base: ONE,
            has_mark: false,
            exposure_override: None,
            expected: QuotePermission::ReducingOnly {
                reducing: Side::Sell,
            },
        },
        Case {
            name: "an_unmarked_long_stays_reducible",
            side: Side::Sell,
            position_base: ONE,
            has_mark: false,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        // Without this case the branch reading has_mark could be deleted and every other assertion
        // in this table would still pass: a mark of zero is an honest valuation, distinct from no
        // mark at all, and threatens the ceiling not at all.
        Case {
            name: "a_mark_of_zero_is_an_honest_valuation",
            side: Side::Buy,
            position_base: ONE,
            has_mark: true,
            exposure_override: Some(0),
            expected: QuotePermission::Both,
        },
    ];
    for case in cases {
        let mut check = ceiling_check(case.side, case.position_base, case.has_mark);
        if let Some(exposure_quote) = case.exposure_override {
            check.exposure_quote = exposure_quote;
        }
        let got = assess_exposure(check);
        assert_eq!(
            got, case.expected,
            "case {}: got {:?}, want {:?}",
            case.name, got, case.expected
        );
    }
}

/// FITNESS: the projection is the order's REAL notional. The strategy must guess with a fixed
/// per-fill step because it has no price for a fill that has not happened; the engine holds the
/// price and size about to be sent, and two quotes that differ only in size must be judged
/// differently. A gate stepping by a constant cannot tell these two apart.
#[test]
fn the_ceiling_projects_the_declared_notional_rather_than_a_fixed_step() {
    // Nine tenths of the way to the ceiling, so the two sizes below straddle it.
    let position = ONE * 9 / 10;
    let small = ExposureCheck {
        qty: Qty(ONE / 10),
        ..ceiling_check(Side::Buy, position, true)
    };
    assert_eq!(
        small.exposure_quote + Price(BID).notional(small.qty),
        CEILING - ONE / 10,
        "the small quote lands just inside the ceiling"
    );
    assert_eq!(
        assess_exposure(small),
        QuotePermission::Both,
        "so it is admitted"
    );

    let large = ExposureCheck {
        qty: Qty(ONE / 5),
        ..small
    };
    assert!(
        large.exposure_quote + Price(BID).notional(large.qty) > CEILING,
        "twice the size clears the ceiling — the scenario needs both sides of it"
    );
    assert_eq!(
        assess_exposure(large),
        QuotePermission::ReducingOnly {
            reducing: Side::Sell
        },
        "the same position and the same ceiling, refused only because the ORDER is bigger"
    );
}

proptest! {
    /// FITNESS: the engine's ceiling never withdraws both sides, whatever the position, the mark,
    /// the order size or the budget. Stated over the gate itself rather than through one scenario:
    /// a control that can withdraw both sides at once has no way back to flat.
    #[test]
    fn the_engine_ceiling_always_leaves_a_side_quotable(
        position_units in -50i64..=50,
        mark_units in 1i64..=500,
        qty_hundredths in 0i64..=500,
        ceiling_units in 1i64..=500,
        has_mark in any::<bool>(),
    ) {
        let position_base = Qty(position_units * ONE);
        let mark = Price(mark_units * ONE);
        let check = |side: Side| ExposureCheck {
            exposure_quote: if has_mark { mark.notional(position_base) } else { 0 },
            position_base,
            has_mark,
            side,
            price: mark,
            qty: Qty(qty_hundredths * ONE / 100),
            ceiling_quote: ceiling_units * ONE,
        };
        let buy = assess_exposure(check(Side::Buy));
        let sell = assess_exposure(check(Side::Sell));
        prop_assert!(
            buy.admits(Side::Buy) || sell.admits(Side::Sell),
            "both sides withdrawn holding {} at mark {} against ceiling {} — the position is trapped",
            position_base.0,
            mark.0,
            ceiling_units * ONE,
        );
    }

    /// FITNESS: whatever the numbers, the side that shrinks a held position is admitted. The
    /// companion to the property above and the stronger statement — it names WHICH side survives,
    /// so a gate that satisfied "some side quotes" by keeping the wrong one still fails here.
    #[test]
    fn the_engine_ceiling_never_withdraws_the_reducing_side(
        position_units in -50i64..=50,
        mark_units in 1i64..=500,
        qty_hundredths in 0i64..=500,
        ceiling_units in 1i64..=500,
        has_mark in any::<bool>(),
    ) {
        let position_base = Qty(position_units * ONE);
        let mark = Price(mark_units * ONE);
        let reducing = match position_units.signum() {
            1 => Side::Sell,
            -1 => Side::Buy,
            _ => return Ok(()),
        };
        let permission = assess_exposure(ExposureCheck {
            exposure_quote: if has_mark { mark.notional(position_base) } else { 0 },
            position_base,
            has_mark,
            side: reducing,
            price: mark,
            qty: Qty(qty_hundredths * ONE / 100),
            ceiling_quote: ceiling_units * ONE,
        });
        prop_assert!(
            permission.admits(reducing),
            "{:?} shrinks a position of {} yet was withdrawn",
            reducing,
            position_base.0,
        );
    }
}

/// One real fill through the inbound path: adopt the order from a reconciliation snapshot, then
/// report it FULLY filled. Closing it matters — a side still holding a working order is blocked by
/// the in-flight rule, and a test reading "no order placed" there would be reading that rule rather
/// than the ceiling.
fn fill_and_close(engine: &mut HotEngine, side: Side, price: i64, qty: i64, when: i64) {
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(side_base(INSTRUMENT, side) + 1, 1);
    let base = ExecEvent {
        qty: Qty(qty),
        ..exec_event(INSTRUMENT, client_id, side, price, when)
    };
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            ..base
        }),
    );
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::Filled),
            last_price: Price(price),
            last_qty: Qty(qty),
            cumulative_qty: Qty(qty),
            cumulative_quote: Price(price).notional(Qty(qty)),
            ..base
        }),
    );
}

/// The message sequence both engine-driven tests below share: come ready, take a long position by
/// the route the caller chose, then see the book that values it at exactly the ceiling.
fn quote_holding_one_unit(
    ceiling_quote: i64,
    restored: &[InstrumentExposure],
    fill: Option<Side>,
) -> Vec<ExecCommand> {
    let (mut engine, mut commands) = quoting_engine(ceiling_quote, restored);
    make_ready(&mut engine, 0);
    if let Some(side) = fill {
        fill_and_close(&mut engine, side, MARK, ONE, 5);
    }
    for message in reseat_book(BID, ASK, 10) {
        engine.dispatch(pop(0, 0), &message);
    }
    spin_at(&mut engine, 1, 20);
    drain_commands(&mut commands)
}

/// FITNESS: a position onto the ceiling withdraws the adding side at the ENGINE, whatever the
/// strategy keeps declaring — and leaves the reducing side quoting — whether the position arrived
/// through a real fill this run or was inherited whole from a restart.
///
/// The fill case is driven twice over one message sequence, differing only in the row's
/// `max_exposure_quote`. The loose run is what makes the tight one mean something: it proves this
/// fixture really does reach the quoting path, so "no order on the buy side" is the ceiling refusing
/// and not the fixture failing to ask. The restored case then proves the same gate reads a
/// cross-session ledger rather than starting the run believing itself flat.
#[test]
fn a_position_at_the_ceiling_stops_the_engine_adding_whether_filled_or_restored() {
    let tight = quote_holding_one_unit(CEILING, &[], Some(Side::Buy));
    assert!(
        !is_placing(&tight, Side::Buy),
        "the position is marked at the ceiling, so nothing may grow it: {tight:?}"
    );
    assert!(
        is_placing(&tight, Side::Sell),
        "and the side that unwinds it must still quote — no quote means no fill, and no fill means \
         no way down: {tight:?}"
    );

    let loose = quote_holding_one_unit(1_000_000 * ONE, &[], Some(Side::Buy));
    assert!(
        is_placing(&loose, Side::Buy),
        "the same sequence under a ceiling far away must quote both sides, or the test above is \
         reading a fixture that never asked for an order: {loose:?}"
    );
    assert!(
        is_placing(&loose, Side::Sell),
        "and the other side: {loose:?}"
    );

    // An engine that BOOTS holding inventory is held to the same ceiling. The ledger is
    // cross-session, so a restart inherits a position no fill this run produced — and the gate must
    // read it off the restored cost basis rather than starting the run believing itself flat.
    let restored = [InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty(ONE),
        cash_quote: -MARK,
        basis_quote: MARK,
    }];
    let inherited = quote_holding_one_unit(CEILING, &restored, None);
    assert!(
        !is_placing(&inherited, Side::Buy),
        "the inherited position is already at the ceiling: {inherited:?}"
    );
    assert!(
        is_placing(&inherited, Side::Sell),
        "and unwinding it is exactly what a restart holding inventory needs to do: {inherited:?}"
    );
}

/// FITNESS: only the VENUE can end a hard-reject streak, so the engine's own refusals never postpone
/// the kill switch.
///
/// The engine synthesises events for requests that never left the process, and one of them leaves
/// its order reading `Live` — an amend the edge would not send, which changes nothing about an order
/// still resting. Nobody at the venue accepted anything there. Reading it as acceptance resets the
/// streak, and the arrangement that produces those refusals is a dead socket, which is exactly when
/// the venue is most likely to be rejecting everything else too.
#[test]
fn an_edge_refusal_never_ends_a_venue_reject_streak() {
    let mut halting = engine_holding_one_live_order();
    for reject in 0..REJECT_STREAK {
        dispatch_exec(&mut halting.engine, hard_reject(100 + i64::from(reject)));
    }
    spin_at(&mut halting.engine, 1, 200);
    assert!(
        has_halted(&mut halting.commands),
        "the streak the kill switch is configured for did not halt, so the case below proves nothing"
    );

    let mut interrupted = engine_holding_one_live_order();
    for reject in 0..REJECT_STREAK - 1 {
        dispatch_exec(
            &mut interrupted.engine,
            hard_reject(100 + i64::from(reject)),
        );
    }
    dispatch_exec(
        &mut interrupted.engine,
        ExecEvent {
            kind: ExecKind::AmendNotSent,
            ..live_order_event(150)
        },
    );
    dispatch_exec(&mut interrupted.engine, hard_reject(160));
    spin_at(&mut interrupted.engine, 1, 200);
    assert!(
        has_halted(&mut interrupted.commands),
        "an amend the engine refused itself reset the venue's reject count, and the kill switch \
         never fired"
    );
}

/// FITNESS: a post-only refusal neither extends nor ends a hard-reject streak.
///
/// A maker earns these all day. Counting one toward the kill switch parks the engine exactly when
/// the market is moving; letting one reset the count lets a venue that refuses everything else keep
/// the engine quoting for as long as a single quote crosses now and then. The halt is read off the
/// workstation feed, so the reason asserted is the one an operator is shown.
#[test]
fn a_post_only_refusal_neither_extends_nor_ends_a_hard_reject_streak() {
    let mut extended = engine_holding_one_live_order();
    for reject in 0..REJECT_STREAK - 1 {
        dispatch_exec(&mut extended.engine, hard_reject(100 + i64::from(reject)));
    }
    for refusal in 0..REJECT_STREAK {
        dispatch_exec(
            &mut extended.engine,
            refused_reject(150 + i64::from(refusal)),
        );
    }
    spin_at(&mut extended.engine, 1, 200);
    assert!(
        !has_halted(&mut extended.commands),
        "post-only refusals were counted toward the hard-reject streak and halted the engine"
    );
    assert_eq!(
        latest_halt(&mut extended.ui_events),
        Some(ExecHalt::Armed),
        "one hard reject short of the limit, the kill switch must still read armed"
    );

    let mut ended = engine_holding_one_live_order();
    for reject in 0..REJECT_STREAK - 1 {
        dispatch_exec(&mut ended.engine, hard_reject(100 + i64::from(reject)));
    }
    dispatch_exec(&mut ended.engine, refused_reject(150));
    dispatch_exec(&mut ended.engine, hard_reject(160));
    spin_at(&mut ended.engine, 1, 200);
    assert!(
        has_halted(&mut ended.commands),
        "a post-only refusal reset the hard-reject streak, and the kill switch never fired"
    );
    let halt = latest_halt(&mut ended.ui_events);
    assert!(
        matches!(
            halt,
            Some(ExecHalt::Halted {
                reason: HaltReason::RejectStreak,
                ..
            })
        ),
        "the halt must be reported as the reject streak: {halt:?}"
    );
}

/// The configured streak in [`exec_settings`], named so the case above cannot drift from it.
const REJECT_STREAK: u32 = 5;

/// A ready engine holding one order the venue's answers can land on. Rejects fold onto a slot or
/// nowhere at all, so without a seated order every answer below would be counted as an orphan.
fn engine_holding_one_live_order() -> QuotingEngine {
    let mut built = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(1_000_000 * ONE),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings: exec_settings(),
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    make_ready(&mut built.engine, 0);
    dispatch_exec(
        &mut built.engine,
        ExecEvent {
            kind: ExecKind::SnapshotOrder,
            ..live_order_event(10)
        },
    );
    dispatch_exec(
        &mut built.engine,
        ExecEvent {
            kind: ExecKind::ReportNew,
            ..live_order_event(20)
        },
    );
    built
}

fn live_order_event(when: i64) -> ExecEvent {
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(side_base(INSTRUMENT, Side::Buy) + 1, 1);
    ExecEvent {
        qty: QUOTE_QTY,
        ..exec_event(INSTRUMENT, client_id, Side::Buy, BID, when)
    }
}

/// The venue refusing a request against an order it is still holding — the class that counts toward
/// the streak, as opposed to the post-only refusals a maker earns all day.
fn hard_reject(when: i64) -> ExecEvent {
    ExecEvent {
        kind: ExecKind::AckFailed,
        reject: Some(RejectClass::StillLive),
        ..live_order_event(when)
    }
}

/// The venue refusing a post-only order the book has moved through: exactly what post-only asks for,
/// and the routine cost of making markets.
fn refused_reject(when: i64) -> ExecEvent {
    ExecEvent {
        kind: ExecKind::AckFailed,
        reject: Some(RejectClass::Refused),
        ..live_order_event(when)
    }
}

fn dispatch_exec(engine: &mut HotEngine, event: ExecEvent) {
    engine.dispatch(pop(0, 0), &InboundMessage::Exec(event));
}

/// The kill switch as the workstation was last told it. The engine restates it every spin, so the
/// newest frame is its current answer.
fn latest_halt(ui_events: &mut Consumer<UiEvent>) -> Option<ExecHalt> {
    std::iter::from_fn(|| ui_events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Execution { halt, .. } => Some(halt),
            _ => None,
        })
        .last()
}
