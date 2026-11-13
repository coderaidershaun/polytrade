//! The kill switch on rejects in a row: only the venue's own refusals count toward it, and only
//! the venue can end a streak, because a strategy bug cannot get past it.

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{ClientIdLayout, HaltReason, side_base};
use engine::msg::exec::{ExecEvent, ExecKind, RejectClass};
use engine::msg::ids::Side;
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::LocalTs;
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::RecordedTables;
use rtrb::Consumer;

use crate::engine_support::{ALL_TABLES, ONE, exec_event, ts};
use crate::exec_support::{
    BID, INSTRUMENT, QUOTE_QTY, QuotingEngine, QuotingSetup, TwoSidedQuoter, built_quoting_engine,
    exec_settings, has_halted, make_ready, row_with_ceiling, spin_at,
};

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
        dispatch_exec(
            &mut halting.engine,
            hard_reject(100 + i64::from(reject)),
            ts(100 + i64::from(reject)),
        );
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
            ts(100 + i64::from(reject)),
        );
    }
    dispatch_exec(
        &mut interrupted.engine,
        ExecEvent {
            kind: ExecKind::AmendNotSent,
            ..live_order_event(150)
        },
        ts(150),
    );
    dispatch_exec(&mut interrupted.engine, hard_reject(160), ts(160));
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
        dispatch_exec(
            &mut extended.engine,
            hard_reject(100 + i64::from(reject)),
            ts(100 + i64::from(reject)),
        );
    }
    for refusal in 0..REJECT_STREAK {
        dispatch_exec(
            &mut extended.engine,
            refused_reject(150 + i64::from(refusal)),
            ts(150 + i64::from(refusal)),
        );
    }
    spin_at(&mut extended.engine, 1, 200);
    assert!(
        !has_halted(&mut extended.commands),
        "post-only refusals were counted toward the hard-reject streak and halted the engine"
    );
    assert_eq!(
        latest_halt(&mut extended.ui_events),
        Some(UiHalt::Armed),
        "one hard reject short of the limit, the kill switch must still read armed"
    );

    let mut ended = engine_holding_one_live_order();
    for reject in 0..REJECT_STREAK - 1 {
        dispatch_exec(
            &mut ended.engine,
            hard_reject(100 + i64::from(reject)),
            ts(100 + i64::from(reject)),
        );
    }
    dispatch_exec(&mut ended.engine, refused_reject(150), ts(150));
    dispatch_exec(&mut ended.engine, hard_reject(160), ts(160));
    spin_at(&mut ended.engine, 1, 200);
    assert!(
        has_halted(&mut ended.commands),
        "a post-only refusal reset the hard-reject streak, and the kill switch never fired"
    );
    let halt = latest_halt(&mut ended.ui_events);
    assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
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
        ts(10),
    );
    dispatch_exec(
        &mut built.engine,
        ExecEvent {
            kind: ExecKind::ReportNew,
            ..live_order_event(20)
        },
        ts(20),
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

fn dispatch_exec(engine: &mut HotEngine, event: ExecEvent, received_ts_us: LocalTs) {
    engine.dispatch(&InboundMessage::new(
        received_ts_us,
        MessageBody::Exec(event),
    ));
}

/// The kill switch as the workstation was last told it. The engine restates it every spin, so the
/// newest frame is its current answer.
fn latest_halt(ui_events: &mut Consumer<UiEvent>) -> Option<UiHalt> {
    std::iter::from_fn(|| ui_events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Execution { halt, .. } => Some(halt),
            _ => None,
        })
        .last()
}
