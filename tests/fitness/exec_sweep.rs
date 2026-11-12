//! FITNESS: reconciliation may retire only orders that predate its question, and conservative side
//! occupancy must halt rather than admit a quote beyond the configured cap.

use polysim::config::{RecordedTables, TableKind};
use polysim::hot::dispatch::HotEngine;
use polysim::hot::exec::{
    ClientIdLayout, ExecHalt, ExecSettings, HaltReason, OrderClaim, OrderState, OrderTable,
    QuoteLevel, ReconcilePass, apply_exec_event, side_base,
};
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, OrderStyle, RejectClass, VenueOrderStatus,
};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::ui::UiEvent;
use polysim::time::TsUs;
use rtrb::Consumer;

use crate::engine_support::{ONE, exec_event, pop};
use crate::exec_support::{
    BID, CEILING, INSTRUMENT, QuotingEngine, QuotingSetup, TwoSidedQuoter, built_quoting_engine,
    drain_commands, exec_settings, make_ready, reseat_book, row_with_ceiling, spin_at,
};

fn sweeping_engine_with(settings: ExecSettings) -> QuotingEngine {
    built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings,
        tables: RecordedTables::new(&[TableKind::Orders]),
        run_nonce: 0,
    })
}

fn claim_at(
    orders: &mut OrderTable,
    instrument: InstrumentId,
    level: QuoteLevel,
    recon_seq: u64,
) -> usize {
    let (index, _) = orders
        .claim(OrderClaim {
            instrument,
            side: Side::Buy,
            level,
            price: Price(100),
            qty: Qty(1),
            style: OrderStyle::PostOnly,
            claimed_ts_us: TsUs::from_micros(0),
            recon_seq,
        })
        .expect("the keyed level has a generation slot");
    index
}

fn sweep(orders: &mut OrderTable, instrument: InstrumentId, recon_seq: u64) -> usize {
    let mut closed = 0;
    orders.sweep_unseen(
        ReconcilePass {
            instrument,
            recon_seq,
            recon_ts_us: TsUs::from_micros(1_000),
        },
        &mut |_, _| closed += 1,
    );
    closed
}

#[test]
fn a_sweep_retires_what_predates_the_pass_and_spares_what_does_not() {
    let mut orders = OrderTable::new(0);
    let placed_under = claim_at(&mut orders, INSTRUMENT, QuoteLevel::ZERO, 7);
    assert_eq!(sweep(&mut orders, INSTRUMENT, 7), 0);
    assert!(orders.slot(placed_under).state.is_working());

    let named = claim_at(
        &mut orders,
        INSTRUMENT,
        QuoteLevel::new(1).expect("level one"),
        0,
    );
    orders.slot_mut(named).seen_recon_seq = 8;
    assert_eq!(sweep(&mut orders, INSTRUMENT, 8), 1);
    assert!(orders.slot(named).state.is_working());
    assert!(!orders.slot(placed_under).state.is_working());
}

fn resurrected_slot(orders: &mut OrderTable, recon_seq: u64) -> usize {
    let index = claim_at(orders, INSTRUMENT, QuoteLevel::ZERO, recon_seq);
    let client_id = orders.slot(index).client_id;
    let base = ExecEvent {
        qty: Qty(1),
        ..exec_event(INSTRUMENT, client_id, Side::Buy, 100, 10)
    };
    apply_exec_event(
        orders.slot_mut(index),
        &ExecEvent {
            kind: ExecKind::ReportCanceled,
            status: Some(VenueOrderStatus::Canceled),
            ..base
        },
    );
    apply_exec_event(
        orders.slot_mut(index),
        &ExecEvent {
            kind: ExecKind::SnapshotOrder,
            status: Some(VenueOrderStatus::New),
            recon_seq,
            ..base
        },
    );
    index
}

/// FITNESS: a later pass retires an unknown slot it does not name, and a pass on one instrument never
/// retires another instrument's orders — the same reconciliation identity rule read from two edges:
/// what makes a slot eligible for retirement, and what scopes retirement in the first place.
#[test]
fn a_later_pass_retires_an_unknown_slot_it_does_not_name_and_stays_within_its_instrument() {
    let mut orders = OrderTable::new(0);
    let index = resurrected_slot(&mut orders, 5);
    assert_eq!(orders.slot(index).state, OrderState::Unknown);
    assert_eq!(sweep(&mut orders, INSTRUMENT, 5), 0);
    assert_eq!(sweep(&mut orders, INSTRUMENT, 6), 1);
    assert!(!orders.slot(index).state.is_working());

    let elsewhere = claim_at(&mut orders, InstrumentId(1), QuoteLevel::ZERO, 0);
    assert_eq!(sweep(&mut orders, InstrumentId(0), 9), 0);
    assert!(orders.slot(elsewhere).state.is_working());
    assert_eq!(sweep(&mut orders, InstrumentId(1), 9), 1);
}

#[test]
fn a_probe_refused_at_the_gateway_leaves_the_doubt_standing() {
    let mut orders = OrderTable::new(0);
    let index = resurrected_slot(&mut orders, 5);
    let client_id = orders.slot(index).client_id;
    apply_exec_event(
        orders.slot_mut(index),
        &ExecEvent {
            kind: ExecKind::AckFailed,
            reject: Some(RejectClass::StillLive),
            reject_code: -1003,
            ..exec_event(INSTRUMENT, client_id, Side::Buy, 100, 20)
        },
    );
    assert_eq!(orders.slot(index).state, OrderState::Unknown);

    for (level, state) in [
        (
            QuoteLevel::new(1).expect("level one"),
            OrderState::CancelInFlight,
        ),
        (
            QuoteLevel::new(2).expect("level two"),
            OrderState::AmendInFlight,
        ),
    ] {
        let other = claim_at(&mut orders, INSTRUMENT, level, 5);
        let id = orders.slot(other).client_id;
        orders.slot_mut(other).state = state;
        apply_exec_event(
            orders.slot_mut(other),
            &ExecEvent {
                kind: ExecKind::AckFailed,
                reject: Some(RejectClass::StillLive),
                reject_code: -1003,
                ..exec_event(INSTRUMENT, id, Side::Buy, 100, 20)
            },
        );
        assert_eq!(orders.slot(other).state, OrderState::Live);
    }
}

fn adopt_live(engine: &mut HotEngine, generation_offset: usize, side: Side, when: i64) {
    let slot = side_base(INSTRUMENT, side) + generation_offset;
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(slot, 1);
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            status: Some(VenueOrderStatus::New),
            qty: Qty(ONE / 10),
            ..exec_event(INSTRUMENT, client_id, side, BID, when)
        }),
    );
}

fn halt_of(events: &mut Consumer<UiEvent>) -> Option<ExecHalt> {
    std::iter::from_fn(|| events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Execution { halt, .. } => Some(halt),
            _ => None,
        })
        .last()
}

#[test]
fn conservative_occupancy_above_the_cap_halts_and_sweeps() {
    let QuotingEngine {
        mut engine,
        mut commands,
        mut ui_events,
        ..
    } = sweeping_engine_with(exec_settings());
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, crate::exec_support::ASK, 10) {
        engine.dispatch(pop(0, 0), &message);
    }

    adopt_live(&mut engine, 1, Side::Buy, 20);
    spin_at(&mut engine, 1, 30);
    assert!(!matches!(
        halt_of(&mut ui_events),
        Some(ExecHalt::Halted { .. })
    ));
    drain_commands(&mut commands);

    adopt_live(&mut engine, 2, Side::Buy, 40);
    spin_at(&mut engine, 2, 50);
    assert!(matches!(
        halt_of(&mut ui_events),
        Some(ExecHalt::Halted {
            reason: HaltReason::DuplicateResting,
            ..
        })
    ));
    assert!(
        drain_commands(&mut commands)
            .iter()
            .any(|command| matches!(command, ExecCommand::CancelOurs { .. }))
    );
}
