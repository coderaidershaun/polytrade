//! FITNESS: on a venue whose markets EXPIRE, an order must never be resting when one does.
//!
//! Every other gate in the engine protects against a price or a size. This one protects against a
//! clock, and it is the only gate whose failure cannot be traded out of afterwards: a fill taken in
//! the last moment of a five-minute market leaves a position on contracts that settle at nothing or
//! at everything, with no book left to place an order into. The strategy is expected to withdraw
//! first; this is what holds when it does not.
//!
//! Both halves are pinned, and the second is why the first is not enough. Refusing new declarations
//! stops the ladder GROWING, and the reconciler then withdraws what is already there — one order per
//! side per spin, so a full ladder comes down over eight of them. The sweep takes the whole thing in
//! one pass.
//!
//! The gate is driven entirely by message stamps: the window comes from a `MarketRotation` and the
//! instant from the spin tick, so a replay refuses and sweeps at exactly the same points.

use polysim::config::RecordedTables;
use polysim::hot::dispatch::HotEngine;
use polysim::hot::exec::{
    DesiredQuote, ExecHalt, ExecSettings, HaltReason, MAX_ORDER_SLOTS, OrderSlot, OrderState,
    QuoteLevel, RejectReason,
};
use polysim::hot::strategy::{Strategy, StrategyCtx, WindowInfo};
use polysim::ids::{Price, Side};
use polysim::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, OrderStyle, VenueOrderStatus,
};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::msg::ui::UiEvent;
use polysim::time::{DurationUs, TsUs};
use rtrb::Consumer;

use crate::engine_support::{ALL_TABLES, ONE, exec_event, local_refusals, pop, rotation};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, QuotingSetup, TwoSidedQuoter, built_quoting_engine,
    drain_commands, exec_settings, make_ready, reseat_book, row_with_ceiling, spin_at,
};

/// A window is 300 seconds and quoting stops 3 seconds before it closes, which is the shipped
/// polymarket shape.
const WINDOW: DurationUs = DurationUs::from_secs(300);
const MARGIN: DurationUs = DurationUs::from_secs(3);
const OPEN: i64 = 1_000_000_000;
const CLOSE: i64 = OPEN + WINDOW.micros();

fn quoting_engine(
    strategy: Box<dyn Strategy>,
    max_orders_per_side: usize,
) -> (HotEngine, Consumer<ExecLaneItem>, Consumer<UiEvent>) {
    let built = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy,
        restored: &[],
        settings: ExecSettings {
            quote_stop_margin: MARGIN,
            max_orders_per_side,
            ..exec_settings()
        },
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    (built.engine, built.commands, built.ui_events)
}

fn places(commands: &[ExecCommand]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, ExecCommand::Place { .. }))
        .count()
}

fn cancels(commands: &[ExecCommand]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, ExecCommand::Cancel { .. }))
        .count()
}

#[test]
fn the_quote_window_opens_at_the_open_and_shuts_a_margin_before_the_close() {
    let window = WindowInfo {
        open_ts_us: TsUs::from_micros(OPEN),
        close_ts_us: TsUs::from_micros(CLOSE),
    };
    let at = |micros: i64| TsUs::from_micros(micros);

    assert!(
        !window.admits_quote_at(at(OPEN - 1), MARGIN),
        "one microsecond before the open the market is not there to quote into"
    );
    assert!(
        window.admits_quote_at(at(OPEN), MARGIN),
        "the open is inside the window it opens"
    );
    assert!(
        window.admits_quote_at(at(CLOSE - MARGIN.micros() - 1), MARGIN),
        "the last instant before the stop still quotes"
    );
    assert!(
        !window.admits_quote_at(at(CLOSE - MARGIN.micros()), MARGIN),
        "the stop itself is out — the margin is what the ladder needs to come down in"
    );
    assert!(
        window.is_past_quote_stop(at(CLOSE - MARGIN.micros()), MARGIN),
        "and the same instant is what arms the sweep"
    );
    assert!(
        !window.is_past_quote_stop(at(CLOSE - MARGIN.micros() - 1), MARGIN),
        "one microsecond earlier it does not"
    );
    assert_eq!(
        WINDOW,
        window.close_ts_us.diff(window.open_ts_us),
        "the fixture really is a whole window, not a sliver either boundary sits outside"
    );
}

struct LadderQuoter;

const LADDER_DEPTH: usize = 3;

impl Strategy for LadderQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        for level in 0..LADDER_DEPTH {
            ctx.quote(
                INSTRUMENT,
                Side::Buy,
                QuoteLevel::new(level as u8).expect("within the fixed ladder"),
                Some(DesiredQuote {
                    price: Price(BID - level as i64 * ONE),
                    qty: QUOTE_QTY,
                    style: OrderStyle::PostOnly,
                }),
            );
        }
    }
}

fn seat_ladder(
    engine: &mut HotEngine,
    commands: &mut Consumer<ExecLaneItem>,
    first_seq: u64,
    when: i64,
) -> u64 {
    for step in 0..LADDER_DEPTH as u64 {
        let at = when + step as i64 * 1_000_000;
        spin_at(engine, first_seq + step, at);
        let placed = drain_commands(commands);
        assert_eq!(
            places(&placed),
            1,
            "one placement per spin is the reconciler's whole-side rule: {placed:?}"
        );
        ack_placed(engine, &placed, at + 100_000);
    }
    first_seq + LADDER_DEPTH as u64
}

/// FITNESS: the quote stop pulls the whole ladder and cancels nothing twice, and the mirror control —
/// an instrument that carries no window at all quotes whatever the margin says, because there is no
/// stop to reach. Without the second half, an engine that had simply stopped quoting for some other
/// reason would read exactly like the window gate working.
#[test]
fn the_quote_stop_pulls_the_whole_ladder_and_an_unwindowed_instrument_never_stops() {
    let (mut engine, mut commands, mut ui_events) =
        quoting_engine(Box::new(LadderQuoter), LADDER_DEPTH);
    make_ready(&mut engine, OPEN);
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::MarketRotation(rotation(0, OPEN, CLOSE, OPEN)),
    );
    for message in reseat_book(BID, ASK, OPEN) {
        engine.dispatch(pop(0, 0), &message);
    }
    drain_commands(&mut commands);
    let seq = seat_ladder(&mut engine, &mut commands, 1, OPEN + 10 * 1_000_000);

    let stop = CLOSE - MARGIN.micros();
    spin_at(&mut engine, seq, stop);
    let swept = drain_commands(&mut commands);
    assert_eq!(cancels(&swept), LADDER_DEPTH);
    assert_eq!(places(&swept), 0, "and nothing new may go on it: {swept:?}");
    assert!(
        local_refusals(&mut ui_events).contains(&RejectReason::OutsideWindow),
        "the reason the side stopped quoting is the window, and it must say so"
    );

    spin_at(&mut engine, seq + 1, stop + 1_000_000);
    let again = drain_commands(&mut commands);
    assert_eq!(cancels(&again), 0);

    ack_cancelled(&mut engine, &swept, stop + 2_000_000);
    let next_open = CLOSE;
    let next_close = next_open + WINDOW.micros();
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::MarketRotation(rotation(0, next_open, next_close, next_open)),
    );
    for message in reseat_book(BID, ASK, next_open + 1_000_000) {
        engine.dispatch(pop(0, 0), &message);
    }
    drain_commands(&mut commands);
    let seq = seat_ladder(&mut engine, &mut commands, seq + 2, next_open + 2_000_000);

    spin_at(&mut engine, seq, next_close - MARGIN.micros());
    let swept_again = drain_commands(&mut commands);
    assert_eq!(cancels(&swept_again), LADDER_DEPTH);

    let (mut unwindowed, mut unwindowed_commands, _ui_events) = quoting_engine(
        Box::new(TwoSidedQuoter),
        exec_settings().max_orders_per_side,
    );
    make_ready(&mut unwindowed, 0);
    for message in reseat_book(BID, ASK, 10) {
        unwindowed.dispatch(pop(0, 0), &message);
    }
    drain_commands(&mut unwindowed_commands);

    // An instant that would be deep past the stop of any window, on an instrument that has none.
    spin_at(&mut unwindowed, 1, 20 * ONE);
    let unwindowed_commands = drain_commands(&mut unwindowed_commands);
    assert!(places(&unwindowed_commands) > 0);
    assert_eq!(
        cancels(&unwindowed_commands),
        0,
        "and nothing to sweep: {unwindowed_commands:?}"
    );
}

#[test]
fn a_sweep_pulls_resting_quotes_and_never_a_marketable_order() {
    let quote = |state: OrderState| OrderSlot {
        state,
        style: Some(OrderStyle::PostOnly),
        ..OrderSlot::EMPTY
    };
    assert!(quote(OrderState::Live).is_resting_quote());
    for unanswered in [
        OrderState::PendingNew,
        OrderState::CancelInFlight,
        OrderState::AmendInFlight,
        OrderState::Unknown,
    ] {
        assert!(!quote(unanswered).is_resting_quote());
    }
    assert!(
        !OrderSlot {
            state: OrderState::Live,
            style: Some(OrderStyle::Immediate),
            ..OrderSlot::EMPTY
        }
        .is_resting_quote()
    );
}

/// One reconciliation request a spin, against a bank sized to the order table plus a few commands
/// per instrument, behind a lane that takes two items a spin until it is full: twice the order table
/// outlasts both.
const JAM_SPINS: i64 = 2 * MAX_ORDER_SLOTS as i64;

/// FITNESS: a full command bank halts the engine, and a cancel it refuses leaves the order `Live`.
///
/// The bank fills only when the edge has stopped draining, and the halt that follows is exactly when
/// the reconciler and the closing window both try to pull every resting quote, so those cancels are
/// refused too. A slot marked cancel-in-flight is one the engine waits on rather than retries, so a
/// cancel that never left would leave the quote resting on the venue with nothing asking again.
#[test]
fn a_full_command_bank_halts_and_leaves_the_order_it_could_not_cancel_live() {
    let built = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings: ExecSettings {
            quote_stop_margin: MARGIN,
            // A reconciliation request every spin, into a lane this test never drains.
            exec_silence_spins: 1,
            ..exec_settings()
        },
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    let (mut engine, mut commands, mut ui_events) = (built.engine, built.commands, built.ui_events);
    make_ready(&mut engine, OPEN);
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::MarketRotation(rotation(0, OPEN, CLOSE, OPEN)),
    );
    for message in reseat_book(BID, ASK, OPEN) {
        engine.dispatch(pop(0, 0), &message);
    }
    spin_at(&mut engine, 1, OPEN + 1_000_000);
    let placed = drain_commands(&mut commands);
    assert_eq!(
        places(&placed),
        2,
        "both sides must be resting before the lane jams: {placed:?}"
    );
    ack_placed(&mut engine, &placed, OPEN + 1_100_000);

    let jammed_from = OPEN + 2_000_000;
    let mut screen = (None, Vec::new());
    for step in 0..JAM_SPINS {
        spin_at(&mut engine, 2 + step as u64, jammed_from + step * 10_000);
        screen = read_screen(&mut ui_events);
        if screen.0.is_some_and(ExecHalt::is_halted) {
            break;
        }
    }
    let (halt, working) = screen;
    assert!(
        matches!(
            halt,
            Some(ExecHalt::Halted {
                reason: HaltReason::CommandBankOverflow,
                ..
            })
        ),
        "{JAM_SPINS} spins into an undrained lane must halt on the full bank: {halt:?}"
    );
    assert_eq!(
        working,
        [OrderState::Live; 2],
        "the halted reconciler tried to pull both quotes and the bank refused, so neither may read \
         as cancel-in-flight"
    );

    spin_at(&mut engine, 2 + JAM_SPINS as u64, CLOSE - MARGIN.micros());
    let (_, working) = read_screen(&mut ui_events);
    assert_eq!(
        working,
        [OrderState::Live; 2],
        "the closing window's cancels were refused too, so both quotes still read as resting"
    );
}

/// What the workstation was told since the last read: the newest kill-switch frame, and the state of
/// every working order the order snapshots listed.
fn read_screen(ui_events: &mut Consumer<UiEvent>) -> (Option<ExecHalt>, Vec<OrderState>) {
    let mut halt = None;
    let mut working = Vec::new();
    while let Ok(event) = ui_events.pop() {
        match event {
            UiEvent::Execution { halt: latest, .. } => halt = Some(latest),
            UiEvent::OrderSnapshot {
                detail_len, orders, ..
            } => working.extend(
                orders[..usize::from(detail_len)]
                    .iter()
                    .map(|order| order.state),
            ),
            _ => {}
        }
    }
    (halt, working)
}

fn ack_placed(engine: &mut HotEngine, commands: &[ExecCommand], when: i64) {
    for command in commands {
        let ExecCommand::Place {
            client_id,
            side,
            price,
            ..
        } = command
        else {
            continue;
        };
        engine.dispatch(
            pop(0, 0),
            &InboundMessage::Exec(ExecEvent {
                kind: ExecKind::AckPlaced,
                status: Some(VenueOrderStatus::New),
                ..exec_event(INSTRUMENT, *client_id, *side, price.0, when)
            }),
        );
    }
}

fn ack_cancelled(engine: &mut HotEngine, commands: &[ExecCommand], when: i64) {
    for command in commands {
        let ExecCommand::Cancel { client_id, .. } = command else {
            continue;
        };
        engine.dispatch(
            pop(0, 0),
            &InboundMessage::Exec(ExecEvent {
                kind: ExecKind::ReportCanceled,
                status: Some(VenueOrderStatus::Canceled),
                ..exec_event(INSTRUMENT, *client_id, Side::Buy, BID, when)
            }),
        );
    }
}
