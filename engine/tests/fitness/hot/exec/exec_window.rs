//! On a venue whose markets EXPIRE, no order may rest when one does.
//!
//! Every other gate in the engine protects against a price or a size. This one protects against a
//! clock, and it is the only gate whose failure cannot be traded out of afterwards: a fill taken in
//! the last moment of a five-minute market leaves a position on contracts that settle at nothing or
//! at everything, with no book left to place an order into. The strategy is expected to withdraw
//! first; this is what holds when it does not.
//!
//! Both halves are checked, and the second is why the first is not enough. Refusing new
//! declarations stops the ladder GROWING, and the reconciler then withdraws what is already there,
//! one order per side per spin, so a full ladder comes down over eight spins. The sweep takes it
//! all in one pass.
//!
//! The gate is driven entirely by message stamps: the window comes from a `MarketRotation` and the
//! instant from the spin tick, so a replay refuses and sweeps at exactly the same points.

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{
    DesiredQuote, ExecCapacity, ExecSettings, HaltReason, OrderClaim, OrderState, OrderTable,
    QuoteLevel, RejectReason,
};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx, WindowInfo};
use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, OrderStyle, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, InstrumentId, Notional, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::persist::{IncidentKind, IncidentRow, PersistRecord};
use engine::msg::time::{DurationUs, LocalTs, Stamper, VenueTs};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{RecordedTables, TrackerSpec};
use engine::runtime::registry::InstrumentRow;
use proptest::prelude::{prop_assert, prop_assert_eq};
use proptest::test_runner::{Config, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ALL_TABLES, FillPen, Idle, ONE, detached_setup, exec_capacity, exec_event, instrument_row,
    local_refusals, persist_ring_for, rotation, ts,
};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, QuotingSetup, TwoSidedQuoter, built_quoting_engine,
    drain_commands, exec_settings, has_halted, make_ready, reseat_book, row_with_ceiling, spin_at,
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

fn the_quote_window_opens_at_the_open_and_shuts_a_margin_before_the_close() {
    let window = WindowInfo {
        open_ts_us: VenueTs::from_micros(OPEN),
        close_ts_us: VenueTs::from_micros(CLOSE),
    };
    let at = |micros: i64| LocalTs::from_micros(micros);

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
                    price: Price::from_mantissa(BID - level as i64 * ONE),
                    qty: QUOTE_QTY,
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

/// the quote stop pulls the whole ladder and cancels nothing twice, and the mirror control —
/// an instrument that carries no window at all quotes whatever the margin says, because there is no
/// stop to reach. Without the second half, an engine that had simply stopped quoting for some other
/// reason would read exactly like the window gate working.
fn the_quote_stop_pulls_the_whole_ladder_and_an_unwindowed_instrument_never_stops() {
    let (mut engine, mut commands, mut ui_events) =
        quoting_engine(Box::new(LadderQuoter), LADDER_DEPTH);
    make_ready(&mut engine, OPEN);
    engine.dispatch(&rotation(0, OPEN, CLOSE, OPEN));
    for message in reseat_book(BID, ASK, OPEN) {
        engine.dispatch(&message);
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
    engine.dispatch(&rotation(0, next_open, next_close, next_open));
    for message in reseat_book(BID, ASK, next_open + 1_000_000) {
        engine.dispatch(&message);
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
        unwindowed.dispatch(&message);
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

fn a_sweep_pulls_resting_quotes_and_never_a_marketable_order() {
    let is_resting = |style: OrderStyle, state: OrderState| {
        let mut orders = OrderTable::new(&exec_capacity(1), 0);
        let (index, client_id) = orders
            .claim(OrderClaim {
                instrument: INSTRUMENT,
                side: Side::Buy,
                level: QuoteLevel::ZERO,
                price: Price::from_mantissa(BID),
                qty: QUOTE_QTY,
                style,
                claimed_ts_us: ts(0),
                recon_seq: 0,
            })
            .expect("a fresh table has a free slot");
        let placed = ExecEvent {
            kind: ExecKind::AckPlaced,
            ..exec_event(INSTRUMENT, client_id, Side::Buy, BID, 0)
        };
        match state {
            OrderState::PendingNew => {}
            OrderState::Unknown => {
                orders.mark_timed_out(index, ts(0));
            }
            _ => {
                orders.apply(index, &placed, ts(0));
            }
        }
        match state {
            OrderState::CancelInFlight => {
                orders.mark_cancel_sent(index);
            }
            OrderState::AmendInFlight => {
                orders.mark_amend_sent(index);
            }
            _ => {}
        }
        assert_eq!(orders.slot(index).state(), state);
        orders.slot(index).is_resting_quote()
    };
    assert!(is_resting(OrderStyle::PostOnly, OrderState::Live));
    for unanswered in [
        OrderState::PendingNew,
        OrderState::CancelInFlight,
        OrderState::AmendInFlight,
        OrderState::Unknown,
    ] {
        assert!(!is_resting(OrderStyle::PostOnly, unanswered));
    }
    assert!(!is_resting(OrderStyle::Immediate, OrderState::Live));
}

/// CONTRACT: a bank that holds its whole capacity refuses the next command and halts quoting, and a
/// cancel that it refuses never reaches the lane, so the order it would have pulled stays `Live`,
/// also when the window closes after the halt. A cancel marked as sent but never banked would leave
/// a quote resting with nothing asking again.
#[test]
fn full_command_bank_halts_and_leaves_the_order_live() {
    let built = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings: ExecSettings {
            quote_stop_margin: MARGIN,
            stream_silence: DurationUs::from_micros(1),
            ..exec_settings()
        },
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    let (mut engine, mut commands, mut ui_events) = (built.engine, built.commands, built.ui_events);
    let bank = ExecCapacity::of(&[row_with_ceiling(CEILING)])
        .expect("one instrument is addressable")
        .command_bank();
    // Each spin asks the venue for its open orders. Until the lane is full each spin puts at least
    // one item into it, and after that at least one command into the bank, so the halt comes in time.
    let jam_spins = (commands.buffer().capacity() + bank) as i64;
    make_ready(&mut engine, OPEN);
    engine.dispatch(&rotation(0, OPEN, CLOSE, OPEN));
    for message in reseat_book(BID, ASK, OPEN) {
        engine.dispatch(&message);
    }
    spin_at(&mut engine, 1, OPEN + 1_000_000);
    let placed = drain_commands(&mut commands);
    assert_eq!(
        places(&placed),
        2,
        "both sides rest before the lane jams: {placed:?}"
    );
    ack_placed(&mut engine, &placed, OPEN + 1_100_000);

    let jammed_from = OPEN + 2_000_000;
    let mut screen = (None, Vec::new());
    for step in 0..jam_spins {
        spin_at(&mut engine, 2 + step as u64, jammed_from + step * 10_000);
        screen = read_screen(&mut ui_events);
        if screen.0.is_some_and(UiHalt::is_halted) {
            break;
        }
    }
    let (halt, working) = screen;
    assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
                reason: HaltReason::CommandBankOverflow,
                ..
            })
        ),
        "the bank refuses a command and quoting halts: {halt:?}"
    );
    assert_eq!(
        working,
        [OrderState::Live; 2],
        "the refused cancels leave both orders live"
    );

    spin_at(&mut engine, 2 + jam_spins as u64, CLOSE - MARGIN.micros());
    let (_, working) = read_screen(&mut ui_events);
    assert_eq!(
        working,
        [OrderState::Live; 2],
        "the closing window's cancels were refused too, so both quotes still read as resting"
    );

    let lane = drain_commands(&mut commands);
    assert!(
        !lane.iter().any(|command| matches!(
            command,
            ExecCommand::Cancel { .. } | ExecCommand::CancelOurs { .. }
        )),
        "no cancel reached the lane: {lane:?}"
    );
    let banked = engine.counters().orders_submitted as usize;
    assert_eq!(
        banked - placed.len() - lane.len(),
        bank,
        "the bank refused only once it held its whole capacity"
    );
}

/// What the workstation was told since the last read: the newest kill-switch frame, and the state of
/// every working order the order snapshots listed.
fn read_screen(ui_events: &mut Consumer<UiEvent>) -> (Option<UiHalt>, Vec<OrderState>) {
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
        engine.dispatch(&InboundMessage::new(
            ts(when),
            MessageBody::Exec(ExecEvent {
                kind: ExecKind::AckPlaced,
                status: Some(VenueOrderStatus::New),
                ..exec_event(INSTRUMENT, *client_id, *side, price.mantissa(), when)
            }),
        ));
    }
}

fn ack_cancelled(engine: &mut HotEngine, commands: &[ExecCommand], when: i64) {
    for command in commands {
        let ExecCommand::Cancel { client_id, .. } = command else {
            continue;
        };
        engine.dispatch(&InboundMessage::new(
            ts(when),
            MessageBody::Exec(ExecEvent {
                kind: ExecKind::ReportCanceled,
                status: Some(VenueOrderStatus::Canceled),
                ..exec_event(INSTRUMENT, *client_id, Side::Buy, BID, when)
            }),
        ));
    }
}

/// FITNESS: no quote rests when a window closes. A fill in the last moment of an expiring market is
/// a position on contracts that settle at nothing or at everything, with no book left to trade out.
///
/// Over generated windows (open, length, stop margin, and spin interval) for a ladder of two levels
/// a side, with each placement and cancel answered within its spin: before the open nothing is
/// placed; from the stop on nothing is placed, and the first spin there cancels each resting quote,
/// so none rests after it. The named cases hold the window edges, a stop that pulls a whole ladder
/// while an instrument with no window quotes on, and a sweep that never cancels a marketable order.
#[test]
fn no_order_rests_when_a_window_closes() {
    the_quote_window_opens_at_the_open_and_shuts_a_margin_before_the_close();
    the_quote_stop_pulls_the_whole_ladder_and_an_unwindowed_instrument_never_stops();
    a_sweep_pulls_resting_quotes_and_never_a_marketable_order();

    let windows = (0i64..=60, 20i64..=300, 1i64..=10, 1i64..=7, 1i64..=3);
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(&windows, |(offset, length, margin, interval, lead)| {
        let open = OPEN + offset * 1_000_000;
        let close = open + length * 1_000_000;
        let stop = close - margin * 1_000_000;
        let built = built_quoting_engine(QuotingSetup {
            row: row_with_ceiling(1_000_000 * ONE),
            strategy: Box::new(TwoLevelsEachSide),
            restored: &[],
            settings: ExecSettings {
                quote_stop_margin: DurationUs::from_secs(margin),
                max_orders_per_side: 2,
                ..exec_settings()
            },
            tables: RecordedTables::new(&ALL_TABLES),
            run_nonce: 0,
        });
        let (mut engine, mut commands) = (built.engine, built.commands);
        let first = open - lead * interval * 1_000_000;
        make_ready(&mut engine, first - 2);
        for message in reseat_book(BID, ASK, first - 2) {
            engine.dispatch(&message);
        }
        engine.dispatch(&rotation(0, open, close, first - 1));
        let mut live = Vec::new();
        let mut placed_in_window = 0;
        for (seq, at) in (1u64..)
            .zip((first..=close + interval * 1_000_000).step_by(interval as usize * 1_000_000))
        {
            spin_at(&mut engine, seq, at);
            let sent = drain_commands(&mut commands);
            for command in &sent {
                match command {
                    ExecCommand::Place { client_id, .. } => live.push(*client_id),
                    ExecCommand::Cancel { client_id, .. } => live.retain(|id| id != client_id),
                    _ => {}
                }
            }
            ack_placed(&mut engine, &sent, at + 1);
            ack_cancelled(&mut engine, &sent, at + 1);
            if at < open || at >= stop {
                prop_assert_eq!(places(&sent), 0, "a placement outside the window at {}", at);
            } else {
                placed_in_window += places(&sent);
            }
            if at >= stop {
                prop_assert!(
                    live.is_empty(),
                    "{:?} still rest at {} past the stop {}",
                    live,
                    at,
                    stop
                );
            }
        }
        let has_window_spin = (first..stop)
            .step_by(interval as usize * 1_000_000)
            .any(|at| at >= open);
        prop_assert!(
            !has_window_spin || placed_in_window > 0,
            "the window placed nothing"
        );
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("a quote rested at a window's close: {failure}");
    }
}

/// Quotes a bid and an offer at each of two levels, each spin.
struct TwoLevelsEachSide;

impl Strategy for TwoLevelsEachSide {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        for level in 0..2u8 {
            let step = i64::from(level) * ONE;
            for (side, price) in [(Side::Buy, BID - step), (Side::Sell, ASK + step)] {
                let quote = DesiredQuote {
                    price: Price::from_mantissa(price),
                    qty: QUOTE_QTY,
                };
                let level = QuoteLevel::new(level).expect("two levels");
                ctx.quote(INSTRUMENT, side, level, Some(quote));
            }
        }
    }
}

/// The quote asset of instrument 2.
const USDC: AssetId = AssetId(2);

/// Three instruments, the last one quoted in USDC, under a loss budget of `budget`, with the
/// command lane and the tape lane.
fn three_markets(budget: i64) -> (HotEngine, Consumer<ExecLaneItem>, Consumer<PersistRecord>) {
    let rows = [
        instrument_row(0, TrackerSpec::default(), 64),
        instrument_row(1, TrackerSpec::default(), 64),
        InstrumentRow {
            quote_asset: USDC,
            ..instrument_row(2, TrackerSpec::default(), 64)
        },
    ];
    let (persistence, records) = persist_ring_for(1_024, RecordedTables::new(&ALL_TABLES));
    let (producer, commands) = RingBuffer::new(1_024);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: Some(ExecWiring {
            sink: ExecSink::new(producer, Stamper::Off),
            settings: ExecSettings {
                max_session_loss_quote: Notional::from_mantissa(budget),
                ..exec_settings()
            },
            run_nonce: 0,
        }),
        ..detached_setup(&rows, Box::new(Idle))
    });
    (engine, commands, records)
}

fn stranded(records: &mut Consumer<PersistRecord>) -> Vec<IncidentRow> {
    let mut incidents = Vec::new();
    while let Ok(record) = records.pop() {
        if let PersistRecord::Incident(row) = record {
            incidents.push(row);
        }
    }
    incidents
}

/// Instrument 2 loses 0.3 on a round trip in the window that closes at 300 s, then buys 10 at 0.42,
/// and the window rotates.
fn strand_ten_at_a_basis_of_four_point_two(engine: &mut HotEngine, pen: &mut FillPen) {
    engine.dispatch(&rotation(2, 0, 300_000_000, 10));
    for message in [
        pen.fill(Side::Buy, 72_000_000, ONE, 20),
        pen.fill(Side::Sell, 42_000_000, ONE, 30),
        pen.fill(Side::Buy, 42_000_000, 10 * ONE, 40),
    ]
    .concat()
    {
        engine.dispatch(&message);
    }
    engine.dispatch(&rotation(2, 300_000_000, 600_000_000, 50));
}

/// CONTRACT: a rotation with a position leaves one incident record, and the basis of the position
/// counts against the loss budget of its quote asset. Its contracts can settle at nothing, and no
/// order can trade them any more, so a warning alone would let the next window spend the same
/// budget again.
#[test]
fn rotation_with_position_records_it_and_keeps_its_basis() {
    let (mut engine, mut commands, mut records) = three_markets(5 * ONE);
    let mut pen = FillPen::new(2);
    strand_ten_at_a_basis_of_four_point_two(&mut engine, &mut pen);
    let incidents = stranded(&mut records);
    assert_eq!(
        incidents.len(),
        1,
        "one record for the stranded position: {incidents:?}"
    );
    let row = incidents[0];
    assert_eq!(
        (row.kind, row.instrument, row.client_id, row.qty, row.amount),
        (
            IncidentKind::StrandedPosition,
            InstrumentId(2),
            None,
            Qty::from_mantissa(10 * ONE),
            42 * ONE / 10
        )
    );
    assert_eq!(
        row.window_close_ts_us,
        Some(VenueTs::from_micros(300_000_000))
    );
    spin_at(&mut engine, 1, 60);
    assert!(
        !has_halted(&mut commands),
        "0.3 realised and 4.2 stranded use 4.5 of 5.0"
    );

    for message in pen.fill(Side::Buy, 10_000_000, 10 * ONE, 70) {
        engine.dispatch(&message);
    }
    engine.dispatch(&rotation(2, 600_000_000, 900_000_000, 80));
    assert_eq!(
        stranded(&mut records).len(),
        1,
        "the second stranded position"
    );
    spin_at(&mut engine, 2, 90);
    assert!(
        has_halted(&mut commands),
        "a second stranded basis of 1.0 makes 5.5 of 5.0, and quoting goes on"
    );

    let (mut engine, mut commands, _records) = three_markets(44 * ONE / 10);
    strand_ten_at_a_basis_of_four_point_two(&mut engine, &mut FillPen::new(2));
    spin_at(&mut engine, 1, 60);
    assert!(
        has_halted(&mut commands),
        "the realised 0.3 is carried with the stranded 4.2: 4.5 of 4.4"
    );
}
