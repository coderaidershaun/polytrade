//! Booting with a position you already hold. The ledger became cross-session when exposure started
//! surviving restarts, and that created a trap no test starting flat can reach.
//!
//! A restored long boots with `position_base` positive, `cash_quote` at MINUS its cost basis, and
//! `mark: None` — every part of that legitimate, because the load path restores cost basis and a
//! mark is not honest until a two-sided book has produced one. But `pnl_quote()` is `cash +
//! exposure`, and with no mark exposure is zero, so **raw PnL at boot reads as the entire cost basis
//! being lost**. A session loss limit compared against that halts a perfectly healthy engine before
//! it places an order, and does it on every restart that carries inventory — looking exactly like a
//! risk control working correctly.
//!
//! The gate avoids it by taking each instrument's baseline at its FIRST MARK rather than at
//! construction, so the figure it measures is a mark-to-market delta and the restored cost basis
//! cancels. The first test pins that, and its counterweight beside it in the same fn: the baseline
//! must not be so generous that the limit stops working, because a baseline captured at CONSTRUCTION
//! would also let the engine quote — while silently carrying the whole restored position's cost as
//! headroom, so no session loss could ever trip it. They need a seeded ledger, which is why they live
//! here beside the restore rather than with the other risk-gate tests.
//!
//! The last two are the other end of the same promise. Restoring a position is worth nothing unless
//! the run that takes fills WRITES one, so they drive the whole path — engine, snapshot, ring, writer
//! thread, file — and pin that the final snapshot survives the sink's owner going away.

use polysim::config::TrackerSpec;
use polysim::exposure::{
    ExposureError, ExposureSnapshot, ExposureState, InstrumentExposure, MAX_EXPOSURE_INSTRUMENTS,
};
use polysim::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use polysim::hot::exec::{DesiredQuote, ExecSettings, QuoteLevel};
use polysim::hot::strategy::{Strategy, StrategyCtx};
use polysim::ids::{ClientOrderId, InstrumentId, Qty, Side};
use polysim::msg::exec::{ExecCommand, ExecEvent, ExecKind, ExecLaneItem, OrderStyle};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::registry::InstrumentRow;
use polysim::sink::{ExecSink, ExposureSink};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, ONE, book_reset, detached_setup, exec_event, exposure_ring, instrument_row,
    persist_ring, pop, snapshot_pair,
};
use crate::exec_support::{drain_commands, exec_settings, make_ready, spin_at};
use crate::exposure_state::{
    BINANCE_SOURCE, drain_writer, load_live, registry_for, spawn_live_writer,
};
use crate::parquet_readback::TempDir;

const INSTRUMENT: InstrumentId = InstrumentId(0);

/// One whole base unit bought at 60,000 — the shape a restart while holding inventory actually has.
const RESTORED_POSITION: i64 = ONE;
const RESTORED_COST: i64 = -60_000 * ONE;

/// Small enough that the restored cost basis dwarfs it by four orders of magnitude, which is the
/// whole point: if raw PnL ever reaches the gate, this budget is gone before the first order.
const MAX_SESSION_LOSS: i64 = 5 * ONE;

/// Declares a resting quote on both sides at the touch, every spin — the level-triggered contract
/// `ctx.quote` asks for. What the engine does with those declarations is what these tests read.
struct TwoSidedQuoter;

impl Strategy for TwoSidedQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        record_readings(ctx);
        let book = ctx.book(INSTRUMENT);
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        let qty = Qty(ONE / 100);
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: bid.price,
                qty,
                style: OrderStyle::PostOnly,
            }),
        );
        ctx.quote(
            INSTRUMENT,
            Side::Sell,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: ask.price,
                qty,
                style: OrderStyle::PostOnly,
            }),
        );
    }
}

/// A run that inherits `restored` and writes its own changes nowhere — the shape every test below
/// wants, because they are about what the ledger DOES with a restored position.
fn restored_engine(
    instruments: &[InstrumentRow],
    restored: &[InstrumentExposure],
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let (sink, _snapshots) = exposure_ring(64);
    engine_with_exposure(instruments, ExposureWiring { restored, sink })
}

fn engine_with_exposure(
    instruments: &[InstrumentRow],
    exposure: ExposureWiring<'_>,
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let (persistence, _persist) = persist_ring(1_024);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(256);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer),
            // Wide band and a long book age: these tests are about the LOSS gate, and a quote
            // refused by the price band or a stale book would read the same as one the gate pulled.
            settings: ExecSettings {
                max_session_loss_quote: MAX_SESSION_LOSS,
                ..exec_settings()
            },
            run_nonce: 0,
        }),
        exposure,
        ..detached_setup(instruments, Box::new(TwoSidedQuoter))
    });
    (engine, commands)
}

fn dispatch(engine: &mut HotEngine, message: &InboundMessage) {
    engine.dispatch(pop(0, 0), message);
}

/// Restate the whole book so the mark moves with no transient crossed top to reason about.
fn reseat_book(engine: &mut HotEngine, bid: i64, ask: i64, when: i64) {
    let (bids, asks) = snapshot_pair(0, &[(bid, ONE)], &[(ask, ONE)], when);
    dispatch(engine, &InboundMessage::BookReset(book_reset(0, when)));
    dispatch(engine, &InboundMessage::Book(bids));
    dispatch(engine, &InboundMessage::Book(asks));
}

fn placed(commands: &[ExecCommand], wanted: Side) -> Option<ClientOrderId> {
    commands.iter().find_map(|command| match command {
        ExecCommand::Place {
            client_id, side, ..
        } if *side == wanted => Some(*client_id),
        _ => None,
    })
}

fn is_cancelled(commands: &[ExecCommand], wanted: ClientOrderId) -> bool {
    commands.iter().any(
        |command| matches!(command, ExecCommand::Cancel { client_id, .. } if *client_id == wanted),
    )
}

fn ack_placed(engine: &mut HotEngine, client_id: ClientOrderId, side: Side, price: i64, when: i64) {
    dispatch(
        engine,
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::AckPlaced,
            qty: Qty(ONE / 100),
            ..exec_event(INSTRUMENT, client_id, side, price, when)
        }),
    );
}

fn ack_canceled(engine: &mut HotEngine, client_id: ClientOrderId, side: Side, when: i64) {
    dispatch(
        engine,
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::AckCanceled,
            ..exec_event(INSTRUMENT, client_id, side, 0, when)
        }),
    );
}

fn restored_long() -> [InstrumentExposure; 1] {
    [InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty(RESTORED_POSITION),
        cash_quote: RESTORED_COST,
        basis_quote: -RESTORED_COST,
    }]
}

/// FITNESS: an engine that boots holding inventory quotes, and the session limit still bites once it
/// does.
///
/// The two go together deliberately: both are the loss gate's baseline-at-first-mark rule read from its two
/// directions. The restored cost basis is 60,000 against a 5-unit loss budget, so any gate reading
/// raw PnL — or taking its baseline before the first mark — withdraws both sides on boot and never
/// places an order again; a construction-time baseline would swallow that same 60,000 units as
/// headroom, so no session loss could ever reach the limit and the gate would be decoration. Driving
/// a real session loss AFTER the baseline is set proves it is not: the adding side is withdrawn while
/// the reducing side is not.
#[test]
fn a_restored_position_boots_quoting_and_the_session_limit_still_bites() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (mut engine, mut commands) = restored_engine(&instruments, &restored_long());
    make_ready(&mut engine, 0);
    // A spin BEFORE any book, which a real start reaches long before the first snapshot lands. The
    // ledger's raw PnL here is minus the whole cost basis, so anything that reads it — or records it
    // as the baseline — has already gone wrong by the time a book arrives.
    spin_at(&mut engine, 1, 5);
    drain_commands(&mut commands);

    // The market has moved AGAINST the restored position: marked at 59,000 against a 60,000 basis,
    // raw PnL is -1,000 — two hundred times the loss budget, and entirely last session's.
    reseat_book(&mut engine, 58_990 * ONE, 59_010 * ONE, 10);
    spin_at(&mut engine, 2, 20);

    let boot = drain_commands(&mut commands);
    assert!(
        placed(&boot, Side::Buy).is_some(),
        "the side that would ADD to the restored position must still quote — the loss it appears to \
         carry is last session's cost basis, not this session's result: {boot:?}"
    );
    assert!(
        placed(&boot, Side::Sell).is_some(),
        "and so must the side that would reduce it: {boot:?}"
    );

    let buy = placed(&boot, Side::Buy).expect("the buy side quoted at the first mark");
    let sell = placed(&boot, Side::Sell).expect("the sell side quoted at the first mark");
    // Seat both orders so the next pass judges resting quotes rather than waiting on in-flight ones.
    ack_placed(&mut engine, buy, Side::Buy, 58_990 * ONE, 30);
    ack_placed(&mut engine, sell, Side::Sell, 59_010 * ONE, 30);

    // A 1,000-unit adverse move on one whole base unit — two hundred times the budget, and all of it
    // since the baseline. A construction-time baseline would swallow it whole.
    reseat_book(&mut engine, 57_990 * ONE, 58_010 * ONE, 40);
    spin_at(&mut engine, 3, 50);

    let second = drain_commands(&mut commands);
    assert!(
        is_cancelled(&second, buy),
        "a session loss past the budget must withdraw the side that would ADD to the position: \
         {second:?}"
    );
    assert!(
        is_cancelled(&second, sell),
        "the reducing quote also moved price, so serial requoting must cancel it first: {second:?}"
    );

    ack_canceled(&mut engine, buy, Side::Buy, 60);
    ack_canceled(&mut engine, sell, Side::Sell, 60);
    spin_at(&mut engine, 4, 70);
    let after_confirmation = drain_commands(&mut commands);
    assert!(
        placed(&after_confirmation, Side::Buy).is_none(),
        "the adding side was re-placed after the session limit withdrew it: {after_confirmation:?}"
    );
    assert!(
        placed(&after_confirmation, Side::Sell).is_some(),
        "the reducing side was not re-placed after its requote cancel became terminal: \
         {after_confirmation:?}"
    );
}

/// FITNESS: a restored position with no mark is expressible, and does not read as flat. Anything
/// that treats "no valuation" as "no position" sizes the next order against inventory it does not
/// know it holds.
#[test]
fn a_restored_position_without_a_mark_is_not_flat() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (mut engine, _commands) = restored_engine(&instruments, &restored_long());

    spin_at(&mut engine, 1, 10);
    let readings = last_readings();
    assert_eq!(
        readings.position_base,
        Qty(RESTORED_POSITION),
        "the restored position is there from the first message"
    );
    assert!(
        !readings.has_mark,
        "and carries no valuation — no book has produced one yet"
    );
    assert_eq!(
        readings.exposure_quote, 0,
        "so exposure is zero: a position with no honest mark must not invent one"
    );
    assert_eq!(
        readings.pnl_quote, RESTORED_COST,
        "and raw PnL reads as the whole cost basis lost, which is exactly why no loss limit may \
         compare against it"
    );
}

/// FITNESS: the write half, driven at the engine's own seam. A fill dispatched into `HotEngine` has
/// to reach the file the NEXT boot reads, through every link between: the ledger fold, the snapshot,
/// the sink's ring, the writer thread, and the drain. Sever any one of them and the file still holds
/// what the boot seeded — which is precisely what an engine wired with no sink at all looks like, and
/// what every other test in this suite passes through without noticing.
///
/// The registry is the real one, because the file is keyed by VENUE SYMBOL: a snapshot the writer
/// cannot name an instrument for is dropped rather than written, so a hand-built row would let this
/// pass while writing nothing.
#[test]
fn a_fill_reaches_the_file_the_next_boot_reads() {
    let root = TempDir::new("exposure-write-through");
    let registry = registry_for(BINANCE_SOURCE);
    let (writer, sink) = spawn_live_writer(root.path(), &registry, &ExposureState::default());
    let (mut engine, _commands) = engine_with_exposure(
        registry.instruments(),
        ExposureWiring {
            restored: &[],
            sink,
        },
    );

    make_ready(&mut engine, 0);
    let mut pen = FillPen::new(0);
    for message in pen.fill(Side::Buy, 60_000 * ONE, ONE / 100, 10) {
        dispatch(&mut engine, &message);
    }
    // Half of it back out at a profit, so what reaches disk is a position whose COST is not minus
    // the cash beside it. Without this leg the two mirror, and a file that derived the basis from
    // the cash — the defect `InstrumentExposure::basis_quote` exists to stop — would pass.
    for message in pen.fill(Side::Sell, 62_000 * ONE, ONE / 200, 20) {
        dispatch(&mut engine, &message);
    }
    // The hot thread ending: dropping the engine drops the sink, which flushes anything it was still
    // holding. The runtime gets the same ordering by joining the hot thread before draining the writer.
    drop(engine);
    drain_writer(writer).expect("the final write succeeds");

    let state = load_live(root.path(), &registry).expect("the run's own file loads");
    assert_eq!(
        state.instruments(),
        [InstrumentExposure {
            instrument: INSTRUMENT,
            position_base: Qty(ONE / 200),
            cash_quote: -290 * ONE,
            basis_quote: 300 * ONE,
        }],
        "0.005 base still held cost 300; the cash is that less the 10 banked selling the other half"
    );
}

/// FITNESS: the snapshot a sink is still holding when its owner dies is the run's FINAL position, and
/// it must not die with it. The ring is deliberately one deep and already full, so the last push has
/// nowhere to go until the writer's next poll — the exact shape a busy shutdown has, and the one an
/// output sink that merely dropped on full would lose.
#[test]
fn the_last_snapshot_leaves_the_sink_when_its_owner_does() {
    let (producer, mut consumer) = RingBuffer::<ExposureSnapshot>::new(1);
    let mut sink = ExposureSink::new(producer);
    sink.push(final_position(1, ONE));
    sink.push(final_position(2, 2 * ONE));
    assert!(
        sink.is_pending(),
        "a one-deep ring cannot hold the second snapshot, so the sink must still be carrying it"
    );

    // The writer's poll, arriving only AFTER the hot side has let go of the sink.
    let reader = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        let mut seen = Vec::new();
        for _ in 0..2_000 {
            match consumer.pop() {
                Ok(snapshot) => seen.push(snapshot),
                Err(_) if seen.len() < 2 => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(_) => break,
            }
            if seen.len() == 2 {
                break;
            }
        }
        seen
    });
    drop(sink);

    let seen = reader.join().expect("the reader thread finished");
    assert_eq!(
        seen.len(),
        2,
        "both snapshots reach the writer: the one that fitted, and the one the destructor flushed"
    );
    assert_eq!(
        seen[1].active(),
        [InstrumentExposure {
            instrument: INSTRUMENT,
            position_base: Qty(2 * ONE),
            cash_quote: 0,
            basis_quote: 0,
        }],
        "and the second is the newest state, which is the one the next boot restores"
    );
}

/// FITNESS: a writer that dies never reports success. `drain()` answering `Ok` over a dead writer
/// is the worst shape this subsystem has — `decide_exit` reads that as a graceful shutdown and the
/// run exits 0, while the file on disk still holds whatever the boot seeded. The position that was
/// never written is the one the next boot restores and trades against.
#[test]
fn a_writer_that_died_cannot_report_a_clean_drain() {
    let root = TempDir::new("exposure-writer-panic");
    let registry = registry_for(BINANCE_SOURCE);
    let (writer, mut sink) = spawn_live_writer(root.path(), &registry, &ExposureState::default());

    // More rows than the fixed array holds is corrupt state, and the writer must die on it rather
    // than serialise it. The writer thread's own panic dump on stderr is part of a green run.
    let mut corrupt = final_position(1, ONE);
    corrupt.len = MAX_EXPOSURE_INSTRUMENTS as u8 + 1;
    sink.push(corrupt);
    drop(sink);

    let outcome = drain_writer(writer);
    assert!(
        matches!(outcome, Err(ExposureError::WriterPanicked { .. })),
        "a panicked writer must surface as WriterPanicked so the exit is non-graceful, got {outcome:?}"
    );
}

fn final_position(seq: u64, position_base: i64) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::EMPTY;
    snapshot.instruments[0] = InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty(position_base),
        cash_quote: 0,
        basis_quote: 0,
    };
    snapshot.len = 1;
    snapshot.seq = seq;
    snapshot
}

/// What the ledger says, read through the strategy seam — the only surface a test has on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Readings {
    position_base: Qty,
    has_mark: bool,
    exposure_quote: i64,
    pnl_quote: i64,
}

thread_local! {
    static READINGS: std::cell::Cell<Option<Readings>> = const { std::cell::Cell::new(None) };
}

fn record_readings(ctx: &StrategyCtx<'_>) {
    READINGS.with(|cell| {
        cell.set(Some(Readings {
            position_base: ctx.position_base(INSTRUMENT),
            has_mark: ctx.has_mark(INSTRUMENT),
            exposure_quote: ctx.exposure_quote(INSTRUMENT),
            pnl_quote: ctx.pnl_quote(INSTRUMENT),
        }));
    });
}

fn last_readings() -> Readings {
    READINGS
        .with(std::cell::Cell::get)
        .expect("the spin ran, so the strategy read the ledger")
}
