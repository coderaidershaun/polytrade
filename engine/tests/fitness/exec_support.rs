//! Shared fixture for the engine-driven execution tests: a quoting engine on a two-sided book, the
//! three readiness legs, and the helpers that read its command stream.

use polysim::adapters::exec::open_orders_snapshot_end;
use polysim::config::{RecordedTables, TrackerSpec};
use polysim::exposure::InstrumentExposure;
use polysim::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use polysim::hot::exec::{
    DesiredQuote, ExecLimits, ExecSettings, FeeModel, OrderBudget, QuoteLevel,
};
use polysim::hot::strategy::{Strategy, StrategyCtx};
use polysim::ids::{AssetId, InstrumentId, Qty, Side};
use polysim::msg::exec::{CancelReason, ExecCommand, ExecLaneItem, OrderStyle};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::msg::ui::UiEvent;
use polysim::registry::InstrumentRow;
use polysim::sink::ExecSink;
use polysim::time::DurationUs;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ALL_TABLES, ONE, account_chunk, book_reset, detached_setup, exposure_ring, instrument_row,
    persist_ring_for, pop, snapshot_pair, spin, stream_ready, ts, ui_event_ring,
};

/// Drop the book and restate it whole, so the mark moves with no transient crossing to reason about.
pub(crate) fn reseat_book(bid: i64, ask: i64, when: i64) -> [InboundMessage; 3] {
    let (bids, asks) = snapshot_pair(0, &[(bid, ONE)], &[(ask, ONE)], when);
    [
        InboundMessage::BookReset(book_reset(0, when)),
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
    ]
}

pub(crate) const INSTRUMENT: InstrumentId = InstrumentId(0);
const BASE_ASSET: AssetId = AssetId(0);
const QUOTE_ASSET: AssetId = AssetId(1);

/// Small enough that ONE whole base unit marked at the prices below sits exactly on it, so every
/// scenario reaches the boundary rather than a comfortable interior.
pub(crate) const CEILING: i64 = 100 * ONE;

/// The declared size, and the number the "real notional, not a fixed step" pin turns on.
pub(crate) const QUOTE_QTY: Qty = Qty(ONE / 10);

pub(crate) const BID: i64 = 99 * ONE;
pub(crate) const ASK: i64 = 101 * ONE;

/// Declares a resting quote on both sides at the touch, every spin — the level-triggered contract
/// `ctx.quote` asks for. What the ENGINE does with those declarations is what the tests read,
/// and this strategy knows nothing about any ceiling: that is the point, since the gate under test
/// is the one a strategy cannot opt out of.
pub(crate) struct TwoSidedQuoter;

impl Strategy for TwoSidedQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let book = ctx.book(INSTRUMENT);
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        for (side, price) in [(Side::Buy, bid.price), (Side::Sell, ask.price)] {
            ctx.quote(
                INSTRUMENT,
                side,
                QuoteLevel::ZERO,
                Some(DesiredQuote {
                    price,
                    qty: QUOTE_QTY,
                    style: OrderStyle::PostOnly,
                }),
            );
        }
    }
}

pub(crate) fn row_with_ceiling(ceiling_quote: i64) -> InstrumentRow {
    InstrumentRow {
        max_exposure_quote: ceiling_quote,
        ..instrument_row(0, TrackerSpec::default(), 64)
    }
}

/// Every limit except the ceiling set so wide it cannot be what refuses a quote — a band, a stale
/// book or a funds floor biting instead would read exactly like the gate under test working.
pub(crate) fn exec_settings() -> ExecSettings {
    ExecSettings {
        limits: ExecLimits {
            requote_threshold_ticks: 1,
            max_quote_distance_centi_bps: 100_000_000,
            max_book_age: DurationUs::from_secs(3_600),
            max_order_notional_quote: 1_000_000 * ONE,
        },
        max_orders_per_side: 1,
        min_base_balance: 0,
        min_quote_balance: 0,
        max_consecutive_rejects: 5,
        max_session_loss_quote: 1_000_000 * ONE,
        inflight_timeout: DurationUs::from_secs(3_600),
        // No silence sweep: a reconciliation request in the command stream would be noise to filter
        // rather than a behaviour these tests are about.
        exec_silence_spins: u32::MAX,
        order_reap_window: DurationUs::from_secs(3_600),
        quote_stop_margin: DurationUs::ZERO,
        flatten_slack_ticks: 0,
        order_budget: OrderBudget::NONE,
        fee_model: FeeModel::None,
        taker_fee_rate: 0,
        holds_reservations_until_settled: true,
    }
}

/// What the engine builders below vary. It is a struct because the wrappers differ in only a few of
/// these fields, and a long positional argument list would say nothing at the call site.
pub(crate) struct QuotingSetup<'a> {
    pub(crate) row: InstrumentRow,
    /// The strategy that declares the quotes. Most callers want [`TwoSidedQuoter`]; the ones
    /// testing a declaration it cannot make bring their own.
    pub(crate) strategy: Box<dyn Strategy>,
    pub(crate) restored: &'a [InstrumentExposure],
    pub(crate) settings: ExecSettings,
    /// Which audit tables are banked. `Orders` is what makes an engine-driven transition — a sweep,
    /// a timeout — observable at all: nothing else in the process records one.
    pub(crate) tables: RecordedTables,
    /// Run nonce encoded in client order IDs.
    pub(crate) run_nonce: u32,
}

/// The engine and the consumers its tests read. A wrapper that does not read one drops it, which is
/// what a run with no workstation attached does anyway.
pub(crate) struct QuotingEngine {
    pub(crate) engine: HotEngine,
    pub(crate) commands: Consumer<ExecLaneItem>,
    pub(crate) ui_events: Consumer<UiEvent>,
}

pub(crate) fn built_quoting_engine(setup: QuotingSetup<'_>) -> QuotingEngine {
    let instruments = [setup.row];
    let (persistence, _records) = persist_ring_for(1_024, setup.tables);
    let (ui_event_sink, ui_events) = ui_event_ring(256);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(256);
    let (exposure_sink, _exposure) = exposure_ring(64);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink,
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer),
            settings: setup.settings,
            run_nonce: setup.run_nonce,
        }),
        exposure: ExposureWiring {
            restored: setup.restored,
            sink: exposure_sink,
        },
        ..detached_setup(&instruments, setup.strategy)
    });
    QuotingEngine {
        engine,
        commands,
        ui_events,
    }
}

pub(crate) fn quoting_engine_with_ui(
    ceiling_quote: i64,
    restored: &[InstrumentExposure],
) -> (HotEngine, Consumer<ExecLaneItem>, Consumer<UiEvent>) {
    let built = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(ceiling_quote),
        strategy: Box::new(TwoSidedQuoter),
        restored,
        settings: exec_settings(),
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    (built.engine, built.commands, built.ui_events)
}

/// The same engine for a caller that reads only the command stream. Dropping the UI consumer is
/// what a run with no workstation attached does anyway — the tee then fails and is counted.
pub(crate) fn quoting_engine(
    ceiling_quote: i64,
    restored: &[InstrumentExposure],
) -> (HotEngine, Consumer<ExecLaneItem>) {
    let (engine, commands, _ui_events) = quoting_engine_with_ui(ceiling_quote, restored);
    (engine, commands)
}

/// The three facts that must hold before ONE order may be sent: the stream is up, the open-order
/// snapshot has landed, and balances are known. Without them nothing quotes and every assertion
/// after the call would pass for the wrong reason.
pub(crate) fn make_ready(engine: &mut HotEngine, when: i64) {
    stream_and_balances(engine, when);
    open_orders_snapshot(engine, when);
}

/// Two of the three legs. Split out so the resync test that withholds the third leg can prove
/// it is load bearing: an engine that quotes without it would make the marker's absence
/// undetectable.
pub(crate) fn stream_and_balances(engine: &mut HotEngine, when: i64) {
    engine.dispatch(pop(0, 0), &stream_ready(INSTRUMENT, when));
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Account(account_chunk(
            &[(BASE_ASSET, 10 * ONE), (QUOTE_ASSET, 1_000_000 * ONE)],
            when,
        )),
    );
}

/// The third leg, built by the SAME constructor the Binance actor sends it with. A hand-built marker
/// would let every readiness-dependent test stay green while production sent none.
pub(crate) fn open_orders_snapshot(engine: &mut HotEngine, when: i64) {
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(open_orders_snapshot_end(INSTRUMENT, ts(when))),
    );
}

pub(crate) fn spin_at(engine: &mut HotEngine, seq: u64, when: i64) {
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(seq, when)));
}

pub(crate) fn drain_commands(commands: &mut Consumer<ExecLaneItem>) -> Vec<ExecCommand> {
    let mut drained = Vec::new();
    while let Ok(item) = commands.pop() {
        if let ExecLaneItem::Command(stamped) = item {
            drained.push(stamped.command);
        }
    }
    drained
}

/// Whether the engine asked the venue for a NEW order on this side — a placement either on its own
/// or as the second leg of a replace, since both put a quote on the book.
pub(crate) fn is_placing(commands: &[ExecCommand], wanted: Side) -> bool {
    commands.iter().any(|command| match command {
        ExecCommand::Place { side, .. } => *side == wanted,
        _ => false,
    })
}

/// Whether the engine pulled every order it owns because a kill switch tripped. That command is what
/// a halt DOES, so it is the honest way to observe one from outside.
pub(crate) fn has_halted(commands: &mut Consumer<ExecLaneItem>) -> bool {
    drain_commands(commands).iter().any(|command| {
        matches!(
            command,
            ExecCommand::CancelOurs {
                reason: CancelReason::Halt,
                ..
            }
        )
    })
}
