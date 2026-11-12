//! Shared builders for the dispatch fitness tests (zero-alloc + replay): synthetic messages,
//! a fully-configured instrument row, and a persistence ring wired to a `PersistSink`.

use polysim::config::{BinanceMarket, Subscriptions, VenueMarket};
use polysim::config::{
    CandlesSpec, EwmaVolSpec, ImbalanceSpec, Instruments, KlineInterval, RecordedTables, SpinField,
    SpinSampledSpec, StrategySpec, TableKind, TrackerSpec, VolumeBarsSpec, VolumeThreshold,
    WindowsSpec,
};
use polysim::exposure::ExposureSnapshot;
use polysim::hot::dispatch::{
    ExecWiring, ExposureWiring, HotEngine, HotEngineSetup, LinkWiring, PersistWiring,
};
use polysim::hot::exec::{
    BookTop, ClientIdLayout, ExecSettings, FundsView, RejectOrigin, RejectReason, side_base,
};
use polysim::hot::ingress::QueueSample;
use polysim::hot::metrics::MetricsSnapshot;
use polysim::hot::strategy::{EngineView, Strategy, StrategyConfig};
use polysim::ids::{AssetId, ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, QueueId, Side};
use polysim::link::{OutboundLink, RunState};
use polysim::log::LogRecord;
use polysim::msg::exec::{
    ACCOUNT_CHUNK_ASSETS, AccountChunk, AccountChunkKind, AssetBalance, ExecEvent, ExecKind,
    ExecLaneItem, Liquidity, Provenance, VenueOrderStatus,
};
use polysim::msg::inbound::{
    BOOK_CHUNK_LEVELS, BookChunk, BookChunkKind, BookReset, InboundMessage, KlineEvent, Level,
    MarketRotation, RunControl, SpinTick, TradeEvent,
};
use polysim::msg::persist::{FeatureId, FillRow, OrderRow, PersistRecord};
use polysim::msg::ui::{UI_BOOK_RING_CAPACITY, UI_EVENT_RING_CAPACITY, UiBookSnapshot, UiEvent};
use polysim::registry::InstrumentRow;
use polysim::shutdown::{RunAssertion, RunControlGate};
use polysim::sink::{
    ExecSink, ExposureSink, LinkSink, MetricsSink, PersistSink, StrategyLogSink, UiBookSink,
    UiEventSink,
};
use polysim::time::{DurationUs, TsUs};
use rtrb::{Consumer, RingBuffer};

use crate::micro_strategy::MicroRecorder;

pub const ONE: i64 = 100_000_000;

pub fn ts(us: i64) -> TsUs {
    TsUs::from_micros(us)
}

pub fn trade(instrument: u16, price: i64, qty: i64, side: Side, when: i64) -> TradeEvent {
    TradeEvent {
        instrument: InstrumentId(instrument),
        price: Price(price),
        qty: Qty(qty),
        side,
        exchange_ts_us: ts(when),
        exchange_sent_ts_us: None,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

fn chunk(
    instrument: u16,
    kind: BookChunkKind,
    side: Side,
    levels: &[(i64, i64)],
    is_last_chunk: bool,
    when: i64,
) -> BookChunk {
    let mut filled = [Level {
        price: Price(0),
        qty: Qty(0),
    }; BOOK_CHUNK_LEVELS];
    for (slot, &(price, qty)) in filled.iter_mut().zip(levels) {
        *slot = Level {
            price: Price(price),
            qty: Qty(qty),
        };
    }
    BookChunk {
        instrument: InstrumentId(instrument),
        kind,
        side,
        levels: filled,
        len: levels.len() as u8,
        is_last_chunk,
        update_id: when as u64,
        exchange_ts_us: None,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

/// Bids-not-last then asks-last; each side as its own last chunk would trip the implicit reset.
pub fn snapshot_pair(
    instrument: u16,
    bids: &[(i64, i64)],
    asks: &[(i64, i64)],
    when: i64,
) -> (BookChunk, BookChunk) {
    (
        chunk(
            instrument,
            BookChunkKind::Snapshot,
            Side::Buy,
            bids,
            false,
            when,
        ),
        chunk(
            instrument,
            BookChunkKind::Snapshot,
            Side::Sell,
            asks,
            true,
            when,
        ),
    )
}

pub fn delta_chunk(instrument: u16, side: Side, levels: &[(i64, i64)], when: i64) -> BookChunk {
    chunk(instrument, BookChunkKind::Delta, side, levels, true, when)
}

pub fn partial_snapshot_chunk(
    instrument: u16,
    side: Side,
    levels: &[(i64, i64)],
    when: i64,
) -> BookChunk {
    chunk(
        instrument,
        BookChunkKind::Snapshot,
        side,
        levels,
        false,
        when,
    )
}

pub fn last_snapshot_chunk(
    instrument: u16,
    side: Side,
    levels: &[(i64, i64)],
    when: i64,
) -> BookChunk {
    chunk(
        instrument,
        BookChunkKind::Snapshot,
        side,
        levels,
        true,
        when,
    )
}

pub fn partial_delta_chunk(
    instrument: u16,
    side: Side,
    levels: &[(i64, i64)],
    when: i64,
) -> BookChunk {
    chunk(instrument, BookChunkKind::Delta, side, levels, false, when)
}

pub fn book_reset(instrument: u16, when: i64) -> BookReset {
    BookReset {
        instrument: InstrumentId(instrument),
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

/// Emitted at subscribe, so `open`/`close` are the window bounds while `when` is the (earlier)
/// receipt stamp — mirrors the pre-open phase where `window_open_ts_us` exceeds `received_ts_us`.
pub fn rotation(instrument: u16, open: i64, close: i64, when: i64) -> MarketRotation {
    MarketRotation {
        instrument: InstrumentId(instrument),
        window_open_ts_us: ts(open),
        window_close_ts_us: ts(close),
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

pub fn kline(
    instrument: u16,
    interval: KlineInterval,
    ohlc: (i64, i64, i64, i64),
    is_closed: bool,
    when: i64,
) -> KlineEvent {
    let (open, high, low, close) = ohlc;
    KlineEvent {
        instrument: InstrumentId(instrument),
        interval,
        open_ts_us: ts(when),
        open: Price(open),
        high: Price(high),
        low: Price(low),
        close: Price(close),
        base_volume: Qty(ONE),
        quote_volume: 42,
        trade_count: 3,
        is_closed,
        exchange_ts_us: ts(when),
        exchange_sent_ts_us: None,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

pub fn spin(seq: u64, when: i64) -> SpinTick {
    SpinTick {
        seq,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

/// What the hot loop hands dispatch for a non-spin message: no total-backlog reading, because only
/// a spin tick carries one.
pub fn pop(queue: u8, depth: usize) -> QueueSample {
    QueueSample {
        queue_id: QueueId(queue),
        depth,
        spin_backlog: None,
    }
}

/// The spin-tick form, which is the only one that folds the backlog EWMA.
pub fn spin_pop(queue: u8, backlog: usize) -> QueueSample {
    QueueSample {
        spin_backlog: Some(backlog),
        ..pop(queue, 0)
    }
}

fn all_spin_fields() -> Vec<SpinField> {
    vec![
        SpinField::Microprice,
        SpinField::Spread,
        SpinField::BestBid,
        SpinField::BestAsk,
        SpinField::Mid,
        SpinField::Imbalance,
        SpinField::LastTradePrice,
        SpinField::LastTradeQty,
        SpinField::TradedQty,
        SpinField::TradedNotional,
        SpinField::TradeCount,
        SpinField::BuyTradeCount,
        SpinField::SellTradeCount,
    ]
}

/// [`MicroRecorder`] sizes its EGARCH window from `candles.keep` and refuses to construct below the
/// fit's 300-close floor, so every recorder-driving fixture needs retention that clears it.
///
/// [`MicroRecorder`]: crate::micro_strategy::MicroRecorder
const CANDLES_KEEP: usize = 320;

pub fn tracker_spec_all(halflife_events: u32) -> TrackerSpec {
    TrackerSpec {
        trades_all: Some(WindowsSpec { windows: vec![8] }),
        trades_buy: Some(WindowsSpec { windows: vec![8] }),
        trades_sell: Some(WindowsSpec { windows: vec![8] }),
        microprice: Some(WindowsSpec { windows: vec![8] }),
        imbalance: Some(ImbalanceSpec {
            top_n: 3,
            windows: vec![8],
        }),
        candles: Some(CandlesSpec { keep: CANDLES_KEEP }),
        spin_sampled: Some(SpinSampledSpec {
            fields: all_spin_fields(),
            window: 8,
        }),
        volume_bars: Some(VolumeBarsSpec {
            threshold: VolumeThreshold::Fixed(1),
            keep: 64,
            sampled: Some(SpinSampledSpec {
                fields: all_spin_fields(),
                window: 64,
            }),
        }),
        ewma_vol: Some(EwmaVolSpec { halflife_events }),
        intensity: None,
    }
}

/// Exposure budget for built test rows: wide enough (1e6 quote units) that no suite scenario quotes
/// itself into the ceiling — the cap has its own test, and every other one is about something else.
pub const TEST_MAX_EXPOSURE_QUOTE: i64 = 1_000_000 * FIXED_SCALE;

pub fn instrument_row(id: u16, tracker: TrackerSpec, book_capacity: usize) -> InstrumentRow {
    InstrumentRow {
        instrument_id: InstrumentId(id),
        market: VenueMarket::Binance(BinanceMarket::Perpetual),
        venue_symbol: "btcusdt".into(),
        display: "BTC/USDT perpetual".into(),
        base: "BTC".into(),
        quote: "USDT".into(),
        base_asset: AssetId(0),
        quote_asset: AssetId(1),
        tick_size: None,
        lot_size: None,
        min_qty: None,
        min_notional: None,
        max_num_orders: None,
        max_num_order_amends: None,
        max_price: None,
        qty_scale: FIXED_SCALE,
        subscriptions: Subscriptions::default(),
        kline_intervals: vec![KlineInterval::OneMinute],
        book_capacity,
        max_exposure_quote: TEST_MAX_EXPOSURE_QUOTE,
        tracker,
    }
}

/// All instruments, all-default params. Generic over `P` so the one builder serves strategies with
/// different `Params` types — inference picks it up from the `from_spec` the spec is handed to.
pub fn recorder_spec<P: Default>(tables: Vec<TableKind>) -> StrategySpec<P> {
    StrategySpec {
        instruments: Instruments::All,
        tables,
        params: P::default(),
    }
}

/// The shipped recorder's column names, in the declaration order that fixes every feature index.
pub fn recorder_feature_names() -> &'static [&'static str] {
    MicroRecorder::from_spec(&recorder_spec(Vec::new()), engine_view(NOMINAL_SPIN)).features()
}

/// The id the engine will assign `name`. A test that pins a column by NUMBER instead silently
/// repoints at its neighbour the moment a feature is inserted above it; asking by name cannot.
/// Allocates (it builds a recorder to read the order), so callers under a counting allocator
/// resolve every id before the measured window opens.
pub fn recorder_feature_id(name: &str) -> FeatureId {
    let position = recorder_feature_names()
        .iter()
        .position(|column| *column == name)
        .unwrap_or_else(|| panic!("{name} is not a recorder column"));
    FeatureId(u16::try_from(position).expect("the recorder declares fewer than 65536 columns"))
}

/// A strategy sizes its windows off the spin interval, so each test declares the cadence its own
/// spin ticks actually run at — the derived lengths are only honest if the two agree.
pub fn engine_view(spin_interval: DurationUs) -> EngineView {
    EngineView { spin_interval }
}

/// Stand-in cadence for tests whose strategy derives nothing from the spin interval; tests that do
/// derive windows from it pass their own tick cadence.
pub const NOMINAL_SPIN: DurationUs = DurationUs::from_micros(100_000);

/// These tests assert on what the strategy sees from the very first message, so they opt out of
/// warmup rather than inherit the config default and silently assert on a suppressed prefix.
/// `warmup_suppresses_callbacks_by_message_time` covers the suppressing engine.
pub fn engine_without_warmup(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
    persistence: PersistWiring,
    strategy_log_sink: StrategyLogSink,
    metrics_sink: MetricsSink,
) -> HotEngine {
    ui_engine(
        instruments,
        strategy,
        Some(persistence),
        strategy_log_sink,
        metrics_sink,
        DurationUs::ZERO,
        None,
    )
    .0
}

/// The persistence-off configuration: no output ring exists at all, so a caller has nothing to
/// observe and asserts instead that dispatch runs (and allocates) exactly as it does with one.
pub fn engine_without_persistence(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
    strategy_log_sink: StrategyLogSink,
    metrics_sink: MetricsSink,
) -> HotEngine {
    ui_engine(
        instruments,
        strategy,
        None,
        strategy_log_sink,
        metrics_sink,
        DurationUs::ZERO,
        None,
    )
    .0
}

/// Like [`engine_without_warmup`] but keeps the UI feed consumers so a test can reconstruct the
/// book/quote state the engine emits. `warmup` is explicit — most callers pass [`DurationUs::ZERO`];
/// the warmup-emission test passes a real span.
pub fn engine_with_ui(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
    persistence: PersistWiring,
    strategy_log_sink: StrategyLogSink,
    metrics_sink: MetricsSink,
    warmup: DurationUs,
) -> (HotEngine, Consumer<UiBookSnapshot>, Consumer<UiEvent>) {
    ui_engine(
        instruments,
        strategy,
        Some(persistence),
        strategy_log_sink,
        metrics_sink,
        warmup,
        None,
    )
}

/// [`engine_with_ui`] with a command ring attached, which is what makes the engine ARMED: the gate
/// reports itself only where an order could actually be sent, so a test asserting on `Execution`
/// frames must build the engine this way or measure an engine that is silent by design.
pub fn engine_with_ui_and_exec(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
    persistence: PersistWiring,
    strategy_log_sink: StrategyLogSink,
    metrics_sink: MetricsSink,
    run_nonce: u32,
) -> (
    HotEngine,
    Consumer<UiBookSnapshot>,
    Consumer<UiEvent>,
    Consumer<ExecLaneItem>,
) {
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(1024);
    let exec = ExecWiring {
        sink: ExecSink::new(commands_producer),
        settings: ExecSettings::disabled(),
        run_nonce,
    };
    let (engine, ui_books, ui_events) = ui_engine(
        instruments,
        strategy,
        Some(persistence),
        strategy_log_sink,
        metrics_sink,
        DurationUs::ZERO,
        Some(exec),
    );
    (engine, ui_books, ui_events, commands)
}

fn ui_engine(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
    persistence: Option<PersistWiring>,
    strategy_log_sink: StrategyLogSink,
    metrics_sink: MetricsSink,
    warmup: DurationUs,
    exec: Option<ExecWiring>,
) -> (HotEngine, Consumer<UiBookSnapshot>, Consumer<UiEvent>) {
    let (ui_book_sink, ui_books) = ui_book_ring(UI_BOOK_RING_CAPACITY);
    let (ui_event_sink, ui_events) = ui_event_ring(UI_EVENT_RING_CAPACITY);
    let engine = HotEngine::new(HotEngineSetup {
        exec,
        persistence,
        strategy_log_sink,
        metrics_sink,
        ui_book_sink,
        ui_event_sink,
        warmup,
        ..detached_setup(instruments, strategy)
    });
    (engine, ui_books, ui_events)
}

/// Everything a control/link test drives an engine through: the persist tape it records into, the
/// outbound frames a strategy banked, and the pair the control path converges.
pub struct LinkedEngine {
    pub engine: HotEngine,
    pub persist: Consumer<PersistRecord>,
    pub outbound: Consumer<OutboundLink>,
    pub control: RunControlGate,
}

pub struct LinkedSetup<'a> {
    pub instruments: &'a [InstrumentRow],
    pub strategy: Box<dyn Strategy>,
    pub tables: RecordedTables,
    pub warmup: DurationUs,
}

/// An engine with persistence AND a link. The desired half of `control` is left untouched by the
/// engine itself — dispatch derives its run state from recorded markers alone, so a test that
/// only sets the latch must see nothing happen.
pub fn engine_with_link(setup: LinkedSetup<'_>) -> LinkedEngine {
    let LinkedSetup {
        instruments,
        strategy,
        tables,
        warmup,
    } = setup;
    let (persistence, persist) = persist_ring_for(1024, tables);
    let (link_producer, outbound) = RingBuffer::<OutboundLink>::new(256);
    let control = RunControlGate::new();
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_book_sink: ui_book_ring(UI_BOOK_RING_CAPACITY).0,
        ui_event_sink: ui_event_ring(UI_EVENT_RING_CAPACITY).0,
        link: Some(LinkWiring {
            sink: LinkSink::new(link_producer),
            acknowledged: control.acknowledged().clone(),
        }),
        warmup,
        ..detached_setup(instruments, strategy)
    });
    LinkedEngine {
        engine,
        persist,
        outbound,
        control,
    }
}

/// The marker the link actor would push for `assertion`, stamped at `when`.
pub fn run_control(assertion: RunAssertion, when: i64) -> InboundMessage {
    InboundMessage::RunControl(RunControl {
        desired: assertion,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    })
}

pub fn idle_at(epoch: u64) -> RunAssertion {
    RunAssertion {
        state: RunState::Idle,
        epoch,
    }
}

pub fn running_at(epoch: u64) -> RunAssertion {
    RunAssertion {
        state: RunState::Running,
        epoch,
    }
}

/// Persistence on with every table recorded, so a test asserting on an emit lane sees it land.
/// [`persist_ring_for`] drives the gate itself.
pub fn persist_ring(capacity: usize) -> (PersistWiring, Consumer<PersistRecord>) {
    persist_ring_for(capacity, RecordedTables::new(&ALL_TABLES))
}

pub const ALL_TABLES: [TableKind; 4] = [
    TableKind::Features,
    TableKind::Trades,
    TableKind::BookEvents,
    TableKind::Klines,
];

pub fn persist_ring_for(
    capacity: usize,
    tables: RecordedTables,
) -> (PersistWiring, Consumer<PersistRecord>) {
    let (producer, consumer) = RingBuffer::<PersistRecord>::new(capacity);
    let wiring = PersistWiring {
        sink: PersistSink::new(producer),
        tables,
    };
    (wiring, consumer)
}

/// The strategy log lane, mirroring [`persist_ring`]. Keep the consumer to drain the banked log
/// records; drop it and the ring merely fills and counts (never fatal).
pub fn strategy_log_ring(capacity: usize) -> (StrategyLogSink, Consumer<LogRecord>) {
    let (producer, consumer) = RingBuffer::<LogRecord>::new(capacity);
    (StrategyLogSink::new(producer), consumer)
}

/// Keep the returned consumer alive or every snapshot push fails. Mirrors [`persist_ring`].
pub fn metrics_ring(capacity: usize) -> (MetricsSink, Consumer<MetricsSnapshot>) {
    let (producer, consumer) = RingBuffer::<MetricsSnapshot>::new(capacity);
    (MetricsSink::new(producer), consumer)
}

/// The UI book-snapshot ring. Keep the consumer to read the emitted snapshots; drop it and the ring
/// fills then drops-and-counts (never fatal). Mirrors [`metrics_ring`].
pub fn ui_book_ring(capacity: usize) -> (UiBookSink, Consumer<UiBookSnapshot>) {
    let (producer, consumer) = RingBuffer::<UiBookSnapshot>::new(capacity);
    (UiBookSink::new(producer), consumer)
}

/// The UI event ring, mirroring [`ui_book_ring`].
pub fn ui_event_ring(capacity: usize) -> (UiEventSink, Consumer<UiEvent>) {
    let (producer, consumer) = RingBuffer::<UiEvent>::new(capacity);
    (UiEventSink::new(producer), consumer)
}

/// The exposure ring, mirroring [`metrics_ring`]. Keep the consumer to read the cost basis the
/// engine publishes; [`detached_setup`] is the shape for a test that does not.
pub fn exposure_ring(capacity: usize) -> (ExposureSink, Consumer<ExposureSnapshot>) {
    let (producer, consumer) = RingBuffer::<ExposureSnapshot>::new(capacity);
    (ExposureSink::new(producer), consumer)
}

/// An engine wired to nothing: no venue, persistence, link or warmup, no restored position, and
/// every output ring's consumer dropped. A caller that ignores an output still exercises the code
/// that writes it, which fills the ring and then drops and counts, never stalling the hot thread.
/// The exposure sink's destructor sees its abandoned ring and returns at once rather than waiting
/// out its flush budget, so ignoring exposure costs a test nothing. A test overrides only the
/// fields it is about.
pub fn detached_setup(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
) -> HotEngineSetup<'_> {
    HotEngineSetup {
        instruments,
        strategy,
        persistence: None,
        strategy_log_sink: strategy_log_ring(64).0,
        metrics_sink: metrics_ring(64).0,
        ui_book_sink: ui_book_ring(64).0,
        ui_event_sink: ui_event_ring(256).0,
        link: None,
        warmup: DurationUs::ZERO,
        exec: None,
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_ring(16).0,
        },
    }
}

/// Opens positions through the REAL inbound fill path, which is the only path the engine has for
/// a fill.
///
/// A fill needs an ORDER to belong to, so each side adopts one slot from a reconciliation snapshot
/// and then reports trades against it. The venue's totals are cumulative and the engine folds the
/// delta, so this keeps a running total per side rather than sending each fill's own size — which is
/// exactly the property that makes the fold idempotent, exercised here by construction.
pub struct FillPen {
    instrument: InstrumentId,
    /// `(cumulative base, cumulative quote)` per side, in `Side as usize` order.
    cumulative: [(i64, i64); 2],
    is_adopted: [bool; 2],
}

impl FillPen {
    pub fn new(instrument: u16) -> Self {
        Self {
            instrument: InstrumentId(instrument),
            cumulative: [(0, 0); 2],
            is_adopted: [false; 2],
        }
    }

    /// The client id addressing this side's first slot. `run_nonce` is 0 because every fitness
    /// engine is constructed with that constant — the whole point of it being a parameter.
    fn client_id(&self, side: Side) -> ClientOrderId {
        ClientIdLayout { run_nonce: 0 }.encode(side_base(self.instrument, side) + 1, 1)
    }

    /// The messages that make one fill of `qty` at `price` happen on `side`. Dispatch them in order.
    ///
    /// Re-dispatching a batch is a REDELIVERY, not a second fill: the totals it carries are the ones
    /// it carried the first time, so the engine's cumulative fold ignores it. That is the property
    /// `a_fill_folds_before_its_callback_and_a_redelivery_moves_the_money_once` drives.
    pub fn fill(&mut self, side: Side, price: i64, qty: i64, when: i64) -> Vec<InboundMessage> {
        let mut messages = Vec::new();
        if let Some(adoption) = self.adopt(side, price, when) {
            messages.push(adoption);
        }
        messages.push(self.report(side, price, qty, when));
        messages
    }

    /// Seats this side's order, which a fill needs before it can belong anywhere. `None` once the
    /// side is already seated. Split out of [`FillPen::fill`] for the zero-alloc window, which cannot
    /// take a `Vec` of anything.
    pub fn adopt(&mut self, side: Side, price: i64, when: i64) -> Option<InboundMessage> {
        if self.is_adopted[side as usize] {
            return None;
        }
        self.is_adopted[side as usize] = true;
        Some(InboundMessage::Exec(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            // A large size, because the slot must outlive every fill this pen reports into it.
            qty: Qty(i64::MAX / 4),
            ..exec_event(self.instrument, self.client_id(side), side, price, when)
        }))
    }

    /// One more fill on an already-seated side, as a single allocation-free message.
    pub fn report(&mut self, side: Side, price: i64, qty: i64, when: i64) -> InboundMessage {
        let total = &mut self.cumulative[side as usize];
        total.0 += qty;
        total.1 += Price(price).notional(Qty(qty));
        let (cumulative_qty, cumulative_quote) = (Qty(total.0), total.1);
        InboundMessage::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::PartiallyFilled),
            last_price: Price(price),
            last_qty: Qty(qty),
            cumulative_qty,
            cumulative_quote,
            qty: Qty(i64::MAX / 4),
            ..exec_event(self.instrument, self.client_id(side), side, price, when)
        })
    }

    /// The same fill from a venue that did NOT say whether we made or took.
    ///
    /// [`exec_event`] reports `Some(Maker)`, which is the common case and the right default for
    /// every other caller — but it makes "absent" unreachable, and absent is a distinct claim from
    /// maker in a fee-determining column. A named variant rather than a flag on
    /// [`FillPen::report`]: a `bool` at the call site would not say which way it pointed.
    pub fn silent_report(&mut self, side: Side, price: i64, qty: i64, when: i64) -> InboundMessage {
        let InboundMessage::Exec(event) = self.report(side, price, qty, when) else {
            unreachable!("report builds an Exec message");
        };
        InboundMessage::Exec(ExecEvent {
            liquidity: None,
            ..event
        })
    }
}

/// A zero-valued event with the identity fields set, so each caller states only what it varies.
pub fn exec_event(
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: i64,
    when: i64,
) -> ExecEvent {
    ExecEvent {
        instrument,
        client_id,
        venue_order_id: None,
        trade_id: None,
        kind: ExecKind::ReportNew,
        status: None,
        reject: None,
        provenance: Provenance::Mine,
        side,
        liquidity: Some(Liquidity::Maker),
        price: Price(price),
        qty: Qty(0),
        last_price: Price(0),
        last_qty: Qty(0),
        cumulative_qty: Qty(0),
        cumulative_quote: 0,
        commission: 0,
        commission_asset: AssetId::UNKNOWN,
        reject_code: 0,
        // Zero is the venue's STRONGEST claim — this order gets no more amends — and every
        // engine-level fitness event is built from here, so a zero would have all of them make it.
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: ts(when),
        request_sent_ts_us: None,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

pub fn stream_ready(instrument: InstrumentId, when: i64) -> InboundMessage {
    InboundMessage::Exec(ExecEvent {
        kind: ExecKind::StreamReady,
        ..exec_event(instrument, ClientOrderId(0), Side::Buy, 0, when)
    })
}

/// A whole-account snapshot in one chunk with nothing locked, all three stamps at `when`.
pub fn account_chunk(balances: &[(AssetId, i64)], when: i64) -> AccountChunk {
    let mut slots = [AssetBalance {
        asset: AssetId::UNKNOWN,
        free: 0,
        locked: 0,
    }; ACCOUNT_CHUNK_ASSETS];
    for (slot, &(asset, free)) in balances.iter().enumerate() {
        slots[slot] = AssetBalance {
            asset,
            free,
            locked: 0,
        };
    }
    AccountChunk {
        kind: AccountChunkKind::Snapshot,
        balances: slots,
        len: u8::try_from(balances.len()).expect("a chunk holds a handful of balances"),
        is_last_chunk: true,
        venue_update_ts_ms: 1,
        exchange_ts_us: ts(when),
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

pub fn local_refusals(ui_events: &mut Consumer<UiEvent>) -> Vec<RejectReason> {
    let mut reasons = Vec::new();
    while let Ok(event) = ui_events.pop() {
        if let UiEvent::Reject {
            origin: RejectOrigin::Local(reason),
            ..
        } = event
        {
            reasons.push(reason);
        }
    }
    reasons
}

#[derive(Default)]
pub struct Banked {
    pub orders: Vec<OrderRow>,
    pub fills: Vec<FillRow>,
}

pub fn drain_orders_and_fills(records: &mut Consumer<PersistRecord>) -> Banked {
    let mut banked = Banked::default();
    while let Ok(record) = records.pop() {
        match record {
            PersistRecord::Order(row) => banked.orders.push(row),
            PersistRecord::Fill(row) => banked.fills.push(row),
            other => panic!("only orders and fills are recorded here, got {other:?}"),
        }
    }
    banked
}

/// A two-sided book around the midpoint, fresh as of the spin that reads it.
pub fn top(bid: i64, ask: i64) -> BookTop {
    BookTop {
        best_bid: Some(Price(bid)),
        best_ask: Some(Price(ask)),
        mid: Price(bid + (ask - bid) / 2),
        is_valid: true,
        last_commit_ts_us: ts(1_000),
        now_ts_us: ts(1_000),
    }
}

/// Funded far past anything a case asks for, so the funds gate is not silently what refuses.
pub fn rich() -> FundsView {
    FundsView {
        spendable: i64::MAX / 4,
        floor: 0,
    }
}

/// One price level from bare mantissas.
pub fn level(price: i64, qty: i64) -> Level {
    Level {
        price: Price(price),
        qty: Qty(qty),
    }
}

/// Whole seconds as a timestamp. [`ts`] takes microseconds.
pub fn ts_secs(secs: i64) -> TsUs {
    TsUs::from_micros(secs * 1_000_000)
}

/// Declares nothing and reads nothing, so every command and record a test sees is the engine's.
pub struct Idle;

impl Strategy for Idle {}

/// Everything the engine has persisted since the last drain.
pub fn drain_persist(linked: &mut LinkedEngine) -> Vec<PersistRecord> {
    let mut records = Vec::new();
    while let Ok(record) = linked.persist.pop() {
        records.push(record);
    }
    records
}

/// The values a probe strategy recorded under its first feature, in order.
pub fn call_codes(records: &[PersistRecord]) -> Vec<f64> {
    records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::Feature(row) if row.feature == FeatureId(0) => Some(row.value),
            _ => None,
        })
        .collect()
}
