//! The replay driver folds a sequence as the hot thread's driver does: the same state after each
//! message, and the same commands and records.

use std::cell::Cell;
use std::num::{NonZeroU32, NonZeroU64};

use engine::hot::dispatch::StateDigest;
use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup, PersistWiring};
use engine::hot::exec::{
    DesiredQuote, ExecLimits, ExecSettings, FeeModel, OrderBudget, QuoteLevel,
};
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::{
    AuditBank, AuditSink, ExecSink, ExposureSink, ResearchSink, StrategyLogSink, UiBookSink,
    UiEventSink,
};
use engine::hot::spawn::{HotDriver, Step};
use engine::hot::strategy::{Fill, OrderReject, OrderUpdate, Registration, Strategy, StrategyCtx};
use engine::link::LinkFrame;
use engine::msg::exec::{ExecCommand, ExecLaneItem, StampedExecCommand};
use engine::msg::ids::{AssetId, FIXED_SCALE, InstrumentId, Notional, Qty, Side, VenueId};
use engine::msg::inbound::{
    BookChunk, InboundMessage, KlineEvent, MarketRotation, SpinTick, TradeEvent,
};
use engine::msg::persist::{AuditRecord, FeatureId, ResearchRecord};
use engine::msg::time::{DurationUs, Stamper};
use engine::msg::ui::{UiBookSnapshot, UiEvent};
use engine::runtime::config::{
    BinanceMarket, CandlesSpec, EwmaVolSpec, ImbalanceSpec, KlineInterval, RecordedTables,
    Subscriptions, TableKind, TrackerSpec, VenueMarket, WindowsSpec,
};
use engine::runtime::registry::InstrumentRow;
use lab::backtest::ReplayDriver;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::draws::{KINDS, kind_of, placing_and_filling_steps, steps};
use crate::messages::{Builder, ONE, QUOTE_QTY};

/// Far more than one message writes to any lane. Each lane is emptied after each message, so no
/// ring fills and each lane holds each record that the fold wrote.
const LANE: usize = 4_096;

const MARKETS: u16 = 2;

/// Long enough that the drawn sequences start inside it, and short enough that they leave it.
const WARMUP: DurationUs = DurationUs::from_millis(500);

/// A seal every few spins, so the seal records are compared too.
const SEAL_INTERVAL_SPINS: u64 = 7;

/// Writes one feature row for each callback, its value the code of the callback, so the records
/// hold the order of callbacks. Each spin it quotes both sides of each market at the touch, so
/// orders go out and the answers of the venue fold back in.
#[derive(Default)]
struct Probe {
    call: Option<FeatureId>,
}

impl Probe {
    fn record(&self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId, code: f64) {
        ctx.emit(self.call.expect("features registered"), instrument, code);
    }
}

impl Strategy for Probe {
    fn features(&self) -> &'static [&'static str] {
        &["call"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        self.record(ctx, event.instrument, 1.0);
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        self.record(ctx, chunk.instrument, 2.0);
    }

    fn on_book_reset(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        self.record(ctx, instrument, 3.0);
    }

    fn on_kline(&mut self, ctx: &mut StrategyCtx<'_>, event: &KlineEvent) {
        self.record(ctx, event.instrument, 4.0);
    }

    fn on_market_rotation(&mut self, ctx: &mut StrategyCtx<'_>, rotation: &MarketRotation) {
        self.record(ctx, rotation.instrument, 5.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        self.record(ctx, InstrumentId(0), 6.0);
        for market in 0..MARKETS {
            let instrument = InstrumentId(market);
            let book = ctx.book(instrument);
            let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
                continue;
            };
            for (side, price) in [(Side::Buy, bid.price), (Side::Sell, ask.price)] {
                let quote = DesiredQuote {
                    price,
                    qty: QUOTE_QTY,
                };
                ctx.quote(instrument, side, QuoteLevel::ZERO, Some(quote));
            }
        }
    }

    fn on_link(&mut self, ctx: &mut StrategyCtx<'_>, _frame: &LinkFrame) {
        self.record(ctx, InstrumentId(0), 7.0);
    }

    fn on_fill(&mut self, ctx: &mut StrategyCtx<'_>, fill: &Fill) {
        self.record(ctx, fill.instrument, 8.0);
    }

    fn on_order_update(&mut self, ctx: &mut StrategyCtx<'_>, update: &OrderUpdate) {
        self.record(ctx, update.instrument, 9.0);
    }

    fn on_reject(&mut self, ctx: &mut StrategyCtx<'_>, reject: &OrderReject) {
        self.record(ctx, reject.instrument, 10.0);
    }

    fn on_resume(&mut self, ctx: &mut StrategyCtx<'_>) {
        self.record(ctx, InstrumentId(0), 11.0);
    }
}

fn tracker() -> TrackerSpec {
    TrackerSpec {
        trades_all: Some(WindowsSpec { windows: vec![8] }),
        microprice: Some(WindowsSpec { windows: vec![8] }),
        imbalance: Some(ImbalanceSpec {
            top_n: 3,
            windows: vec![8],
        }),
        candles: Some(CandlesSpec { keep: 16 }),
        ewma_vol: Some(EwmaVolSpec { halflife_events: 1 }),
        ..TrackerSpec::default()
    }
}

fn rows() -> Vec<InstrumentRow> {
    (0..MARKETS)
        .map(|id| InstrumentRow {
            instrument_id: InstrumentId(id),
            venue: VenueId(0),
            market: VenueMarket::Binance(BinanceMarket::Perpetual),
            venue_symbol: format!("btcusdt-{id}").into(),
            display: format!("BTC/USDT {id}").into(),
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
            book_capacity: 64,
            max_exposure_quote: Notional::from_mantissa(1_000_000 * ONE),
            tracker: tracker(),
        })
        .collect()
}

/// Every limit so wide that none of them is what refuses a quote.
fn exec_settings() -> ExecSettings {
    ExecSettings {
        limits: ExecLimits {
            requote_threshold_ticks: 1,
            max_quote_distance_centi_bps: 100_000_000,
            max_book_age: DurationUs::from_secs(3_600),
            max_order_notional_quote: Notional::from_mantissa(1_000_000 * ONE),
        },
        max_orders_per_side: 1,
        min_base_balance: Qty::from_mantissa(0),
        min_quote_balance: Notional::from_mantissa(0),
        max_consecutive_rejects: 5,
        max_session_loss_quote: Notional::from_mantissa(1_000_000 * ONE),
        inflight_timeout: DurationUs::from_secs(3_600),
        max_status_probes: u8::MAX,
        stream_silence: DurationUs::from_micros(i64::MAX),
        max_stream_resets: 2,
        order_reap_window: DurationUs::from_secs(3_600),
        quote_stop_margin: DurationUs::ZERO,
        flatten_slack_ticks: 0,
        order_budget: OrderBudget::NONE,
        fee_model: FeeModel::None,
        taker_fee_rate: 0,
        holds_reservations_until_settled: true,
    }
}

/// The read ends of the output lanes of one engine, but its command lane.
struct Lanes {
    research: Consumer<ResearchRecord>,
    audit: Consumer<AuditRecord>,
    ui_books: Consumer<UiBookSnapshot>,
    ui_events: Consumer<UiEvent>,
}

/// One engine of the shared setup: two markets, the probe, one venue, persistence that seals, and
/// a warmup. It returns the read end of its command lane apart, for the driver that owns it.
fn engine() -> (HotEngine, Consumer<ExecLaneItem>, Lanes) {
    let rows = rows();
    let (research_producer, research) = RingBuffer::<ResearchRecord>::new(LANE);
    let (audit_producer, audit) = RingBuffer::<AuditRecord>::new(LANE);
    let bank = AuditBank {
        rows: LANE,
        high_mark: LANE,
    };
    let (ui_book_producer, ui_books) = RingBuffer::<UiBookSnapshot>::new(LANE);
    let (ui_event_producer, ui_events) = RingBuffer::<UiEvent>::new(LANE);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(LANE);
    let tables = RecordedTables::new(&[
        TableKind::Trades,
        TableKind::BookEvents,
        TableKind::Klines,
        TableKind::Features,
        TableKind::LinkFrames,
        TableKind::Orders,
        TableKind::Fills,
    ]);
    let engine = HotEngine::new(HotEngineSetup {
        instruments: &rows,
        strategy: Box::new(Probe::default()),
        persistence: Some(PersistWiring {
            audit: AuditSink::new(audit_producer, bank, NonZeroU32::MAX),
            research: ResearchSink::new(research_producer, NonZeroU32::MAX),
            tables,
            seal_interval_spins: NonZeroU64::new(SEAL_INTERVAL_SPINS).expect("not zero"),
        }),
        strategy_log_sink: StrategyLogSink::new(RingBuffer::new(LANE).0),
        ui_book_sink: UiBookSink::new(ui_book_producer),
        ui_event_sink: UiEventSink::new(ui_event_producer),
        link: None,
        warmup: WARMUP,
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }],
        exposure: ExposureWiring {
            restored: &[],
            sink: ExposureSink::lane(16, rows.len()).0,
        },
    });
    (
        engine,
        commands,
        Lanes {
            research,
            audit,
            ui_books,
            ui_events,
        },
    )
}

/// What one fold leaves behind: the digest after each message, and each record of each lane as
/// text, so a float compares by its text and a failure prints the record.
#[derive(Default)]
struct Folded {
    digests: Vec<StateDigest>,
    commands: Vec<String>,
    records: Vec<String>,
    ui_books: Vec<String>,
    ui_events: Vec<String>,
    places: usize,
    fills: usize,
}

impl Folded {
    fn drain(&mut self, lanes: &mut Lanes, sent: &mut Vec<StampedExecCommand>) {
        for command in sent.drain(..) {
            self.places += usize::from(matches!(command.command, ExecCommand::Place { .. }));
            self.commands.push(format!("{command:?}"));
        }
        while let Ok(record) = lanes.audit.pop() {
            self.records.push(format!("{record:?}"));
        }
        while let Ok(record) = lanes.research.pop() {
            self.records.push(format!("{record:?}"));
        }
        while let Ok(snapshot) = lanes.ui_books.pop() {
            self.ui_books.push(format!("{snapshot:?}"));
        }
        while let Ok(event) = lanes.ui_events.pop() {
            self.fills += usize::from(matches!(event, UiEvent::Fill { .. }));
            self.ui_events.push(format!("{event:?}"));
        }
    }

    fn lanes(&self) -> [(&'static str, &[String]); 4] {
        [
            ("commands", &self.commands),
            ("records", &self.records),
            ("ui books", &self.ui_books),
            ("ui events", &self.ui_events),
        ]
    }
}

/// Folds `sequence` by the hot thread's driver, each message pushed into its one input queue and
/// its command lane emptied after each step, as the edge empties it.
fn fold_by_hot_driver(sequence: &[InboundMessage]) -> Folded {
    let (engine, mut commands, mut lanes) = engine();
    let (mut producer, consumer) = RingBuffer::<InboundMessage>::new(1);
    let mut driver = HotDriver::new(IngressQueues::new(vec![consumer]), engine, None);
    let mut folded = Folded::default();
    let mut sent = Vec::new();
    for message in sequence {
        producer
            .push(*message)
            .expect("the queue is empty before each push");
        assert_eq!(
            driver.step(),
            Step::Folded,
            "the driver folds the message it was given"
        );
        while let Ok(item) = commands.pop() {
            if let ExecLaneItem::Command(command) = item {
                sent.push(command);
            }
        }
        folded.digests.push(driver.engine().state_digest());
        folded.drain(&mut lanes, &mut sent);
    }
    assert_no_drop(driver.engine());
    folded
}

fn fold_by_replay_driver(sequence: &[InboundMessage]) -> Folded {
    let (engine, commands, mut lanes) = engine();
    let mut driver = ReplayDriver::new(engine, vec![commands]);
    let mut folded = Folded::default();
    let mut sent = Vec::new();
    for message in sequence {
        driver.step(message, &mut sent);
        folded.digests.push(driver.state_digest());
        folded.drain(&mut lanes, &mut sent);
    }
    assert_eq!(
        driver.folded(),
        sequence.len() as u64,
        "the driver counts each message it folds"
    );
    assert_no_drop(driver.engine());
    folded
}

fn assert_no_drop(engine: &HotEngine) {
    let counters = engine.counters();
    assert_eq!(
        (
            counters.research_dropped.total() + counters.audit_lost,
            counters.ui_books_dropped,
            counters.ui_events_dropped
        ),
        (0, 0, 0),
        "a ring dropped a record, so a lane holds less than the fold wrote"
    );
}

/// The first place where the two folds part: the first message after which their digests differ,
/// then the first record in which a lane differs.
fn first_difference(sequence: &[InboundMessage], hot: &Folded, replay: &Folded) -> Option<String> {
    let parted = hot
        .digests
        .iter()
        .zip(&replay.digests)
        .position(|(a, b)| a != b);
    if let Some(index) = parted {
        return Some(format!(
            "the digests part after message {index}, {:?}: {:?} by the hot driver, {:?} by the \
             replay driver",
            sequence[index], hot.digests[index], replay.digests[index]
        ));
    }
    for ((lane, ours), (_, theirs)) in hot.lanes().into_iter().zip(replay.lanes()) {
        if let Some(index) = ours.iter().zip(theirs).position(|(a, b)| a != b) {
            return Some(format!(
                "lane {lane} parts at record {index}: {} by the hot driver, {} by the replay driver",
                ours[index], theirs[index]
            ));
        }
        if ours.len() != theirs.len() {
            return Some(format!(
                "lane {lane} holds {} records by the hot driver, {} by the replay driver",
                ours.len(),
                theirs.len()
            ));
        }
    }
    None
}

fn drivers_agree(sequence: &[InboundMessage]) -> Result<Folded, String> {
    let hot = fold_by_hot_driver(sequence);
    let replay = fold_by_replay_driver(sequence);
    match first_difference(sequence, &hot, &replay) {
        Some(difference) => Err(difference),
        None => Ok(hot),
    }
}

/// FITNESS: a replay folds what the hot thread folds. Over a sequence that places and fills, and
/// over drawn sequences that hold each kind of message, the replay driver and the hot thread's
/// driver hold the same state after each message and send the same commands, write the same
/// records and tee the same events. A replay that parted from the hot thread would judge a run
/// that never traded.
#[test]
fn replay_driver_equals_hot_thread() {
    let placing = Builder::sequence(&placing_and_filling_steps(WARMUP.micros()));
    let folded = drivers_agree(&placing)
        .unwrap_or_else(|difference| panic!("the placing sequence parts: {difference}"));
    assert!(
        folded.places > 0 && folded.fills > 0,
        "the placing sequence placed {} orders and folded {} fills",
        folded.places,
        folded.fills
    );

    let kinds = Cell::new([0_u64; KINDS.len()]);
    let sent = Cell::new(0_usize);
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(&steps(), |steps| {
        let sequence = Builder::sequence(&steps);
        let mut seen = kinds.get();
        for message in &sequence {
            seen[kind_of(message)] += 1;
        }
        kinds.set(seen);
        let folded = drivers_agree(&sequence).map_err(TestCaseError::fail)?;
        sent.set(sent.get() + folded.commands.len());
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("a drawn sequence parts: {failure}");
    }
    for (kind, count) in KINDS.iter().zip(kinds.get()) {
        assert!(count > 0, "no drawn sequence held a {kind} message");
    }
    assert!(
        sent.get() > 0,
        "the drawn sequences sent no command, so the command lanes were never compared"
    );
    println!(
        "replay driver: {} commands sent over the drawn sequences, kinds {:?}",
        sent.get(),
        kinds.get()
    );
}
