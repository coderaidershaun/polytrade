//! The fold is a pure function of its messages: two folds of one sequence hold the same state after
//! each message and write the same records, whether or not the engine measures its latency.

use std::cell::Cell;

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup, StateDigest};
use engine::hot::exec::{ClientIdLayout, side_base};
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{
    DesiredQuote, Fill, OrderReject, OrderStyle, OrderUpdate, QuoteLevel, Registration, Strategy,
    StrategyCtx,
};
use engine::link::{LinkFrame, LinkHash, LinkOrigin, LinkPayload, TopicId};
use engine::log::{HotRecord, TextRecord, open_hot_lane, open_text_lane};
use engine::msg::exec::{
    AccountChunk, ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{
    BookChunk, InboundMessage, KlineEvent, MarketRotation, MessageBody, MessageKind, SpinTick,
    TradeEvent,
};
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::run_state::{RunAssertion, RunState};
use engine::msg::time::{DurationUs, Stamper};
use engine::msg::ui::{UiBookSnapshot, UiEvent};
use engine::runtime::config::{KlineInterval, RecordedTables, TableKind};
use engine::runtime::exposure::ExposureSnapshot;
use engine::runtime::registry::InstrumentRow;
use engine::venues::exec::open_orders_snapshot_end;
use proptest::prelude::{Just, any, prop, prop_assert_eq, prop_oneof};
use proptest::strategy::Strategy as Generator;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ALL_TABLES, FillPen, ONE, account_chunk, book_reset, call_codes, delta_chunk, detached_setup,
    exec_event, exposure_ring, instrument_row, kline, last_snapshot_chunk, partial_snapshot_chunk,
    persist_ring_for, rotation, run_control, snapshot_pair, spin, strategy_log_ring, stream_ready,
    tracker_spec_all, trade, ts, ui_book_ring, ui_event_ring, venue_ts,
};
use crate::exec_support::exec_settings;

/// Far more than one message writes to any lane. Each lane is drained after each message, so no
/// ring fills and each lane holds each record that the fold wrote.
const LANE: usize = 4_096;

const SEQUENCE_LEN: usize = 200;

/// The size that the probe quotes on each side.
const QUOTE_QTY: Qty = Qty::from_mantissa(ONE / 10);

/// Writes one feature row and one note line for each callback, the value of the row the code of
/// the callback, so the tape holds the order of callbacks. Each spin it quotes both sides of each
/// market at the touch, so orders go out and the answers of the venue fold back in.
#[derive(Default)]
struct Probe {
    call: Option<FeatureId>,
    markets: u16,
}

impl Probe {
    fn record(&self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId, code: f64) {
        ctx.emit(self.call.expect("features registered"), instrument, code);
        engine::note!(
            ctx,
            "probe callback {} on instrument {}",
            code,
            instrument.0
        );
    }

    fn quote_touch(ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        let book = ctx.book(instrument);
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        for (side, price) in [(Side::Buy, bid.price), (Side::Sell, ask.price)] {
            let quote = DesiredQuote {
                price,
                qty: QUOTE_QTY,
                style: OrderStyle::PostOnly,
            };
            ctx.quote(instrument, side, QuoteLevel::ZERO, Some(quote));
        }
    }
}

impl Strategy for Probe {
    fn features(&self) -> &'static [&'static str] {
        &["call"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
        self.markets = u16::try_from(registration.instruments.len()).expect("a few markets");
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
        for market in 0..self.markets {
            Self::quote_touch(ctx, InstrumentId(market));
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

/// The strategy of the feed sequence: each spin it writes one feature and declares one bid, so a
/// fold with no venue still tees each kind of event that it can.
#[derive(Default)]
struct FeedProbe {
    feature: Option<FeatureId>,
}

impl Strategy for FeedProbe {
    fn features(&self) -> &'static [&'static str] {
        &["mixed"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.feature = registration.features.first().copied();
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.emit(self.feature.expect("registered"), InstrumentId(0), 7.0);
        let bid = DesiredQuote {
            price: Price::from_mantissa(100 * ONE),
            qty: Qty::from_mantissa(ONE),
            style: OrderStyle::PostOnly,
        };
        ctx.quote(InstrumentId(0), Side::Buy, QuoteLevel::ZERO, Some(bid));
    }
}

/// How a fold starts: its markets, its strategy, and whether a venue answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    /// Two markets, the probe, a command lane and a warmup.
    TwoMarkets,
    /// One market and the strategy of the feed sequence, with no venue.
    Feed,
    /// One market and the probe, with no venue and no warmup.
    Callbacks,
}

impl Start {
    fn rows(self) -> Vec<InstrumentRow> {
        let markets = match self {
            Start::TwoMarkets => 2,
            Start::Feed | Start::Callbacks => 1,
        };
        (0..markets)
            .map(|id| {
                let mut row = instrument_row(id, tracker_spec_all(1), 64);
                row.venue_symbol = format!("btcusdt-{id}").into();
                row
            })
            .collect()
    }

    fn strategy(self) -> Box<dyn Strategy> {
        match self {
            Start::TwoMarkets | Start::Callbacks => Box::new(Probe::default()),
            Start::Feed => Box::new(FeedProbe::default()),
        }
    }

    fn tables(self) -> RecordedTables {
        match self {
            Start::TwoMarkets => RecordedTables::new(&[
                TableKind::Trades,
                TableKind::BookEvents,
                TableKind::Klines,
                TableKind::Features,
                TableKind::LinkFrames,
                TableKind::Orders,
                TableKind::Fills,
            ]),
            Start::Feed | Start::Callbacks => RecordedTables::new(&ALL_TABLES),
        }
    }

    fn warmup(self) -> DurationUs {
        match self {
            Start::TwoMarkets => DurationUs::from_millis(500),
            Start::Feed | Start::Callbacks => DurationUs::ZERO,
        }
    }

    fn has_venue(self) -> bool {
        self == Start::TwoMarkets
    }
}

/// Whether the fold measures its own latency as an engine start wires it: a real counter in the
/// command bank, and a read of the counters of the fold on each spin, as the driver reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Measure {
    On,
    Off,
}

/// The read ends of each output lane of one engine.
struct Readers {
    persist: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    ui_books: Consumer<UiBookSnapshot>,
    ui_events: Consumer<UiEvent>,
    exposure: Consumer<ExposureSnapshot>,
    strategy_log: Consumer<HotRecord>,
}

fn engine(start: Start, stamper: Stamper) -> (HotEngine, Readers) {
    let rows = start.rows();
    let (persistence, persist) = persist_ring_for(LANE, start.tables());
    let (strategy_log_sink, strategy_log) = strategy_log_ring(LANE);
    let (ui_book_sink, ui_books) = ui_book_ring(LANE);
    let (ui_event_sink, ui_events) = ui_event_ring(LANE);
    let (exposure_sink, exposure) = exposure_ring(LANE);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(LANE);
    let exec = start.has_venue().then(|| ExecWiring {
        sink: ExecSink::new(commands_producer, stamper),
        settings: exec_settings(),
        run_nonce: 0,
    });
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        strategy_log_sink,
        ui_book_sink,
        ui_event_sink,
        warmup: start.warmup(),
        exec,
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_sink,
        },
        ..detached_setup(&rows, start.strategy())
    });
    let readers = Readers {
        persist,
        commands,
        ui_books,
        ui_events,
        exposure,
        strategy_log,
    };
    (engine, readers)
}

/// What one fold leaves behind: the digest after each message, and each record of each lane.
#[derive(Debug, Default)]
struct Folded {
    digests: Vec<StateDigest>,
    tape_rows: Vec<PersistRecord>,
    commands: Vec<ExecLaneItem>,
    ui_books: Vec<UiBookSnapshot>,
    ui_events: Vec<UiEvent>,
    exposure: Vec<ExposureSnapshot>,
    strategy_log: Vec<HotRecord>,
    hot_log: Vec<HotRecord>,
    /// Hot calls that ran before the first message, made into text at once. Their engine time is a
    /// clock reading, so only the text counts.
    text_log: Vec<String>,
}

impl Folded {
    /// Each lane as the text of its records, so a float compares by the text of its value and a
    /// failure can print the record. A command counts by its bank stamp, never by the latency tick
    /// that it carries.
    fn lanes(&self) -> [(&'static str, Vec<String>); 8] {
        let commands = self.commands.iter().map(|item| match item {
            ExecLaneItem::Command(stamped) => {
                format!("{:?} {:?}", stamped.command, stamped.issued_ts_us)
            }
            ExecLaneItem::Watermark(at) => format!("watermark {at:?}"),
        });
        [
            ("tape rows", texts(&self.tape_rows)),
            ("commands", commands.collect()),
            ("ui books", texts(&self.ui_books)),
            ("ui events", texts(&self.ui_events)),
            ("exposure", texts(&self.exposure)),
            (
                "strategy log",
                self.strategy_log.iter().map(rendered).collect(),
            ),
            ("hot log", self.hot_log.iter().map(rendered).collect()),
            ("text log", self.text_log.clone()),
        ]
    }

    fn places(&self) -> usize {
        self.commands
            .iter()
            .filter(|item| match item {
                ExecLaneItem::Command(stamped) => {
                    matches!(stamped.command, ExecCommand::Place { .. })
                }
                ExecLaneItem::Watermark(_) => false,
            })
            .count()
    }

    fn fills(&self) -> usize {
        self.ui_events
            .iter()
            .filter(|event| matches!(event, UiEvent::Fill { .. }))
            .count()
    }
}

fn texts<T: std::fmt::Debug>(records: &[T]) -> Vec<String> {
    records.iter().map(|record| format!("{record:?}")).collect()
}

fn rendered(record: &HotRecord) -> String {
    let mut line = format!("{:?} {:?} ", record.callsite().level(), record.at());
    record
        .render(&mut line)
        .expect("a string takes every write");
    line
}

fn drain<T>(consumer: &mut Consumer<T>, into: &mut Vec<T>) {
    while let Ok(record) = consumer.pop() {
        into.push(record);
    }
}

/// One fold on a thread of its own with its own log lanes, as the hot thread runs.
fn fold(start: Start, sequence: &[InboundMessage], measure: Measure) -> Folded {
    let sequence = sequence.to_vec();
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || fold_on_this_thread(start, &sequence, measure))
        .expect("the test can spawn a thread")
        .join()
        .expect("the fold does not panic")
}

fn fold_on_this_thread(start: Start, sequence: &[InboundMessage], measure: Measure) -> Folded {
    let mut hot_log = open_hot_lane(LANE);
    let mut text_log = open_text_lane(LANE);
    let stamper = match measure {
        Measure::On => Stamper::Counter(quanta::Clock::new()),
        Measure::Off => Stamper::Off,
    };
    let (mut engine, mut readers) = engine(start, stamper);
    let mut folded = Folded::default();
    for message in sequence {
        engine.dispatch(message);
        if measure == Measure::On && matches!(message.body, MessageBody::SpinTick(_)) {
            std::hint::black_box(engine.counters());
        }
        folded.digests.push(engine.state_digest());
        drain(&mut readers.persist, &mut folded.tape_rows);
        drain(&mut readers.commands, &mut folded.commands);
        drain(&mut readers.ui_books, &mut folded.ui_books);
        drain(&mut readers.ui_events, &mut folded.ui_events);
        drain(&mut readers.exposure, &mut folded.exposure);
        drain(&mut readers.strategy_log, &mut folded.strategy_log);
        while let Some(record) = hot_log.pop() {
            folded.hot_log.push(record);
        }
        while let Some(record) = text_log.pop() {
            folded.text_log.push(text_of(&record));
        }
    }
    let counters = engine.counters();
    assert_eq!(
        (
            counters.persist_dropped,
            counters.ui_books_dropped,
            counters.ui_events_dropped,
            counters.exposure_superseded,
            hot_log.dropped(),
        ),
        (0, 0, 0, 0, 0),
        "a ring dropped a record, so a lane holds less than the fold wrote"
    );
    folded
}

fn text_of(record: &TextRecord) -> String {
    format!("{:?} {}", record.callsite().level(), record.text())
}

/// The first place where two folds of `sequence` part: the first message after which their digests
/// differ, then the first record in which a lane differs.
fn first_difference(
    sequence: &[InboundMessage],
    first: &Folded,
    second: &Folded,
) -> Option<String> {
    let parted = first
        .digests
        .iter()
        .zip(&second.digests)
        .position(|(ours, theirs)| ours != theirs);
    if let Some(index) = parted {
        return Some(format!(
            "the digests part after message {index}, {:?}: {:?} against {:?}",
            sequence[index], first.digests[index], second.digests[index]
        ));
    }
    for ((lane, ours), (_, theirs)) in first.lanes().into_iter().zip(second.lanes()) {
        if let Some(index) = ours.iter().zip(&theirs).position(|(a, b)| a != b) {
            return Some(format!(
                "lane {lane} parts at record {index}: {} against {}",
                ours[index], theirs[index]
            ));
        }
        if ours.len() != theirs.len() {
            return Some(format!(
                "lane {lane} holds {} records against {}",
                ours.len(),
                theirs.len()
            ));
        }
    }
    None
}

/// Folds `sequence` two times from `start` and returns the first fold, or the first difference.
fn two_folds_agree(start: Start, sequence: &[InboundMessage]) -> Result<Folded, String> {
    let first = fold(start, sequence, Measure::Off);
    let second = fold(start, sequence, Measure::Off);
    assert_eq!(
        first.digests.len(),
        sequence.len(),
        "one digest after each message"
    );
    match first_difference(sequence, &first, &second) {
        Some(difference) => Err(difference),
        None => Ok(first),
    }
}

/// One name for each kind of message, in the order of [`kind_of`].
const KINDS: [&str; 10] = [
    "trade",
    "book",
    "book reset",
    "market rotation",
    "kline",
    "spin",
    "link",
    "run control",
    "exec",
    "account",
];

/// No arm catches the rest, so a new kind of message stops this file compiling here. Its name then
/// joins [`KINDS`], and the property fails until the generator draws it.
fn kind_of(message: &InboundMessage) -> usize {
    match &message.body {
        MessageBody::Trade(_) => 0,
        MessageBody::Book(_) => 1,
        MessageBody::BookReset(_) => 2,
        MessageBody::MarketRotation(_) => 3,
        MessageBody::Kline(_) => 4,
        MessageBody::SpinTick(_) => 5,
        MessageBody::Link(_) => 6,
        MessageBody::RunControl(_) => 7,
        MessageBody::Exec(_) => 8,
        MessageBody::Account(_) => 9,
    }
}

/// The values of one drawn message. Each variant makes one kind of message.
#[derive(Debug, Clone, Copy)]
enum Draw {
    Trade {
        market: u16,
        offset: i64,
        lots: i64,
        side: Side,
    },
    Book(BookDraw),
    BookReset {
        market: u16,
    },
    Rotation {
        market: u16,
    },
    Kline {
        market: u16,
        offset: i64,
        is_closed: bool,
        is_tracked: bool,
    },
    Spin,
    Link {
        value: i32,
    },
    RunControl {
        is_running: bool,
        is_new_epoch: bool,
    },
    Exec(ExecDraw),
    Account {
        base_units: i64,
        quote_units: i64,
    },
}

/// One book chunk of one market, its prices a few units from 100 on its own side.
#[derive(Debug, Clone, Copy)]
struct BookDraw {
    market: u16,
    chunk: Chunk,
    side: Side,
    offset: i64,
    lots: i64,
}

/// A book chunk: a delta, or the first or the last chunk of a snapshot.
#[derive(Debug, Clone, Copy)]
enum Chunk {
    Delta,
    SnapshotBids,
    SnapshotAsks,
}

/// What the venue says about the orders of one side of one market.
#[derive(Debug, Clone, Copy)]
struct ExecDraw {
    market: u16,
    side: Side,
    answer: Answer,
}

/// What the venue says about orders.
#[derive(Debug, Clone, Copy)]
enum Answer {
    StreamReady,
    SnapshotEnd,
    /// A fill of an order that an earlier run left resting.
    PriorFill {
        offset: i64,
        lots: i64,
    },
    /// An answer about one of the first slots of a side, which this run may have placed.
    Slot {
        slot: usize,
        generation: u16,
        kind: ExecKind,
        status: VenueOrderStatus,
    },
}

/// One drawn message and the gap of message time before it.
#[derive(Debug, Clone, Copy)]
struct Step {
    draw: Draw,
    gap_us: i64,
}

fn side() -> impl Generator<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn answer() -> impl Generator<Value = Answer> {
    let kind = prop_oneof![
        Just((ExecKind::AckPlaced, VenueOrderStatus::New)),
        Just((ExecKind::AckCanceled, VenueOrderStatus::Canceled)),
        Just((ExecKind::ReportTrade, VenueOrderStatus::Filled)),
        Just((ExecKind::ReportCanceled, VenueOrderStatus::Canceled)),
    ];
    prop_oneof![
        Just(Answer::StreamReady),
        Just(Answer::SnapshotEnd),
        (0..5_i64, 1..4_i64).prop_map(|(offset, lots)| Answer::PriorFill { offset, lots }),
        (0..4_usize, 1..4_u16, kind).prop_map(|(slot, generation, (kind, status))| {
            Answer::Slot {
                slot,
                generation,
                kind,
                status,
            }
        }),
    ]
}

fn draw() -> impl Generator<Value = Draw> {
    let market = 0..2_u16;
    let chunk = prop_oneof![
        2 => Just(Chunk::Delta),
        1 => Just(Chunk::SnapshotBids),
        1 => Just(Chunk::SnapshotAsks),
    ];
    prop_oneof![
        3 => (market.clone(), 0..5_i64, 1..4_i64, side())
            .prop_map(|(market, offset, lots, side)| Draw::Trade { market, offset, lots, side }),
        4 => (market.clone(), chunk, side(), 0..5_i64, 0..4_i64).prop_map(
            |(market, chunk, side, offset, lots)| {
                Draw::Book(BookDraw { market, chunk, side, offset, lots })
            }
        ),
        1 => market.clone().prop_map(|market| Draw::BookReset { market }),
        1 => market.clone().prop_map(|market| Draw::Rotation { market }),
        2 => (market.clone(), 0..5_i64, any::<bool>(), any::<bool>()).prop_map(
            |(market, offset, is_closed, is_tracked)| Draw::Kline {
                market,
                offset,
                is_closed,
                is_tracked
            }
        ),
        4 => Just(Draw::Spin),
        1 => any::<i32>().prop_map(|value| Draw::Link { value }),
        1 => (any::<bool>(), any::<bool>()).prop_map(|(is_running, is_new_epoch)| {
            Draw::RunControl { is_running, is_new_epoch }
        }),
        4 => (market.clone(), side(), answer())
            .prop_map(|(market, side, answer)| Draw::Exec(ExecDraw { market, side, answer })),
        1 => (0..20_i64, 0..2_000_000_i64).prop_map(|(base_units, quote_units)| Draw::Account {
            base_units,
            quote_units
        }),
    ]
}

fn steps() -> impl Generator<Value = Vec<Step>> {
    let step = (draw(), 0..20_000_i64).prop_map(|(draw, gap_us)| Step { draw, gap_us });
    prop::collection::vec(step, SEQUENCE_LEN)
}

/// A sequence that is sure to place: the account stream is up, the open orders have landed, the
/// balances are known, both sides of market 0 are booked, and a spin past the warmup of market 0
/// lets the probe quote its touch. Drawn sequences place only by chance.
fn placing_steps() -> Vec<Step> {
    let ready = |answer| {
        Draw::Exec(ExecDraw {
            market: 0,
            side: Side::Buy,
            answer,
        })
    };
    let book = |chunk, side| {
        Draw::Book(BookDraw {
            market: 0,
            chunk,
            side,
            offset: 0,
            lots: 1,
        })
    };
    let account = Draw::Account {
        base_units: 10,
        quote_units: 1_000_000,
    };
    let past_warmup_us = Start::TwoMarkets.warmup().micros() + 100_000;
    [
        (ready(Answer::StreamReady), 0),
        (ready(Answer::SnapshotEnd), 0),
        (account, 0),
        (book(Chunk::SnapshotBids, Side::Buy), 0),
        (book(Chunk::SnapshotAsks, Side::Sell), 0),
        (Draw::Spin, past_warmup_us),
    ]
    .map(|(draw, gap_us)| Step { draw, gap_us })
    .to_vec()
}

/// The placing sequence, then a fill: the first answer seats an order that an earlier run left
/// resting on market 0, and the second fills it.
fn placing_and_filling_steps() -> Vec<Step> {
    let prior_fill = Step {
        draw: Draw::Exec(ExecDraw {
            market: 0,
            side: Side::Buy,
            answer: Answer::PriorFill { offset: 0, lots: 1 },
        }),
        gap_us: 1_000,
    };
    let mut steps = placing_steps();
    steps.extend([prior_fill, prior_fill]);
    steps
}

/// What a drawn sequence keeps from one message to the next: message time, the next spin and peer
/// frame numbers, the run epoch, and the fills of the order an earlier run left on each market.
struct Builder {
    when: i64,
    spin_seq: u64,
    link_seq: u64,
    epoch: u64,
    pens: [FillPen; 2],
}

impl Builder {
    fn new() -> Self {
        Self {
            when: 1_000_000,
            spin_seq: 0,
            link_seq: 0,
            epoch: 0,
            pens: [FillPen::new(0), FillPen::new(1)],
        }
    }

    fn sequence(steps: &[Step]) -> Vec<InboundMessage> {
        let mut builder = Self::new();
        steps.iter().map(|step| builder.message(step)).collect()
    }

    fn message(&mut self, step: &Step) -> InboundMessage {
        self.when += step.gap_us;
        let when = self.when;
        match step.draw {
            Draw::Trade {
                market,
                offset,
                lots,
                side,
            } => {
                let price = (98 + offset) * ONE;
                InboundMessage::new(
                    ts(when),
                    MessageBody::Trade(trade(market, price, lots * ONE / 10, side, when)),
                )
            }
            Draw::Book(book) => InboundMessage::new(ts(when), MessageBody::Book(book.chunk(when))),
            Draw::BookReset { market } => book_reset(market, when),
            Draw::Rotation { market } => rotation(market, when, when + 300_000_000, when),
            Draw::Kline {
                market,
                offset,
                is_closed,
                is_tracked,
            } => {
                let interval =
                    if is_tracked { KlineInterval::OneMinute } else { KlineInterval::FiveMinutes };
                let close = (99 + offset) * ONE;
                let ohlc = (100 * ONE, 104 * ONE, 98 * ONE, close);
                InboundMessage::new(
                    ts(when),
                    MessageBody::Kline(kline(market, interval, ohlc, is_closed, when)),
                )
            }
            Draw::Spin => {
                self.spin_seq += 1;
                spin(self.spin_seq, when)
            }
            Draw::Link { value } => {
                self.link_seq += 1;
                InboundMessage::new(
                    ts(when),
                    MessageBody::Link(peer_frame(self.link_seq, f64::from(value), when)),
                )
            }
            Draw::RunControl {
                is_running,
                is_new_epoch,
            } => {
                self.epoch += u64::from(is_new_epoch);
                let state = if is_running { RunState::Running } else { RunState::Idle };
                let desired = RunAssertion {
                    state,
                    epoch: self.epoch,
                };
                run_control(desired, when)
            }
            Draw::Exec(answer) => self.answer(answer, when),
            Draw::Account {
                base_units,
                quote_units,
            } => {
                let balances = [
                    (AssetId(0), base_units * ONE),
                    (AssetId(1), quote_units * ONE),
                ];
                // Each report carries its own venue time, or the engine sets it aside as stale.
                let venue_update_ts_ms = u64::try_from(when / 1_000).expect("time is positive");
                InboundMessage::new(
                    ts(when),
                    MessageBody::Account(AccountChunk {
                        venue_update_ts_ms,
                        ..account_chunk(&balances, when)
                    }),
                )
            }
        }
    }

    fn answer(&mut self, draw: ExecDraw, when: i64) -> InboundMessage {
        let ExecDraw {
            market,
            side,
            answer,
        } = draw;
        let instrument = InstrumentId(market);
        match answer {
            Answer::StreamReady => stream_ready(instrument, when),
            Answer::SnapshotEnd => InboundMessage::new(
                ts(when),
                MessageBody::Exec(open_orders_snapshot_end(instrument, ts(when))),
            ),
            Answer::PriorFill { offset, lots } => {
                let pen = &mut self.pens[usize::from(market)];
                let price = (99 + offset) * ONE;
                pen.adopt(side, price, when)
                    .unwrap_or_else(|| pen.report(side, price, lots * ONE / 10, when))
            }
            Answer::Slot {
                slot,
                generation,
                kind,
                status,
            } => {
                let client_id = ClientIdLayout { run_nonce: 0 }
                    .encode(side_base(instrument, side) + slot, generation);
                let price = if side == Side::Buy { 99 * ONE } else { 101 * ONE };
                let is_fill = kind == ExecKind::ReportTrade;
                let filled = if is_fill { QUOTE_QTY } else { Qty::from_mantissa(0) };
                InboundMessage::new(
                    ts(when),
                    MessageBody::Exec(ExecEvent {
                        kind,
                        status: Some(status),
                        qty: QUOTE_QTY,
                        last_price: Price::from_mantissa(if is_fill { price } else { 0 }),
                        last_qty: filled,
                        cumulative_qty: filled,
                        cumulative_quote: Price::from_mantissa(price).notional(filled),
                        ..exec_event(instrument, client_id, side, price, when)
                    }),
                )
            }
        }
    }
}

impl BookDraw {
    fn chunk(self, when: i64) -> BookChunk {
        let BookDraw {
            market,
            chunk,
            side,
            offset,
            lots,
        } = self;
        let bid = (99 - offset) * ONE;
        let ask = (101 + offset) * ONE;
        match chunk {
            Chunk::Delta => {
                let price = if side == Side::Buy { bid } else { ask };
                delta_chunk(market, side, &[(price, lots * ONE / 2)], when)
            }
            Chunk::SnapshotBids => {
                partial_snapshot_chunk(market, Side::Buy, &[(bid, ONE), (bid - ONE, 2 * ONE)], when)
            }
            Chunk::SnapshotAsks => last_snapshot_chunk(
                market,
                Side::Sell,
                &[(ask, ONE), (ask + ONE, 2 * ONE)],
                when,
            ),
        }
    }
}

/// A state frame of a peer engine, carrying one value.
fn peer_frame(seq: u64, value: f64, when: i64) -> LinkFrame {
    LinkFrame {
        origin: LinkOrigin {
            sender_te_hash: LinkHash::of_name("peer-te"),
            boot_ts_us: venue_ts(1),
            topic: TopicId::FIRST_STRATEGY,
            seq,
        },
        payload: LinkPayload::new(
            LinkHash::of_name("peer-schema"),
            venue_ts(when - 1),
            &[value],
        ),
    }
}

/// Two snapshot chunks, a trade, a delta, a kline, a spin, a reset, then a rotation with the reset
/// and the fresh snapshot that follow it: one callback each.
fn callback_sequence() -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(
        0,
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        1,
    );
    vec![
        InboundMessage::new(ts(1), MessageBody::Book(bids)),
        InboundMessage::new(ts(1), MessageBody::Book(asks)),
        InboundMessage::new(
            ts(2),
            MessageBody::Trade(trade(0, 100 * ONE, 2_000_000, Side::Buy, 2)),
        ),
        InboundMessage::new(
            ts(3),
            MessageBody::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 5 * ONE)], 3)),
        ),
        InboundMessage::new(
            ts(4),
            MessageBody::Kline(kline(
                0,
                KlineInterval::OneMinute,
                (100 * ONE, 103 * ONE, 98 * ONE, 101 * ONE),
                true,
                4,
            )),
        ),
        spin(1, 5),
        book_reset(0, 6),
        // The rotation is stamped at subscribe, 7, and its window opens later, at 300 s.
        rotation(0, 300_000_000, 600_000_000, 7),
        book_reset(0, 8),
        InboundMessage::new(
            ts(9),
            MessageBody::Book(snapshot_pair(0, &[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 9).0),
        ),
        InboundMessage::new(
            ts(9),
            MessageBody::Book(snapshot_pair(0, &[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 9).1),
        ),
    ]
}

/// Books, prints, a fill of a probe order on each print with its order update, spins that tee a
/// feature, a quote and a position, deltas, and a rotation.
fn feed_sequence() -> Vec<InboundMessage> {
    let mut messages = Vec::new();
    let mut pen = FillPen::new(0);
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 10);
    messages.push(InboundMessage::new(ts(10), MessageBody::Book(bids)));
    messages.push(InboundMessage::new(ts(10), MessageBody::Book(asks)));
    for seq in 0..5_u64 {
        let when = 100 + seq as i64 * 10;
        let side = if seq % 2 == 0 { Side::Buy } else { Side::Sell };
        messages.push(InboundMessage::new(
            ts(when),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, side, when)),
        ));
        messages.extend(pen.fill(side, 100 * ONE, ONE, when + 1));
        messages.push(spin(seq, when + 2));
        messages.push(InboundMessage::new(
            ts(when + 5),
            MessageBody::Book(delta_chunk(
                0,
                Side::Buy,
                &[(100 * ONE, (seq as i64 + 2) * ONE)],
                when + 5,
            )),
        ));
    }
    messages.push(rotation(0, 300 * ONE, 600 * ONE, 200));
    messages
}

/// Each kind of event that a fold with no venue can tee. Balance and Reject need messages that the
/// feed sequence does not carry, and Execution and OrderSnapshot need a command lane.
fn assert_feed_reaches_each_kind(feed: &Folded) {
    let mut kinds = [false; 7];
    for event in &feed.ui_events {
        let slot = match event {
            UiEvent::Quote { .. } => 0,
            UiEvent::Trade { .. } => 1,
            UiEvent::OrderUpdate { .. } => 2,
            UiEvent::Feature { .. } => 3,
            UiEvent::Fill { .. } => 4,
            UiEvent::Rotation { .. } => 5,
            UiEvent::Position { .. } => 6,
            UiEvent::OrderSnapshot { .. } | UiEvent::Execution { .. } => continue,
            UiEvent::Balance { .. } | UiEvent::Reject { .. } => continue,
        };
        kinds[slot] = true;
    }
    assert!(
        kinds.iter().all(|&seen| seen),
        "the comparison must cover every event kind the feed reaches, saw {kinds:?}"
    );
    assert!(
        !feed.ui_books.is_empty(),
        "the feed actually emitted book snapshots"
    );
}

fn runner(cases: u32) -> TestRunner {
    TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    })
}

/// FITNESS: the fold is a pure function of its messages. Two folds of one sequence from one start
/// give the same digest after each message and the same records on each lane. A digest that
/// differs while every lane agrees is a drift that no row shows yet. The drawn sequences hold each
/// kind of message, the feed sequence tees each kind of event that a fold with no venue can, and
/// the callback sequence pins that state folds first and then one callback runs for each message.
#[test]
fn fold_twice_gives_equal_digest_at_each_message() {
    let callbacks =
        two_folds_agree(Start::Callbacks, &callback_sequence()).unwrap_or_else(|difference| {
            panic!("two folds of the callback sequence part: {difference}")
        });
    assert_eq!(
        call_codes(&callbacks.tape_rows),
        vec![2.0, 2.0, 1.0, 2.0, 4.0, 6.0, 3.0, 5.0, 3.0, 2.0, 2.0],
        "state-before-callback dispatch order"
    );
    assert!(
        !callbacks.strategy_log.is_empty(),
        "the probe banked strategy log lines"
    );
    assert!(
        callbacks.digests.windows(2).all(|pair| pair[0] != pair[1]),
        "each message of the callback case tees an event, which moves the state, so each moves \
         the digest"
    );

    let feed = two_folds_agree(Start::Feed, &feed_sequence())
        .unwrap_or_else(|difference| panic!("two folds of the feed sequence part: {difference}"));
    assert_feed_reaches_each_kind(&feed);

    let filling = two_folds_agree(
        Start::TwoMarkets,
        &Builder::sequence(&placing_and_filling_steps()),
    )
    .unwrap_or_else(|difference| {
        panic!("two folds of the placing and filling sequence part: {difference}")
    });
    assert!(
        filling.places() > 0 && filling.fills() > 0,
        "the placing and filling sequence placed {} orders and folded {} fills",
        filling.places(),
        filling.fills()
    );

    let kinds = Cell::new([0_u64; KINDS.len()]);
    let (places, fills) = (Cell::new(filling.places()), Cell::new(filling.fills()));
    let outcome = runner(64).run(&steps(), |steps| {
        let sequence = Builder::sequence(&steps);
        let mut seen = kinds.get();
        for message in &sequence {
            seen[kind_of(message)] += 1;
        }
        kinds.set(seen);
        let folded = two_folds_agree(Start::TwoMarkets, &sequence).map_err(TestCaseError::fail)?;
        places.set(places.get() + folded.places());
        fills.set(fills.get() + folded.fills());
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("two folds of a drawn sequence part: {failure}");
    }
    for (kind, count) in KINDS.iter().zip(kinds.get()) {
        assert!(count > 0, "no drawn sequence held a {kind} message");
    }
    assert!(
        places.get() > 0 && fills.get() > 0,
        "the drawn folds placed orders and folded fills: {} placements, {} fills",
        places.get(),
        fills.get()
    );
}

/// FITNESS: measurement changes no state. A fold that stamps each banked command with a real
/// counter, and reads the counters of the fold on each spin as the driver does, holds the same
/// state after each message as a fold that measures nothing, and writes the same records but for
/// the latency tick of each command.
#[test]
fn state_is_equal_with_measurement_on_and_off() {
    let placing = Builder::sequence(&placing_steps());
    let measured = fold(Start::TwoMarkets, &placing, Measure::On);
    let unmeasured = fold(Start::TwoMarkets, &placing, Measure::Off);
    if let Some(difference) = first_difference(&placing, &measured, &unmeasured) {
        panic!("measurement changed the fold of the placing sequence: {difference}");
    }
    assert!(
        measured.places() > 0,
        "the placing sequence banked no command, so the bank never read its counter"
    );
    let places = Cell::new(measured.places());
    let outcome = runner(16).run(&steps(), |steps| {
        let sequence = Builder::sequence(&steps);
        let measured = fold(Start::TwoMarkets, &sequence, Measure::On);
        let unmeasured = fold(Start::TwoMarkets, &sequence, Measure::Off);
        if let Some(difference) = first_difference(&sequence, &measured, &unmeasured) {
            return Err(TestCaseError::fail(difference));
        }
        places.set(places.get() + measured.places());
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("measurement changed a fold: {failure}");
    }
    assert!(
        places.get() > 0,
        "the measured folds banked commands, so the bank read its counter"
    );
}

/// The kind that each body names, written apart from the engine's own match so that a wrong arm
/// there shows here. No arm catches the rest.
fn kind_named_by(body: &MessageBody) -> MessageKind {
    match body {
        MessageBody::Trade(_) => MessageKind::Trade,
        MessageBody::Book(_) => MessageKind::Book,
        MessageBody::BookReset(_) => MessageKind::BookReset,
        MessageBody::MarketRotation(_) => MessageKind::MarketRotation,
        MessageBody::Kline(_) => MessageKind::Kline,
        MessageBody::SpinTick(_) => MessageKind::SpinTick,
        MessageBody::Link(_) => MessageKind::Link,
        MessageBody::RunControl(_) => MessageKind::RunControl,
        MessageBody::Exec(_) => MessageKind::Exec,
        MessageBody::Account(_) => MessageKind::Account,
    }
}

/// FITNESS: the received stamp in the header is the key that orders the fold. Each kind of
/// message comes out of an input queue with the kind its body names, the received stamp it was
/// built with, the queued stamp its producer set, and its body unchanged. A key that a kind lost
/// or moved would reorder the fold with every other test green.
#[test]
fn each_kind_round_trips_with_header_key() {
    let kinds = Cell::new([0_u64; KINDS.len()]);
    let outcome = runner(64).run(&steps(), |steps| {
        let mut builder = Builder::new();
        let (mut producer, consumer) = RingBuffer::<InboundMessage>::new(1);
        let mut ingress = IngressQueues::new(vec![consumer]);
        let mut seen = kinds.get();
        for step in &steps {
            let mut message = builder.message(step);
            let queued = ts(builder.when + 1);
            message.header.queued_ts_us = queued;
            producer
                .push(message)
                .expect("the ring is empty before each push");
            let (_, popped) = ingress.pop_next().expect("one message waits");
            prop_assert_eq!(popped.kind(), kind_named_by(&popped.body));
            prop_assert_eq!(popped.header.received_ts_us, ts(builder.when));
            prop_assert_eq!(popped.header.queued_ts_us, queued);
            prop_assert_eq!(popped, message);
            seen[kind_of(&popped)] += 1;
        }
        kinds.set(seen);
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("a message lost its kind or its key in a queue: {failure}");
    }
    for (kind, count) in KINDS.iter().zip(kinds.get()) {
        assert!(count > 0, "no drawn sequence held a {kind} message");
    }
}
