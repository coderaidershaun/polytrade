//! The output lanes of the fold when their readers fall behind. A ring of one slot that its reader
//! empties once a message changes no state: each lane loses records only as its own policy says.

use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use engine::hot::dispatch::{
    ExecWiring, ExposureWiring, HotEngine, HotEngineSetup, LinkWiring, StateDigest,
};
use engine::hot::sink::{ExecSink, LinkSink};
use engine::hot::strategy::{
    DesiredQuote, Fill, OrderReject, OrderStyle, OrderUpdate, QuoteLevel, Registration, Strategy,
    StrategyCtx,
};
use engine::link::{LinkFrame, LinkHash, LinkOrigin, LinkPayload, OutboundLink, TopicId};
use engine::log::{HotRecord, LaneReader, TextRecord, open_hot_lane, open_text_lane};
use engine::msg::exec::{ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus};
use engine::msg::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{BookChunk, InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::run_state::{RunAssertion, RunState};
use engine::msg::time::Stamper;
use engine::msg::ui::{UiBookSnapshot, UiEvent};
use engine::runtime::config::{RecordedTables, TableKind};
use engine::runtime::exposure::ExposureSnapshot;
use engine::runtime::registry::InstrumentRow;
use engine::runtime::shutdown::RunControlGate;
use engine::venues::exec::open_orders_snapshot_end;
use proptest::prelude::{Just, any, prop, prop_oneof};
use proptest::strategy::Strategy as Generator;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, account_chunk, delta_chunk, detached_setup, exec_event, exposure_ring, instrument_row,
    last_snapshot_chunk, partial_snapshot_chunk, persist_ring_for, run_control, snapshot_pair,
    spin, strategy_log_ring, stream_ready, tracker_spec_all, trade, ts, ui_book_ring,
    ui_event_ring, venue_ts,
};
use crate::exec_support::exec_settings;

/// Run A reads each record of each lane after each message, so no ring of this size fills.
const ROOMY: usize = 4_096;

/// Run B: one slot in each ring, and a reader that takes one record of each after each message.
const TIGHT: usize = 1;

const STEPS: usize = 160;

const MARKETS: u16 = 2;

const START: i64 = 1_000_000;

/// The size that the probe quotes on each side.
const QUOTE_QTY: Qty = Qty::from_mantissa(ONE / 10);

/// The venue fills each third placement at once and in full, and rests the others.
const FILL_EVERY: u64 = 3;

/// Each callback writes one feature row and one note line. Each spin does that for each market, and
/// sends one link frame and quotes both sides at the touch for each market, so a spin writes more
/// than one record to each lane that the strategy feeds.
#[derive(Default)]
struct Probe {
    call: Option<FeatureId>,
    topic: Option<TopicId>,
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

    fn link_fields(&self) -> &'static [&'static str] {
        &["spin"]
    }

    fn link_topics(&self) -> &'static [&'static str] {
        &["probe"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
        self.topic = registration.link_topics.first().copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        self.record(ctx, event.instrument, 1.0);
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        self.record(ctx, chunk.instrument, 2.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        let topic = self.topic.expect("topics registered");
        for market in 0..MARKETS {
            self.record(ctx, InstrumentId(market), 6.0);
            ctx.link_send(topic, &[tick.seq as f64]);
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
}

/// What a reader took from each lane, each record as its text. A command counts by what it asks
/// and not by its stamp, and an event keeps its sequence number.
#[derive(Debug, Default)]
struct Records {
    tape_rows: Vec<String>,
    commands: Vec<String>,
    ui_books: Vec<String>,
    ui_events: Vec<UiEvent>,
    exposure: Vec<String>,
    strategy_log: Vec<String>,
    link: Vec<String>,
    hot_log: Vec<String>,
    text_log: Vec<String>,
}

/// The read ends of each output ring of one engine, with the log lanes of its thread.
struct Lanes {
    persist: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    ui_books: Consumer<UiBookSnapshot>,
    ui_events: Consumer<UiEvent>,
    exposure: Consumer<ExposureSnapshot>,
    strategy_log: Consumer<HotRecord>,
    strategy_log_drops: Arc<AtomicU64>,
    link: Consumer<OutboundLink>,
    hot_log: LaneReader<HotRecord>,
    text_log: LaneReader<TextRecord>,
}

fn take<T>(consumer: &mut Consumer<T>, at_most: usize) -> impl Iterator<Item = T> + '_ {
    std::iter::from_fn(move || consumer.pop().ok()).take(at_most)
}

fn texts<T: std::fmt::Debug>(records: impl Iterator<Item = T>) -> impl Iterator<Item = String> {
    records.map(|record| format!("{record:?}"))
}

fn rendered(record: &HotRecord) -> String {
    let mut line = format!("{:?} {:?} ", record.callsite().level(), record.at());
    record
        .render(&mut line)
        .expect("a string takes every write");
    line
}

impl Lanes {
    /// Takes at most `at_most` records of each lane, and gives back the commands among them for a
    /// venue to answer. A watermark only marks the progress of the lane.
    fn read(&mut self, at_most: usize, records: &mut Records) -> Vec<ExecCommand> {
        let commands: Vec<ExecCommand> = take(&mut self.commands, at_most)
            .filter_map(|item| match item {
                ExecLaneItem::Command(stamped) => Some(stamped.command),
                ExecLaneItem::Watermark(_) => None,
            })
            .collect();
        records.commands.extend(texts(commands.iter()));
        records
            .tape_rows
            .extend(texts(take(&mut self.persist, at_most)));
        records
            .ui_books
            .extend(texts(take(&mut self.ui_books, at_most)));
        records.ui_events.extend(take(&mut self.ui_events, at_most));
        records
            .exposure
            .extend(texts(take(&mut self.exposure, at_most)));
        records
            .strategy_log
            .extend(take(&mut self.strategy_log, at_most).map(|record| rendered(&record)));
        records.link.extend(texts(take(&mut self.link, at_most)));
        let hot_log = std::iter::from_fn(|| self.hot_log.pop()).take(at_most);
        records
            .hot_log
            .extend(hot_log.map(|record| rendered(&record)));
        let text_log = std::iter::from_fn(|| self.text_log.pop()).take(at_most);
        records.text_log.extend(
            text_log.map(|record| format!("{:?} {}", record.callsite().level(), record.text())),
        );
        commands
    }
}

fn rows() -> Vec<InstrumentRow> {
    (0..MARKETS)
        .map(|id| {
            let mut row = instrument_row(id, tracker_spec_all(1), 64);
            row.venue_symbol = format!("btcusdt-{id}").into();
            row
        })
        .collect()
}

/// An engine with two markets, the probe, a venue, a link, and each output ring of `capacity`
/// slots. Its log lanes are those of the calling thread.
fn engine(capacity: usize) -> (HotEngine, Lanes) {
    let (hot_log, text_log) = (open_hot_lane(capacity), open_text_lane(capacity));
    let rows = rows();
    let tables = RecordedTables::new(&[
        TableKind::Trades,
        TableKind::BookEvents,
        TableKind::Klines,
        TableKind::Features,
        TableKind::LinkFrames,
        TableKind::Orders,
        TableKind::Fills,
    ]);
    let (persistence, persist) = persist_ring_for(capacity, tables);
    let (strategy_log_sink, strategy_log) = strategy_log_ring(capacity);
    let strategy_log_drops = strategy_log_sink.drops_handle();
    let (ui_book_sink, ui_books) = ui_book_ring(capacity);
    let (ui_event_sink, ui_events) = ui_event_ring(capacity);
    let (exposure_sink, exposure) = exposure_ring(capacity);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(capacity);
    let (link_producer, link) = RingBuffer::<OutboundLink>::new(capacity);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        strategy_log_sink,
        ui_book_sink,
        ui_event_sink,
        link: Some(LinkWiring {
            sink: LinkSink::new(link_producer),
            acknowledged: RunControlGate::new().acknowledged().clone(),
        }),
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }),
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_sink,
        },
        ..detached_setup(&rows, Box::new(Probe::default()))
    });
    let lanes = Lanes {
        persist,
        commands,
        ui_books,
        ui_events,
        exposure,
        strategy_log,
        strategy_log_drops,
        link,
        hot_log,
        text_log,
    };
    (engine, lanes)
}

/// An order that the venue of run A holds.
#[derive(Debug, Clone, Copy)]
struct Resting {
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: Price,
}

impl Resting {
    fn event(self, kind: ExecKind, status: VenueOrderStatus, when: i64) -> InboundMessage {
        InboundMessage::new(
            ts(when),
            MessageBody::Exec(ExecEvent {
                kind,
                status: Some(status),
                ..exec_event(
                    self.instrument,
                    self.client_id,
                    self.side,
                    self.price.mantissa(),
                    when,
                )
            }),
        )
    }

    fn fill(self, qty: Qty, when: i64) -> InboundMessage {
        InboundMessage::new(
            ts(when),
            MessageBody::Exec(ExecEvent {
                kind: ExecKind::ReportTrade,
                status: Some(VenueOrderStatus::Filled),
                qty,
                last_price: self.price,
                last_qty: qty,
                cumulative_qty: qty,
                cumulative_quote: self.price.notional(qty),
                ..exec_event(
                    self.instrument,
                    self.client_id,
                    self.side,
                    self.price.mantissa(),
                    when,
                )
            }),
        )
    }
}

/// The venue of run A. It answers each command when run A reads it: it rests each placement, fills
/// each third one at once, and ends each order that a cancel names. It answers no other command.
#[derive(Default)]
struct Venue {
    resting: Vec<Resting>,
    placements: u64,
}

impl Venue {
    fn answer(&mut self, command: ExecCommand, when: i64) -> Vec<InboundMessage> {
        match command {
            ExecCommand::Place {
                instrument,
                client_id,
                side,
                price,
                qty,
                ..
            } => {
                self.placements += 1;
                let order = Resting {
                    instrument,
                    client_id,
                    side,
                    price,
                };
                let placed = order.event(ExecKind::AckPlaced, VenueOrderStatus::New, when);
                if self.placements.is_multiple_of(FILL_EVERY) {
                    return vec![placed, order.fill(qty, when)];
                }
                self.resting.push(order);
                vec![placed]
            }
            ExecCommand::Cancel { client_id, .. } => self.end(
                |order| order.client_id == client_id,
                ExecKind::AckCanceled,
                when,
            ),
            ExecCommand::CancelOurs { instrument, .. } => self.end(
                |order| order.instrument == instrument,
                ExecKind::ReportCanceled,
                when,
            ),
            ExecCommand::AmendQty { .. }
            | ExecCommand::ReconcileOrder { .. }
            | ExecCommand::ReconcileOpenOrders { .. }
            | ExecCommand::CancelPriorRun { .. } => Vec::new(),
        }
    }

    fn end(
        &mut self,
        is_named: impl Fn(&Resting) -> bool,
        kind: ExecKind,
        when: i64,
    ) -> Vec<InboundMessage> {
        let (ended, resting): (Vec<Resting>, Vec<Resting>) =
            self.resting.iter().partition(|order| is_named(order));
        self.resting = resting;
        ended
            .into_iter()
            .map(|order| order.event(kind, VenueOrderStatus::Canceled, when))
            .collect()
    }
}

/// The values of one drawn market message. Each variant makes one kind of message.
#[derive(Debug, Clone, Copy)]
enum Draw {
    Trade {
        market: u16,
        offset: i64,
        lots: i64,
        side: Side,
    },
    Book {
        market: u16,
        chunk: Chunk,
        side: Side,
        offset: i64,
        lots: i64,
    },
    Spin,
    Link {
        value: i32,
    },
    RunControl {
        is_running: bool,
    },
    Account {
        base_units: i64,
        quote_units: i64,
    },
}

/// A book chunk: a delta, or the first or the last chunk of a snapshot.
#[derive(Debug, Clone, Copy)]
enum Chunk {
    Delta,
    SnapshotBids,
    SnapshotAsks,
}

#[derive(Debug, Clone, Copy)]
struct Step {
    draw: Draw,
    gap_us: i64,
}

fn side() -> impl Generator<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn draw() -> impl Generator<Value = Draw> {
    let market = 0..MARKETS;
    let chunk = prop_oneof![
        2 => Just(Chunk::Delta),
        1 => Just(Chunk::SnapshotBids),
        1 => Just(Chunk::SnapshotAsks),
    ];
    prop_oneof![
        3 => (market.clone(), 0..5_i64, 1..4_i64, side())
            .prop_map(|(market, offset, lots, side)| Draw::Trade { market, offset, lots, side }),
        4 => (market, chunk, side(), 0..5_i64, 0..4_i64).prop_map(
            |(market, chunk, side, offset, lots)| Draw::Book { market, chunk, side, offset, lots }
        ),
        4 => Just(Draw::Spin),
        1 => any::<i32>().prop_map(|value| Draw::Link { value }),
        1 => any::<bool>().prop_map(|is_running| Draw::RunControl { is_running }),
        1 => (0..20_i64, 0..2_000_000_i64).prop_map(|(base_units, quote_units)| Draw::Account {
            base_units,
            quote_units
        }),
    ]
}

fn steps() -> impl Generator<Value = Vec<Step>> {
    let step = (draw(), 0..20_000_i64).prop_map(|(draw, gap_us)| Step { draw, gap_us });
    prop::collection::vec(step, STEPS)
}

/// What a drawn sequence keeps from one message to the next: message time, the next spin and peer
/// frame numbers, and the run epoch.
struct Market {
    when: i64,
    spin_seq: u64,
    link_seq: u64,
    epoch: u64,
}

impl Market {
    /// The readiness of the venue and a two-sided book on each market, then the drawn steps, then a
    /// last spin, which sends a seal that a park left waiting.
    fn sequence(steps: &[Step]) -> Vec<InboundMessage> {
        let mut market = Market {
            when: START,
            spin_seq: 0,
            link_seq: 0,
            epoch: 0,
        };
        let mut messages = Market::opening();
        messages.extend(steps.iter().map(|step| market.message(step)));
        let last_spin = Step {
            draw: Draw::Spin,
            gap_us: 1,
        };
        messages.push(market.message(&last_spin));
        messages
    }

    fn opening() -> Vec<InboundMessage> {
        let mut messages = Vec::new();
        for market in 0..MARKETS {
            let instrument = InstrumentId(market);
            let (bids, asks) =
                snapshot_pair(market, &[(99 * ONE, ONE)], &[(101 * ONE, ONE)], START);
            messages.extend([
                stream_ready(instrument, START),
                InboundMessage::new(
                    ts(START),
                    MessageBody::Exec(open_orders_snapshot_end(instrument, ts(START))),
                ),
                InboundMessage::new(ts(START), MessageBody::Book(bids)),
                InboundMessage::new(ts(START), MessageBody::Book(asks)),
            ]);
        }
        let balances = [(AssetId(0), 1_000 * ONE), (AssetId(1), 1_000_000 * ONE)];
        messages.push(InboundMessage::new(
            ts(START),
            MessageBody::Account(account_chunk(&balances, START)),
        ));
        messages
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
            } => InboundMessage::new(
                ts(when),
                MessageBody::Trade(trade(
                    market,
                    (98 + offset) * ONE,
                    lots * ONE / 10,
                    side,
                    when,
                )),
            ),
            Draw::Book {
                market,
                chunk,
                side,
                offset,
                lots,
            } => {
                let (bid, ask) = ((99 - offset) * ONE, (101 + offset) * ONE);
                let chunk = match chunk {
                    Chunk::Delta => {
                        let price = if side == Side::Buy { bid } else { ask };
                        delta_chunk(market, side, &[(price, lots * ONE / 2)], when)
                    }
                    Chunk::SnapshotBids => {
                        partial_snapshot_chunk(market, Side::Buy, &[(bid, ONE)], when)
                    }
                    Chunk::SnapshotAsks => {
                        last_snapshot_chunk(market, Side::Sell, &[(ask, ONE)], when)
                    }
                };
                InboundMessage::new(ts(when), MessageBody::Book(chunk))
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
            Draw::RunControl { is_running } => {
                self.epoch += 1;
                let state = if is_running { RunState::Running } else { RunState::Idle };
                let desired = RunAssertion {
                    state,
                    epoch: self.epoch,
                };
                run_control(desired, when)
            }
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
                    MessageBody::Account({
                        let mut chunk = account_chunk(&balances, when);
                        chunk.venue_update_ts_ms = venue_update_ts_ms;
                        chunk
                    }),
                )
            }
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

fn on_own_thread<T: Send + 'static>(run: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(run)
        .expect("the test can spawn a thread")
        .join()
        .expect("the fold does not panic")
}

/// Run A: each message that it folded, its answers included, the digest and the count of banked
/// commands after each one, and each record of each lane.
struct RunA {
    inputs: Vec<InboundMessage>,
    digests: Vec<StateDigest>,
    banked: Vec<u64>,
    records: Records,
}

/// Folds the market messages through roomy rings, and folds each answer of the venue right after
/// the message after which it read the command.
fn run_a(market: Vec<InboundMessage>) -> RunA {
    on_own_thread(move || {
        let (mut engine, mut lanes) = engine(ROOMY);
        let mut venue = Venue::default();
        let mut run = RunA {
            inputs: Vec::new(),
            digests: Vec::new(),
            banked: Vec::new(),
            records: Records::default(),
        };
        let mut market = market.into_iter();
        let mut answers = VecDeque::new();
        while let Some(message) = answers.pop_front().or_else(|| market.next()) {
            engine.dispatch(&message);
            run.digests.push(engine.state_digest());
            run.banked.push(engine.counters().orders_submitted);
            let when = message.header.received_ts_us.micros();
            for command in lanes.read(usize::MAX, &mut run.records) {
                answers.extend(venue.answer(command, when));
            }
            run.inputs.push(message);
        }
        let lost = Lost::of(&engine, &lanes, 0);
        assert_eq!(lost, Lost::default(), "run A lost no record");
        drop(engine);
        lanes.read(usize::MAX, &mut run.records);
        run
    })
}

/// What run B lost of each lane: the drops that each sink counted, the position snapshots that a
/// newer one replaced, and the most commands that its bank held at once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Lost {
    tape_rows: u64,
    ui_books: u64,
    ui_events: u64,
    exposure: u64,
    strategy_log: u64,
    link: u64,
    hot_log: u64,
    text_log: u64,
    commands_held: u64,
}

impl Lost {
    fn of(engine: &HotEngine, lanes: &Lanes, commands_held: u64) -> Lost {
        let counters = engine.counters();
        Lost {
            tape_rows: counters.persist_dropped,
            ui_books: counters.ui_books_dropped,
            ui_events: counters.ui_events_dropped,
            exposure: counters.exposure_superseded,
            strategy_log: lanes.strategy_log_drops.load(Ordering::Relaxed),
            link: counters.link_dropped,
            hot_log: lanes.hot_log.dropped(),
            text_log: lanes.text_log.dropped(),
            commands_held,
        }
    }

    fn plus(self, other: Lost) -> Lost {
        Lost {
            tape_rows: self.tape_rows + other.tape_rows,
            ui_books: self.ui_books + other.ui_books,
            ui_events: self.ui_events + other.ui_events,
            exposure: self.exposure + other.exposure,
            strategy_log: self.strategy_log + other.strategy_log,
            link: self.link + other.link,
            hot_log: self.hot_log + other.hot_log,
            text_log: self.text_log + other.text_log,
            commands_held: self.commands_held.max(other.commands_held),
        }
    }
}

/// How run B ended.
enum Ending {
    /// B held the state of A after each message.
    Equal { records: Box<Records>, lost: Lost },
    /// B's bank refused a command that A's bank took, and a full bank halts quoting. A halt that
    /// follows ring space is the one place where the two runs may part, so the case ends here.
    BankFull,
}

/// Folds the messages of run A through rings of one slot, and takes one record of each after each
/// message. The first event that B reads after a message is the first that the message wrote, so
/// its number is the count of the events before it: those read, and those dropped and counted.
fn run_b(a: &RunA) -> Result<Ending, String> {
    let (inputs, digests, banked) = (a.inputs.clone(), a.digests.clone(), a.banked.clone());
    on_own_thread(move || {
        let (mut engine, mut lanes) = engine(TIGHT);
        let mut records = Records::default();
        let (mut events_dropped_before, mut commands_held) = (0, 0);
        for (index, message) in inputs.iter().enumerate() {
            engine.dispatch(message);
            if engine.state_digest() != digests[index] {
                if engine.counters().orders_submitted < banked[index] {
                    return Ok(Ending::BankFull);
                }
                return Err(format!(
                    "the digests part after message {index}: {message:?}"
                ));
            }
            let events_read = records.ui_events.len();
            lanes.read(TIGHT, &mut records);
            if let Some(event) = records.ui_events.get(events_read) {
                let expected = events_read as u64 + events_dropped_before;
                if event.seq() != expected {
                    return Err(format!(
                        "after message {index} B read event {}, but {events_read} read and \
                         {events_dropped_before} dropped before it say {expected}",
                        event.seq()
                    ));
                }
            }
            events_dropped_before = engine.dropped_ui_events();
            let held = engine.counters().orders_submitted - records.commands.len() as u64;
            commands_held = commands_held.max(held);
        }
        let lost = Lost::of(&engine, &lanes, commands_held);
        drop(engine);
        lanes.read(TIGHT, &mut records);
        Ok(Ending::Equal {
            records: Box::new(records),
            lost,
        })
    })
}

fn is_subsequence(part: &[String], whole: &[String]) -> bool {
    let mut whole = whole.iter();
    part.iter()
        .all(|record| whole.any(|candidate| candidate == record))
}

/// Each record that B read of a lane is one that A wrote, in the order A wrote them, and each one
/// that B lacks is one that B counted.
fn check_lane(lane: &str, written: &[String], read: &[String], lost: u64) -> Result<(), String> {
    if read.len() as u64 + lost != written.len() as u64 {
        return Err(format!(
            "{lane}: B read {} and counted {lost}, but A wrote {}",
            read.len(),
            written.len()
        ));
    }
    if !is_subsequence(read, written) {
        return Err(format!(
            "{lane}: B read a record that A did not write, or two out of order"
        ));
    }
    Ok(())
}

fn check(written: &Records, read: &Records, lost: Lost) -> Result<(), String> {
    let lanes = [
        (
            "tape rows",
            &written.tape_rows,
            &read.tape_rows,
            lost.tape_rows,
        ),
        ("ui books", &written.ui_books, &read.ui_books, lost.ui_books),
        ("exposure", &written.exposure, &read.exposure, lost.exposure),
        (
            "strategy log",
            &written.strategy_log,
            &read.strategy_log,
            lost.strategy_log,
        ),
        ("link frames", &written.link, &read.link, lost.link),
        ("hot log", &written.hot_log, &read.hot_log, lost.hot_log),
        ("text log", &written.text_log, &read.text_log, lost.text_log),
    ];
    for (lane, written, read, lost) in lanes {
        check_lane(lane, written, read, lost)?;
    }
    if read.ui_events.len() as u64 + lost.ui_events != written.ui_events.len() as u64 {
        return Err(format!(
            "ui events: B read {} and counted {}, but A wrote {}",
            read.ui_events.len(),
            lost.ui_events,
            written.ui_events.len()
        ));
    }
    let renumbered = read
        .ui_events
        .iter()
        .find(|event| written.ui_events.get(event.seq() as usize) != Some(*event));
    if let Some(event) = renumbered {
        return Err(format!(
            "ui events: B read {event:?}, which is not the event that A wrote with its number"
        ));
    }
    if written.exposure.last() != read.exposure.last() {
        return Err("exposure: the last snapshot that B read is not the last that A wrote".into());
    }
    if !written.commands.starts_with(&read.commands) {
        return Err(
            "commands: B sent a command that A did not, or sent the commands out of order".into(),
        );
    }
    Ok(())
}

/// FITNESS: an output ring that its reader leaves full changes no state. Run B folds each message
/// that run A folded, the answers of A's venue included, through rings of one slot that a reader
/// empties once a message. After each message B holds the state of A. Each lane that drops counts
/// each record that B lacks, and the numbers of the events count through each drop. B ends on the
/// last position snapshot of A, and each command that did not fit waits in the bank, in order. A
/// fold that branched on a full ring would trade differently each time a reader fell behind, and
/// no record would show it.
#[test]
fn full_output_rings_change_no_state() {
    let total = Cell::new(Lost::default());
    let (placements, fills) = (Cell::new(0), Cell::new(0));
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(&steps(), |steps| {
        let a = run_a(Market::sequence(&steps));
        let (records, lost) = match run_b(&a).map_err(TestCaseError::fail)? {
            Ending::Equal { records, lost } => (records, lost),
            Ending::BankFull => return Ok(()),
        };
        check(&a.records, &records, lost).map_err(TestCaseError::fail)?;
        total.set(total.get().plus(lost));
        let placed = a.records.commands.iter().filter(|c| c.starts_with("Place"));
        placements.set(placements.get() + placed.count());
        let filled = a.records.ui_events.iter();
        fills.set(fills.get() + filled.filter(|e| matches!(e, UiEvent::Fill { .. })).count());
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("a full output ring changed the fold: {failure}");
    }
    // A message writes at most one book snapshot and one position snapshot, so a reader that takes
    // one record after each message never leaves those two rings full, and no case here drops or
    // replaces one.
    let total = total.get();
    assert!(
        total.tape_rows > 0
            && total.ui_events > 0
            && total.strategy_log > 0
            && total.link > 0
            && total.commands_held > 0,
        "each other lane of run B dropped or held a record in some case: {total:?}"
    );
    assert!(
        placements.get() > 0 && fills.get() > 0,
        "run A placed orders and folded fills: {} placements, {} fills",
        placements.get(),
        fills.get()
    );
}
