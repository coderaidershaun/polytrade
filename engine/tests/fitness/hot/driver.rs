//! The hot loop's driver over real input rings. Its measurement must change no byte of any output
//! of the fold, and its stamps must reach the metrics thread with the times their stages claim.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{
    ClientIdLayout, DesiredQuote, ExecLimits, ExecSettings, FeeModel, OrderBudget, QuoteLevel,
    side_base,
};
use engine::hot::ingress::IngressQueues;
use engine::hot::metrics::DriverSamples;
use engine::hot::sink::ExecSink;
use engine::hot::spawn::{HotDriver, HotLoop, HotThreadConfig, IdleWait, Step, spawn_hot_thread};
use engine::hot::strategy::{Strategy, StrategyConfig, StrategyCtx};
use engine::log::{HotRecord, LaneReader, TextRecord, open_hot_lane, open_text_lane};
use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, Provenance, StampedExecCommand,
    VenueOrderStatus,
};
use engine::msg::ids::{AssetId, FIXED_SCALE, InstrumentId, Notional, Price, Qty, QueueId, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::latency::{Category, CommandKind, Stage, WireSample};
use engine::msg::persist::PersistRecord;
use engine::msg::time::{DurationUs, Stamper};
use engine::msg::ui::{UiBookSnapshot, UiEvent};
use engine::runtime::config::{
    BinanceMarket, Instruments, IntensitySpec, LatencyBudgets, PolySeries, RecordedTables,
    StageBudget, StrategySpec, Subscriptions, TableKind, TrackerSpec, VenueMarket,
};
use engine::runtime::exposure::ExposureSnapshot;
use engine::runtime::metrics::{SampleProducers, SinkKind};
use engine::runtime::pin_to_core;
use engine::runtime::registry::InstrumentRow;
use engine::runtime::shutdown::{DrainSignal, FatalSignal};
use engine::venues::binance::depth::{ChainRule, DepthSequencer, DiffOutcome, SnapshotOutcome};
use engine::venues::binance::parse::{
    DepthDiff, DepthSnapshot, ParseContext, parse_agg_trade, parse_combined_frame,
    parse_spot_depth_diff,
};
use engine::venues::exec::open_orders_snapshot_end;
use engine::venues::polymarket::actor::{DriverEffect, PolyDriverCore, SlotLegs};
use engine::venues::polymarket::discovery::PolySchedule;
use engine::venues::polymarket::parse::{PolyFrame, is_frameless, parse_market_frame};
use engine::venues::polymarket::rotation::{OutcomeLeg, TokenId, WindowAssignment};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    Idle, ONE, account_chunk, detached_setup, engine_view, exec_event, exposure_ring,
    instrument_row, level, persist_ring_for, snapshot_pair, spin, strategy_log_ring, stream_ready,
    tracker_spec_all, ts, ui_book_ring, ui_event_ring, venue_ts,
};
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher};
use crate::runtime::metrics::{Harness, Rows};

const BINANCE_TRADES: &str = include_str!("../../../fixtures/binance/spot_aggTrade.jsonl");
const BINANCE_DEPTH: &str = include_str!("../../../fixtures/binance/spot_depth.jsonl");
const POLYMARKET_MARKET: &str = include_str!("../../../fixtures/polymarket/poly_market.jsonl");

/// The cadence of the timer actor in the tapes below.
const SPIN_US: i64 = 250_000;

/// How long after its venue stamp each recorded Binance frame reaches the engine.
const WIRE_DELAY_US: i64 = 3_000;

/// Inside the window that the Polymarket capture was recorded in, and 20 ms between its frames.
const POLYMARKET_START_US: i64 = 1_784_449_240_000_000;
const POLYMARKET_FRAME_GAP_US: i64 = 20_000;
const POLYMARKET_WINDOW_OPEN_US: i64 = 1_784_449_200_000_000;
const POLYMARKET_WINDOW_US: i64 = 300_000_000;

/// The nonce of an earlier run, so the venue's report on its order is one this run did not place.
const PRIOR_NONCE: u32 = 0xABCD;

const OUTPUT_RING: usize = 65_536;

/// The two trading engines that ship, each with a tape from the recorded frames of its venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shipped {
    Recorder,
    Publisher,
}

/// Each output lane of one run, as text that holds every byte of each record.
#[derive(Debug, Clone, Default, PartialEq)]
struct Outputs {
    tape_rows: Vec<String>,
    /// The bank stamp of each command, never the latency tick it carries.
    commands: Vec<String>,
    ui_books: Vec<String>,
    ui_events: Vec<UiEvent>,
    exposure: Vec<String>,
    hot_log: Vec<String>,
    /// Hot calls that ran where no hot lane was, rendered at once; their engine time is a clock
    /// reading, so only the text counts.
    text_log: Vec<String>,
    strategy_log: Vec<String>,
}

/// A count and a fixed hash of one lane. The hash is FNV-1a over each record and a line break.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Digest {
    lane: &'static str,
    count: usize,
    hash: u64,
}

impl Outputs {
    fn digests(&self) -> [Digest; 8] {
        [
            digest("tape rows", &self.tape_rows),
            digest("commands", &self.commands),
            digest("ui books", &self.ui_books),
            digest("ui events", &self.event_lines()),
            digest("exposure", &self.exposure),
            digest("hot log", &self.hot_log),
            digest("text log", &self.text_log),
            digest("strategy log", &self.strategy_log),
        ]
    }

    fn event_lines(&self) -> Vec<String> {
        self.ui_events
            .iter()
            .map(|event| format!("{event:?}"))
            .collect()
    }

    /// The first record in which two runs part, for the message of a failure.
    fn first_difference(&self, other: &Outputs) -> String {
        let lanes = [
            ("tape rows", self.tape_rows.clone(), other.tape_rows.clone()),
            ("commands", self.commands.clone(), other.commands.clone()),
            ("ui books", self.ui_books.clone(), other.ui_books.clone()),
            ("ui events", self.event_lines(), other.event_lines()),
            ("exposure", self.exposure.clone(), other.exposure.clone()),
            ("hot log", self.hot_log.clone(), other.hot_log.clone()),
            ("text log", self.text_log.clone(), other.text_log.clone()),
            (
                "strategy log",
                self.strategy_log.clone(),
                other.strategy_log.clone(),
            ),
        ];
        for (lane, ours, theirs) in lanes {
            let parted = ours.iter().zip(&theirs).position(|(a, b)| a != b);
            match parted {
                Some(index) => {
                    return format!("{lane} #{index}: {} against {}", ours[index], theirs[index]);
                }
                None if ours.len() != theirs.len() => {
                    return format!("{lane}: {} records against {}", ours.len(), theirs.len());
                }
                None => {}
            }
        }
        "no record differs".to_owned()
    }

    /// The lanes of the fold that no observation ring feeds.
    fn without_logs(&self) -> Outputs {
        Outputs {
            hot_log: Vec::new(),
            text_log: Vec::new(),
            strategy_log: Vec::new(),
            ..self.clone()
        }
    }
}

/// The name under which the recorded hashes spell every stamp: they were taken while a venue stamp
/// and a local stamp were one type. Each record spells both stamp types by it before it is hashed,
/// and the fields and their values are hashed as they are.
const RECORDED_STAMP_TYPE: &str = "TsUs(";

fn spelled_as_recorded(record: &str) -> String {
    record
        .replace("LocalTs(", RECORDED_STAMP_TYPE)
        .replace("VenueTs(", RECORDED_STAMP_TYPE)
}

fn digest(lane: &'static str, records: &[String]) -> Digest {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for record in records {
        let record = spelled_as_recorded(record);
        for byte in record.bytes().chain(std::iter::once(b'\n')) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    Digest {
        lane,
        count: records.len(),
        hash,
    }
}

/// The readers of every output of one engine, drained after each message so no ring fills.
struct Lanes {
    persist: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    ui_books: Consumer<UiBookSnapshot>,
    ui_events: Consumer<UiEvent>,
    exposure: Consumer<ExposureSnapshot>,
    strategy_log: Consumer<HotRecord>,
    strategy_log_drops: Arc<AtomicU64>,
}

impl Lanes {
    fn drain_fold_into(&mut self, outputs: &mut Outputs) {
        while let Ok(record) = self.persist.pop() {
            outputs.tape_rows.push(format!("{record:?}"));
        }
        while let Ok(item) = self.commands.pop() {
            outputs.commands.push(match item {
                ExecLaneItem::Command(stamped) => {
                    format!("{:?} {:?}", stamped.command, stamped.issued_ts_us)
                }
                ExecLaneItem::Watermark(at) => format!("watermark {at:?}"),
            });
        }
        while let Ok(snapshot) = self.ui_books.pop() {
            outputs.ui_books.push(format!("{snapshot:?}"));
        }
        while let Ok(event) = self.ui_events.pop() {
            outputs.ui_events.push(event);
        }
        while let Ok(snapshot) = self.exposure.pop() {
            outputs.exposure.push(format!("{snapshot:?}"));
        }
    }

    fn drain_strategy_log_into(&mut self, outputs: &mut Outputs) {
        while let Ok(record) = self.strategy_log.pop() {
            outputs.strategy_log.push(rendered(&record));
        }
    }
}

fn rendered(record: &HotRecord) -> String {
    let mut line = format!("{:?} {:?} ", record.callsite().level(), record.at());
    record
        .render(&mut line)
        .expect("a string takes every write");
    line
}

fn text_line(record: &TextRecord) -> String {
    format!("{:?} {}", record.callsite().level(), record.text())
}

fn all_tables() -> RecordedTables {
    RecordedTables::new(&[
        TableKind::Trades,
        TableKind::BookEvents,
        TableKind::Klines,
        TableKind::Features,
        TableKind::LinkFrames,
        TableKind::Orders,
        TableKind::Fills,
    ])
}

fn settings(fee_model: FeeModel, holds_reservations_until_settled: bool) -> ExecSettings {
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
        inflight_timeout: DurationUs::from_secs(5),
        max_status_probes: 3,
        exec_silence_spins: u32::MAX,
        stream_silence: DurationUs::from_micros(i64::MAX),
        max_stream_resets: 2,
        order_reap_window: DurationUs::from_secs(3_600),
        quote_stop_margin: DurationUs::from_millis(3_000),
        flatten_slack_ticks: 2,
        order_budget: OrderBudget::NONE,
        fee_model,
        taker_fee_rate: 0,
        holds_reservations_until_settled,
    }
}

fn recorder_row() -> InstrumentRow {
    let mut row = instrument_row(0, tracker_spec_all(100), 128);
    row.market = VenueMarket::Binance(BinanceMarket::Spot);
    row.tick_size = Some(Price::from_mantissa(ONE / 100));
    row.lot_size = Some(Qty::from_mantissa(1_000));
    row.min_qty = Some(Qty::from_mantissa(1_000));
    row.min_notional = Some(Notional::from_mantissa(5 * ONE));
    row.tracker.intensity = Some(IntensitySpec {
        max_depth_ticks: 16,
        half_life_secs: 600.0,
        min_events: 5.0,
    });
    row
}

fn publisher_rows() -> Vec<InstrumentRow> {
    ["a-up", "a-down", "b-up", "b-down"]
        .iter()
        .enumerate()
        .map(|(index, leg)| InstrumentRow {
            market: VenueMarket::Polymarket(PolySeries::BtcUpDown5m),
            venue_symbol: format!("btc-updown-5m-{leg}").into(),
            display: format!("btc-updown-5m-{leg}").into(),
            base: "BTC".into(),
            quote: "USDC".into(),
            tick_size: Some(Price::from_mantissa(FIXED_SCALE / 100)),
            lot_size: Some(Qty::from_mantissa(FIXED_SCALE / 100)),
            min_qty: Some(Qty::from_mantissa(5 * FIXED_SCALE)),
            max_price: Some(Price::from_mantissa(FIXED_SCALE * 99 / 100)),
            max_num_orders: Some(32),
            subscriptions: Subscriptions {
                klines: false,
                ..Subscriptions::default()
            },
            kline_intervals: Vec::new(),
            max_exposure_quote: Notional::from_mantissa(500 * FIXED_SCALE),
            tracker: TrackerSpec {
                intensity: Some(IntensitySpec {
                    max_depth_ticks: 32,
                    half_life_secs: 120.0,
                    min_events: 3.0,
                }),
                ..TrackerSpec::default()
            },
            ..instrument_row(index as u16, TrackerSpec::default(), 128)
        })
        .collect()
}

fn rows(shipped: Shipped) -> Vec<InstrumentRow> {
    match shipped {
        Shipped::Recorder => vec![recorder_row()],
        Shipped::Publisher => publisher_rows(),
    }
}

fn strategy(shipped: Shipped) -> Box<dyn Strategy> {
    let view = engine_view(DurationUs::from_micros(SPIN_US));
    match shipped {
        Shipped::Recorder => Box::new(MicroRecorder::from_spec(
            &StrategySpec {
                instruments: Instruments::All,
                tables: vec![TableKind::Features],
                params: MicroRecorderParams::default(),
            },
            view,
        )),
        Shipped::Publisher => Box::new(PolyUpPublisher::from_spec(
            &StrategySpec {
                instruments: Instruments::All,
                tables: Vec::new(),
                params: PolyUpParams {
                    enabled: true,
                    ..PolyUpParams::default()
                },
            },
            view,
        )),
    }
}

pub(crate) fn exec_settings(shipped: Shipped) -> ExecSettings {
    match shipped {
        Shipped::Recorder => settings(FeeModel::None, true),
        Shipped::Publisher => settings(FeeModel::BinaryOutcome, false),
    }
}

/// The three facts that let an order be sent: the stream is up, the open orders are known, and so
/// are the balances.
pub(crate) fn readiness(at: i64) -> [InboundMessage; 3] {
    [
        stream_ready(InstrumentId(0), at),
        InboundMessage::new(
            ts(at),
            MessageBody::Exec(open_orders_snapshot_end(InstrumentId(0), ts(at))),
        ),
        InboundMessage::new(
            ts(at),
            MessageBody::Account(account_chunk(
                &[(AssetId(0), 1_000 * ONE), (AssetId(1), 1_000_000 * ONE)],
                at,
            )),
        ),
    ]
}

/// A fill that the venue reports on an order an earlier run left resting: the position moves, the
/// engine cancels the order, and each output lane hears of it.
fn prior_run_fill(price: i64, cumulative: i64, at: i64) -> InboundMessage {
    let client_id = ClientIdLayout {
        run_nonce: PRIOR_NONCE,
    }
    .encode(side_base(InstrumentId(0), Side::Buy) + 1, 1);
    InboundMessage::new(
        ts(at),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            provenance: Provenance::PriorRun,
            status: Some(VenueOrderStatus::PartiallyFilled),
            qty: Qty::from_mantissa(4 * cumulative),
            last_price: Price::from_mantissa(price),
            last_qty: Qty::from_mantissa(cumulative / 2),
            cumulative_qty: Qty::from_mantissa(cumulative),
            cumulative_quote: Price::from_mantissa(price).notional(Qty::from_mantissa(cumulative)),
            ..exec_event(InstrumentId(0), client_id, Side::Buy, price, at)
        }),
    )
}

/// Market messages in receive order, with a spin each time the timer would fire, the venue's
/// readiness at the start, and two fills of an inherited order a third and two thirds of the way.
fn with_spins_and_answers(
    market: Vec<InboundMessage>,
    fill_price: i64,
    fill_qty: i64,
) -> Vec<InboundMessage> {
    let first = market
        .first()
        .map_or(0, |message| message.header.received_ts_us.micros());
    let fills = [market.len() / 3, 2 * market.len() / 3];
    let mut tape: Vec<InboundMessage> = readiness(first).into();
    let mut next_spin = first + SPIN_US;
    let mut spin_seq = 0;
    for (index, message) in market.into_iter().enumerate() {
        let at = message.header.received_ts_us.micros();
        while next_spin <= at {
            tape.push(spin(spin_seq, next_spin));
            spin_seq += 1;
            next_spin += SPIN_US;
        }
        if let Some(nth) = fills.iter().position(|&fill| fill == index) {
            tape.push(prior_run_fill(fill_price, fill_qty * (nth as i64 + 1), at));
        }
        tape.push(message);
    }
    tape.push(spin(spin_seq, next_spin));
    tape
}

/// The recorded spot trades and depth diffs, each received a few milliseconds after its venue
/// stamp, the depth sequenced onto a snapshot as the Binance edge sequences it.
fn recorder_tape() -> Vec<InboundMessage> {
    let placeholder = ParseContext {
        instrument: InstrumentId(0),
        received_ts_us: ts(1_784_410_630_000_000),
    };
    let mut frames: Vec<(i64, Frame)> = Vec::new();
    for line in BINANCE_TRADES
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let frame = parse_combined_frame(line).expect("a recorded frame has its envelope");
        let trade = parse_agg_trade(&frame.data, placeholder)
            .expect("a recorded trade parses")
            .trade;
        let at = trade.exchange_ts_us.micros() + WIRE_DELAY_US;
        frames.push((at, Frame::Trade(trade)));
    }
    for line in BINANCE_DEPTH.lines().filter(|line| !line.trim().is_empty()) {
        let frame = parse_combined_frame(line).expect("a recorded frame has its envelope");
        let mut diff =
            parse_spot_depth_diff(&frame.data, placeholder).expect("a recorded diff parses");
        let at = diff.exchange_ts_us.micros() + WIRE_DELAY_US;
        diff.received_ts_us = ts(at);
        frames.push((at, Frame::Depth(diff)));
    }
    frames.sort_by_key(|(at, _)| *at);

    let first_update_id = frames
        .iter()
        .find_map(|(_, frame)| match frame {
            Frame::Depth(diff) => Some(diff.first_update_id),
            Frame::Trade(_) => None,
        })
        .expect("the capture holds depth");
    let start = frames[0].0 - WIRE_DELAY_US;
    let mut sequencer = DepthSequencer::new(ChainRule::Spot, InstrumentId(0), 1_024);
    let mut market = Vec::new();
    let outcome = sequencer.apply_snapshot(
        DepthSnapshot {
            instrument: InstrumentId(0),
            last_update_id: first_update_id - 1,
            bids: vec![level(64_680 * ONE, ONE), level(64_670 * ONE, 2 * ONE)],
            asks: vec![level(64_690 * ONE, ONE), level(64_700 * ONE, 2 * ONE)],
            received_ts_us: ts(start),
        },
        &mut |message, _| market.push(message),
    );
    assert_eq!(
        outcome,
        SnapshotOutcome::Applied,
        "the snapshot opens the book"
    );
    for (at, frame) in frames {
        match frame {
            Frame::Trade(trade) => {
                market.push(InboundMessage::new(ts(at), MessageBody::Trade(trade)));
            }
            Frame::Depth(diff) => {
                let outcome = sequencer.on_diff(diff, &mut |message, _| market.push(message));
                assert_eq!(
                    outcome,
                    DiffOutcome::Applied,
                    "the capture chains diff to diff"
                );
            }
        }
    }
    with_spins_and_answers(market, 64_682 * ONE, ONE / 100)
}

enum Frame {
    Trade(TradeEvent),
    Depth(DepthDiff),
}

/// The recorded Polymarket market frames, routed by the Polymarket edge's own driver for a window
/// whose two legs are the two tokens of the capture.
fn publisher_tape() -> Vec<InboundMessage> {
    let mut tokens: Vec<Box<str>> = Vec::new();
    let mut frames = Vec::new();
    let lines = POLYMARKET_MARKET.lines().filter(|line| !is_frameless(line));
    for (index, line) in lines.enumerate() {
        let at = POLYMARKET_START_US + index as i64 * POLYMARKET_FRAME_GAP_US;
        let Ok(frame) = parse_market_frame(line, ts(at)) else {
            continue;
        };
        note_tokens(&frame, &mut tokens);
        frames.push((at, frame));
    }
    let [up, down] = [&tokens[0], &tokens[1]];
    let mut core = PolyDriverCore::new(
        [
            SlotLegs {
                up: InstrumentId(0),
                down: InstrumentId(1),
            },
            SlotLegs {
                up: InstrumentId(2),
                down: InstrumentId(3),
            },
        ],
        PolySchedule::BTC_5M,
    );
    let mut market = Vec::new();
    let mut keep = |effect: DriverEffect| {
        if let DriverEffect::Emit(message) = effect {
            market.push(message);
        }
    };
    core.on_window_resolved(
        ts(POLYMARKET_START_US - 1_000),
        WindowAssignment {
            up: OutcomeLeg {
                instrument: InstrumentId(0),
                token: TokenId::from(&**up),
            },
            down: OutcomeLeg {
                instrument: InstrumentId(1),
                token: TokenId::from(&**down),
            },
            window_open_ts_us: venue_ts(POLYMARKET_WINDOW_OPEN_US),
            window_close_ts_us: venue_ts(POLYMARKET_WINDOW_OPEN_US + POLYMARKET_WINDOW_US),
            condition_id: "fitness-condition".into(),
        },
        &mut keep,
    );
    for (at, frame) in &frames {
        core.on_frame(ts(*at), frame, &mut keep);
    }
    with_spins_and_answers(market, FIXED_SCALE * 40 / 100, 5 * FIXED_SCALE)
}

fn note_tokens(frame: &PolyFrame, tokens: &mut Vec<Box<str>>) {
    let mut note = |asset_id: &str| {
        if !tokens.iter().any(|token| &**token == asset_id) {
            tokens.push(asset_id.into());
        }
    };
    match frame {
        PolyFrame::Book(book) => note(&book.asset_id),
        PolyFrame::PriceChange(change) => {
            for delta in &change.changes {
                note(&delta.asset_id);
            }
        }
        PolyFrame::Trade(trade) => note(&trade.asset_id),
        PolyFrame::Batch(frames) => {
            for inner in frames {
                note_tokens(inner, tokens);
            }
        }
        PolyFrame::TickSizeChange(_) | PolyFrame::Ignored => {}
    }
}

fn tape(shipped: Shipped) -> Vec<InboundMessage> {
    match shipped {
        Shipped::Recorder => recorder_tape(),
        Shipped::Publisher => publisher_tape(),
    }
}

/// The engine of `shipped`, armed with a command lane whose bank stamps with `stamper`, and the
/// readers of every lane it writes.
fn armed_engine(shipped: Shipped, log_lane: usize, stamper: Stamper) -> (HotEngine, Lanes) {
    let rows = rows(shipped);
    let (persistence, persist) = persist_ring_for(OUTPUT_RING, all_tables());
    let (strategy_log_sink, strategy_log) = strategy_log_ring(log_lane);
    let strategy_log_drops = strategy_log_sink.drops_handle();
    let (ui_book_sink, ui_books) = ui_book_ring(OUTPUT_RING);
    let (ui_event_sink, ui_events) = ui_event_ring(OUTPUT_RING);
    let (exposure_sink, exposure) = exposure_ring(OUTPUT_RING);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(OUTPUT_RING);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        strategy_log_sink,
        ui_book_sink,
        ui_event_sink,
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer, stamper),
            settings: exec_settings(shipped),
            run_nonce: 0,
        }),
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_sink,
        },
        ..detached_setup(&rows, strategy(shipped))
    });
    let lanes = Lanes {
        persist,
        commands,
        ui_books,
        ui_events,
        exposure,
        strategy_log,
        strategy_log_drops,
    };
    (engine, lanes)
}

/// How a run measures, and how large and how often read its observation rings are.
#[derive(Debug, Clone, Copy)]
struct Observation {
    /// The slots of the stage sample ring. `None` reads no counter and pushes no sample.
    stage_ring: Option<u64>,
    /// The slots of each log lane.
    log_lane: usize,
    are_logs_read: bool,
    are_samples_read: bool,
}

/// What a run lost on its observation rings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Dropped {
    stage_samples: u64,
    counter_samples: u64,
    hot_log: u64,
    strategy_log: u64,
}

/// One run through the hot loop's own driver, over a real input ring, on a thread of its own.
fn drive_tape(shipped: Shipped, observation: Observation) -> (Outputs, Dropped) {
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || drive_on_this_thread(shipped, observation))
        .expect("the test can spawn a thread")
        .join()
        .expect("the fold does not panic")
}

fn drive_on_this_thread(shipped: Shipped, observation: Observation) -> (Outputs, Dropped) {
    const INTERVAL_US: i64 = 60_000_000;
    let mut hot_log = open_hot_lane(observation.log_lane);
    let mut text_log = open_text_lane(observation.log_lane);
    let stamper = match observation.stage_ring {
        Some(_) => Stamper::Counter(quanta::Clock::new()),
        None => Stamper::Off,
    };
    let (engine, mut lanes) = armed_engine(shipped, observation.log_lane, stamper.clone());
    let (samples, mut metrics) = match observation.stage_ring {
        Some(capacity) => {
            let Harness {
                metrics, samples, ..
            } = Harness::with_stage_ring(capacity);
            let SampleProducers {
                stages, counters, ..
            } = samples;
            let samples = DriverSamples {
                stamper,
                stages,
                counters,
            };
            (Some(samples), Some((metrics, Rows::default())))
        }
        None => (None, None),
    };
    let tape = tape(shipped);
    let (mut input, queue) = RingBuffer::new(tape.len());
    for message in tape {
        input
            .push(message)
            .expect("the input ring holds the whole tape");
    }
    let mut driver = HotDriver::new(IngressQueues::new(vec![queue]), engine, samples);
    let mut outputs = Outputs::default();
    while driver.step() == Step::Folded {
        lanes.drain_fold_into(&mut outputs);
        if observation.are_logs_read {
            lanes.drain_strategy_log_into(&mut outputs);
            while let Some(record) = hot_log.pop() {
                outputs.hot_log.push(rendered(&record));
            }
            while let Some(record) = text_log.pop() {
                outputs.text_log.push(text_line(&record));
            }
        }
        if let (true, Some((metrics, rows))) = (observation.are_samples_read, metrics.as_mut()) {
            metrics.pass(ts(0), rows);
        }
    }
    let mut dropped = Dropped {
        hot_log: hot_log.dropped(),
        strategy_log: lanes.strategy_log_drops.load(Ordering::Relaxed),
        ..Dropped::default()
    };
    if let Some((mut metrics, mut rows)) = metrics {
        metrics.finish(ts(INTERVAL_US), &mut rows);
        dropped.stage_samples = rows.dropped_total(SinkKind::StageSamples).unwrap_or(0);
        dropped.counter_samples = rows.dropped_total(SinkKind::CounterSamples).unwrap_or(0);
    }
    (outputs, dropped)
}

/// The outputs of each tape, recorded once. A change to the fold that moves one byte of one lane
/// fails here.
const RECORDED: [(Shipped, [Digest; 8]); 2] = [
    (Shipped::Recorder, RECORDER_OUTPUTS),
    (Shipped::Publisher, PUBLISHER_OUTPUTS),
];

const RECORDER_OUTPUTS: [Digest; 8] = [
    recorded("tape rows", 46_416, 0x02c3_829d_306f_29d1),
    recorded("commands", 241, 0x60ea_828f_819b_c86e),
    recorded("ui books", 601, 0xd1ca_5f6f_2e33_bab2),
    recorded("ui events", 48_281, 0x24ad_47c2_3baf_320e),
    recorded("exposure", 2, 0x27cc_3d45_433e_2022),
    recorded("hot log", 208, 0x13df_027a_e374_48f7),
    recorded("text log", 0, 0xcbf2_9ce4_8422_2325),
    recorded("strategy log", 2_160, 0x3e15_33ff_fb87_83af),
];

const PUBLISHER_OUTPUTS: [Digest; 8] = [
    recorded("tape rows", 12, 0xd94c_dcad_5454_6e50),
    recorded("commands", 80, 0xc4c1_7609_303b_edd7),
    recorded("ui books", 1_734, 0x4116_b312_52d1_9732),
    recorded("ui events", 1_245, 0xc5fe_6b6d_338d_87c0),
    recorded("exposure", 2, 0xe632_e3fd_5bf1_4ad7),
    recorded("hot log", 10, 0x180a_a374_73c5_2996),
    recorded("text log", 0, 0xcbf2_9ce4_8422_2325),
    recorded("strategy log", 0, 0xcbf2_9ce4_8422_2325),
];

const fn recorded(lane: &'static str, count: usize, hash: u64) -> Digest {
    Digest { lane, count, hash }
}

/// FITNESS: the recorded tapes fold through the driver, with measurement off, into exactly the
/// outputs that were recorded.
#[test]
fn fold_outputs_equal_the_recorded_outputs_of_the_entry_state() {
    let mut report = String::new();
    let replay = Observation {
        stage_ring: None,
        log_lane: OUTPUT_RING,
        are_logs_read: true,
        are_samples_read: false,
    };
    for (shipped, expected) in RECORDED {
        let digests = drive_tape(shipped, replay).0.digests();
        for digest in digests {
            writeln!(report, "{shipped:?} {digest:?}").expect("a string takes every write");
        }
        assert_eq!(
            digests, expected,
            "the {shipped:?} tape folds into other outputs than the recorded ones:\n{report}"
        );
    }
}

/// The spin is the oldest head, so the driver pops it first, and the backlog it reports is what the
/// spin left behind in each queue.
#[test]
fn backlog_counts_each_queue_and_not_the_popped_message() {
    const INTERVAL_US: i64 = 60_000_000;
    let mut harness = Harness::new(10);
    let SampleProducers {
        stages, counters, ..
    } = harness.samples;
    let samples = DriverSamples {
        stamper: harness.counter.stamper.clone(),
        stages,
        counters,
    };
    let mut consumers = Vec::new();
    for spins in [[(0, 10), (1, 20), (2, 30)], [(3, 15), (4, 25), (5, 35)]] {
        let (mut producer, consumer) = RingBuffer::new(spins.len());
        for (seq, when) in spins {
            producer
                .push(spin(seq, when))
                .expect("the ring holds its own spins");
        }
        consumers.push(consumer);
    }
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let engine = HotEngine::new(detached_setup(&instruments, Box::new(Idle)));
    let mut driver = HotDriver::new(IngressQueues::new(consumers), engine, Some(samples));

    assert_eq!(driver.step(), Step::Folded);
    harness.metrics.pass(ts(INTERVAL_US), &mut harness.rows);

    let waiting = [0, 1].map(|index| harness.rows.backlog_max(QueueId::new(index).unwrap()));
    assert_eq!(
        waiting,
        [Some(2), Some(3)],
        "the spin's own queue counts only what waits behind it, and the other queue counts all of \
         its messages"
    );
}

/// FITNESS: measurement on and off fold the same messages into the same outputs. The first run
/// reads a real counter in the driver and in the command bank, as an engine start wires them, into
/// a stage ring too small for its samples. The second reads none. Only the tick of each command is
/// left out of the comparison.
#[test]
fn measurement_on_and_off_give_same_state_and_orders() {
    let measured = Observation {
        stage_ring: Some(1),
        log_lane: OUTPUT_RING,
        are_logs_read: true,
        are_samples_read: false,
    };
    let unmeasured = Observation {
        stage_ring: None,
        ..measured
    };
    for shipped in [Shipped::Recorder, Shipped::Publisher] {
        let (on, dropped) = drive_tape(shipped, measured);
        let (off, _) = drive_tape(shipped, unmeasured);
        assert!(
            dropped.stage_samples > 0,
            "the {shipped:?} run with measurement on filled its stage ring: {dropped:?}"
        );
        assert!(
            on == off,
            "measurement changed the {shipped:?} fold at {}",
            on.first_difference(&off)
        );
    }
}

/// FITNESS: observation rings that are full, and never read, change no output of the fold. A fold
/// that waited for a log lane or a sample ring, or branched on a push, would trade differently
/// whenever a reader fell behind.
#[test]
fn full_observation_rings_change_nothing() {
    let full = Observation {
        stage_ring: Some(1),
        log_lane: 1,
        are_logs_read: false,
        are_samples_read: false,
    };
    let roomy = Observation {
        stage_ring: Some(32_768),
        log_lane: OUTPUT_RING,
        are_logs_read: true,
        are_samples_read: true,
    };
    for shipped in [Shipped::Recorder, Shipped::Publisher] {
        let (choked, dropped) = drive_tape(shipped, full);
        let (free, free_dropped) = drive_tape(shipped, roomy);
        assert!(
            dropped.stage_samples > 0 && dropped.counter_samples > 0 && dropped.hot_log > 0,
            "the {shipped:?} run filled each of its observation rings: {dropped:?}"
        );
        assert_eq!(
            free_dropped,
            Dropped::default(),
            "the roomy {shipped:?} run lost nothing"
        );
        let (choked, free) = (choked.without_logs(), free.without_logs());
        assert!(
            choked == free,
            "full observation rings changed the {shipped:?} fold at {}",
            choked.first_difference(&free)
        );
    }
}

/// The metrics thread's intervals that a budgeted run spreads its tape over.
const BUDGET_INTERVALS: usize = 3;

/// One run of a tape whose every sample the metrics thread reads under `budget`, with the tape
/// spread over [`BUDGET_INTERVALS`] intervals of the metrics thread's time.
fn drive_with_budget(shipped: Shipped, budget: LatencyBudgets) -> (Outputs, Rows) {
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || drive_with_budget_on_this_thread(shipped, budget))
        .expect("the test can spawn a thread")
        .join()
        .expect("neither the fold nor the metrics thread panics")
}

fn drive_with_budget_on_this_thread(shipped: Shipped, budget: LatencyBudgets) -> (Outputs, Rows) {
    const INTERVAL_US: i64 = 60_000_000;
    let mut hot_log = open_hot_lane(OUTPUT_RING);
    let mut text_log = open_text_lane(OUTPUT_RING);
    let stamper = Stamper::Counter(quanta::Clock::new());
    let (engine, mut lanes) = armed_engine(shipped, OUTPUT_RING, stamper.clone());
    let Harness {
        mut metrics,
        samples,
        ..
    } = Harness::with_budget(budget);
    let SampleProducers {
        stages, counters, ..
    } = samples;
    let samples = DriverSamples {
        stamper,
        stages,
        counters,
    };
    let tape = tape(shipped);
    let tape_len = tape.len();
    let (mut input, queue) = RingBuffer::new(tape_len);
    for message in tape {
        input
            .push(message)
            .expect("the input ring holds the whole tape");
    }
    let mut driver = HotDriver::new(IngressQueues::new(vec![queue]), engine, Some(samples));
    let mut outputs = Outputs::default();
    let mut metric_rows = Rows::default();
    let mut folded = 0;
    while driver.step() == Step::Folded {
        folded += 1;
        lanes.drain_fold_into(&mut outputs);
        lanes.drain_strategy_log_into(&mut outputs);
        while let Some(record) = hot_log.pop() {
            outputs.hot_log.push(rendered(&record));
        }
        while let Some(record) = text_log.pop() {
            outputs.text_log.push(text_line(&record));
        }
        let interval = (folded * BUDGET_INTERVALS / tape_len) as i64;
        metrics.pass(ts(interval * INTERVAL_US), &mut metric_rows);
    }
    metrics.finish(ts(BUDGET_INTERVALS as i64 * INTERVAL_US), &mut metric_rows);
    while let Some(record) = text_log.pop() {
        outputs.text_log.push(text_line(&record));
    }
    (outputs, metric_rows)
}

/// FITNESS: a stage over its budget gives a warning, a flag on its rows and a count, and nothing
/// else. A breach that reached the fold would let a number of this host's clock decide what the
/// engine trades.
#[test]
fn budget_breach_changes_no_state() {
    const ONE_US: StageBudget = StageBudget {
        p99_us: Some(1),
        p999_us: Some(1),
    };
    let budget = LatencyBudgets {
        venue_to_engine: Some(ONE_US),
        wire_to_message: Some(ONE_US),
        message_to_decision: Some(ONE_US),
        decision_to_wire: Some(ONE_US),
    };
    for shipped in [Shipped::Recorder, Shipped::Publisher] {
        let (breached, breached_rows) = drive_with_budget(shipped, budget);
        let (plain, plain_rows) = drive_with_budget(shipped, LatencyBudgets::default());
        let fold_of = |outputs: &Outputs| Outputs {
            text_log: Vec::new(),
            ..outputs.clone()
        };
        assert!(
            fold_of(&breached) == fold_of(&plain),
            "a budget changed the {shipped:?} fold at {}",
            fold_of(&breached).first_difference(&fold_of(&plain))
        );
        assert!(
            plain_rows.latency().iter().all(|row| !row.is_over_budget),
            "a run with no budget flags no row"
        );
        assert!(
            !plain.text_log.iter().any(|line| line.contains("budget")),
            "a run with no budget writes no budget warning"
        );

        let venue: Vec<_> = breached_rows
            .latency()
            .into_iter()
            .filter(|row| row.stage == Stage::VenueToEngine && row.count > 0)
            .collect();
        let trades: Vec<_> = venue
            .iter()
            .filter(|row| row.category == Category::Trade)
            .collect();
        if shipped == Shipped::Recorder {
            let intervals: std::collections::BTreeSet<_> =
                trades.iter().map(|row| row.interval_start_ts_us).collect();
            assert_eq!(
                intervals.len(),
                BUDGET_INTERVALS,
                "the {shipped:?} tape times the venue of a trade in each interval"
            );
        }
        for row in &trades {
            assert!(
                row.is_over_budget && row.budget_p99_ns == Some(1_000),
                "a trade that reaches the engine milliseconds after its venue sent it breaks a \
                 budget of one microsecond: {row:?}"
            );
        }
        let broken_intervals: std::collections::BTreeSet<_> = venue
            .iter()
            .filter(|row| row.is_over_budget)
            .map(|row| row.interval_start_ts_us)
            .collect();
        let venue_warnings = breached
            .text_log
            .iter()
            .filter(|line| line.starts_with("Warn") && line.contains("budget"))
            .filter(|line| line.contains(Stage::VenueToEngine.label()))
            .count();
        assert_eq!(
            venue_warnings,
            broken_intervals.len(),
            "one warning for each interval in which the stage broke its budget"
        );
        assert!(
            breached_rows
                .latency()
                .iter()
                .filter(|row| row.stage == Stage::Processing)
                .all(|row| !row.is_over_budget && row.budget_p99_ns.is_none()),
            "a part of a stage carries no budget of its own"
        );
    }
}

/// The spin at which [`ThreeBids`] starts to quote.
const QUOTING_SPIN: u64 = 5;

/// The instruments of [`ThreeBids`]. A side holds one unanswered command at a time, so three
/// placements in one pass need three instruments.
const BID_INSTRUMENTS: [u16; 3] = [0, 1, 2];

/// Declares a bid on each of [`BID_INSTRUMENTS`] from [`QUOTING_SPIN`] on, so that one execution
/// pass banks three placements.
struct ThreeBids;

impl Strategy for ThreeBids {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        if tick.seq < QUOTING_SPIN {
            return;
        }
        for instrument in BID_INSTRUMENTS {
            ctx.quote(
                InstrumentId(instrument),
                Side::Buy,
                QuoteLevel::ZERO,
                Some(DesiredQuote {
                    price: Price::from_mantissa(99 * ONE),
                    qty: Qty::from_mantissa(ONE / 10),
                }),
            );
        }
    }
}

fn commands_of(lane: &mut Consumer<ExecLaneItem>) -> Vec<ExecLaneItem> {
    std::iter::from_fn(|| lane.pop().ok()).collect()
}

fn stamped(item: ExecLaneItem) -> StampedExecCommand {
    match item {
        ExecLaneItem::Command(stamped) => stamped,
        ExecLaneItem::Watermark(at) => panic!("a watermark at {at:?} where a command was due"),
    }
}

/// A command that the full lane holds in the bank keeps the tick it read when it entered the bank,
/// so the time it waited there shows in its stage. Read again when it leaves, the tick would hide
/// that wait, and a full lane would look like a fast one.
#[test]
fn held_command_keeps_its_issue_tick() {
    const INTERVAL_US: i64 = 60_000_000;
    let mut harness = Harness::new(10);
    let instruments = BID_INSTRUMENTS.map(|id| InstrumentRow {
        tick_size: Some(Price::from_mantissa(ONE)),
        lot_size: Some(Qty::from_mantissa(1)),
        ..instrument_row(id, TrackerSpec::default(), 64)
    });
    let (commands_producer, mut lane) = RingBuffer::<ExecLaneItem>::new(2);
    let engine = HotEngine::new(HotEngineSetup {
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer, harness.counter.stamper.clone()),
            settings: settings(FeeModel::None, true),
            run_nonce: 0,
        }),
        ..detached_setup(&instruments, Box::new(ThreeBids))
    });
    let (mut input, queue) = RingBuffer::new(32);
    let mut start: Vec<InboundMessage> = readiness(1).into();
    for instrument in BID_INSTRUMENTS {
        let (bids, asks) = snapshot_pair(instrument, &[(99 * ONE, ONE)], &[(101 * ONE, ONE)], 1);
        start.push(InboundMessage::new(ts(1), MessageBody::Book(bids)));
        start.push(InboundMessage::new(ts(1), MessageBody::Book(asks)));
        start.push(InboundMessage::new(
            ts(1),
            MessageBody::Exec(open_orders_snapshot_end(InstrumentId(instrument), ts(1))),
        ));
    }
    for message in start {
        input.push(message).expect("the input ring has room");
    }
    let mut driver = HotDriver::new(IngressQueues::new(vec![queue]), engine, None);
    let mut fold_spin = |input: &mut rtrb::Producer<InboundMessage>, seq: u64| {
        input
            .push(spin(seq, 10 + seq as i64))
            .expect("the input ring has room");
        while driver.step() == Step::Folded {}
    };
    for seq in 0..QUOTING_SPIN {
        fold_spin(&mut input, seq);
        commands_of(&mut lane);
    }

    let banked_at = harness.counter.at(100);
    fold_spin(&mut input, QUOTING_SPIN);
    let held_spin = commands_of(&mut lane);
    assert_eq!(
        held_spin.len(),
        2,
        "the lane takes two, and the third waits in the bank"
    );
    for item in held_spin {
        assert_eq!(
            stamped(item).issue_tick,
            banked_at,
            "each placement read the bank's tick"
        );
    }

    harness.counter.at(1_000_500);
    fold_spin(&mut input, QUOTING_SPIN + 1);
    let mut next_spin = commands_of(&mut lane).into_iter();
    let held = stamped(next_spin.next().expect("the held placement goes out first"));
    assert!(
        matches!(held.command, ExecCommand::Place { .. }),
        "the third placement: {held:?}"
    );
    assert_eq!(
        held.issue_tick, banked_at,
        "the held placement kept the tick of its bank entry, read no second one"
    );
    assert!(
        matches!(next_spin.next(), Some(ExecLaneItem::Watermark(_))),
        "then the watermark of its spin"
    );

    let pop_tick = harness.counter.at(1_000_500);
    let wire_tick = harness.counter.at(1_000_800);
    let ExecCommand::Place { client_id, .. } = held.command else {
        unreachable!("matched above");
    };
    harness.samples.wires.push(WireSample {
        issue_tick: held.issue_tick,
        pop_tick,
        wire_tick,
        issued_ts_us: held.issued_ts_us,
        client_id,
        kind: CommandKind::Place,
        queue: QueueId::new(0).unwrap(),
    });
    harness.pass(INTERVAL_US);
    let stage = |stage: Stage| {
        harness
            .rows
            .stage(Category::Command, stage)
            .map(|row| (row.count, row.max_ns))
    };
    assert_eq!(
        stage(Stage::BankAndLaneWait),
        Some((1, 1_000_400)),
        "the held spin shows in the wait"
    );
    assert_eq!(stage(Stage::DecisionToWire), Some((1, 1_000_700)));
    assert_eq!(
        harness.rows.dropped_total(SinkKind::WireSamples),
        Some(0),
        "no sample lost"
    );
}

/// Keeps the sequence number of each spin it is called for, in the order the fold calls it.
struct SeenSpins(Arc<Mutex<Vec<u64>>>);

impl Strategy for SeenSpins {
    fn on_spin(&mut self, _ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        self.0
            .lock()
            .expect("the probe lock is never poisoned")
            .push(tick.seq);
    }
}

/// The park count of the counters line that the metrics thread wrote on this thread's text lane.
fn idle_parks_logged(text_log: &mut LaneReader<TextRecord>) -> Option<u64> {
    let mut parks = None;
    while let Some(record) = text_log.pop() {
        let Some(counters) = record.text().strip_prefix("metrics counters") else {
            continue;
        };
        parks = counters
            .split_whitespace()
            .find_map(|word| word.strip_prefix("idle_parks="))
            .and_then(|count| count.parse().ok());
    }
    parks
}

/// A parking hot loop sleeps while every queue is empty, and it still folds each message once and
/// in push order. The count of its sleeps reaches the metrics thread with each spin.
#[test]
fn park_setting_parks_idle_loop() {
    const INTERVAL_US: i64 = 60_000_000;
    const GAP: Duration = Duration::from_millis(20);
    let mut text_log = open_text_lane(64);
    let mut harness = Harness::new(10);
    let SampleProducers {
        stages, counters, ..
    } = harness.samples;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let strategy = SeenSpins(Arc::clone(&seen));
    let engine = HotEngine::new(detached_setup(&instruments, Box::new(strategy)));
    let (mut input, queue) = RingBuffer::new(4);
    let drain = DrainSignal::new();
    let samples = DriverSamples {
        stamper: harness.counter.stamper.clone(),
        stages,
        counters,
    };
    let hot_drain = drain.clone();
    let hot = spawn_hot_thread(
        HotThreadConfig {
            core_id: None,
            tag: "fitness-idle",
        },
        pin_to_core,
    )
    .expect("a thread with no core never refuses its pin")
    .start(move || HotLoop {
        driver: HotDriver::new(IngressQueues::new(vec![queue]), engine, Some(samples)),
        idle: IdleWait::Park(Duration::from_micros(50)),
        fatal: FatalSignal::new(),
        drain: hot_drain,
    });
    for seq in 0..3u64 {
        if seq > 0 {
            std::thread::sleep(GAP);
        }
        input
            .push(spin(seq, 10 * (seq as i64 + 1)))
            .expect("the input ring has room");
    }
    drain.request();
    hot.join()
        .expect("the hot thread ends once the drain finds every queue empty");

    let folded = seen
        .lock()
        .expect("the probe lock is never poisoned")
        .clone();
    assert_eq!(
        folded,
        vec![0, 1, 2],
        "each message folded once, in push order"
    );
    harness.metrics.pass(ts(INTERVAL_US), &mut harness.rows);
    let parks = idle_parks_logged(&mut text_log);
    assert!(
        parks.is_some_and(|count| count > 0),
        "the loop slept in the gaps between pushes, and its counter sample said so: {parks:?}"
    );
}
