//! FITNESS: the hot loop folds the same messages into the same outputs, byte for byte, whatever its
//! measurement does. A fold that read a clock, or that measurement fed back into, would still print
//! plausible rows and orders: only a comparison of every output lane shows the difference.

use std::fmt::Write as _;

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{
    ClientIdLayout, ExecLimits, ExecSettings, FeeModel, OrderBudget, side_base,
};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyConfig};
use engine::log::{HotRecord, TextRecord, open_hot_lane, open_text_lane};
use engine::msg::exec::{ExecEvent, ExecKind, ExecLaneItem, Provenance, VenueOrderStatus};
use engine::msg::ids::{AssetId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, TradeEvent};
use engine::msg::persist::PersistRecord;
use engine::msg::time::DurationUs;
use engine::msg::ui::{UiBookSnapshot, UiEvent};
use engine::runtime::config::{
    BinanceMarket, Instruments, IntensitySpec, PolySeries, RecordedTables, StrategySpec,
    Subscriptions, TableKind, TrackerSpec, VenueMarket,
};
use engine::runtime::exposure::ExposureSnapshot;
use engine::runtime::registry::InstrumentRow;
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
    ONE, account_chunk, detached_setup, engine_view, exec_event, exposure_ring, instrument_row,
    level, persist_ring_for, pop, spin, strategy_log_ring, stream_ready, tracker_spec_all, ts,
    ui_book_ring, ui_event_ring,
};
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher};

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
enum Shipped {
    Recorder,
    Publisher,
}

/// Each output lane of one run, as text that holds every byte of each record.
#[derive(Debug, Default, PartialEq, Eq)]
struct Outputs {
    tape_rows: Vec<String>,
    /// The bank stamp of each command, never the latency tick it carries.
    commands: Vec<String>,
    ui_books: Vec<String>,
    /// Numbered from zero again, with the latency event of each spin left out: that event carries a
    /// clock reading.
    ui_events: Vec<String>,
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
            digest("ui events", &self.ui_events),
            digest("exposure", &self.exposure),
            digest("hot log", &self.hot_log),
            digest("text log", &self.text_log),
            digest("strategy log", &self.strategy_log),
        ]
    }
}

fn digest(lane: &'static str, records: &[String]) -> Digest {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for record in records {
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
}

impl Lanes {
    fn drain_into(&mut self, outputs: &mut Outputs) {
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
        while let Ok(mut event) = self.ui_events.pop() {
            if matches!(event, UiEvent::Latency { .. }) {
                continue;
            }
            event.set_seq(outputs.ui_events.len() as u64);
            outputs.ui_events.push(format!("{event:?}"));
        }
        while let Ok(snapshot) = self.exposure.pop() {
            outputs.exposure.push(format!("{snapshot:?}"));
        }
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
            max_order_notional_quote: 1_000_000 * ONE,
        },
        max_orders_per_side: 1,
        min_base_balance: 0,
        min_quote_balance: 0,
        max_consecutive_rejects: 5,
        max_session_loss_quote: 1_000_000 * ONE,
        inflight_timeout: DurationUs::from_secs(5),
        exec_silence_spins: u32::MAX,
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
    row.tick_size = Some(Price(ONE / 100));
    row.lot_size = Some(Qty(1_000));
    row.min_qty = Some(Qty(1_000));
    row.min_notional = Some(5 * ONE);
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
            tick_size: Some(Price(FIXED_SCALE / 100)),
            lot_size: Some(Qty(FIXED_SCALE / 100)),
            min_qty: Some(Qty(5 * FIXED_SCALE)),
            max_price: Some(Price(FIXED_SCALE * 99 / 100)),
            max_num_orders: Some(32),
            subscriptions: Subscriptions {
                klines: false,
                ..Subscriptions::default()
            },
            kline_intervals: Vec::new(),
            max_exposure_quote: 500 * FIXED_SCALE,
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

fn exec_settings(shipped: Shipped) -> ExecSettings {
    match shipped {
        Shipped::Recorder => settings(FeeModel::None, true),
        Shipped::Publisher => settings(FeeModel::BinaryOutcome, false),
    }
}

/// The three facts that let an order be sent: the stream is up, the open orders are known, and so
/// are the balances.
fn readiness(at: i64) -> [InboundMessage; 3] {
    [
        stream_ready(InstrumentId(0), at),
        InboundMessage::Exec(open_orders_snapshot_end(InstrumentId(0), ts(at))),
        InboundMessage::Account(account_chunk(
            &[(AssetId(0), 1_000 * ONE), (AssetId(1), 1_000_000 * ONE)],
            at,
        )),
    ]
}

/// A fill that the venue reports on an order an earlier run left resting: the position moves, the
/// engine cancels the order, and each output lane hears of it.
fn prior_run_fill(price: i64, cumulative: i64, at: i64) -> InboundMessage {
    let client_id = ClientIdLayout {
        run_nonce: PRIOR_NONCE,
    }
    .encode(side_base(InstrumentId(0), Side::Buy) + 1, 1);
    InboundMessage::Exec(ExecEvent {
        kind: ExecKind::ReportTrade,
        provenance: Provenance::PriorRun,
        status: Some(VenueOrderStatus::PartiallyFilled),
        qty: Qty(4 * cumulative),
        last_price: Price(price),
        last_qty: Qty(cumulative / 2),
        cumulative_qty: Qty(cumulative),
        cumulative_quote: Price(price).notional(Qty(cumulative)),
        ..exec_event(InstrumentId(0), client_id, Side::Buy, price, at)
    })
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
        .map_or(0, |message| message.received_ts_us().micros());
    let fills = [market.len() / 3, 2 * market.len() / 3];
    let mut tape: Vec<InboundMessage> = readiness(first).into();
    let mut next_spin = first + SPIN_US;
    let mut spin_seq = 0;
    for (index, message) in market.into_iter().enumerate() {
        let at = message.received_ts_us().micros();
        while next_spin <= at {
            tape.push(InboundMessage::SpinTick(spin(spin_seq, next_spin)));
            spin_seq += 1;
            next_spin += SPIN_US;
        }
        if let Some(nth) = fills.iter().position(|&fill| fill == index) {
            tape.push(prior_run_fill(fill_price, fill_qty * (nth as i64 + 1), at));
        }
        tape.push(message);
    }
    tape.push(InboundMessage::SpinTick(spin(spin_seq, next_spin)));
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
        let mut trade = parse_agg_trade(&frame.data, placeholder)
            .expect("a recorded trade parses")
            .trade;
        let at = trade.exchange_ts_us.micros() + WIRE_DELAY_US;
        trade.received_ts_us = ts(at);
        trade.queued_ts_us = ts(at);
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
    for (_, frame) in frames {
        match frame {
            Frame::Trade(trade) => market.push(InboundMessage::Trade(trade)),
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
            window_open_ts_us: ts(POLYMARKET_WINDOW_OPEN_US),
            window_close_ts_us: ts(POLYMARKET_WINDOW_OPEN_US + POLYMARKET_WINDOW_US),
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

/// The engine of `shipped`, armed with a command lane, and the readers of every lane it writes.
fn armed_engine(shipped: Shipped, rows: &[InstrumentRow]) -> (HotEngine, Lanes) {
    let (persistence, persist) = persist_ring_for(OUTPUT_RING, all_tables());
    let (strategy_log_sink, strategy_log) = strategy_log_ring(OUTPUT_RING);
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
            sink: ExecSink::new(commands_producer),
            settings: exec_settings(shipped),
            run_nonce: 0,
        }),
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_sink,
        },
        ..detached_setup(rows, strategy(shipped))
    });
    let lanes = Lanes {
        persist,
        commands,
        ui_books,
        ui_events,
        exposure,
        strategy_log,
    };
    (engine, lanes)
}

/// One run on a thread of its own, so the hot and text lanes it opens are its own.
fn fold_tape(shipped: Shipped) -> Outputs {
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || fold_on_this_thread(shipped))
        .expect("the test can spawn a thread")
        .join()
        .expect("the fold does not panic")
}

fn fold_on_this_thread(shipped: Shipped) -> Outputs {
    let mut hot_log = open_hot_lane(OUTPUT_RING);
    let mut text_log = open_text_lane(OUTPUT_RING);
    let rows = rows(shipped);
    let (mut engine, mut lanes) = armed_engine(shipped, &rows);
    let mut outputs = Outputs::default();
    for message in tape(shipped) {
        engine.dispatch(pop(0, 0), &message);
        lanes.drain_into(&mut outputs);
        while let Some(record) = hot_log.pop() {
            outputs.hot_log.push(rendered(&record));
        }
        while let Some(record) = text_log.pop() {
            outputs.text_log.push(text_line(&record));
        }
    }
    outputs
}

/// The outputs of each tape, recorded once. A change to the fold that moves one byte of one lane
/// fails here.
const RECORDED: [(Shipped, [Digest; 8]); 2] = [
    (Shipped::Recorder, RECORDER_OUTPUTS),
    (Shipped::Publisher, PUBLISHER_OUTPUTS),
];

const RECORDER_OUTPUTS: [Digest; 8] = [
    recorded("tape rows", 46_415, 0x6a6d_77e4_2f76_4512),
    recorded("commands", 241, 0xa5c3_8b1f_1c6d_e8ff),
    recorded("ui books", 601, 0xd1ca_5f6f_2e33_bab2),
    recorded("ui events", 48_281, 0x24ad_47c2_3baf_320e),
    recorded("exposure", 2, 0x27cc_3d45_433e_2022),
    recorded("hot log", 207, 0xdd2c_5282_3961_7e45),
    recorded("text log", 0, 0xcbf2_9ce4_8422_2325),
    recorded("strategy log", 2_160, 0x3e15_33ff_fb87_83af),
];

const PUBLISHER_OUTPUTS: [Digest; 8] = [
    recorded("tape rows", 11, 0x22fa_8faf_4d66_cae5),
    recorded("commands", 80, 0xad4e_f8d5_f78b_5301),
    recorded("ui books", 1_734, 0x4116_b312_52d1_9732),
    recorded("ui events", 1_245, 0xc5fe_6b6d_338d_87c0),
    recorded("exposure", 2, 0xe632_e3fd_5bf1_4ad7),
    recorded("hot log", 9, 0x63ff_97f6_c5b0_f309),
    recorded("text log", 0, 0xcbf2_9ce4_8422_2325),
    recorded("strategy log", 0, 0xcbf2_9ce4_8422_2325),
];

const fn recorded(lane: &'static str, count: usize, hash: u64) -> Digest {
    Digest { lane, count, hash }
}

#[test]
fn fold_outputs_equal_the_recorded_outputs_of_the_entry_state() {
    let mut report = String::new();
    for (shipped, expected) in RECORDED {
        let digests = fold_tape(shipped).digests();
        for digest in digests {
            writeln!(report, "{shipped:?} {digest:?}").expect("a string takes every write");
        }
        assert_eq!(
            digests, expected,
            "the {shipped:?} tape folds into other outputs than the recorded ones:\n{report}"
        );
    }
}
