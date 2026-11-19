//! The shipped Binance recorder over three tapes: two seeded books whose candles warm its models
//! until it quotes, and the recorded spot capture. The tapes are copies that no other test shares,
//! so a change of another test's fixture cannot move the golden file.

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::StrategyConfig;
use engine::msg::exec::ExecLaneItem;
use engine::msg::ids::{AssetId, InstrumentId, Notional, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, TradeEvent};
use engine::msg::time::{DurationUs, Stamper};
use engine::runtime::config::{
    BinanceMarket, IntensitySpec, KlineInterval, TableKind, VenueMarket,
};
use engine::runtime::registry::InstrumentRow;
use engine::venues::binance::depth::{ChainRule, DepthSequencer, DiffOutcome, SnapshotOutcome};
use engine::venues::binance::parse::{
    DepthDiff, DepthSnapshot, ParseContext, parse_agg_trade, parse_combined_frame,
    parse_spot_depth_diff,
};
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::RingBuffer;

use super::record::{DeclaredQuotes, Recording, with_spins};
use crate::engine_support::{
    ONE, account_chunk, book_reset, delta_chunk, detached_setup, engine_view, instrument_row,
    kline, level, persist_ring, recorder_spec, snapshot_pair, spin, stream_ready, tracker_spec_all,
    trade, ts, ui_event_ring,
};
use crate::exec_support::exec_settings;
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};

const TRADES: &str = include_str!("../../../fixtures/binance/spot_aggTrade.jsonl");
const DEPTH: &str = include_str!("../../../fixtures/binance/spot_depth.jsonl");

const INSTRUMENT: InstrumentId = InstrumentId(0);
const TICK: i64 = ONE / 100;

/// The seeded tapes spin once for each step, and one candle closes each step, so the EGARCH fit
/// reaches its floor of closes inside the tape.
const SEEDED_SPIN: DurationUs = DurationUs::from_secs(3);
const SEEDED_STEPS: i64 = 900;
const SEEDED_STEP_US: i64 = 500_000;

/// The timer of the recorded capture, and how long after its venue stamp each frame arrives.
const RECORDED_SPIN_US: i64 = 250_000;
const WIRE_DELAY_US: i64 = 3_000;

/// A receive time near the capture. The parser clamps each venue stamp to a band around the receive
/// time, so a placeholder far from the capture would move each stamp onto the edge of that band.
const RECORDED_AT_US: i64 = 1_784_410_630_000_000;

/// Each tape: its name in the golden file, the price of its seeded book, and its lot step.
const SEEDED_TAPES: [(&str, i64, i64); 2] = [
    ("quotes", 100 * ONE, ONE / 1_000),
    ("min_distance", 10_000 * ONE, ONE / 100_000),
];

pub(super) fn declared_quotes() -> DeclaredQuotes {
    let mut quotes = DeclaredQuotes::default();
    for (tape, base, step) in SEEDED_TAPES {
        let mut recording = recorder(
            tape,
            seeded_row(step),
            SEEDED_SPIN,
            vec![TableKind::Features],
        );
        recording.play_all(&seeded_tape(base), &mut quotes);
    }
    let spin_interval = DurationUs::from_micros(RECORDED_SPIN_US);
    let mut recording = recorder("spot", spot_row(), spin_interval, vec![TableKind::Features]);
    recording.play_all(&spot_tape(), &mut quotes);
    quotes
}

/// The quotes of the seeded tape `quotes` with `tables` recorded.
pub(super) fn seeded_quotes_with(tables: Vec<TableKind>) -> DeclaredQuotes {
    let (tape, base, step) = SEEDED_TAPES[0];
    let mut quotes = DeclaredQuotes::default();
    let mut recording = recorder(tape, seeded_row(step), SEEDED_SPIN, tables);
    recording.play_all(&seeded_tape(base), &mut quotes);
    quotes
}

/// The recorder with its shipped params, an execution lane, and `tables` recorded.
fn recorder(
    tape: &'static str,
    row: InstrumentRow,
    spin_interval: DurationUs,
    tables: Vec<TableKind>,
) -> Recording {
    let rows = [row];
    let (persistence, persist) = persist_ring(16_384);
    let (ui_event_sink, events) = ui_event_ring(16_384);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(1_024);
    let spec = recorder_spec::<MicroRecorderParams>(tables);
    let strategy = Box::new(MicroRecorder::from_spec(&spec, engine_view(spin_interval)));
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink,
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }],
        ..detached_setup(&rows, strategy)
    });
    Recording {
        tape,
        engine,
        events,
        persist,
        commands,
    }
}

/// The stream is up, the open orders are known, and so are the balances: without all three the
/// recorder declares a quote that the engine never sends.
fn readiness(when: i64) -> [InboundMessage; 3] {
    [
        stream_ready(INSTRUMENT, when),
        InboundMessage::new(
            ts(when),
            MessageBody::Exec(open_orders_snapshot_end(INSTRUMENT, ts(when))),
        ),
        InboundMessage::new(
            ts(when),
            MessageBody::Account(account_chunk(
                &[(AssetId(0), 1_000 * ONE), (AssetId(1), 1_000_000 * ONE)],
                when,
            )),
        ),
    ]
}

fn seeded_row(step: i64) -> InstrumentRow {
    let mut row = instrument_row(0, tracker_spec_all(100), 128);
    row.tick_size = Some(Price::from_mantissa(TICK));
    row.lot_size = Some(Qty::from_mantissa(step));
    row.min_qty = Some(Qty::from_mantissa(step));
    row.min_notional = Some(Notional::from_mantissa(ONE));
    row.tracker.intensity = Some(IntensitySpec {
        max_depth_ticks: 16,
        half_life_secs: 600.0,
        min_events: 5.0,
    });
    row
}

fn spot_row() -> InstrumentRow {
    let mut row = seeded_row(1_000);
    row.market = VenueMarket::Binance(BinanceMarket::Spot);
    row.min_notional = Some(Notional::from_mantissa(5 * ONE));
    row
}

/// A two-tick touch, then one step for each spin: a print into its own side of the book, a depth
/// change, and a closed candle. Prints of both sides fit an intensity on each side, and a wide
/// spread would overflow the Guéant term `A·e^(k·half_spread_ticks)` on a steep fitted k.
fn seeded_tape(base: i64) -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(
        0,
        &[(base, 2 * ONE), (base - 5 * TICK, ONE)],
        &[(base + 2 * TICK, ONE), (base + 7 * TICK, 2 * ONE)],
        1,
    );
    let mut tape: Vec<InboundMessage> = readiness(0).into();
    tape.extend([
        book_reset(0, 0),
        InboundMessage::new(ts(1), MessageBody::Book(bids)),
        InboundMessage::new(ts(1), MessageBody::Book(asks)),
    ]);
    for index in 0..SEEDED_STEPS {
        tape.extend(seeded_step(base, index, index * SEEDED_STEP_US));
    }
    tape
}

fn seeded_step(base: i64, index: i64, when: i64) -> [InboundMessage; 4] {
    let (side, price) = match index % 2 == 0 {
        true => (Side::Buy, base + 2 * TICK + (index % 4) * TICK),
        false => (Side::Sell, base - (index % 4) * TICK),
    };
    let (level_side, level_price) = match index % 2 == 0 {
        true => (Side::Sell, base + 7 * TICK),
        false => (Side::Buy, base - 5 * TICK),
    };
    let close = base + (index % 5) * TICK;
    [
        InboundMessage::new(
            ts(when),
            MessageBody::Trade(trade(0, price, ONE / 100, side, when)),
        ),
        InboundMessage::new(
            ts(when),
            MessageBody::Book(delta_chunk(
                0,
                level_side,
                &[(level_price, (1 + index % 5) * ONE)],
                when,
            )),
        ),
        InboundMessage::new(
            ts(when),
            MessageBody::Kline(kline(
                0,
                KlineInterval::OneMinute,
                (base, base + 5 * TICK, base - 5 * TICK, close),
                true,
                when,
            )),
        ),
        spin(index as u64, when),
    ]
}

enum Frame {
    Trade(TradeEvent),
    Depth(DepthDiff),
}

/// The recorded spot trades and depth changes, each received a few milliseconds after its venue
/// stamp, the depth sequenced onto a snapshot as the Binance edge sequences it.
fn spot_tape() -> Vec<InboundMessage> {
    let placeholder = ParseContext {
        instrument: INSTRUMENT,
        received_ts_us: ts(RECORDED_AT_US),
    };
    let mut frames: Vec<(i64, Frame)> = Vec::new();
    for line in TRADES.lines().filter(|line| !line.trim().is_empty()) {
        let frame = parse_combined_frame(line).expect("a recorded frame has its envelope");
        let trade = parse_agg_trade(&frame.data, placeholder)
            .expect("a recorded trade parses")
            .trade;
        let at = trade.exchange_ts_us.micros() + WIRE_DELAY_US;
        frames.push((at, Frame::Trade(trade)));
    }
    for line in DEPTH.lines().filter(|line| !line.trim().is_empty()) {
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
    let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 1_024);
    let mut market: Vec<InboundMessage> = readiness(start).into();
    let outcome = sequencer.apply_snapshot(
        DepthSnapshot {
            instrument: INSTRUMENT,
            last_update_id: first_update_id - 1,
            bids: vec![level(64_680 * ONE, ONE), level(64_670 * ONE, 2 * ONE)],
            asks: vec![level(64_690 * ONE, ONE), level(64_700 * ONE, 2 * ONE)],
            received_ts_us: ts(start),
        },
        &mut |message| market.push(message),
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
                let outcome = sequencer.on_diff(diff, &mut |message| market.push(message));
                assert_eq!(
                    outcome,
                    DiffOutcome::Applied,
                    "the capture chains diff to diff"
                );
            }
        }
    }
    with_spins(market, RECORDED_SPIN_US)
}
