//! The inputs of the runs that the re-fold tests record: the recorded venue captures through the
//! real parsers, the shipped configs with the few lines each test changes, and a probe strategy that
//! quotes the touch so the run places orders.

use std::path::Path;

use engine::hot::strategy::{
    DesiredQuote, EngineView, QuoteLevel, Strategy, StrategyConfig, StrategyCtx,
};
use engine::msg::ids::Price;
use engine::msg::ids::{InstrumentId, Qty, Side};
use engine::msg::inbound::Level;
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::time::{LocalTs, VenueTs};
use engine::runtime::config::{NoParams, StrategySpec};
use engine::venues::binance::depth::{ChainRule, DepthSequencer, DiffOutcome, SnapshotOutcome};
use engine::venues::binance::parse::{
    DepthDiff, DepthSnapshot, ParseContext, parse_agg_trade, parse_combined_frame,
    parse_spot_depth_diff,
};
use engine::venues::polymarket::actor::{DriverEffect, PolyDriverCore, SlotLegs};
use engine::venues::polymarket::discovery::PolySchedule;
use engine::venues::polymarket::parse::{PolyFrame, is_frameless, parse_market_frame};
use engine::venues::polymarket::rotation::{OutcomeLeg, TokenId, WindowAssignment};

const ONE: i64 = 100_000_000;

const BINANCE_TRADES: &str = include_str!("../../../engine/fixtures/binance/spot_aggTrade.jsonl");
const BINANCE_DEPTH: &str = include_str!("../../../engine/fixtures/binance/spot_depth.jsonl");
const POLYMARKET_MARKET: &str =
    include_str!("../../../engine/fixtures/polymarket/poly_market.jsonl");

pub(crate) const BINANCE_CONFIG: &str =
    include_str!("../../../strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml");
pub(crate) const POLYMARKET_CONFIG: &str = include_str!(
    "../../../strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/config.yaml"
);

/// The spin of the shipped Binance engine, and how long after its venue stamp each recorded frame
/// arrives.
const BINANCE_SPIN_US: i64 = 1_000_000;
const WIRE_DELAY_US: i64 = 3_000;

/// A receive time near the capture. The parser clamps each venue stamp to a band around the receive
/// time, so a placeholder far from the capture would move each stamp onto the edge of that band.
const BINANCE_RECEIVED_US: i64 = 1_784_410_630_000_000;

const POLYMARKET_SPIN_US: i64 = 100_000;
const POLYMARKET_START_US: i64 = 1_784_449_240_000_000;
const POLYMARKET_FRAME_GAP_US: i64 = 20_000;
const POLYMARKET_WINDOW_OPEN_US: i64 = 1_784_449_200_000_000;
const POLYMARKET_WINDOW_US: i64 = 300_000_000;

/// The size of each quote of the probe: 0.0001 BTC.
const TOUCH_QTY: Qty = Qty::from_mantissa(10_000);

/// Each spin after the gate is live, one bid at the best bid and one ask at the best ask.
pub(crate) struct TouchQuoter;

impl Strategy for TouchQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let instrument = InstrumentId(0);
        let book = ctx.book(instrument);
        let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        for (side, price) in [(Side::Buy, bid.price), (Side::Sell, ask.price)] {
            let quote = DesiredQuote {
                price,
                qty: TOUCH_QTY,
            };
            ctx.quote(instrument, side, QuoteLevel::ZERO, Some(quote));
        }
    }
}

impl StrategyConfig for TouchQuoter {
    type Params = NoParams;

    fn from_spec(_spec: &StrategySpec<NoParams>, _engine: EngineView) -> Self {
        TouchQuoter
    }
}

/// The shipped config with each `(from, to)` of `changes` made, each one asserted, so a change of
/// the shipped file fails here rather than in a fold. The persistence root is always `root`.
pub(crate) fn config_text(shipped: &str, root: &Path, changes: &[(&str, &str)]) -> String {
    let mut text = shipped.to_owned();
    let dir = format!("dir: {}", root.display());
    let logs = format!("dir: {}", root.join("logs").display());
    let mut all: Vec<(&str, &str)> = vec![
        ("dir: ./data", dir.as_str()),
        ("dir: ./logs", logs.as_str()),
    ];
    all.extend_from_slice(changes);
    for (from, to) in all {
        assert!(
            text.contains(from),
            "the shipped config no longer holds {from:?}"
        );
        text = text.replacen(from, to, 1);
    }
    text
}

/// The changes of each Binance case: the warmup of SP-01, and a stream silence longer than the
/// capture, so no reconciliation request waits for an answer that the test venue never gives.
pub(crate) const BINANCE_CHANGES: [(&str, &str); 2] = [
    ("warmup_secs: 60", "warmup_secs: 10"),
    ("stream_silence_ms: 30000", "stream_silence_ms: 600000"),
];

/// The probe takes no params.
pub(crate) const NO_PARAMS: (&str, &str) = (
    "  params:\n    # Dollars per order.\n    order_notional: 10\n",
    "",
);

enum Frame {
    Trade(TradeEvent),
    Depth(DepthDiff),
}

/// The recorded spot trades and depth changes, each received a few milliseconds after its venue
/// stamp, the depth sequenced onto one snapshot as the Binance edge sequences it, with a spin each
/// second.
pub(crate) fn binance_tape() -> Vec<InboundMessage> {
    let placeholder = ParseContext {
        instrument: InstrumentId(0),
        received_ts_us: LocalTs::from_micros(BINANCE_RECEIVED_US),
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
        frames.push((
            trade.exchange_ts_us.micros() + WIRE_DELAY_US,
            Frame::Trade(trade),
        ));
    }
    for line in BINANCE_DEPTH.lines().filter(|line| !line.trim().is_empty()) {
        let frame = parse_combined_frame(line).expect("a recorded frame has its envelope");
        let mut diff =
            parse_spot_depth_diff(&frame.data, placeholder).expect("a recorded diff parses");
        let at = diff.exchange_ts_us.micros() + WIRE_DELAY_US;
        diff.received_ts_us = LocalTs::from_micros(at);
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
    let level = |price: i64, qty: i64| Level {
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
    };
    let outcome = sequencer.apply_snapshot(
        DepthSnapshot {
            instrument: InstrumentId(0),
            last_update_id: first_update_id - 1,
            bids: vec![level(64_680 * ONE, ONE), level(64_670 * ONE, 2 * ONE)],
            asks: vec![level(64_690 * ONE, ONE), level(64_700 * ONE, 2 * ONE)],
            received_ts_us: LocalTs::from_micros(start),
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
            Frame::Trade(trade) => market.push(InboundMessage::new(
                LocalTs::from_micros(at),
                MessageBody::Trade(trade),
            )),
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
    with_spins(market, BINANCE_SPIN_US)
}

/// The recorded Polymarket market frames, routed by the edge's own driver for a window whose two
/// legs are the two tokens of the capture, with a spin each 100 ms.
pub(crate) fn polymarket_tape() -> Vec<InboundMessage> {
    let mut tokens: Vec<Box<str>> = Vec::new();
    let mut frames = Vec::new();
    let lines = POLYMARKET_MARKET.lines().filter(|line| !is_frameless(line));
    for (index, line) in lines.enumerate() {
        let at = POLYMARKET_START_US + index as i64 * POLYMARKET_FRAME_GAP_US;
        let Ok(frame) = parse_market_frame(line, LocalTs::from_micros(at)) else {
            continue;
        };
        note_tokens(&frame, &mut tokens);
        frames.push((at, frame));
    }
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
        LocalTs::from_micros(POLYMARKET_START_US - 1_000),
        WindowAssignment {
            legs: [
                OutcomeLeg {
                    instrument: InstrumentId(0),
                    token: TokenId::from(&*tokens[0]),
                },
                OutcomeLeg {
                    instrument: InstrumentId(1),
                    token: TokenId::from(&*tokens[1]),
                },
            ],
            window_open_ts_us: VenueTs::from_micros(POLYMARKET_WINDOW_OPEN_US),
            window_close_ts_us: VenueTs::from_micros(
                POLYMARKET_WINDOW_OPEN_US + POLYMARKET_WINDOW_US,
            ),
            condition_id: "refold-condition".into(),
        },
        &mut keep,
    );
    for (at, frame) in &frames {
        core.on_frame(LocalTs::from_micros(*at), frame, &mut keep);
    }
    with_spins(market, POLYMARKET_SPIN_US)
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

/// Market messages in receive order, with a spin each time a timer of period `spin_us` would fire
/// before a message, and one spin after the last message.
fn with_spins(market: Vec<InboundMessage>, spin_us: i64) -> Vec<InboundMessage> {
    let first = market
        .first()
        .map_or(0, |message| message.header.received_ts_us.micros());
    let mut tape = Vec::with_capacity(market.len());
    let mut next_spin = first + spin_us;
    let mut seq = 0;
    let spin = |seq: u64, at: i64| {
        InboundMessage::new(
            LocalTs::from_micros(at),
            MessageBody::SpinTick(SpinTick { seq }),
        )
    };
    for message in market {
        let at = message.header.received_ts_us.micros();
        while next_spin <= at {
            tape.push(spin(seq, next_spin));
            seq += 1;
            next_spin += spin_us;
        }
        tape.push(message);
    }
    tape.push(spin(seq, next_spin));
    tape
}
