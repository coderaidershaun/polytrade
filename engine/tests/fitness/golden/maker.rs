//! The shipped Polymarket maker over the recorded market capture and over the cases of its own
//! tests: the calendar of two windows, a held position, and a holding below the venue minimum.
//! The tapes are copies that no other test shares, so a change of another test's fixture cannot
//! move the golden file.

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{ExecSettings, FeeModel};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::StrategyConfig;
use engine::msg::exec::ExecLaneItem;
use engine::msg::ids::{AssetId, FIXED_SCALE, InstrumentId, Notional, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, Stamper};
use engine::runtime::config::{
    Instruments, IntensitySpec, PolySeries, StrategySpec, Subscriptions, TrackerSpec, VenueMarket,
};
use engine::runtime::registry::InstrumentRow;
use engine::venues::exec::open_orders_snapshot_end;
use engine::venues::polymarket::actor::{DriverEffect, PolyDriverCore, SlotLegs};
use engine::venues::polymarket::discovery::PolySchedule;
use engine::venues::polymarket::parse::{PolyFrame, is_frameless, parse_market_frame};
use engine::venues::polymarket::rotation::{OutcomeLeg, TokenId, WindowAssignment};
use rtrb::RingBuffer;

use super::record::{DeclaredQuotes, Recording, with_spins};
use crate::engine_support::{
    FillPen, account_chunk, detached_setup, engine_view, instrument_row, persist_ring, rotation,
    snapshot_pair, spin, stream_ready, ts, ui_event_ring, venue_ts,
};
use crate::exec_support::exec_settings;
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher};

const MARKET: &str = include_str!("../../../fixtures/polymarket/poly_market.jsonl");

const LEGS: [&str; 4] = ["a-up", "a-down", "b-up", "b-down"];
const A_UP: u16 = 0;
const TICK: i64 = FIXED_SCALE / 100;
const WINDOW: i64 = 300_000_000;
const SPIN_US: i64 = 250_000;

/// The shipped quote stop lead, so the cases spin on each side of the instant quoting stops.
const QUOTE_STOP_LEAD_US: i64 = 3_500_000;

/// The cases: a grid boundary far from zero, and the touch and depth of each slot.
const FIRST_OPEN: i64 = 1_000_000_000_000;
const QUOTES: [(i64, i64); 4] = [(40, 42), (58, 60), (30, 32), (68, 70)];
const DEPTHS: [(i64, i64); 4] = [(11, 12), (21, 22), (31, 32), (41, 42)];

/// A holding that is not a whole number of orders, and one below the venue minimum of 5 shares.
const HELD_SHARES: i64 = 65 * FIXED_SCALE / 10;
const RESIDUE_SHARES: i64 = 3 * FIXED_SCALE;

/// The recorded capture: inside the window it was recorded in, 20 ms between its frames.
const RECORDED_START_US: i64 = 1_784_449_240_000_000;
const RECORDED_FRAME_GAP_US: i64 = 20_000;
const RECORDED_WINDOW_OPEN_US: i64 = 1_784_449_200_000_000;

pub(super) fn declared_quotes() -> DeclaredQuotes {
    let mut quotes = DeclaredQuotes::default();
    maker("recorded").play_all(&recorded_tape(), &mut quotes);
    maker("calendar").play_all(&calendar_tape(), &mut quotes);
    maker("held").play_all(&held_tape(HELD_SHARES), &mut quotes);
    maker("residue").play_all(&held_tape(RESIDUE_SHARES), &mut quotes);
    quotes
}

/// The maker with its shipped params, quoting switched on, and an execution lane.
fn maker(tape: &'static str) -> Recording {
    let rows = rows();
    let strategy = PolyUpPublisher::from_spec(
        &StrategySpec {
            instruments: Instruments::All,
            tables: Vec::new(),
            params: PolyUpParams {
                enabled: true,
                ..PolyUpParams::default()
            },
        },
        engine_view(DurationUs::from_micros(SPIN_US)),
    );
    let (persistence, persist) = persist_ring(16_384);
    let (ui_event_sink, events) = ui_event_ring(16_384);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(1_024);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink,
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: settings(),
            run_nonce: 0,
        }],
        ..detached_setup(&rows, Box::new(strategy))
    });
    Recording {
        tape,
        engine,
        events,
        persist,
        commands,
    }
}

fn settings() -> ExecSettings {
    ExecSettings {
        quote_stop_margin: DurationUs::from_millis(3_000),
        flatten_slack_ticks: 2,
        fee_model: FeeModel::BinaryOutcome,
        holds_reservations_until_settled: false,
        ..exec_settings()
    }
}

/// The four slots of the series, with the grid that the Polymarket preflight stamps.
fn rows() -> Vec<InstrumentRow> {
    LEGS.iter()
        .enumerate()
        .map(|(index, leg)| InstrumentRow {
            market: VenueMarket::Polymarket(PolySeries::BtcUpDown5m),
            venue_symbol: format!("btc-updown-5m-{leg}").into(),
            display: format!("btc-updown-5m-{leg}").into(),
            base: "BTC".into(),
            quote: "USDC".into(),
            tick_size: Some(Price::from_mantissa(TICK)),
            lot_size: Some(Qty::from_mantissa(FIXED_SCALE / 100)),
            min_qty: Some(Qty::from_mantissa(5 * FIXED_SCALE)),
            max_price: Some(Price::from_mantissa(FIXED_SCALE - TICK)),
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

/// Stream up, open orders known, balances known: without all three nothing is placed.
fn readiness(when: i64) -> [InboundMessage; 3] {
    [
        stream_ready(InstrumentId(A_UP), when),
        InboundMessage::new(
            ts(when),
            MessageBody::Exec(open_orders_snapshot_end(InstrumentId(A_UP), ts(when))),
        ),
        InboundMessage::new(
            ts(when),
            MessageBody::Account(account_chunk(
                &[
                    (AssetId(0), 1_000 * FIXED_SCALE),
                    (AssetId(1), 1_000 * FIXED_SCALE),
                ],
                when,
            )),
        ),
    ]
}

/// A tape of the cases, which spins at the instants it names, each spin with the next number.
struct Case {
    messages: Vec<InboundMessage>,
    spin_seq: u64,
}

impl Case {
    /// Slot a hosts the first window and slot b the second, each slot is priced, and the maker is
    /// ready. Rotations come before the books, because a rotation resets the state of a slot.
    fn armed() -> Self {
        let mut messages = Vec::new();
        for (slot, open) in [
            (0, FIRST_OPEN),
            (1, FIRST_OPEN),
            (2, FIRST_OPEN + WINDOW),
            (3, FIRST_OPEN + WINDOW),
        ] {
            messages.push(rotation(slot, open, open + WINDOW, FIRST_OPEN - 1_000));
        }
        for (slot, (&(bid, ask), &(bid_qty, ask_qty))) in QUOTES.iter().zip(&DEPTHS).enumerate() {
            let (bids, asks) = snapshot_pair(
                slot as u16,
                &[(bid * TICK, bid_qty * FIXED_SCALE)],
                &[(ask * TICK, ask_qty * FIXED_SCALE)],
                FIRST_OPEN - 500,
            );
            messages.push(InboundMessage::new(
                ts(FIRST_OPEN - 500),
                MessageBody::Book(bids),
            ));
            messages.push(InboundMessage::new(
                ts(FIRST_OPEN - 500),
                MessageBody::Book(asks),
            ));
        }
        messages.extend(readiness(FIRST_OPEN - 400));
        Self {
            messages,
            spin_seq: 0,
        }
    }

    fn spin_at(&mut self, instants: &[i64]) {
        for &now in instants {
            self.spin_seq += 1;
            self.messages.push(spin(self.spin_seq, now));
        }
    }
}

/// Before the first open, at it, on each side of the stop of each window, at the shared instant
/// where the first window closes and the second opens, and after the second window closes.
fn calendar_tape() -> Vec<InboundMessage> {
    let first_close = FIRST_OPEN + WINDOW;
    let second_close = first_close + WINDOW;
    let mut case = Case::armed();
    case.spin_at(&[
        FIRST_OPEN - 1,
        FIRST_OPEN,
        FIRST_OPEN + 1,
        FIRST_OPEN + WINDOW / 2,
        first_close - QUOTE_STOP_LEAD_US - 1,
        first_close - QUOTE_STOP_LEAD_US,
        first_close - 1,
        first_close,
        first_close + 1,
        second_close - QUOTE_STOP_LEAD_US - 1,
        second_close - QUOTE_STOP_LEAD_US,
        second_close + 1,
    ]);
    case.messages
}

/// A long of `shares` on the up leg of the open window, opened through the fill path, then spins
/// before and after the stop.
fn held_tape(shares: i64) -> Vec<InboundMessage> {
    let close = FIRST_OPEN + WINDOW;
    let mut case = Case::armed();
    let mut pen = FillPen::new(A_UP);
    let (bid, _) = QUOTES[usize::from(A_UP)];
    case.messages
        .extend(pen.fill(Side::Buy, bid * TICK, shares, FIRST_OPEN + 1));
    case.spin_at(&[
        FIRST_OPEN + 2,
        close - QUOTE_STOP_LEAD_US - 1,
        close - QUOTE_STOP_LEAD_US,
        close - QUOTE_STOP_LEAD_US + 100_000,
    ]);
    case.messages
}

/// The recorded Polymarket market frames, routed by the edge's own driver for a window whose two
/// legs are the two tokens of the capture.
fn recorded_tape() -> Vec<InboundMessage> {
    let mut tokens: Vec<Box<str>> = Vec::new();
    let mut frames = Vec::new();
    let lines = MARKET.lines().filter(|line| !is_frameless(line));
    for (index, line) in lines.enumerate() {
        let at = RECORDED_START_US + index as i64 * RECORDED_FRAME_GAP_US;
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
    let first = RECORDED_START_US - 1_000;
    let mut market: Vec<InboundMessage> = readiness(first).into();
    let mut keep = |effect: DriverEffect| {
        if let DriverEffect::Emit(message) = effect {
            market.push(message);
        }
    };
    core.on_window_resolved(
        ts(first),
        WindowAssignment {
            legs: [
                OutcomeLeg {
                    instrument: InstrumentId(0),
                    token: TokenId::from(&**up),
                },
                OutcomeLeg {
                    instrument: InstrumentId(1),
                    token: TokenId::from(&**down),
                },
            ],
            window_open_ts_us: venue_ts(RECORDED_WINDOW_OPEN_US),
            window_close_ts_us: venue_ts(RECORDED_WINDOW_OPEN_US + WINDOW),
            condition_id: "golden-condition".into(),
        },
        &mut keep,
    );
    for (at, frame) in &frames {
        core.on_frame(ts(*at), frame, &mut keep);
    }
    with_spins(market, SPIN_US)
}

/// The tokens of the capture, in the order they first appear.
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
