//! A market's warmup holds the callbacks of that market by message time, never by the clock, so a
//! replay calls back at the same points. Only callbacks wait: state, the tracker, the ledger and the
//! feed of the workstation fold from the first message. A market that starts late waits for its own
//! span, and no order of it goes out before.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, QuoteLevel, RejectReason};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Registration, Strategy, StrategyCtx, StrategyGate};
use engine::msg::exec::{ExecCommand, ExecLaneItem, OrderStyle};
use engine::msg::ids::{AssetId, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{BookChunk, InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::time::{DurationUs, Stamper};
use engine::msg::ui::UiEvent;
use engine::runtime::registry::InstrumentRow;
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, Idle, ONE, account_chunk, call_codes, delta_chunk, detached_setup, engine_with_ui,
    instrument_row, local_refusals, persist_ring, rotation, snapshot_pair, spin, strategy_log_ring,
    stream_ready, tracker_spec_all, trade, ts, ui_event_ring, venue_ts,
};
use crate::exec_support::{QUOTE_QTY, drain_commands, exec_settings};

const WARMUP_SPAN: DurationUs = DurationUs::from_secs(10);

/// One `call` row for each callback, its value a code of the callback: 1 trade, 2 book, 6 spin.
/// Each book update also writes the EWMA volatility, and each trade the position of market 0.
#[derive(Default)]
struct Probe {
    call: Option<FeatureId>,
    ewma: Option<FeatureId>,
    position: Option<FeatureId>,
}

impl Strategy for Probe {
    fn features(&self) -> &'static [&'static str] {
        &["call", "ewma", "position"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
        self.ewma = registration.features.get(1).copied();
        self.position = registration.features.get(2).copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        ctx.emit(self.call.expect("registered"), event.instrument, 1.0);
        let position = ctx.position_base(event.instrument).mantissa() as f64;
        ctx.emit(
            self.position.expect("registered"),
            event.instrument,
            position,
        );
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        ctx.emit(self.call.expect("registered"), chunk.instrument, 2.0);
        if let Some(vol) = ctx.ewma_vol(chunk.instrument) {
            ctx.emit(self.ewma.expect("registered"), chunk.instrument, vol);
        }
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.emit(self.call.expect("registered"), InstrumentId(0), 6.0);
    }
}

fn rows(consumer: &mut Consumer<PersistRecord>) -> Vec<PersistRecord> {
    std::iter::from_fn(|| consumer.pop().ok()).collect()
}

fn engine(warmup: DurationUs) -> (HotEngine, Consumer<PersistRecord>, Consumer<UiEvent>) {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let (persistence, persist) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(256);
    let (engine, _books, events) = engine_with_ui(
        &instruments,
        Box::new(Probe::default()),
        persistence,
        log_sink,
        warmup,
    );
    (engine, persist, events)
}

/// A fresh engine with a strategy that does nothing and a 10 s warmup, and its event lane.
fn idle_engine() -> (HotEngine, Consumer<UiEvent>) {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let (persistence, _persist) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(64);
    let (engine, _books, events) = engine_with_ui(
        &instruments,
        Box::new(Idle),
        persistence,
        log_sink,
        WARMUP_SPAN,
    );
    (engine, events)
}

fn fold(sequence: &[InboundMessage], warmup: DurationUs) -> Vec<PersistRecord> {
    let (mut engine, mut persist, _events) = engine(warmup);
    for message in sequence {
        engine.dispatch(message);
    }
    rows(&mut persist)
}

fn book_delta(qty_units: i64, when: i64) -> InboundMessage {
    let chunk = delta_chunk(0, Side::Buy, &[(100 * ONE, qty_units * ONE)], when);
    InboundMessage::new(ts(when), MessageBody::Book(chunk))
}

fn print(qty: i64, side: Side, when: i64) -> InboundMessage {
    let event = trade(0, 100 * ONE, qty, side, when);
    InboundMessage::new(ts(when), MessageBody::Trade(event))
}

/// Six messages inside a 10 s warmup that opens at 0, the last a microsecond short of the border,
/// the message at the border, then two after it. Each delta moves the microprice, so the EWMA can
/// carry a value past the border only if the warmup kept feeding it.
fn warmup_sequence() -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 0);
    vec![
        InboundMessage::new(ts(0), MessageBody::Book(bids)),
        InboundMessage::new(ts(0), MessageBody::Book(asks)),
        print(ONE, Side::Buy, 1_000_000),
        book_delta(3, 2_000_000),
        book_delta(5, 5_000_000),
        book_delta(7, 9_999_999),
        book_delta(9, 10_000_000),
        print(ONE, Side::Buy, 11_000_000),
        spin(1, 12_000_000),
    ]
}

/// FITNESS: the warmup holds callbacks by message time. With a span of 10 s from the first message
/// at 0, no callback runs before 10,000,000 and each runs from there on, while the state folds the
/// whole time: the EWMA, the ledger, and the prints and rotations on the feed of the workstation.
#[test]
fn warmup_holds_callbacks_by_message_time() {
    let warmed = fold(&warmup_sequence(), WARMUP_SPAN);
    assert_eq!(
        warmed,
        fold(&warmup_sequence(), WARMUP_SPAN),
        "warmed replay diverged"
    );
    assert_eq!(
        call_codes(&warmed),
        vec![2.0, 1.0, 6.0],
        "only the boundary delta, the trade and the spin after it reach the strategy"
    );
    let unsuppressed = fold(&warmup_sequence(), DurationUs::ZERO);
    assert_eq!(
        call_codes(&unsuppressed),
        vec![2.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0, 1.0, 6.0],
        "without warmup every message calls back"
    );
    assert_eq!(
        warmed.as_slice(),
        &unsuppressed[unsuppressed.len() - warmed.len()..],
        "state advanced identically under suppression — same records, values included"
    );
    let ewma = warmed.iter().find_map(|record| match record {
        PersistRecord::Feature(row) if row.feature == FeatureId(1) => Some(row.value),
        _ => None,
    });
    assert!(
        ewma.is_some_and(|value| value > 0.0),
        "the first delivered book update carries an EwmaVol warmed entirely by suppressed messages, got {ewma:?}"
    );

    // A fill inside the warmup moves the ledger, and the first trade callback after the border
    // reads that position.
    let mut pen = FillPen::new(0);
    let mut filled = warmup_sequence();
    filled.splice(3..3, pen.fill(Side::Buy, 100 * ONE, ONE, 1_500_000));
    let positions: Vec<f64> = fold(&filled, WARMUP_SPAN)
        .iter()
        .filter_map(|record| match record {
            PersistRecord::Feature(row) if row.feature == FeatureId(2) => Some(row.value),
            _ => None,
        })
        .collect();
    assert_eq!(
        positions,
        vec![ONE as f64],
        "a fill inside the warmup moved the ledger before any callback could see it"
    );

    // A print and a rotation inside the warmup reach the event lane with their values.
    let (mut engine, mut events) = idle_engine();
    engine.dispatch(&print(3 * ONE, Side::Sell, 1_000_000));
    let teed: Vec<UiEvent> = std::iter::from_fn(|| events.pop().ok()).collect();
    assert_eq!(
        teed,
        vec![UiEvent::Trade {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: venue_ts(1_000_000),
            aggressor: Side::Sell,
            price: Price::from_mantissa(100 * ONE),
            qty: Qty::from_mantissa(3 * ONE),
        }],
        "a print inside the warmup tees one Trade event with its values"
    );
    let (mut engine, mut events) = idle_engine();
    engine.dispatch(&rotation(0, 300 * ONE, 600 * ONE, 30));
    let teed: Vec<UiEvent> = std::iter::from_fn(|| events.pop().ok()).collect();
    assert_eq!(
        teed,
        vec![UiEvent::Rotation {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: venue_ts(30),
        }],
        "a rotation inside the warmup tees one Rotation event with its values"
    );
}

const LATE_SPAN: DurationUs = DurationUs::from_secs(10);
const MARKET_0: InstrumentId = InstrumentId(0);
const MARKET_1: InstrumentId = InstrumentId(1);
const MARKETS: [InstrumentId; 2] = [MARKET_0, MARKET_1];

/// The gate of each market as a callback read it through its context, with the stamp of the
/// message under fold.
type GateReads = Arc<Mutex<Vec<(i64, [StrategyGate; 2])>>>;

/// One `call` row for each callback of a market, its value a code (1 trade, 2 book) and its
/// instrument the market. Each spin writes one `spin` row and declares a bid at the touch of each
/// market. Each callback reads the gate of each market.
#[derive(Default)]
struct LateProbe {
    call: Option<FeatureId>,
    spin: Option<FeatureId>,
    reads: GateReads,
}

impl LateProbe {
    fn read_gates(&self, ctx: &StrategyCtx<'_>) {
        let gates = MARKETS.map(|market| ctx.gate(market));
        let mut reads = self.reads.lock().expect("one thread folds");
        reads.push((ctx.event_ts().micros(), gates));
    }
}

impl Strategy for LateProbe {
    fn features(&self) -> &'static [&'static str] {
        &["call", "spin"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        [self.call, self.spin] = registration.feature_ids_of(&["call", "spin"]).map(Some);
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        self.read_gates(ctx);
        ctx.emit(self.call.expect("registered"), event.instrument, 1.0);
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        self.read_gates(ctx);
        ctx.emit(self.call.expect("registered"), chunk.instrument, 2.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        self.read_gates(ctx);
        ctx.emit(self.spin.expect("registered"), MARKET_0, 6.0);
        for market in MARKETS {
            let Some(bid) = ctx.book(market).best_bid() else {
                continue;
            };
            let quote = DesiredQuote {
                price: bid.price,
                qty: QUOTE_QTY,
                style: OrderStyle::PostOnly,
            };
            ctx.quote(market, Side::Buy, QuoteLevel::ZERO, Some(quote));
        }
    }
}

/// Two markets with a 10 s warmup and a venue that answers nothing, ready to quote: the account
/// stream is up, balances are known and the open orders have landed, all at 0.
struct LateEngine {
    engine: HotEngine,
    persist: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    ui_events: Consumer<UiEvent>,
    reads: GateReads,
}

fn late_engine() -> LateEngine {
    let rows: Vec<InstrumentRow> = MARKETS
        .iter()
        .map(|market| InstrumentRow {
            venue_symbol: format!("btcusdt-{}", market.0).into(),
            ..instrument_row(market.0, tracker_spec_all(1), 64)
        })
        .collect();
    let (persistence, persist) = persist_ring(256);
    let (ui_event_sink, ui_events) = ui_event_ring(1_024);
    let (producer, commands) = RingBuffer::<ExecLaneItem>::new(256);
    let reads = GateReads::default();
    let probe = LateProbe {
        reads: Arc::clone(&reads),
        ..LateProbe::default()
    };
    let mut engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink,
        warmup: LATE_SPAN,
        exec: Some(ExecWiring {
            sink: ExecSink::new(producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }),
        ..detached_setup(&rows, Box::new(probe))
    });
    engine.dispatch(&stream_ready(MARKET_0, 0));
    engine.dispatch(&InboundMessage::new(
        ts(0),
        MessageBody::Account(account_chunk(
            &[(AssetId(0), 10 * ONE), (AssetId(1), 1_000_000 * ONE)],
            0,
        )),
    ));
    engine.dispatch(&InboundMessage::new(
        ts(0),
        MessageBody::Exec(open_orders_snapshot_end(MARKET_0, ts(0))),
    ));
    LateEngine {
        engine,
        persist,
        commands,
        ui_events,
        reads,
    }
}

fn market_book(market: InstrumentId, when: i64) -> [InboundMessage; 2] {
    let (bids, asks) = snapshot_pair(market.0, &[(99 * ONE, ONE)], &[(101 * ONE, ONE)], when);
    [
        InboundMessage::new(ts(when), MessageBody::Book(bids)),
        InboundMessage::new(ts(when), MessageBody::Book(asks)),
    ]
}

fn market_delta(market: InstrumentId, when: i64) -> InboundMessage {
    let chunk = delta_chunk(market.0, Side::Buy, &[(99 * ONE, 2 * ONE)], when);
    InboundMessage::new(ts(when), MessageBody::Book(chunk))
}

/// The markets that each `Place` names, in order.
fn placed_markets(commands: &mut Consumer<ExecLaneItem>) -> Vec<InstrumentId> {
    drain_commands(commands)
        .into_iter()
        .filter_map(|command| match command {
            ExecCommand::Place { instrument, .. } => Some(instrument),
            _ => None,
        })
        .collect()
}

/// The gates that the callbacks of the message at `when` read.
fn gates_read_at(reads: &GateReads, when: i64) -> Vec<[StrategyGate; 2]> {
    let reads = reads.lock().expect("the fold is done");
    reads
        .iter()
        .filter(|(stamp, _)| *stamp == when)
        .map(|(_, gates)| *gates)
        .collect()
}

fn is_warming_since(gate: StrategyGate, when: i64) -> bool {
    matches!(gate, StrategyGate::Warming { since: Some(since) } if since.as_local() == ts(when))
}

/// The callbacks of one market, each as its code and the stamp of its message.
fn market_calls(records: &[PersistRecord], market: InstrumentId) -> Vec<(f64, i64)> {
    records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::Feature(row)
                if row.feature == FeatureId(0) && row.instrument == market =>
            {
                Some((row.value, row.event_ts_us.micros()))
            }
            _ => None,
        })
        .collect()
}

/// FITNESS: a market that starts late waits for its own span, and no order of it goes out before.
/// With a span of 10 s, market 0 first speaks at 0 and market 1 at 60 s. Market 0 calls back from
/// 10 s on. At the spin of 65 s the strategy runs, because market 0 is live, and declares a bid on
/// each market: the engine places the bid of market 0 and holds the bid of market 1, which warms.
/// The first message of market 1 at or past 70 s makes it live, and the next spin places its bid.
/// The context gives the gate of each market, and the held bid counts under its own refusal.
#[test]
fn late_market_waits_for_its_own_span() {
    let mut late = late_engine();
    let mut fold = |messages: &[InboundMessage]| {
        for message in messages {
            late.engine.dispatch(message);
        }
    };
    fold(&market_book(MARKET_0, 0));
    fold(&[market_delta(MARKET_0, 10_000_000)]);
    fold(&market_book(MARKET_1, 60_000_000));
    fold(&[InboundMessage::new(
        ts(62_000_000),
        MessageBody::Trade(trade(0, 99 * ONE, ONE, Side::Sell, 62_000_000)),
    )]);
    assert!(
        drain_commands(&mut late.commands).is_empty(),
        "no command goes out before the first spin"
    );
    local_refusals(&mut late.ui_events);

    late.engine.dispatch(&spin(1, 65_000_000));
    assert_eq!(
        placed_markets(&mut late.commands),
        vec![MARKET_0],
        "at 65 s only the live market places its bid: market 1 warms until 70 s"
    );
    let held = local_refusals(&mut late.ui_events)
        .into_iter()
        .filter(|reason| *reason == RejectReason::Warming)
        .count();
    assert_eq!(held, 1, "the bid of market 1 is held for its warmup");

    late.engine.dispatch(&market_delta(MARKET_1, 70_000_000));
    late.engine.dispatch(&spin(2, 71_000_000));
    assert_eq!(
        placed_markets(&mut late.commands),
        vec![MARKET_1],
        "the spin after market 1 turns live places its bid, and market 0 still waits for its answer"
    );

    assert_eq!(
        gates_read_at(&late.reads, 10_000_000),
        vec![[StrategyGate::Live, StrategyGate::Warming { since: None }]],
        "market 0 is live at the border of its span, and market 1 has sent nothing yet"
    );
    let at_spin = gates_read_at(&late.reads, 65_000_000);
    assert!(
        at_spin.len() == 1
            && at_spin[0][0] == StrategyGate::Live
            && is_warming_since(at_spin[0][1], 60_000_000),
        "the spin of 65 s reads market 0 live and market 1 warming since 60 s, got {at_spin:?}"
    );
    assert_eq!(
        gates_read_at(&late.reads, 71_000_000),
        vec![[StrategyGate::Live, StrategyGate::Live]],
        "the spin after 70 s reads both markets live"
    );

    let records: Vec<PersistRecord> = std::iter::from_fn(|| late.persist.pop().ok()).collect();
    assert_eq!(
        market_calls(&records, MARKET_0),
        vec![(2.0, 10_000_000), (1.0, 62_000_000)],
        "market 0 calls back from the border of its own span on"
    );
    assert_eq!(
        market_calls(&records, MARKET_1),
        vec![(2.0, 70_000_000)],
        "market 1 calls back only from 70 s, ten seconds after its own first message"
    );
}
