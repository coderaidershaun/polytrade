//! A market's warmup holds the callbacks of that market by message time, never by the clock, so a
//! replay calls back at the same points. Only callbacks wait: state, the tracker, the ledger and the
//! feed of the workstation fold from the first message.

use engine::hot::dispatch::HotEngine;
use engine::hot::strategy::{Registration, Strategy, StrategyCtx};
use engine::msg::ids::{InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{BookChunk, InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::time::DurationUs;
use engine::msg::ui::UiEvent;
use rtrb::Consumer;

use crate::engine_support::{
    FillPen, Idle, ONE, call_codes, delta_chunk, engine_with_ui, instrument_row, persist_ring,
    rotation, snapshot_pair, spin, strategy_log_ring, tracker_spec_all, trade, ts, venue_ts,
};

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
