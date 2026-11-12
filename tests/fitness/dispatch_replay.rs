//! Dispatch replay fitness: a fixed synthetic sequence through the real dispatch loop is
//! deterministic — identical emitted records and callback order across runs, warmup included —
//! and a one-sided book (no microprice) must never fold a stale value into the persisted EwmaVol.

use polysim::config::{Instruments, KlineInterval, NoParams, StrategySpec, TableKind};
use polysim::hot::dispatch::HotEngine;
use polysim::hot::strategy::{Registration, Strategy, StrategyConfig, StrategyCtx};
use polysim::ids::{InstrumentId, Side};
use polysim::log::LogRecord;
use polysim::msg::inbound::{
    BookChunk, InboundMessage, KlineEvent, MarketRotation, SpinTick, TradeEvent,
};
use polysim::msg::persist::{FeatureId, PersistRecord};
use polysim::time::DurationUs;

use rtrb::Consumer;

use crate::engine_support::{
    NOMINAL_SPIN, ONE, book_reset, call_codes, delta_chunk, engine_view, engine_with_ui,
    engine_without_warmup, instrument_row, kline, metrics_ring, persist_ring, pop, rotation,
    snapshot_pair, spin, strategy_log_ring, tracker_spec_all, trade,
};
use crate::raw_recorder::RecorderStrategy;

/// One `call` feature per callback (value = a callback code), so the drained stream encodes the
/// exact dispatch order; also re-emits `ewma_vol` on book updates to fold in its evolution.
#[derive(Default)]
struct Probe {
    call: Option<FeatureId>,
    ewma: Option<FeatureId>,
}

impl Probe {
    fn log(&self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId, code: f64) {
        ctx.emit(self.call.expect("features registered"), instrument, code);
        polysim::strategy_info!(ctx, "probe callback {code} on instrument {}", instrument.0);
    }
}

impl Strategy for Probe {
    fn features(&self) -> &'static [&'static str] {
        &["call", "ewma"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
        self.ewma = registration.features.get(1).copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        self.log(ctx, event.instrument, 1.0);
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        self.log(ctx, chunk.instrument, 2.0);
        if let Some(vol) = ctx.ewma_vol(chunk.instrument) {
            ctx.emit(
                self.ewma.expect("features registered"),
                chunk.instrument,
                vol,
            );
        }
    }

    fn on_book_reset(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        self.log(ctx, instrument, 3.0);
    }

    fn on_market_rotation(&mut self, ctx: &mut StrategyCtx<'_>, rotation: &MarketRotation) {
        self.log(ctx, rotation.instrument, 5.0);
    }

    fn on_kline(&mut self, ctx: &mut StrategyCtx<'_>, event: &KlineEvent) {
        self.log(ctx, event.instrument, 4.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        self.log(ctx, InstrumentId(0), 6.0);
    }
}

/// Emits the resident EWMA beside a flag for whether the tracker held a microprice at all, so a
/// one-sided book is distinguishable from a book that simply produced no new value.
#[derive(Default)]
struct EwmaProbe {
    ewma: Option<FeatureId>,
    microprice_present: Option<FeatureId>,
}

impl Strategy for EwmaProbe {
    fn features(&self) -> &'static [&'static str] {
        &["ewma", "microprice_present"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.ewma = registration.features.first().copied();
        self.microprice_present = registration.features.get(1).copied();
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        let present = f64::from(u8::from(
            ctx.tracker(chunk.instrument).last_microprice().is_some(),
        ));
        ctx.emit(
            self.microprice_present.expect("registered"),
            chunk.instrument,
            present,
        );
        if let Some(vol) = ctx.ewma_vol(chunk.instrument) {
            ctx.emit(self.ewma.expect("registered"), chunk.instrument, vol);
        }
    }
}

fn dispatch_capture(
    engine: &mut HotEngine,
    consumer: &mut Consumer<PersistRecord>,
    message: InboundMessage,
) -> (Option<f64>, Option<f64>) {
    engine.dispatch(pop(0, 0), &message);
    let mut ewma = None;
    let mut microprice_present = None;
    while let Ok(record) = consumer.pop() {
        if let PersistRecord::Feature(row) = record {
            if row.feature == FeatureId(0) {
                ewma = Some(row.value);
            } else if row.feature == FeatureId(1) {
                microprice_present = Some(row.value);
            }
        }
    }
    (ewma, microprice_present)
}

fn probe_sequence() -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(
        0,
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        1,
    );
    vec![
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
        InboundMessage::Trade(trade(0, 100 * ONE, 2_000_000, Side::Buy, 2)),
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 5 * ONE)], 3)),
        InboundMessage::Kline(kline(
            0,
            KlineInterval::OneMinute,
            (100 * ONE, 103 * ONE, 98 * ONE, 101 * ONE),
            true,
            4,
        )),
        InboundMessage::SpinTick(spin(1, 5)),
        InboundMessage::BookReset(book_reset(0, 6)),
        // Slot sequence: rotation, then reset, then fresh snapshot. Rotation stamped at subscribe (7),
        // its window opening later (300s).
        InboundMessage::MarketRotation(rotation(0, 300_000_000, 600_000_000, 7)),
        InboundMessage::BookReset(book_reset(0, 8)),
        InboundMessage::Book(snapshot_pair(0, &[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 9).0),
        InboundMessage::Book(snapshot_pair(0, &[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 9).1),
    ]
}

/// One replay's drained output: persisted feature rows and the banked strategy log records, both a
/// pure function of the input sequence.
struct Replayed {
    persist: Vec<PersistRecord>,
    logs: Vec<LogRecord>,
}

fn run_sequence(sequence: &[InboundMessage], warmup: DurationUs) -> Replayed {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let (persist_sink, mut consumer) = persist_ring(256);
    let (strategy_log_sink, mut log_consumer) = strategy_log_ring(256);
    let (metrics_sink, _metrics_consumer) = metrics_ring(64);
    let (mut engine, _ui_books, _ui_events) = engine_with_ui(
        &instruments,
        Box::new(Probe::default()),
        persist_sink,
        strategy_log_sink,
        metrics_sink,
        warmup,
    );
    for message in sequence {
        engine.dispatch(pop(0, 0), message);
    }
    let mut persist = Vec::new();
    while let Ok(record) = consumer.pop() {
        persist.push(record);
    }
    let mut logs = Vec::new();
    while let Ok(record) = log_consumer.pop() {
        logs.push(record);
    }
    Replayed { persist, logs }
}

#[test]
fn dispatch_replay_is_deterministic() {
    let first = run_sequence(&probe_sequence(), DurationUs::ZERO);
    let second = run_sequence(&probe_sequence(), DurationUs::ZERO);
    assert_eq!(first.persist, second.persist, "identical replay diverged");

    // 2 snapshot chunks, trade, delta, kline, spin, reset, then the rotation slot sequence
    // (rotation, reset, 2 snapshot chunks) — one callback each.
    assert_eq!(
        call_codes(&first.persist),
        vec![2.0, 2.0, 1.0, 2.0, 4.0, 6.0, 3.0, 5.0, 3.0, 2.0, 2.0],
        "state-before-callback dispatch order"
    );

    // The strategy log lane is a pure function of the sequence too: records stamp event time with
    // no wall-clock read, so two runs bank byte-identical `LogRecord`s.
    assert!(
        !first.logs.is_empty(),
        "the probe banked strategy log lines"
    );
    assert_eq!(
        first.logs, second.logs,
        "strategy log lane diverged across replays"
    );
}

const WARMUP_SPAN: DurationUs = DurationUs::from_secs(10);

/// Stamps straddle a 10s warmup: six messages inside it (the last a microsecond short of the
/// boundary), the boundary message itself, then two beyond. Every delta moves the microprice, so
/// the EwmaVol resident can only carry a value past the boundary if warmup kept feeding it.
fn warmup_sequence() -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 0);
    vec![
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
        InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 1_000_000)),
        InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, 3 * ONE)],
            2_000_000,
        )),
        InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, 5 * ONE)],
            5_000_000,
        )),
        InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, 7 * ONE)],
            9_999_999,
        )),
        InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, 9 * ONE)],
            10_000_000,
        )),
        InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 11_000_000)),
        InboundMessage::SpinTick(spin(1, 12_000_000)),
    ]
}

/// `engine.warmup_secs`: warmup is decided by message stamps alone. The suppressed prefix is
/// therefore a property of the sequence, identical on every replay — a wall-clock implementation
/// would suppress all nine messages here, since dispatching them takes microseconds. State keeps
/// advancing underneath, so from the boundary onwards the strategy sees exactly what it would
/// have seen had nothing been suppressed.
#[test]
fn warmup_suppresses_callbacks_by_message_time() {
    let warmed = run_sequence(&warmup_sequence(), WARMUP_SPAN).persist;
    assert_eq!(
        warmed,
        run_sequence(&warmup_sequence(), WARMUP_SPAN).persist,
        "warmed replay diverged"
    );
    assert_eq!(
        call_codes(&warmed),
        vec![2.0, 1.0, 6.0],
        "only the boundary delta, the trade and the spin after it reach the strategy"
    );

    let unsuppressed = run_sequence(&warmup_sequence(), DurationUs::ZERO).persist;
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
}

/// `strategy.instruments` as an explicit list is a config promise: record only those instruments.
/// The recorder must drop events for any instrument whose venue symbol is not listed.
#[test]
fn recorder_records_only_configured_instruments() {
    let mut kept = instrument_row(0, tracker_spec_all(1), 64);
    kept.venue_symbol = "kept".into();
    let mut dropped = instrument_row(1, tracker_spec_all(1), 64);
    dropped.venue_symbol = "dropped".into();
    let instruments = [kept, dropped];

    let spec = StrategySpec {
        instruments: Instruments::Explicit(vec!["kept".into()]),
        tables: vec![TableKind::Trades],
        params: NoParams {},
    };
    let (sink, mut consumer) = persist_ring(256);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, _metrics_consumer) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(RecorderStrategy::from_spec(
            &spec,
            engine_view(NOMINAL_SPIN),
        )),
        sink,
        log_sink,
        metrics_producer,
    );

    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 1)),
    );
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Trade(trade(1, 100 * ONE, ONE, Side::Buy, 2)),
    );

    let mut instruments_recorded = Vec::new();
    while let Ok(PersistRecord::Trade(row)) = consumer.pop() {
        instruments_recorded.push(row.instrument);
    }
    assert_eq!(
        instruments_recorded,
        vec![InstrumentId(0)],
        "only the listed instrument's trade is recorded; the excluded one's is dropped"
    );
}

/// A one-sided book computes no microprice, so it must not fold a stale value into EwmaVol (which
/// would deflate the persisted volatility). On resync the return chain re-seeds, no gap return.
#[test]
fn one_sided_book_does_not_perturb_ewma() {
    let instruments = [instrument_row(0, tracker_spec_all(4), 64)];
    let (sink, mut consumer) = persist_ring(256);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, _metrics_consumer) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(EwmaProbe::default()),
        sink,
        log_sink,
        metrics_producer,
    );

    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 1);
    dispatch_capture(&mut engine, &mut consumer, InboundMessage::Book(bids));
    dispatch_capture(&mut engine, &mut consumer, InboundMessage::Book(asks));
    dispatch_capture(
        &mut engine,
        &mut consumer,
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 3 * ONE)], 2)),
    );
    let (built, _) = dispatch_capture(
        &mut engine,
        &mut consumer,
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 5 * ONE)], 3)),
    );
    let vol_before = built.expect("ewma has a value after two moving two-sided updates");

    engine.dispatch(pop(0, 0), &InboundMessage::BookReset(book_reset(0, 4)));
    while consumer.pop().is_ok() {}

    // Partial snapshot: bids only (book stays awaiting). No microprice, so ewma must not move.
    let (rebid, rask) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 5);
    let (ewma_one_sided, present_one_sided) =
        dispatch_capture(&mut engine, &mut consumer, InboundMessage::Book(rebid));
    assert_eq!(
        ewma_one_sided,
        Some(vol_before),
        "one-sided book must not perturb ewma"
    );
    assert_eq!(
        present_one_sided,
        Some(0.0),
        "latest microprice cleared while awaiting snapshot"
    );

    // Completing the snapshot re-seeds the return chain — variance is kept, no gap return.
    let (ewma_reseed, present_reseed) =
        dispatch_capture(&mut engine, &mut consumer, InboundMessage::Book(rask));
    assert_eq!(
        ewma_reseed,
        Some(vol_before),
        "resync must not span a gap return"
    );
    assert_eq!(
        present_reseed,
        Some(1.0),
        "microprice present once two-sided"
    );

    let (ewma_resumed, _) = dispatch_capture(
        &mut engine,
        &mut consumer,
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 9 * ONE)], 6)),
    );
    assert_ne!(
        ewma_resumed,
        Some(vol_before),
        "ewma resumes after the snapshot completes"
    );

    // Mid-session: a qty-0 delta empties the asks with NO reset. The latest slot keeps the
    // stale microprice (only a reset clears it), so the per-event freshness gate is the sole
    // protection here — a reintroduced latest-based guard would fail these asserts.
    let vol_resumed = ewma_resumed.expect("ewma live after resumption");
    let (ewma_emptied, present_emptied) = dispatch_capture(
        &mut engine,
        &mut consumer,
        InboundMessage::Book(delta_chunk(0, Side::Sell, &[(101 * ONE, 0)], 7)),
    );
    assert_eq!(
        ewma_emptied,
        Some(vol_resumed),
        "emptying one side mid-session must not perturb ewma"
    );
    assert_eq!(
        present_emptied,
        Some(1.0),
        "latest stays stale-Some mid-session — the case the freshness gate exists for"
    );
    let (ewma_one_sided_delta, _) = dispatch_capture(
        &mut engine,
        &mut consumer,
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 4 * ONE)], 8)),
    );
    assert_eq!(
        ewma_one_sided_delta,
        Some(vol_resumed),
        "bid updates on a one-sided book must not perturb ewma"
    );
    let (ewma_two_sided_again, _) = dispatch_capture(
        &mut engine,
        &mut consumer,
        InboundMessage::Book(delta_chunk(0, Side::Sell, &[(102 * ONE, 2 * ONE)], 9)),
    );
    assert_ne!(
        ewma_two_sided_again,
        Some(vol_resumed),
        "ewma resumes once the book is two-sided again"
    );
}
