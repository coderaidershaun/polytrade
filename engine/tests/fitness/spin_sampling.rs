//! Spin-sampling fitness: the time clock samples committed state (`latest`), never the live book
//! (a tick can pop mid-update). Holds through one-sided stretches, skips after a reset until commit.
//! The per-side trade counts are per-tick aggregates, so they must zero at every sample point.
//! The last case carries the other half of the contract — a strategy derives its windows from the
//! configured spin, so they must track a change to it.

use polysim::config::{SpinField, TableKind};
use polysim::hot::dispatch::HotEngine;
use polysim::hot::quant::micro;
use polysim::hot::series::FastQueue;
use polysim::hot::strategy::{Registration, Strategy, StrategyConfig, StrategyCtx};
use polysim::ids::{InstrumentId, Price, Side};
use polysim::log::LogRecord;
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::msg::persist::{FeatureId, PersistRecord};
use polysim::time::DurationUs;
use rtrb::Consumer;

use crate::engine_support::{
    ONE, book_reset, delta_chunk, engine_view, engine_without_warmup, instrument_row,
    last_snapshot_chunk, level, metrics_ring, partial_delta_chunk, partial_snapshot_chunk,
    persist_ring, pop, recorder_feature_id, recorder_spec, snapshot_pair, spin, strategy_log_ring,
    tracker_spec_all, trade,
};
use crate::micro_strategy::MicroRecorder;

#[derive(Default)]
struct SpinProbe {
    len: Option<FeatureId>,
    last: Option<FeatureId>,
}

impl Strategy for SpinProbe {
    fn features(&self) -> &'static [&'static str] {
        &["spin_len", "spin_last"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.len = registration.features.first().copied();
        self.last = registration.features.get(1).copied();
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let series = ctx
            .tracker(InstrumentId(0))
            .spin_sampled(SpinField::Microprice);
        let count = series.map_or(0, |queue| queue.len());
        let value = series.and_then(|queue| queue.last());
        ctx.emit(self.len.expect("registered"), InstrumentId(0), count as f64);
        if let Some(value) = value {
            ctx.emit(self.last.expect("registered"), InstrumentId(0), value);
        }
    }
}

fn spin_state(
    engine: &mut HotEngine,
    consumer: &mut Consumer<PersistRecord>,
    tick: SpinTick,
) -> (usize, Option<f64>) {
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(tick));
    let mut len = 0;
    let mut last = None;
    while let Ok(record) = consumer.pop() {
        if let PersistRecord::Feature(row) = record {
            if row.feature == FeatureId(0) {
                len = row.value as usize;
            } else if row.feature == FeatureId(1) {
                last = Some(row.value);
            }
        }
    }
    (len, last)
}

fn apply(engine: &mut HotEngine, consumer: &mut Consumer<PersistRecord>, message: InboundMessage) {
    engine.dispatch(pop(0, 0), &message);
    while consumer.pop().is_ok() {}
}

/// Reads the newest sample of the three fields the microstructure recorder needs beyond the
/// original vocabulary — the plain mid and the trade counts split by side — plus the mid series
/// length, the only way to tell a fresh sample from a held-over one.
struct SidedProbe {
    mid: Option<FeatureId>,
    mid_len: Option<FeatureId>,
    buys: Option<FeatureId>,
    sells: Option<FeatureId>,
}

impl Strategy for SidedProbe {
    fn features(&self) -> &'static [&'static str] {
        &["mid", "mid_len", "buys", "sells"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.mid = registration.features.first().copied();
        self.mid_len = registration.features.get(1).copied();
        self.buys = registration.features.get(2).copied();
        self.sells = registration.features.get(3).copied();
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let tracker = ctx.tracker(InstrumentId(0));
        let mid_series = tracker.spin_sampled(SpinField::Mid);
        let samples = [
            (self.mid, mid_series.and_then(FastQueue::last)),
            (self.mid_len, mid_series.map(|queue| queue.len() as f64)),
            (
                self.buys,
                tracker
                    .spin_sampled(SpinField::BuyTradeCount)
                    .and_then(FastQueue::last),
            ),
            (
                self.sells,
                tracker
                    .spin_sampled(SpinField::SellTradeCount)
                    .and_then(FastQueue::last),
            ),
        ];
        for (feature, value) in samples {
            if let Some(value) = value {
                ctx.emit(feature.expect("registered"), InstrumentId(0), value);
            }
        }
    }
}

fn sided_state(
    engine: &mut HotEngine,
    consumer: &mut Consumer<PersistRecord>,
    tick: SpinTick,
) -> [Option<f64>; 4] {
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(tick));
    let mut sampled = [None; 4];
    while let Ok(record) = consumer.pop() {
        if let PersistRecord::Feature(row) = record {
            sampled[usize::from(row.feature.0)] = Some(row.value);
        }
    }
    sampled
}

/// `mid` is the plain average of the committed best bid/ask (`None` while one-sided, like the
/// other book fields), and the sided counts are since-last-sample aggregates: they carry the
/// trades of the tick just ended and then zero, so a quiet tick reads 0 rather than repeating.
#[test]
fn mid_and_sided_trade_counts_sample_per_tick() {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let (sink, mut consumer) = persist_ring(256);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, _metrics_consumer) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(SidedProbe {
            mid: None,
            mid_len: None,
            buys: None,
            sells: None,
        }),
        sink,
        log_sink,
        metrics_producer,
    );

    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 1);
    apply(&mut engine, &mut consumer, InboundMessage::Book(bids));
    apply(&mut engine, &mut consumer, InboundMessage::Book(asks));
    for (index, side) in [Side::Buy, Side::Sell, Side::Buy, Side::Buy]
        .into_iter()
        .enumerate()
    {
        let when = 2 + index as i64;
        apply(
            &mut engine,
            &mut consumer,
            InboundMessage::Trade(trade(0, 100 * ONE, ONE, side, when)),
        );
    }

    let expected_mid = micro::mid(Price(100 * ONE), Price(101 * ONE));
    assert_eq!(
        sided_state(&mut engine, &mut consumer, spin(1, 10)),
        [Some(expected_mid), Some(1.0), Some(3.0), Some(1.0)],
        "mid averages the committed touch; the counts split three buys from one sell"
    );

    assert_eq!(
        sided_state(&mut engine, &mut consumer, spin(2, 11)),
        [Some(expected_mid), Some(2.0), Some(0.0), Some(0.0)],
        "a tick with no trades samples zero counts, not the previous tick's"
    );

    // After a reset there is no committed touch to average, so mid must stop sampling (its length
    // freezes) exactly like best_bid/best_ask, while the trade counts keep landing — trades have
    // no reset concept.
    apply(
        &mut engine,
        &mut consumer,
        InboundMessage::BookReset(book_reset(0, 12)),
    );
    apply(
        &mut engine,
        &mut consumer,
        InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Sell, 13)),
    );
    assert_eq!(
        sided_state(&mut engine, &mut consumer, spin(3, 14)),
        [Some(expected_mid), Some(2.0), Some(0.0), Some(1.0)],
        "mid stops sampling after a reset; the sided counts still land"
    );
}

#[test]
fn spin_samples_committed_state_only() {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let (sink, mut consumer) = persist_ring(256);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, _metrics_consumer) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(SpinProbe::default()),
        sink,
        log_sink,
        metrics_producer,
    );

    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 1);
    apply(&mut engine, &mut consumer, InboundMessage::Book(bids));
    apply(&mut engine, &mut consumer, InboundMessage::Book(asks));
    let committed = micro::microprice(level(100 * ONE, 2 * ONE), level(101 * ONE, ONE));

    let (len, last) = spin_state(&mut engine, &mut consumer, spin(1, 2));
    assert_eq!(
        (len, last),
        (1, Some(committed)),
        "spin samples the committed microprice"
    );

    // A spin tick pops between the chunks of one venue update (chunks share received_ts —
    // a timer tick can tie-break in between): the half-applied book must not leak into the
    // sampled series.
    let partial = partial_delta_chunk(0, Side::Buy, &[(100 * ONE, 7 * ONE)], 3);
    apply(&mut engine, &mut consumer, InboundMessage::Book(partial));
    let (len, last) = spin_state(&mut engine, &mut consumer, spin(2, 4));
    assert_eq!(
        (len, last),
        (2, Some(committed)),
        "spin between chunks must sample the last committed state, not the partial book"
    );

    let last_chunk = delta_chunk(0, Side::Sell, &[(101 * ONE, 3 * ONE)], 5);
    apply(&mut engine, &mut consumer, InboundMessage::Book(last_chunk));
    let recommitted = micro::microprice(level(100 * ONE, 7 * ONE), level(101 * ONE, 3 * ONE));
    let (len, last) = spin_state(&mut engine, &mut consumer, spin(3, 6));
    assert_eq!(
        (len, last),
        (3, Some(recommitted)),
        "the sampled value moves at the commit"
    );

    // Mid-session one-sided stretch (no reset): continuity intact, hold the committed value.
    let emptied = delta_chunk(0, Side::Sell, &[(101 * ONE, 0)], 7);
    apply(&mut engine, &mut consumer, InboundMessage::Book(emptied));
    let (len, last) = spin_state(&mut engine, &mut consumer, spin(4, 8));
    assert_eq!(
        (len, last),
        (4, Some(recommitted)),
        "one-sided stretch holds the last committed value"
    );

    // A reset declares continuity broken: skip until a resync commits.
    apply(
        &mut engine,
        &mut consumer,
        InboundMessage::BookReset(book_reset(0, 9)),
    );
    let (len, _) = spin_state(&mut engine, &mut consumer, spin(5, 10));
    assert_eq!(len, 4, "no sample after a reset");

    let rebids = partial_snapshot_chunk(0, Side::Buy, &[(100 * ONE, 2 * ONE)], 11);
    apply(&mut engine, &mut consumer, InboundMessage::Book(rebids));
    let (len, _) = spin_state(&mut engine, &mut consumer, spin(6, 12));
    assert_eq!(len, 4, "no sample mid-resync (snapshot uncommitted)");

    let reasks = last_snapshot_chunk(0, Side::Sell, &[(101 * ONE, ONE)], 13);
    apply(&mut engine, &mut consumer, InboundMessage::Book(reasks));
    let (len, last) = spin_state(&mut engine, &mut consumer, spin(7, 14));
    assert_eq!(
        (len, last),
        (5, Some(committed)),
        "sampling resumes once the resync commits"
    );
}

/// One price step of the synthetic paths below — small against the 100.0 base, so log and
/// arithmetic returns are interchangeable to well inside the tolerances asserted.
const TICK: i64 = ONE / 1000;

/// Drives [`MicroRecorder`] over one binance instrument at a declared spin cadence: each price
/// becomes the committed touch, then a spin tick lands one interval later.
struct SpinDriver {
    engine: HotEngine,
    persist: Consumer<PersistRecord>,
    logs: Consumer<LogRecord>,
    realised_vol_st: FeatureId,
    spin_interval: DurationUs,
    seq: u64,
    now: i64,
}

impl SpinDriver {
    fn new(spin_interval: DurationUs) -> Self {
        let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
        let (sink, persist) = persist_ring(64);
        let (log_sink, logs) = strategy_log_ring(64);
        let (metrics_producer, _metrics_consumer) = metrics_ring(64);
        let strategy = Box::new(MicroRecorder::from_spec(
            &recorder_spec(vec![TableKind::Features]),
            engine_view(spin_interval),
        ));
        Self {
            engine: engine_without_warmup(&instruments, strategy, sink, log_sink, metrics_producer),
            persist,
            logs,
            realised_vol_st: recorder_feature_id("realised_vol_st"),
            spin_interval,
            seq: 0,
            now: 0,
        }
    }

    /// Commits `price` as the touch and spins; returns the `realised_vol_st` that spin emitted.
    fn step(&mut self, price: i64) -> Option<f64> {
        let (bids, asks) = snapshot_pair(
            0,
            &[(price - ONE / 2, ONE)],
            &[(price + ONE / 2, ONE)],
            self.now,
        );
        self.engine.dispatch(pop(0, 0), &InboundMessage::Book(bids));
        self.engine.dispatch(pop(0, 0), &InboundMessage::Book(asks));
        while self.persist.pop().is_ok() {}

        self.now += self.spin_interval.micros();
        self.seq += 1;
        self.engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(self.seq, self.now)),
        );
        let mut realised = None;
        while let Ok(record) = self.persist.pop() {
            if let PersistRecord::Feature(row) = record
                && row.feature == self.realised_vol_st
            {
                realised = Some(row.value);
            }
        }
        while self.logs.pop().is_ok() {}
        realised
    }
}

/// The realised-vol window, measured from outside: fill it with a volatile prefix, then hold the
/// price still and count the samples it takes for the prefix to age out entirely — the tick the
/// reported vol collapses to exactly zero is the tick the window last held a nonzero return.
fn observed_window(spin_interval: DurationUs, probe_limit: usize) -> usize {
    let (high, low) = (100 * ONE, 100 * ONE - TICK);
    let mut driver = SpinDriver::new(spin_interval);
    for step in 0..probe_limit {
        driver.step(if step % 2 == 0 { high } else { low });
    }
    // Ending the prefix on the held price makes the join return zero too, so the only nonzero
    // returns left in the window are between alternating samples.
    driver.step(high);

    for held in 1..=probe_limit {
        let vol = driver
            .step(high)
            .expect("the window is long past the emission gate");
        if vol == 0.0 {
            return held + 1;
        }
    }
    panic!("the volatile prefix never aged out within {probe_limit} held samples");
}

/// Derived windows are a fixed span of TIME, so halving the spin doubles the sample count — until
/// the capacity floor takes over, below which a slower spin buys no further shrinkage. Both halves
/// matter: the first keeps the horizon honest, the second keeps a slow run's buffer usable.
#[test]
fn derived_windows_track_the_spin_and_honour_the_capacity_floor() {
    const PROBE_LIMIT: usize = 800;

    let one_second = observed_window(DurationUs::from_secs(1), PROBE_LIMIT);
    let two_seconds = observed_window(DurationUs::from_secs(2), PROBE_LIMIT);
    assert_eq!(
        one_second,
        2 * two_seconds,
        "above the floor the window is a fixed time horizon: {one_second} vs {two_seconds} samples"
    );

    let thirty_seconds = observed_window(DurationUs::from_secs(30), PROBE_LIMIT);
    let sixty_seconds = observed_window(DurationUs::from_secs(60), PROBE_LIMIT);
    assert_eq!(
        thirty_seconds, sixty_seconds,
        "at the floor the window stops tracking the spin: {thirty_seconds} vs {sixty_seconds}"
    );
}
