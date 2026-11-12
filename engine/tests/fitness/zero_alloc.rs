//! Zero-allocation fitness: after construction, driving every message category through the
//! real dispatch loop touches the allocator not at all. The falsifiable proof of the
//! steady-state no-alloc rule, required green forever.

use std::sync::atomic::Ordering;

use polysim::config::{
    IntensitySpec, KlineInterval, RecordedTables, SpinField, TableKind, TrackerSpec,
};
use polysim::hot::book::{Book, SnapshotOutcome};
use polysim::hot::dispatch::{HotEngine, HotEngineSetup, LinkWiring};
use polysim::hot::exec::{DesiredQuote, QuoteLevel};
use polysim::hot::quant::intensity::IntensityFit;
use polysim::hot::strategy::{Registration, Strategy, StrategyConfig, StrategyCtx};
use polysim::hot::tracker::MicroTracker;
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::link::{
    InboundLink, LinkFrame, LinkHash, LinkOrigin, LinkPayload, OutboundLink, RunState, TopicId,
    schema_hash_of_fields,
};
use polysim::log::LogRecord;
use polysim::msg::exec::OrderStyle;
use polysim::msg::inbound::{InboundMessage, RunControl, SpinTick};
use polysim::msg::persist::{FeatureId, PersistRecord};
use polysim::msg::ui::{UiBookSnapshot, UiEvent};
use polysim::shutdown::{RunAssertion, RunControlGate};
use polysim::sink::LinkSink;
use polysim::time::{DurationUs, TsUs};
use rtrb::Consumer;

use crate::engine_support::{
    FillPen, NOMINAL_SPIN, ONE, delta_chunk, detached_setup, engine_view, engine_with_ui,
    engine_with_ui_and_exec, engine_without_persistence, engine_without_warmup, instrument_row,
    kline, metrics_ring, persist_ring, persist_ring_for, pop, recorder_feature_id, recorder_spec,
    rotation, snapshot_pair, spin, strategy_log_ring, tracker_spec_all, trade, ui_event_ring,
};

/// Drains both UI feed consumers so the emission path exercises the ring's success push, not just
/// the full-ring drop — and the pop path itself must not allocate.
fn drain_ui(books: &mut Consumer<UiBookSnapshot>, events: &mut Consumer<UiEvent>) {
    while let Ok(snapshot) = books.pop() {
        std::hint::black_box(snapshot);
    }
    while let Ok(event) = events.pop() {
        std::hint::black_box(event);
    }
}
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
use crate::raw_recorder::RecorderStrategy;

/// Returns `Some(is_closed)` on a kline so the caller can confirm both open and closed flowed.
fn step(engine: &mut HotEngine, consumer: &mut Consumer<PersistRecord>, i: i64) -> Option<bool> {
    let (message, kline_closed) = step_message(i);
    engine.dispatch(pop(0, 0), &message);
    while let Ok(record) = consumer.pop() {
        std::hint::black_box(record);
    }
    kline_closed
}

/// The repeating message mix the steady-state windows drive, shared so the with- and
/// without-persistence configurations are measured over the identical sequence.
fn step_message(i: i64) -> (InboundMessage, Option<bool>) {
    let mut kline_closed = None;
    let message = match i % 7 {
        0 => InboundMessage::Trade(trade(0, 100 * ONE + i, 1_000_000, Side::Buy, i)),
        1 => InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, ONE + (i % 5))], i)),
        2 => {
            // `i % 7` is always 2 in this arm, so the closed flag is derived from `i / 7`, which
            // actually varies.
            let is_closed = (i / 7) % 2 == 0;
            kline_closed = Some(is_closed);
            InboundMessage::Kline(kline(
                0,
                KlineInterval::OneMinute,
                (100 * ONE, 101 * ONE, 99 * ONE, 100 * ONE),
                is_closed,
                i,
            ))
        }
        3 => InboundMessage::SpinTick(spin(i as u64, i)),
        4 => InboundMessage::Trade(trade(1, 200 * ONE + i, 1_000_000, Side::Sell, i)),
        5 => InboundMessage::Book(delta_chunk(1, Side::Sell, &[(201 * ONE, ONE + (i % 5))], i)),
        // Rotation wipes the slot's derived state in place (Vec::clear, no realloc) and never
        // touches the book, so instrument 0's book stays valid for the following deltas.
        _ => InboundMessage::MarketRotation(rotation(0, 300 * ONE + i, 600 * ONE + i, i)),
    };
    (message, kline_closed)
}

fn recorder_all_tables() -> Box<RecorderStrategy> {
    Box::new(RecorderStrategy::from_spec(
        &recorder_spec(vec![
            TableKind::Trades,
            TableKind::BookEvents,
            TableKind::Klines,
            TableKind::Features,
        ]),
        engine_view(NOMINAL_SPIN),
    ))
}

fn seed_two_instruments(engine: &mut HotEngine) {
    for instrument in 0..2u16 {
        let base = 100 * ONE * i64::from(instrument + 1);
        let (bids, asks) = snapshot_pair(
            instrument,
            &[(base, 2 * ONE), (base - ONE, ONE)],
            &[(base + ONE, ONE), (base + 2 * ONE, 2 * ONE)],
            0,
        );
        engine.dispatch(pop(0, 0), &InboundMessage::Book(bids));
        engine.dispatch(pop(0, 0), &InboundMessage::Book(asks));
    }
}

fn seeded_book() -> Book {
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 0);
    let mut book = Book::new(64);
    let first = book.apply_snapshot_chunk(&bids);
    let second = book.apply_snapshot_chunk(&asks);
    assert_eq!(
        (first, second),
        (SnapshotOutcome::Clean, SnapshotOutcome::Clean)
    );
    book
}

/// Two fully-configured instruments pumped across every category; the measured 100k-message
/// window must not touch the allocator.
#[test]
fn steady_state_dispatch_does_not_allocate() {
    let instruments = [
        instrument_row(0, tracker_spec_all(100), 128),
        instrument_row(1, tracker_spec_all(100), 128),
    ];
    let (sink, mut consumer) = persist_ring(4096);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, mut metrics_consumer) = metrics_ring(64);
    let strategy = recorder_all_tables();
    let (mut engine, mut ui_books, mut ui_events) = engine_with_ui(
        &instruments,
        strategy,
        sink,
        log_sink,
        metrics_producer,
        DurationUs::ZERO,
    );

    seed_two_instruments(&mut engine);
    while consumer.pop().is_ok() {}
    drain_ui(&mut ui_books, &mut ui_events);

    for i in 0..10_000i64 {
        step(&mut engine, &mut consumer, i);
        drain_ui(&mut ui_books, &mut ui_events);
    }
    // Empty the metrics ring the warmup filled, so the measured window exercises ~64 real
    // snapshot pushes and THEN the ring-full drop path — not the drop path alone.
    while metrics_consumer.pop().is_ok() {}

    let mut open_klines = 0u64;
    let mut closed_klines = 0u64;
    let before = crate::alloc_count();
    for i in 10_000..110_000i64 {
        match step(&mut engine, &mut consumer, i) {
            Some(true) => closed_klines += 1,
            Some(false) => open_klines += 1,
            None => {}
        }
        drain_ui(&mut ui_books, &mut ui_events);
    }
    let after = crate::alloc_count();

    assert_eq!(after, before, "dispatch allocated in steady state");
    assert!(
        open_klines > 0 && closed_klines > 0,
        "both open and closed klines must flow: open={open_klines} closed={closed_klines}"
    );
    assert!(
        engine.dropped_metrics_snapshots() > 0,
        "measured window must overflow the 64-slot metrics ring so the drop path is proven"
    );

    // A standalone tracker with the same spec samples on every bar close, so the trades in the
    // measured window are large enough to close bars.
    let mut tracker = MicroTracker::new(&tracker_spec_all(100), &[KlineInterval::OneMinute], None);
    let book = seeded_book();
    tracker.on_book(&book);
    for when in 0..8i64 {
        tracker.on_trade(&trade(0, 100 * ONE, 1_000_000, Side::Buy, when));
    }
    let samples = tracker
        .volume_sampled(SpinField::Microprice)
        .map_or(0, |queue| queue.len());
    assert!(samples > 0, "the volume clock samples on every bar close");
}

/// The same window with NO persistence configured — the zero-allocation guarantee binds both
/// configurations, and discarding a row must be at least as allocation-free as recording one.
/// The strategy still emits into all four tables: the config is the authority, so this is the
/// gate's worst case, one discard per emit.
#[test]
fn steady_state_dispatch_without_persistence_does_not_allocate() {
    let instruments = [
        instrument_row(0, tracker_spec_all(100), 128),
        instrument_row(1, tracker_spec_all(100), 128),
    ];
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, mut metrics_consumer) = metrics_ring(64);
    let strategy = recorder_all_tables();
    let mut engine = engine_without_persistence(&instruments, strategy, log_sink, metrics_producer);

    seed_two_instruments(&mut engine);
    for i in 0..2_000i64 {
        let (message, _) = step_message(i);
        engine.dispatch(pop(0, 0), &message);
    }
    while metrics_consumer.pop().is_ok() {}

    let before = crate::alloc_count();
    for i in 2_000..22_000i64 {
        let (message, _) = step_message(i);
        engine.dispatch(pop(0, 0), &message);
    }
    let after = crate::alloc_count();

    assert_eq!(
        after, before,
        "dispatch allocated in steady state with persistence off"
    );
    assert_eq!(
        engine.dropped_persist_records(),
        0,
        "with no ring there is nothing to drop into — the rows never exist"
    );
}

fn intensity_spec() -> TrackerSpec {
    TrackerSpec {
        intensity: Some(IntensitySpec {
            max_depth_ticks: 16,
            half_life_secs: 600.0,
            min_events: 5.0,
        }),
        ..TrackerSpec::default()
    }
}

/// Intensity must stay allocation-free on both halves: inside the measured window the accumulator
/// (decay, bucket increments and rotation clear) AND the strategy's pull-based MLE `fit`
/// (warm-started Nelder-Mead) must touch the allocator not at all. `estimates` proves the fit actually ran and produced output,
/// so a dead no-op cannot pass the allocation assertion by doing nothing.
#[test]
fn intensity_accumulator_and_fit_do_not_allocate() {
    let tick = ONE / 100;
    let mut tracker = MicroTracker::new(&intensity_spec(), &[], Some(Price(tick)));
    let book = seeded_book();
    tracker.on_book(&book);

    // Buy aggressors 0..3 ticks above the ask populate the ask histogram; distinct timestamps make
    // each its own group, so every step also advances the decay clock.
    let feed = |tracker: &mut MicroTracker, i: i64| {
        tracker.on_trade(&trade(
            0,
            101 * ONE + (i % 4) * tick,
            ONE,
            Side::Buy,
            i * 1_000,
        ));
    };
    let mut fit = IntensityFit::default();
    for i in 0..1_000i64 {
        feed(&mut tracker, i);
    }
    // Prime the warm-start cache before the counted window so the in-window fits refit warm.
    fit.fit(
        tracker.intensity().expect("configured"),
        TsUs::from_micros(1_000_000),
    );

    let mut estimates = 0u64;
    let before = crate::alloc_count();
    for i in 1_000..101_000i64 {
        feed(&mut tracker, i);
        if i % 1_000 == 0 {
            let now = TsUs::from_micros(i * 1_000);
            if fit
                .fit(tracker.intensity().expect("configured"), now)
                .ask
                .is_some()
            {
                estimates += 1;
            }
        }
        if i % 5_000 == 0 {
            tracker.on_rotation();
            tracker.on_book(&book);
        }
    }
    let after = crate::alloc_count();
    assert_eq!(
        after, before,
        "intensity accumulator or fit allocated in steady state"
    );
    assert!(
        estimates > 0,
        "the pull-based fit must run inside the measured window"
    );
}

const SECOND_US: i64 = 1_000_000;

/// The [`MicroRecorder`] columns the measured window counts, resolved by name so a feature inserted
/// above one of them cannot silently repoint it at its neighbour.
struct RecorderColumns {
    egarch: FeatureId,
    realised_st: FeatureId,
    realised_st_bps: FeatureId,
    obi: FeatureId,
    resilience_median: FeatureId,
    resilience_mean: FeatureId,
    /// The ask side alone: `micro_step` sends buy aggressors, so no sell print ever reaches the bid
    /// histogram.
    intensity_ask: FeatureId,
    /// The Guéant quote the ask-side (A, k) and the σ rescale that feeds both sides. The price
    /// column is the end of the whole side path — coefficients, half-spread, grid snap — so
    /// counting it proves every step of that path ran.
    gueant_ask_price: FeatureId,
    gueant_sigma: FeatureId,
    /// The volume-clock pair, emitted from `on_volume` rather than the spin: the ~1.01-USD prints
    /// against the fixed 1-USD target close at least one bar per trade, so the fan-out runs — and
    /// must emit — inside the measured window.
    volume_imbalance: FeatureId,
    volume_duration: FeatureId,
    /// The two VPIN toxicity windows and their signed-flow companions, emitted from `on_volume`
    /// once each window has its bucket count. The long window only fills because the instrument row
    /// below retains more closed bars than the shipped fixture keep — see the `keep` override.
    vpin_st: FeatureId,
    vpin_lt: FeatureId,
    vpin_signed_flow_st: FeatureId,
    vpin_signed_flow_lt: FeatureId,
    /// The per-side Hawkes columns emitted from the spin. Only the ask side is asserted live:
    /// `micro_step` sends buy aggressors alone, which hit the ask side, so the bid side never banks
    /// an arrival and its columns stay null, as with `intensity_ask`. λ is the resident live
    /// intensity, the other four the fitted kernel.
    hawkes_lambda_ask: FeatureId,
    hawkes_mu_ask: FeatureId,
    hawkes_alpha_ask: FeatureId,
    hawkes_beta_ask: FeatureId,
    hawkes_branching_ask: FeatureId,
}

impl RecorderColumns {
    /// Allocates, so every measured window resolves its columns before opening the counter.
    fn resolve() -> Self {
        Self {
            egarch: recorder_feature_id("egarch_vol_lt"),
            realised_st: recorder_feature_id("realised_vol_st"),
            realised_st_bps: recorder_feature_id("realised_vol_st_bps"),
            obi: recorder_feature_id("obi_half_bp"),
            resilience_median: recorder_feature_id("resilience_median_1m"),
            resilience_mean: recorder_feature_id("resilience_mean_1m"),
            intensity_ask: recorder_feature_id("intensity_a_ask_per_sec"),
            gueant_ask_price: recorder_feature_id("gueant_ask_price"),
            gueant_sigma: recorder_feature_id("gueant_sigma_bps"),
            volume_imbalance: recorder_feature_id("volume_bar_imbalance"),
            volume_duration: recorder_feature_id("volume_bar_duration_secs"),
            vpin_st: recorder_feature_id("vpin_st"),
            vpin_lt: recorder_feature_id("vpin_lt"),
            vpin_signed_flow_st: recorder_feature_id("vpin_signed_flow_st"),
            vpin_signed_flow_lt: recorder_feature_id("vpin_signed_flow_lt"),
            hawkes_lambda_ask: recorder_feature_id("hawkes_lambda_ask_per_sec"),
            hawkes_mu_ask: recorder_feature_id("hawkes_mu_ask_per_sec"),
            hawkes_alpha_ask: recorder_feature_id("hawkes_alpha_ask_per_sec"),
            hawkes_beta_ask: recorder_feature_id("hawkes_beta_ask_per_sec"),
            hawkes_branching_ask: recorder_feature_id("hawkes_branching_ask"),
        }
    }
}

/// Closed-candle steps (indexed by `i / 6`) that carry a CLOSED kline: enough to push the EGARCH
/// close history past its 300 floor early, so the first (cold) fit runs before the measured window.
const EARLY_CLOSED_KLINES: i64 = 320;

/// First loop index of the measured window — matches the `10_000..110_000` range below.
const MEASURED_START: i64 = 10_000;

/// Inside the measured window, one candle in this many closes. Closed candles must keep arriving so
/// an EGARCH REFIT fires under the counting allocator, but rarely enough that the fit does not run
/// on every spin — a bound on the test's cost, not on what it proves.
const SPARSE_KLINE_STRIDE: i64 = 400;

/// Event-time gap between the spins `micro_step` emits: one message in six, three seconds apart.
const MICRO_SPIN: DurationUs = DurationUs::from_secs(18);

/// The binance grid instrument 0 is stamped with, and the step its buy prints walk out from the ask.
const MICRO_TICK: i64 = ONE / 100;

/// Counts the feature rows the measured window produced, so a silently dead per-venue path
/// cannot pass the allocation assertion by doing nothing.
#[derive(Default)]
struct FeatureEmissions {
    egarch: u64,
    realised_st: u64,
    realised_st_bps: u64,
    obi: u64,
    resilience_median: u64,
    resilience_mean: u64,
    intensity_ask: u64,
    gueant_ask_price: u64,
    gueant_sigma: u64,
    volume_imbalance: u64,
    volume_duration: u64,
    vpin_st: u64,
    vpin_lt: u64,
    vpin_signed_flow_st: u64,
    vpin_signed_flow_lt: u64,
    hawkes_lambda_ask: u64,
    hawkes_mu_ask: u64,
    hawkes_alpha_ask: u64,
    hawkes_beta_ask: u64,
    hawkes_branching_ask: u64,
}

/// One binance instrument: closed klines feed the EGARCH close history, spins sample the mids for
/// realised vol. The cycle stays six phases wide so the spin — and the per-side Hawkes refit it
/// drives, which dominates this test's runtime — keeps a fixed share of the measured window.
/// Returns `Some(is_closed)` on a kline step so the caller can confirm closed candles reach the
/// measured window and thus fire a refit there.
fn micro_step(
    engine: &mut HotEngine,
    consumer: &mut Consumer<PersistRecord>,
    columns: &RecorderColumns,
    emissions: &mut FeatureEmissions,
    i: i64,
) -> Option<bool> {
    let when = i * 3 * SECOND_US;
    let mut kline_closed = None;
    let message = match i % 6 {
        // Buy aggressors reaching 0..3 ticks past the seeded 101-unit ask. Depth must actually
        // vary: prints that all land at the touch drive the fitted k to its search-box ceiling,
        // where re-anchoring A from touch to mid overflows and the Guéant block gates off.
        0 => InboundMessage::Trade(trade(
            0,
            101 * ONE + (i % 4) * MICRO_TICK,
            1_000_000,
            Side::Buy,
            when,
        )),
        // Whole-unit bid qty against the seeded 1-unit ask: a real top-of-book imbalance that
        // clears the resilience dust floor (≥5% TOB imbalance) so a rate is produced. The
        // `i % 5 == 0` deltas set qty == ask qty, so mid == microprice anchors x0 = 0 and the
        // NEXT spin's update dust-skips. Price is untouched, so mid — and thus realised vol —
        // is unaffected.
        1 => InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(100 * ONE, (1 + i % 5) * ONE)],
            when,
        )),
        // Churn on the second level of each side. Deliberately off the touch: the arm above tunes a
        // TOB interaction the resilience gate depends on, and moving the touch here would perturb
        // it while proving nothing extra about allocation. Depth updates outnumbering prints is
        // also the shape of a real binance feed.
        2 => InboundMessage::Book(delta_chunk(
            0,
            Side::Buy,
            &[(99 * ONE, (1 + i % 5) * ONE)],
            when,
        )),
        3 => InboundMessage::Book(delta_chunk(
            0,
            Side::Sell,
            &[(102 * ONE, (1 + i % 5) * ONE)],
            when,
        )),
        4 => {
            let step = i / 6;
            let is_closed = step < EARLY_CLOSED_KLINES
                || (i >= MEASURED_START && step % SPARSE_KLINE_STRIDE == 0);
            kline_closed = Some(is_closed);
            InboundMessage::Kline(kline(
                0,
                KlineInterval::OneMinute,
                (100 * ONE, 101 * ONE, 99 * ONE, (100 + i % 5) * ONE),
                is_closed,
                when,
            ))
        }
        _ => InboundMessage::SpinTick(spin(i as u64, when)),
    };
    engine.dispatch(pop(0, 0), &message);
    while let Ok(record) = consumer.pop() {
        if let PersistRecord::Feature(row) = record {
            if row.feature == columns.egarch {
                emissions.egarch += 1;
            } else if row.feature == columns.realised_st {
                emissions.realised_st += 1;
            } else if row.feature == columns.realised_st_bps {
                emissions.realised_st_bps += 1;
            } else if row.feature == columns.obi {
                assert!(
                    (-1.0..=1.0).contains(&row.value),
                    "obi_half_bp out of [-1,1]: {}",
                    row.value
                );
                emissions.obi += 1;
            } else if row.feature == columns.resilience_median {
                emissions.resilience_median += 1;
            } else if row.feature == columns.resilience_mean {
                emissions.resilience_mean += 1;
            } else if row.feature == columns.intensity_ask {
                emissions.intensity_ask += 1;
            } else if row.feature == columns.gueant_ask_price {
                emissions.gueant_ask_price += 1;
            } else if row.feature == columns.gueant_sigma {
                emissions.gueant_sigma += 1;
            } else if row.feature == columns.volume_imbalance {
                emissions.volume_imbalance += 1;
            } else if row.feature == columns.volume_duration {
                emissions.volume_duration += 1;
            } else if row.feature == columns.vpin_st {
                assert!(
                    (0.0..=1.0).contains(&row.value),
                    "vpin_st out of [0,1]: {}",
                    row.value
                );
                emissions.vpin_st += 1;
            } else if row.feature == columns.vpin_lt {
                assert!(
                    (0.0..=1.0).contains(&row.value),
                    "vpin_lt out of [0,1]: {}",
                    row.value
                );
                emissions.vpin_lt += 1;
            } else if row.feature == columns.vpin_signed_flow_st {
                assert!(
                    (-1.0..=1.0).contains(&row.value),
                    "vpin_signed_flow_st out of [-1,1]: {}",
                    row.value
                );
                emissions.vpin_signed_flow_st += 1;
            } else if row.feature == columns.vpin_signed_flow_lt {
                assert!(
                    (-1.0..=1.0).contains(&row.value),
                    "vpin_signed_flow_lt out of [-1,1]: {}",
                    row.value
                );
                emissions.vpin_signed_flow_lt += 1;
            } else if row.feature == columns.hawkes_lambda_ask {
                // λ = μ + excitation ≥ μ > 0, and the package reports instability rather than
                // clamping, so these bound each column's own domain, not a stationarity window.
                assert!(
                    row.value > 0.0,
                    "hawkes_lambda_ask not positive: {}",
                    row.value
                );
                emissions.hawkes_lambda_ask += 1;
            } else if row.feature == columns.hawkes_mu_ask {
                assert!(row.value > 0.0, "hawkes_mu_ask not positive: {}", row.value);
                emissions.hawkes_mu_ask += 1;
            } else if row.feature == columns.hawkes_alpha_ask {
                assert!(row.value >= 0.0, "hawkes_alpha_ask negative: {}", row.value);
                emissions.hawkes_alpha_ask += 1;
            } else if row.feature == columns.hawkes_beta_ask {
                assert!(
                    row.value > 0.0,
                    "hawkes_beta_ask not positive: {}",
                    row.value
                );
                emissions.hawkes_beta_ask += 1;
            } else if row.feature == columns.hawkes_branching_ask {
                assert!(
                    row.value >= 0.0,
                    "hawkes_branching_ask negative: {}",
                    row.value
                );
                emissions.hawkes_branching_ask += 1;
            }
        }
    }
    kline_closed
}

/// The same guarantee for [`MicroRecorder`]: its per-spin emission, its spin-mid realised vol, its
/// per-closed-bar `on_volume` rows, its per-side Hawkes arrival refit AND an EGARCH refit all run
/// inside the measured window without allocating. The close history is seeded past the fit's 300
/// floor during warmup so the cold fit lands before the counted region; sparse closed candles then
/// fire warm refits inside it. The ask-side Hawkes primes likewise — the buy-aggressor tape banks
/// past the 100-arrival floor and cold-fits its resident evaluator well before the counter, so only
/// warm refits and O(1) intensity reads run in it. The shipped defaults suffice, so no `hawkes`
/// override is needed.
#[test]
fn micro_recorder_dispatch_does_not_allocate() {
    let mut binance = instrument_row(0, tracker_spec_all(100), 128);
    binance.tick_size = Some(Price(MICRO_TICK));
    binance.tracker.intensity = intensity_spec().intensity;
    // The long VPIN window is 250 buckets, but tracker_spec_all retains only 64 closed bars — a cap
    // no amount of trading can beat. Retaining more closed bars lets vpin_lt fill and go live under
    // the counter; the larger backing is a one-time init alloc, steady-state pushes still evict.
    binance
        .tracker
        .volume_bars
        .as_mut()
        .expect("tracker_spec_all configures a fixed volume clock")
        .keep = 300;
    let instruments = [binance];
    let (sink, mut consumer) = persist_ring(4096);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, mut metrics_consumer) = metrics_ring(64);
    let spec = recorder_spec::<MicroRecorderParams>(vec![TableKind::Features]);
    let strategy = Box::new(MicroRecorder::from_spec(&spec, engine_view(MICRO_SPIN)));
    let (mut engine, mut ui_books, mut ui_events) = engine_with_ui(
        &instruments,
        strategy,
        sink,
        log_sink,
        metrics_producer,
        DurationUs::ZERO,
    );

    let (bids, asks) = snapshot_pair(
        0,
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        0,
    );
    engine.dispatch(pop(0, 0), &InboundMessage::Book(bids));
    engine.dispatch(pop(0, 0), &InboundMessage::Book(asks));
    while consumer.pop().is_ok() {}
    drain_ui(&mut ui_books, &mut ui_events);

    let columns = RecorderColumns::resolve();
    let mut emissions = FeatureEmissions::default();
    for i in 0..10_000i64 {
        micro_step(&mut engine, &mut consumer, &columns, &mut emissions, i);
        drain_ui(&mut ui_books, &mut ui_events);
    }
    while metrics_consumer.pop().is_ok() {}

    emissions = FeatureEmissions::default();
    let mut measured_closed_klines = 0u64;
    let before = crate::alloc_count();
    for i in 10_000..110_000i64 {
        if micro_step(&mut engine, &mut consumer, &columns, &mut emissions, i) == Some(true) {
            measured_closed_klines += 1;
        }
        drain_ui(&mut ui_books, &mut ui_events);
    }
    let after = crate::alloc_count();

    assert_eq!(after, before, "micro recorder allocated in steady state");
    assert!(
        emissions.egarch > 0,
        "the binance EGARCH long-horizon path must have run"
    );
    assert!(
        emissions.realised_st > 0,
        "the binance spin-mid realised path must have run"
    );
    assert!(
        emissions.realised_st_bps > 0,
        "the binance realised-vol bps companion must have run"
    );
    assert!(
        emissions.obi > 0,
        "the banded order-book imbalance path must have run"
    );
    assert!(
        emissions.resilience_median > 0,
        "the binance resilience median path must have run"
    );
    assert!(
        emissions.resilience_mean > 0,
        "the binance resilience mean path must have run"
    );
    assert!(
        emissions.intensity_ask > 0,
        "the binance per-side intensity fit must have run"
    );
    assert!(
        emissions.gueant_ask_price > 0,
        "the binance Guéant ask-side depth and grid snap must have run"
    );
    assert!(
        emissions.gueant_sigma > 0,
        "the binance Guéant σ log→bps rescale must have run"
    );
    assert!(
        emissions.volume_imbalance > 0,
        "the volume-bar imbalance row must reach the sink via on_volume"
    );
    assert!(
        emissions.volume_duration > 0,
        "the volume-bar duration row must reach the sink via on_volume"
    );
    // No `kyle_lambda` assertion: `micro_step` prints buy aggressors alone, and the strategy's
    // 0.2-per-sign floor refuses a one-sided window forever. The allocation question is still
    // covered — the same-trade fold and the rolling OLS both run under the counter on every closed
    // bar; only the four emits at the end are gated off. Whether a one-sided tape is estimable is
    // the estimator's own question, which the Kyle's lambda estimator tests answer.
    assert!(
        emissions.vpin_st > 0,
        "the short VPIN toxicity window must reach the sink via on_volume"
    );
    assert!(
        emissions.vpin_lt > 0,
        "the long VPIN toxicity window must reach the sink via on_volume"
    );
    assert!(
        emissions.vpin_signed_flow_st > 0,
        "the short VPIN signed-flow companion must reach the sink"
    );
    assert!(
        emissions.vpin_signed_flow_lt > 0,
        "the long VPIN signed-flow companion must reach the sink"
    );
    assert!(
        measured_closed_klines > 0,
        "a closed candle must land in the measured window so an EGARCH refit fires under the counter"
    );
    assert!(
        emissions.hawkes_lambda_ask > 0,
        "the ask-side Hawkes resident live intensity must reach the sink"
    );
    assert!(
        emissions.hawkes_mu_ask > 0,
        "the ask-side Hawkes baseline must reach the sink"
    );
    assert!(
        emissions.hawkes_alpha_ask > 0,
        "the ask-side Hawkes excitation jump must reach the sink"
    );
    assert!(
        emissions.hawkes_beta_ask > 0,
        "the ask-side Hawkes decay must reach the sink"
    );
    assert!(
        emissions.hawkes_branching_ask > 0,
        "the ask-side Hawkes branching ratio must reach the sink"
    );
}

/// Declares a quote and banks a log line every spin, so the measured window drives the two paths the
/// recorders never touch. The log message formats into the fixed record with no allocation.
struct LaneProbe;

impl Strategy for LaneProbe {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        declare_quote(ctx);
        polysim::strategy_info!(ctx, "spin {}", tick.seq);
    }
}

fn drain_lanes(persist: &mut Consumer<PersistRecord>, logs: &mut Consumer<LogRecord>) {
    while let Ok(record) = persist.pop() {
        std::hint::black_box(record);
    }
    while let Ok(record) = logs.pop() {
        std::hint::black_box(record);
    }
}

/// The declaration and log paths carry the same guarantee: declaring a two-sided quote plus a
/// formatted log line every spin and draining both never touches the allocator — and they are proven
/// live, not dead, by the log records that flow without a drop and the Quote frames the engine tees
/// off the declarations.
#[test]
fn declaration_and_log_lanes_do_not_allocate() {
    let instruments = [instrument_row(0, tracker_spec_all(100), 128)];
    let (sink, mut consumer) = persist_ring(4096);
    let (log_sink, mut log_consumer) = strategy_log_ring(4096);
    let log_drops = log_sink.drops_handle();
    let (metrics_producer, mut metrics_consumer) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(LaneProbe),
        sink,
        log_sink,
        metrics_producer,
    );

    for i in 0..10_000i64 {
        engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(i as u64, i)));
        drain_lanes(&mut consumer, &mut log_consumer);
    }
    // Empty the metrics ring the warmup filled so the measured window overflows it from empty.
    while metrics_consumer.pop().is_ok() {}

    let before = crate::alloc_count();
    for i in 10_000..110_000i64 {
        engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(i as u64, i)));
        drain_lanes(&mut consumer, &mut log_consumer);
    }
    let after = crate::alloc_count();

    assert_eq!(
        after, before,
        "the declaration and log lanes allocated in steady state"
    );
    assert_eq!(
        log_drops.load(Ordering::Relaxed),
        0,
        "the drained log lane never dropped a record"
    );

    // `orders_submitted` counts EXECUTION commands the engine banked, not intents a strategy
    // expressed, so a run with execution off reports zero and must — a non-zero count here would
    // mean the engine banked a command nobody asked it to send.
    let max_orders = std::iter::from_fn(|| metrics_consumer.pop().ok())
        .map(|snapshot| snapshot.counters.orders_submitted)
        .max()
        .expect("a metrics snapshot reached the ring");
    assert_eq!(
        max_orders, 0,
        "execution is off in this run, so no command may have been banked"
    );
}

/// Exercises the UI event tees reachable from a steady-state message mix — Trade, Rotation,
/// Position, Execution and Quote from dispatch, Feature from the strategy bank, Fill from the real
/// inbound execution fold — inside the measured window, banking one of each per cycle. All stamp
/// through the preallocated event ring with no allocation, and the per-kind tallies prove no
/// path is a silent no-op passing the assertion by doing nothing.
///
/// OrderUpdate, Reject and Balance are NOT driven here: each needs a message this steady-state mix
/// does not carry, and seating one per iteration would allocate in the harness rather than in the
/// engine. They share `push_stamped` with the six below, and the UI feed tests pin that each is
/// emitted.
struct UiTeeProbe {
    feature: Option<FeatureId>,
}

impl Strategy for UiTeeProbe {
    fn features(&self) -> &'static [&'static str] {
        &["tee"]
    }
    fn register(&mut self, registration: Registration<'_>) {
        self.feature = registration.features.first().copied();
    }
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.emit(self.feature.expect("registered"), InstrumentId(0), 1.0);
        declare_quote(ctx);
    }
}

/// A two-sided declaration, the shape the engine's per-spin Quote tee reads. Declaring into
/// preallocated level state must not allocate any more than banking a row does.
fn declare_quote(ctx: &mut StrategyCtx<'_>) {
    for side in [Side::Buy, Side::Sell] {
        ctx.quote(
            InstrumentId(0),
            side,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: Price(100 * ONE),
                qty: Qty(ONE),
                style: OrderStyle::PostOnly,
            }),
        );
    }
}

#[derive(Default)]
struct TeeKinds {
    trade: u64,
    feature: u64,
    fill: u64,
    rotation: u64,
    quote: u64,
    position: u64,
    execution: u64,
}

fn drain_ui_kinds(events: &mut Consumer<UiEvent>, kinds: &mut TeeKinds) {
    while let Ok(event) = events.pop() {
        match event {
            UiEvent::Trade { .. } => kinds.trade += 1,
            UiEvent::Feature { .. } => kinds.feature += 1,
            UiEvent::Fill { .. } => kinds.fill += 1,
            UiEvent::Rotation { .. } => kinds.rotation += 1,
            UiEvent::Quote { .. } => kinds.quote += 1,
            UiEvent::Position { .. } => kinds.position += 1,
            UiEvent::Execution { .. } => kinds.execution += 1,
            UiEvent::OrderUpdate { .. }
            | UiEvent::OrderSnapshot { .. }
            | UiEvent::Reject { .. }
            | UiEvent::Balance { .. }
            | UiEvent::Latency { .. } => {}
        }
    }
}

#[test]
fn ui_event_tees_do_not_allocate() {
    let instruments = [instrument_row(0, tracker_spec_all(100), 128)];
    let (sink, mut consumer) = persist_ring(4096);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, _metrics_consumer) = metrics_ring(64);
    // Exec-wired on purpose: the gate reports itself only where an order could actually be sent, so
    // an engine without a command ring tees no `Execution` frame at all and the assertion below
    // would be measuring a silence rather than the tee.
    let (mut engine, mut ui_books, mut ui_events, mut commands) = engine_with_ui_and_exec(
        &instruments,
        Box::new(UiTeeProbe { feature: None }),
        sink,
        log_sink,
        metrics_producer,
        0x5150_0002,
    );

    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 0);
    engine.dispatch(pop(0, 0), &InboundMessage::Book(bids));
    engine.dispatch(pop(0, 0), &InboundMessage::Book(asks));

    // The buy side is seated once, here, so the measured window below only ever reports EXECUTIONS
    // against it — [`FillPen::fill`] returns a `Vec` and would allocate on every iteration.
    let mut pen = FillPen::new(0);
    let adoption = pen
        .adopt(Side::Buy, 100 * ONE, 0)
        .expect("a fresh pen has seated nothing");
    engine.dispatch(pop(0, 0), &adoption);

    // A trade tees Trade and a venue fill tees Fill, a spin tees Feature + Quote + Execution +
    // Position + Latency, a rotation tees Rotation (it clears derived state in place — Vec::clear, no realloc
    // — and never touches the book). The committed delta closing the cycle re-marks the ledger the
    // rotation just cleared, so the per-spin Position tee keeps firing instead of falling silent
    // after the first rotation.
    let feed = |engine: &mut HotEngine, pen: &mut FillPen, i: i64| {
        let message = match i % 5 {
            0 => InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, i)),
            1 => pen.report(Side::Buy, 100 * ONE, ONE, i),
            2 => InboundMessage::SpinTick(spin(i as u64, i)),
            3 => InboundMessage::MarketRotation(rotation(0, 300 * ONE + i, 600 * ONE + i, i)),
            _ => InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 2 * ONE)], i)),
        };
        engine.dispatch(pop(0, 0), &message);
    };

    for i in 0..10_000i64 {
        feed(&mut engine, &mut pen, i);
        while consumer.pop().is_ok() {}
        while commands.pop().is_ok() {}
        drain_ui(&mut ui_books, &mut ui_events);
    }

    let mut kinds = TeeKinds::default();
    let before = crate::alloc_count();
    for i in 10_000..110_000i64 {
        feed(&mut engine, &mut pen, i);
        while consumer.pop().is_ok() {}
        while let Ok(command) = commands.pop() {
            std::hint::black_box(command);
        }
        while let Ok(snapshot) = ui_books.pop() {
            std::hint::black_box(snapshot);
        }
        drain_ui_kinds(&mut ui_events, &mut kinds);
    }
    let after = crate::alloc_count();

    assert_eq!(after, before, "ui event tees allocated in steady state");
    assert!(kinds.trade > 0, "the per-print Trade tee must have run");
    assert!(
        kinds.execution > 0,
        "the per-spin Execution tee must have run — the halt latch is absolute state and a panel \
         that stopped hearing it would keep showing ARMED"
    );
    assert!(kinds.feature > 0, "the FeatureRow tee must have run");
    assert!(
        kinds.fill > 0,
        "the venue-fill Fill tee must have run — and with it the whole inbound exec fold, which \
         moves the ledger on the hot thread and must allocate nothing doing it"
    );
    assert!(
        kinds.rotation > 0,
        "the rotation Rotation tee must have run"
    );
    assert!(kinds.quote > 0, "the simulated Quote tee must have run");
    assert!(
        kinds.position > 0,
        "the per-spin Position tee must have run"
    );
}

/// Names for the link probe's slots; the same list digests into the schema hash every frame carries.
const LINK_FIELDS: [&str; 4] = [
    "peer_mid",
    "peer_spread",
    "peer_intensity",
    "peer_confidence",
];
const LINK_TOPICS: [&str; 1] = ["signals"];

/// Both directions of the link seam plus a persisted feature, so the measured window covers
/// `on_link`, `link_send` and the tape row dispatch tees for every frame.
struct LinkPump {
    received: Option<FeatureId>,
    topic: Option<TopicId>,
}

impl Strategy for LinkPump {
    fn features(&self) -> &'static [&'static str] {
        &["peer"]
    }

    fn link_fields(&self) -> &'static [&'static str] {
        &LINK_FIELDS
    }

    fn link_topics(&self) -> &'static [&'static str] {
        &LINK_TOPICS
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.received = registration.features.first().copied();
        self.topic = registration.link_topics.first().copied();
    }

    fn on_link(&mut self, ctx: &mut StrategyCtx<'_>, frame: &LinkFrame) {
        for value in frame.payload.values() {
            ctx.emit(self.received.expect("registered"), InstrumentId(0), *value);
        }
        ctx.link_send(self.topic.expect("registered"), frame.payload.values());
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        ctx.link_send(
            self.topic.expect("registered"),
            &[tick.seq as f64, 1.0, 2.0, 3.0],
        );
    }
}

fn link_message(seq: u64, when: i64) -> InboundMessage {
    InboundMessage::Link(InboundLink {
        frame: LinkFrame {
            origin: LinkOrigin {
                sender_te_hash: LinkHash::of_name("peer-te"),
                boot_ts_us: TsUs::from_micros(1),
                topic: TopicId::FIRST_STRATEGY,
                seq,
            },
            payload: LinkPayload::new(
                schema_hash_of_fields(&LINK_FIELDS),
                TsUs::from_micros(when - 1),
                &[1.0, 2.0, 3.0, 4.0],
            ),
        },
        received_ts_us: TsUs::from_micros(when),
        queued_ts_us: TsUs::from_micros(when),
    })
}

/// The link seam is bound like every other hot-path lane: `on_link`, the tape row dispatch tees for
/// each value slot, `link_send`'s bank and the drain into the outbound ring must all run without
/// touching the allocator. The run-state marker is in the window too — a park/resume pair is rare,
/// but `resume` walks every instrument and must not allocate while it does.
#[test]
fn link_dispatch_does_not_allocate() {
    let instruments = [instrument_row(0, tracker_spec_all(100), 128)];
    let (persistence, persist_consumer) = persist_ring_for(
        4096,
        RecordedTables::new(&[TableKind::Features, TableKind::LinkFrames]),
    );
    let (link_producer, link_consumer) = rtrb::RingBuffer::<OutboundLink>::new(1024);
    let control = RunControlGate::new();
    let strategy = Box::new(LinkPump {
        received: None,
        topic: None,
    });
    let mut engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink: ui_event_ring(64).0,
        link: Some(LinkWiring {
            sink: LinkSink::new(link_producer),
            acknowledged: control.acknowledged().clone(),
        }),
        ..detached_setup(&instruments, strategy)
    });
    let mut persist_consumer = persist_consumer;
    let mut link_consumer = link_consumer;

    // Warm every lane before measuring, so the window covers steady state and not first-touch.
    for i in 0..1_000i64 {
        engine.dispatch(pop(0, 0), &link_message(i as u64 + 1, i * 10));
        engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(i as u64, i * 10 + 5)),
        );
        while persist_consumer.pop().is_ok() {}
        while link_consumer.pop().is_ok() {}
    }

    let mut rows = 0u64;
    let mut frames = 0u64;
    let before = crate::alloc_count();
    for i in 1_000..101_000i64 {
        engine.dispatch(pop(0, 0), &link_message(i as u64 + 1, i * 10));
        engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(i as u64, i * 10 + 5)),
        );
        if i % 25_000 == 0 {
            let epoch = i as u64 / 25_000;
            let state = if epoch.is_multiple_of(2) { RunState::Running } else { RunState::Idle };
            engine.dispatch(
                pop(0, 0),
                &InboundMessage::RunControl(RunControl {
                    desired: RunAssertion { state, epoch },
                    received_ts_us: TsUs::from_micros(i * 10 + 7),
                    queued_ts_us: TsUs::from_micros(i * 10 + 7),
                }),
            );
        }
        while persist_consumer.pop().is_ok() {
            rows += 1;
        }
        while link_consumer.pop().is_ok() {
            frames += 1;
        }
    }
    let after = crate::alloc_count();

    assert_eq!(after, before, "the link seam allocated in steady state");
    assert!(rows > 0, "the tape and the feature lane both produced rows");
    assert!(frames > 0, "link_send reached the outbound ring");
}
