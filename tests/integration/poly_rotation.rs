//! Live integration: one 12-minute capture of the REAL Polymarket adapter,
//! spanning ≥2 full rotations, asserting the physics the fitness suite cannot reach with recorded
//! fixtures — zero-gap handover, teardown latency, and the streamed book cross-checked against a
//! live CLOB `/book` REST snapshot. The adapter feeds a real hot ring (an [`RotationObserver`]
//! reconstructs the books) AND a real [`PersistWriter`] via the rotations side-channel, so the
//! run also proves the live lineage parquet lands and reads back per leg.
//!
//! This capture runs far longer than a quick live smoke test, because rotation physics set the
//! span. Confirming a NEW window's book goes Valid while the OLD still streams, then measuring how
//! long past nominal close the old tears down, needs ≥2 full ~5-minute grid cycles to coexist.
//! Still one capture, run deliberately by the agent — never CI (`#[ignore]`).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rtrb::{Consumer, RingBuffer};
use tokio::sync::mpsc;

use polysim::adapters::backoff::BackoffCaps;
use polysim::adapters::polymarket::actor::{PolymarketAdapter, PolymarketAdapterContext};
use polysim::adapters::polymarket::rest::PolyRest;
use polysim::config::PolySeries;
use polysim::config::{
    Config, ExecutionMode, RecordedTables, StrategyId, TradingEngineId, VenueMarket,
};
use polysim::hot::spawn::QueueProducer;
use polysim::ids::FIXED_SCALE;
use polysim::log::{self, LogConfig};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::persist::{PersistRecord, RotationRow};
use polysim::persist::{PersistConfig, PersistWriter, RunMeta};
use polysim::registry::Registry;
use polysim::shutdown::{FatalSignal, RunStateCell};
use polysim::time::EngineClock;

use crate::observer::RotationObserver;
use crate::rotations_parquet::rotation_instruments;
use crate::{US, WINDOW_SECS, rest_check, unix_now_s};

/// 12 minutes: boot subscribes the current window, then ≥2 handovers land inside the span.
const RUN_SECS: u64 = 720;
/// Boot this far before the next grid boundary so the first handover lands early in the capture.
const PRE_BOUNDARY_SECS: i64 = 90;
const RING_CAPACITY: usize = 65_536;
const PERSIST_RING_CAPACITY: usize = 4_096;
const ROTATIONS_CHANNEL_CAPACITY: usize = 256;
const POLL: Duration = Duration::from_millis(50);
const PRINT_EVERY: Duration = Duration::from_secs(60);
const STRATEGY_ID: &str = "poly-integration";
const TE_ID: &str = "te-poly-integration";

const CONFIG_YAML: &str = "\
engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
source:
  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  tracker:
    microprice: { windows: [1000] }
strategy:
  instruments: all
  tables: [trades, book_events]
persistence:
  dir: ./data
logging:
  dir: ./logs
";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live network, 12-min capture — agent-run: cargo test --test integration -- --ignored --nocapture"]
async fn poly_rotation_capture_spans_two_rotations_with_zero_gap_handover() {
    let log_handle = log::init(&LogConfig::default());
    log::register_thread("integration");
    let config: Config = Config::from_yaml(CONFIG_YAML).expect("parse integration config");
    let registry = Registry::build(&config).expect("build registry");
    let output_dir = TempDir::new();

    let group = registry
        .producer_groups()
        .iter()
        .find(|group| matches!(group.market, VenueMarket::Polymarket(_)))
        .expect("config has a polymarket group");
    let VenueMarket::Polymarket(series) = group.market else {
        unreachable!("filtered to polymarket above");
    };
    let slot_up = slot_up_instruments(&registry);

    // Align: boot ~90s before the next grid boundary so a handover lands early in the run.
    let now_s = unix_now_s();
    let next_boundary_s = ((now_s / WINDOW_SECS) + 1) * WINDOW_SECS;
    let align_sleep = (next_boundary_s - now_s - PRE_BOUNDARY_SECS).max(0);
    println!(
        "== poly_rotation integration: 12-min ≥2-rotation capture ==\n\
         next boundary in {}s; aligning {align_sleep}s so boot lands ~{PRE_BOUNDARY_SECS}s pre-boundary; output {}",
        next_boundary_s - now_s,
        output_dir.path().display()
    );
    if align_sleep > 0 {
        tokio::time::sleep(Duration::from_secs(align_sleep as u64)).await;
    }
    let reference_boundary_us = next_boundary_s as f64 * US;

    let clock = EngineClock::start();
    let fatal = FatalSignal::new();
    let (producer, mut consumer) = RingBuffer::<InboundMessage>::new(RING_CAPACITY);
    let queue_producer =
        QueueProducer::new(producer, fatal.clone(), group.queue_id, group.source_id);

    // Real persistence: the adapter's rotations side-channel feeds a live PersistWriter so the
    // lineage parquet lands and reads back. The POD record ring stays empty (no hot thread here),
    // and no strategy table is named — venue lineage is engine-emitted, so it lands regardless.
    let (_persist_producer, persist_consumer) =
        RingBuffer::<PersistRecord>::new(PERSIST_RING_CAPACITY);
    let (rotations_tx, rotations_rx) = mpsc::channel::<RotationRow>(ROTATIONS_CHANNEL_CAPACITY);
    let persistence = PersistWriter::spawn(
        PersistConfig {
            dir: output_dir.path().to_path_buf(),
            tables: RecordedTables::new(&[]),
            retention: Duration::from_hours(6),
        },
        run_meta(&registry),
        persist_consumer,
        rotations_rx,
    );

    let context = PolymarketAdapterContext {
        window_assignments: None,
        clock: clock.clone(),
        fatal: fatal.clone(),
        run_state: RunStateCell::new(),
        backoff: BackoffCaps::default(),
        rotations_tx,
    };
    let handle = tokio::runtime::Handle::current();
    let adapter = PolymarketAdapter::spawn(
        group,
        series,
        registry.instruments(),
        queue_producer,
        context,
        &handle,
    );

    let mut observer = RotationObserver::new(&registry, reference_boundary_us);
    let rest = PolyRest::new(PolySeries::BtcUpDown5m).expect("build cross-validation rest client");
    let mut cross_validations: Vec<rest_check::CrossValidation> = Vec::new();
    let mut validated: BTreeSet<i64> = BTreeSet::new();

    let start = Instant::now();
    let run = Duration::from_secs(RUN_SECS);
    let mut next_print = PRINT_EVERY;
    while start.elapsed() < run {
        drain_ring(&mut observer, &mut consumer);
        observer.sample(clock.now().micros());
        if fatal.is_tripped() {
            eprintln!("FATAL tripped: {:?} — aborting capture", fatal.reason());
            break;
        }
        if let Some(validation) =
            rest_check::maybe_cross_validate(&rest, &observer, &slot_up, &mut validated).await
        {
            validation.print();
            cross_validations.push(validation);
        }
        if start.elapsed() >= next_print {
            observer.print_interval(start.elapsed().as_secs());
            next_print += PRINT_EVERY;
        }
        tokio::time::sleep(POLL).await;
    }
    drain_ring(&mut observer, &mut consumer);

    adapter.shutdown().await;
    persistence
        .drain()
        .await
        .expect("persistence drains and closes the rotations parquet");
    let persisted = rotation_instruments(&output_dir.path().join(STRATEGY_ID).join(TE_ID));

    observer.finalize();
    observer.print_report();
    rest_check::print_summary(&cross_validations);
    println!(
        "\nrotations parquet: {} lineage rows read back, instruments {:?}",
        persisted.len(),
        persisted.iter().collect::<BTreeSet<_>>()
    );
    log_handle.drain();

    assert_zero_gap_handover(&observer);
    assert_teardown_measured(&observer);
    rest_check::assert_all_within_tolerance(&cross_validations);
    assert_existence(&observer, &fatal, &persisted);

    println!("\npoly_rotation integration PASSED — see report above");
    output_dir.remove();
}

fn drain_ring(observer: &mut RotationObserver, consumer: &mut Consumer<InboundMessage>) {
    while let Ok(message) = consumer.pop() {
        observer.observe(message);
    }
}

/// The four slot rows map to `[slot A up, slot B up]` leg indices by venue-symbol suffix — the
/// window a REST snapshot must be compared against is chosen by grid parity, so only the Up legs are
/// needed here (one token per window is enough for the cross-check).
fn slot_up_instruments(registry: &Registry) -> [usize; 2] {
    let leg = |suffix: &str| {
        registry
            .instruments()
            .iter()
            .find(|row| row.venue_symbol.ends_with(suffix))
            .map(|row| row.instrument_id.0 as usize)
            .unwrap_or_else(|| panic!("registry missing slot row {suffix}"))
    };
    [leg("-a-up"), leg("-b-up")]
}

fn run_meta(registry: &Registry) -> RunMeta {
    RunMeta {
        strategy_id: StrategyId::new(STRATEGY_ID).expect("valid strategy id"),
        te_id: TradingEngineId::new(TE_ID).expect("valid trading engine id"),
        execution_mode: Some(ExecutionMode::Live),
        fixed_scale: FIXED_SCALE,
        engine_version: env!("CARGO_PKG_VERSION").into(),
        feature_names: Vec::new(),
        instrument_symbols: registry
            .instruments()
            .iter()
            .map(|row| row.venue_symbol.clone())
            .collect(),
        asset_symbols: registry.assets().names().to_vec(),
    }
}

fn assert_zero_gap_handover(observer: &RotationObserver) {
    let handovers = observer.zero_gap_handovers();
    assert!(
        handovers >= 2,
        "need ≥2 zero-gap handovers (new window Valid while old still streams), saw {handovers} — see rotations report"
    );
    assert!(
        observer.max_tails() <= 1,
        "two windows tailed past close at once ({}) — A/B slot alternation would be insufficient",
        observer.max_tails()
    );
    // The sibling_live_at_ready proxy can be fooled — a teardown BookReset refreshes the old
    // window's last-seen stamp, so a handover moments AFTER its teardown still reads "live". Assert
    // the direct datum: where the old window's teardown was captured, the new book reached Valid
    // strictly before it (positive overlap).
    let overlaps: Vec<i64> = observer
        .rotations()
        .iter()
        .filter(|obs| obs.is_zero_gap_handover())
        .filter_map(|obs| observer.overlap_us(obs))
        .collect();
    assert!(
        !overlaps.is_empty(),
        "no handover's sibling teardown was captured — overlap never measured directly"
    );
    for overlap in overlaps {
        assert!(
            overlap > 0,
            "new book reached Valid AFTER the old window tore down (overlap {overlap}us) — not zero-gap"
        );
    }
}

fn assert_teardown_measured(observer: &RotationObserver) {
    assert!(
        !observer.teardowns().is_empty(),
        "no teardown observed — a window's close-to-teardown latency was never measured"
    );
    for teardown in observer.teardowns() {
        assert!(
            teardown.latency_us >= 0,
            "teardown confirmed before nominal close — impossible ordering"
        );
    }
}

fn assert_existence(observer: &RotationObserver, fatal: &FatalSignal, persisted: &[u16]) {
    assert!(
        !fatal.is_tripped(),
        "a fatal (mantissa-overflow) parse tripped mid-capture: {:?}",
        fatal.reason()
    );
    let rotations = observer.rotations().len();
    assert!(
        rotations >= 3,
        "need boot + ≥2 rotations, saw {rotations} distinct window subscribes"
    );
    assert!(
        observer.total_messages() > 1_000,
        "only {} messages in 12 minutes — the stream stalled",
        observer.total_messages()
    );

    // Every leg that rotated must have landed at least one lineage row in the parquet, read back
    // from the rotations table.
    let rotated_legs: BTreeSet<usize> = observer
        .rotations()
        .iter()
        .flat_map(|obs| [obs.slot * 2, obs.slot * 2 + 1])
        .collect();
    let persisted_set: BTreeSet<usize> = persisted.iter().map(|id| *id as usize).collect();
    for leg in &rotated_legs {
        assert!(
            persisted_set.contains(leg),
            "leg instrument {leg} rotated but has no rotations-table row — lineage side-channel gap"
        );
    }
}

/// A scratch output tree for the run's parquet. Only [`TempDir::remove`] deletes it, called on the
/// passing path — a panicking assertion leaves the lineage files behind to inspect. No `tempfile`
/// dep is worth adding just to give a test a working directory.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "polysim-integration-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create integration output dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn remove(&self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
