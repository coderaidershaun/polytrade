//! End-to-end replay fitness: the real runtime below the binary — input rings, hot
//! thread, `PersistWriter` — driven by one fixed sequence through a single queue (so order is
//! deterministic), drained like a SIGTERM, and read back. Covers schema, footer, rotation,
//! drain-flush completeness, and cross-run determinism.

use std::path::Path;
use std::time::Duration;

use rtrb::RingBuffer;
use tokio::sync::mpsc;

use polysim::config::{ExecutionMode, RecordedTables, StrategyId, TableKind, TradingEngineId};
use polysim::hot::ingress::IngressQueues;
use polysim::hot::metrics::{Category, MetricsSnapshot};
use polysim::hot::spawn::{HotThreadConfig, QueueProducer, spawn_hot_thread};
use polysim::hot::strategy::StrategyConfig;
use polysim::ids::{FIXED_SCALE, QueueId, SourceId};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::persist::RotationRow;
use polysim::persist::{PersistConfig, PersistWriter, RunMeta};
use polysim::shutdown::{DrainSignal, FatalSignal};

use crate::e2e_scenario::{HOUR_US, ROTATIONS, message_sequence};
use crate::engine_support::{
    ALL_TABLES, NOMINAL_SPIN, engine_view, engine_without_warmup, instrument_row, metrics_ring,
    persist_ring, recorder_spec, strategy_log_ring, tracker_spec_all,
};
use crate::parquet_readback::{
    Cell, FileData, TempDir, drain_blocking, parquet_files, read_parquet_file, run_dir,
};
use crate::raw_recorder::RecorderStrategy;

/// What a clean run must produce: column names, the rotation-timestamp column, and the exact row
/// count — a full persistence ring would leave the count short.
struct TableExpect {
    name: &'static str,
    fields: &'static [&'static str],
    partition_col: usize,
    total_rows: usize,
}

const TABLES: [TableExpect; 5] = [
    TableExpect {
        name: "trades",
        fields: &[
            "exchange_ts_us",
            "received_ts_us",
            "instrument_id",
            "price",
            "qty",
            "side",
        ],
        partition_col: 1,
        total_rows: 4,
    },
    TableExpect {
        name: "book_events",
        fields: &[
            "received_ts_us",
            "instrument_id",
            "kind",
            "side",
            "price",
            "qty",
            "update_id",
        ],
        partition_col: 0,
        total_rows: 14,
    },
    TableExpect {
        name: "klines",
        fields: &[
            "exchange_ts_us",
            "received_ts_us",
            "instrument_id",
            "interval",
            "open_ts_us",
            "open",
            "high",
            "low",
            "close",
            "base_volume",
            "quote_volume",
            "trade_count",
            "is_closed",
        ],
        partition_col: 1,
        total_rows: 4,
    },
    TableExpect {
        name: "features",
        fields: &["event_ts_us", "instrument_id", "feature_id", "value"],
        partition_col: 0,
        // Two per epoch: each epoch opens with a `MarketRotation` that wipes the slot's EwmaVol, so
        // the feature only appears once a fresh return chain has two microprices — no variance
        // carries across the window boundary (a rotation is a new distribution).
        total_rows: 4,
    },
    TableExpect {
        name: "rotations",
        fields: &[
            "received_ts_us",
            "instrument_id",
            "window_open_ts_us",
            "window_close_ts_us",
            "token_id_up",
            "token_id_down",
            "condition_id",
        ],
        partition_col: 0,
        total_rows: 2,
    },
];

#[test]
fn e2e_reads_back_every_table_and_drains_clean() {
    let dir = TempDir::new("readback");
    let outcome = run_e2e(dir.path());

    assert!(
        !outcome.fatal_tripped,
        "a clean run must never trip the input-queue fatal signal"
    );

    for expect in &TABLES {
        let files = read_table(dir.path(), expect.name);
        assert_eq!(
            files.len(),
            2,
            "{} must rotate across the hour boundary into two files",
            expect.name
        );
        for file in &files {
            let names: Vec<&str> = file.field_names.iter().map(String::as_str).collect();
            assert_eq!(
                names.as_slice(),
                expect.fields,
                "{} schema field names roundtrip",
                expect.name
            );
        }

        let mut buckets: Vec<i64> = files
            .iter()
            .map(|file| file_hour_bucket(file, expect.partition_col))
            .collect();
        buckets.sort_unstable();
        assert_eq!(
            buckets,
            vec![1, 2],
            "{} files partition cleanly on the two hour buckets",
            expect.name
        );

        let total: usize = files.iter().map(|file| file.rows.len()).sum();
        assert_eq!(
            total, expect.total_rows,
            "{} persisted every emitted row (no drops on drain)",
            expect.name
        );

        assert_footer(&files[0].footer);
    }

    // The lineage side-channel landed both rotations, in hour order, with their window bounds and
    // venue strings intact — this is the whole point of the second persistence pathway.
    let rotations = table_rows(dir.path(), "rotations");
    assert_eq!(
        rotations.len(),
        2,
        "both rotations persisted, one per hour bucket"
    );
    for (row, fixture) in rotations.iter().zip(&ROTATIONS) {
        assert_eq!(row[0], Cell::I64(fixture.received_ts_us), "received_ts");
        assert_eq!(row[1], Cell::U16(fixture.instrument), "instrument");
        assert_eq!(row[2], Cell::I64(fixture.window_open_ts_us), "window open");
        assert_eq!(
            row[3],
            Cell::I64(fixture.window_close_ts_us),
            "window close"
        );
        assert_eq!(row[4], Cell::Str(fixture.token_up.to_owned()), "token up");
        assert_eq!(
            row[5],
            Cell::Str(fixture.token_down.to_owned()),
            "token down"
        );
        assert_eq!(
            row[6],
            Cell::Str(fixture.condition_id.to_owned()),
            "condition id"
        );
    }

    assert!(
        !outcome.snapshots.is_empty(),
        "at least one metrics snapshot reached the actor ring"
    );
    assert!(
        outcome
            .snapshots
            .iter()
            .any(|snapshot| snapshot.is_active(Category::Spin)),
        "the spin category is active in a snapshot — the timer path flowed"
    );
    for snapshot in &outcome.snapshots {
        assert_eq!(
            snapshot.counters.persist_dropped, 0,
            "no persistence records dropped"
        );
        assert_eq!(
            snapshot.counters.snapshots_dropped, 0,
            "no metrics snapshots dropped"
        );
        assert!(
            snapshot.queue_count >= 1,
            "occupancy recorded for the input queue"
        );
    }
}

#[test]
fn e2e_replay_is_identical_across_runs() {
    let dir_a = TempDir::new("determinism-a");
    let dir_b = TempDir::new("determinism-b");
    run_e2e(dir_a.path());
    run_e2e(dir_b.path());

    for expect in &TABLES {
        let rows_a = table_rows(dir_a.path(), expect.name);
        let rows_b = table_rows(dir_b.path(), expect.name);
        assert!(!rows_a.is_empty(), "{} produced rows", expect.name);
        assert_eq!(
            rows_a, rows_b,
            "{} row contents differ between two runs of the same input sequence, so the run is \
             reading something the tape does not carry",
            expect.name
        );
    }
}

struct RunOutcome {
    snapshots: Vec<MetricsSnapshot>,
    fatal_tripped: bool,
}

/// Drain exactly as the runtime does on SIGTERM: stop feeding, request drain, join the hot thread
/// (closing both output rings), then drain persistence so every footer is written.
fn run_e2e(dir_root: &Path) -> RunOutcome {
    let fatal = FatalSignal::new();
    let drain = DrainSignal::new();

    let (input_producer, input_consumer) = RingBuffer::<InboundMessage>::new(1024);
    let ingress = IngressQueues::new(vec![input_consumer]);
    let mut producer = QueueProducer::new(input_producer, fatal.clone(), QueueId(0), SourceId(0));

    let (sink, persist_consumer) = persist_ring(4096);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, mut metrics_consumer) = metrics_ring(64);

    let instruments = [instrument_row(0, tracker_spec_all(2), 64)];
    let strategy = RecorderStrategy::from_spec(
        &recorder_spec(vec![
            TableKind::Trades,
            TableKind::BookEvents,
            TableKind::Klines,
            TableKind::Features,
        ]),
        engine_view(NOMINAL_SPIN),
    );
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(strategy),
        sink,
        log_sink,
        metrics_producer,
    );

    let feature_names: Vec<Box<str>> = engine
        .feature_names()
        .iter()
        .map(|name| (*name).into())
        .collect();
    let meta = RunMeta {
        strategy_id: StrategyId::new("recorder").expect("valid strategy id"),
        te_id: TradingEngineId::new("te-recorder").expect("valid trading engine id"),
        execution_mode: Some(ExecutionMode::Live),
        fixed_scale: FIXED_SCALE,
        engine_version: env!("CARGO_PKG_VERSION").into(),
        feature_names,
        instrument_symbols: instruments
            .iter()
            .map(|row| row.venue_symbol.clone())
            .collect(),
        asset_symbols: Vec::new(),
    };
    let (rotations_tx, rotations_rx) = mpsc::channel::<RotationRow>(64);
    let persistence = PersistWriter::spawn(
        PersistConfig {
            dir: dir_root.to_path_buf(),
            tables: RecordedTables::new(&ALL_TABLES),
            retention: Duration::from_hours(6),
        },
        meta,
        persist_consumer,
        rotations_rx,
    );

    let hot = spawn_hot_thread(
        HotThreadConfig {
            core_id: None,
            tag: "e2e-hot",
        },
        ingress,
        fatal.clone(),
        drain.clone(),
        move |pop, message| engine.dispatch(pop, &message),
    );

    for message in message_sequence() {
        producer.push(message);
    }
    // Mirror the adapter: each rotation feeds a `MarketRotation` through the hot rings (above) AND a
    // matching `RotationRow` down the lineage side-channel. Sent in hour order so the sink rotates.
    for fixture in ROTATIONS {
        rotations_tx
            .try_send(fixture.row())
            .expect("rotations side-channel has capacity");
    }
    // Quiesce the side-channel before the drain, exactly as adapters stop before persistence drains.
    drop(rotations_tx);
    drain.request();
    hot.join().expect("hot thread panicked");

    drain_blocking(persistence).expect("persistence drains and closes every file");

    let mut snapshots = Vec::new();
    while let Ok(snapshot) = metrics_consumer.pop() {
        snapshots.push(snapshot);
    }

    RunOutcome {
        snapshots,
        fatal_tripped: fatal.is_tripped(),
    }
}

/// Read a table's files (path-sorted order is hour order within a run) back through the arrow reader.
fn read_table(dir_root: &Path, table: &str) -> Vec<FileData> {
    let dir = run_dir(dir_root).join(table);
    parquet_files(&dir)
        .iter()
        .map(|path| read_parquet_file(path))
        .collect()
}

/// A table's rows across its files in hour order — the sequence compared for replay determinism.
fn table_rows(dir_root: &Path, table: &str) -> Vec<Vec<Cell>> {
    read_table(dir_root, table)
        .into_iter()
        .flat_map(|file| file.rows)
        .collect()
}

/// The single hour bucket every row in a file shares (rotation must never mix hours in a file).
fn file_hour_bucket(file: &FileData, partition_col: usize) -> i64 {
    let mut bucket = None;
    for row in &file.rows {
        let Cell::I64(ts) = row[partition_col] else {
            panic!("partition column is not an i64 timestamp");
        };
        let hour = ts.div_euclid(HOUR_US);
        match bucket {
            Some(existing) => assert_eq!(existing, hour, "a file mixes two hour buckets"),
            None => bucket = Some(hour),
        }
    }
    bucket.expect("a rotated file holds at least one row")
}

fn assert_footer(footer: &[(String, Option<String>)]) {
    let value = |key: &str| {
        footer
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.clone())
    };
    assert_eq!(value("strategy_id").as_deref(), Some("recorder"));
    assert_eq!(value("te_id").as_deref(), Some("te-recorder"));
    assert_eq!(
        value("fixed_scale").as_deref(),
        Some(FIXED_SCALE.to_string().as_str())
    );
    assert_eq!(
        value("engine_version").as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert!(
        value("feature_dictionary")
            .expect("feature dictionary present")
            .contains("ewma_vol"),
        "feature dictionary carries the strategy's declared feature"
    );
    assert!(
        value("instrument_dictionary")
            .expect("instrument dictionary present")
            .contains("btcusdt"),
        "instrument dictionary carries the venue symbol"
    );
}
