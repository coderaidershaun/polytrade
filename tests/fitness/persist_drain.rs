//! Persistence sealing: research data must be READABLE while the engine runs, not only after
//! shutdown, and a per-table failure must not abandon the other tables' footers. Faults one of
//! two real tables and checks the healthy one still seals; separately drives a table past its row
//! cap and reads the sealed files back mid-run.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use polysim::config::{RecordedTables, TableKind};
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::msg::persist::{FeatureId, FeatureRow, PersistRecord, TradeRow};
use polysim::persist::PersistError;
use polysim::time::TsUs;

use crate::engine_support::ONE;
use crate::parquet_readback::{
    BASE_TS_US, Cell, TempDir, drain_blocking, parquet_files, read_parquet_file, run_dir,
    spawn_writer,
};

/// Mirrors the writer's per-table row cap. Pinned here on purpose: the cap is the promise a reader
/// depends on, so moving it must break this test and be a deliberate act.
const ROWS_PER_SEAL: usize = 10_000;
/// Two full seals plus a remainder, so the run is observed mid-flight AND at drain.
const MID_RUN_ROWS: usize = 2 * ROWS_PER_SEAL + 5_000;
const POLL_DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn one_table_failure_still_seals_the_healthy_table() {
    let dir = TempDir::new("persist-seal");
    // Plant a regular FILE where the trades table's directory must go: every trades file-open
    // then fails with ENOTDIR (no directory can be created under a file). A scoped fault that
    // is uid-independent — a root CI would bypass a read-only dir but cannot mkdir under a
    // file either.
    let run_dir = run_dir(dir.path());
    std::fs::create_dir_all(&run_dir).expect("create run dir");
    std::fs::write(run_dir.join("trades"), b"not a directory").expect("plant trades file");

    let result = drive_drain(dir.path(), records_features_then_trade());

    let error = result.expect_err("a faulted table must make the drain return Err");
    assert!(
        error.to_string().contains("trades"),
        "the drain error names the failing table: {error}"
    );

    let feature_files = parquet_files(&run_dir.join("features"));
    assert_eq!(
        feature_files.len(),
        1,
        "the healthy features table wrote its hour file"
    );
    assert!(
        ParquetRecordBatchReaderBuilder::try_new(
            File::open(&feature_files[0]).expect("open the healthy features file"),
        )
        .is_ok(),
        "the healthy table keeps a valid footer even though a sibling table failed"
    );
    assert_eq!(
        read_parquet_file(&feature_files[0]).rows.len(),
        3,
        "every healthy row is sealed into the footer-closed file"
    );
}

#[test]
fn rows_are_readable_mid_run_once_the_row_cap_is_crossed() {
    let dir = TempDir::new("persist-midrun");
    let run_dir = run_dir(dir.path());
    let features_dir = run_dir.join("features");

    let handle = spawn_writer(
        dir.path(),
        features_and_trades(),
        (0..MID_RUN_ROWS).map(feature_record).collect(),
    );

    // Three files means two are sealed: the third only exists because a row arrived AFTER the
    // second seal closed its predecessor.
    let files = wait_for_files(&features_dir, 3);
    let sealed = &files[..2];
    for path in sealed {
        let data = read_parquet_file(path);
        assert_eq!(
            data.rows.len(),
            ROWS_PER_SEAL,
            "a file sealed mid-run reads back its full row cap without any shutdown: {}",
            path.display()
        );
    }
    assert!(
        parquet_files(&run_dir.join("trades")).is_empty(),
        "a table with no rows never opens a file, so seals cannot litter empty ones"
    );

    drain_blocking(handle).expect("clean drain");

    let files = parquet_files(&features_dir);
    let total: usize = files
        .iter()
        .map(|path| read_parquet_file(path).rows.len())
        .sum();
    assert_eq!(
        total, MID_RUN_ROWS,
        "sealing conserves rows — the batch pending when a seal fires lands in that seal's file"
    );
    let values: Vec<f64> = files
        .iter()
        .flat_map(|path| read_parquet_file(path).rows)
        .map(|row| match row[3] {
            Cell::F64Bits(bits) => f64::from_bits(bits),
            ref other => panic!("features value column is f64, got {other:?}"),
        })
        .collect();
    let expected: Vec<f64> = (0..MID_RUN_ROWS).map(|i| i as f64).collect();
    assert_eq!(
        values, expected,
        "path order is seal order and no row is dropped or duplicated across a seal boundary"
    );
}

/// Polls because the writer thread is genuinely concurrent — the assertion under test is that files
/// appear WITHOUT a drain, so the test may not synchronise by draining.
fn wait_for_files(dir: &Path, want: usize) -> Vec<PathBuf> {
    let start = Instant::now();
    loop {
        let files = parquet_files(dir);
        if files.len() >= want {
            return files;
        }
        assert!(
            start.elapsed() < POLL_DEADLINE,
            "only {} of {want} files appeared under {} within {POLL_DEADLINE:?} — the row cap is \
             not sealing mid-run",
            files.len(),
            dir.display()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn feature_record(index: usize) -> PersistRecord {
    PersistRecord::Feature(FeatureRow {
        instrument: InstrumentId(0),
        feature: FeatureId(0),
        value: index as f64,
        event_ts_us: TsUs::from_micros(BASE_TS_US + index as i64),
    })
}

/// Features rows then a trades row: the healthy table opens its file before the trades open
/// faults, so a naive abort-on-first-error would abandon the features footer.
fn records_features_then_trade() -> Vec<PersistRecord> {
    let mut records: Vec<PersistRecord> = (0..3).map(feature_record).collect();
    records.push(PersistRecord::Trade(TradeRow {
        instrument: InstrumentId(0),
        price: Price(100 * ONE),
        qty: Qty(ONE),
        side: Side::Buy,
        exchange_ts_us: TsUs::from_micros(BASE_TS_US),
        received_ts_us: TsUs::from_micros(BASE_TS_US),
    }));
    records
}

/// Spawn the actor, then drain it exactly as the runtime does on SIGTERM.
fn drive_drain(dir_root: &Path, records: Vec<PersistRecord>) -> Result<(), PersistError> {
    drain_blocking(spawn_writer(dir_root, features_and_trades(), records))
}

/// Trades is named but never fed, so its file-open stays a LAZY one — the property
/// `rows_are_readable_mid_run` asserts on. Gating has its own module.
fn features_and_trades() -> RecordedTables {
    RecordedTables::new(&[TableKind::Features, TableKind::Trades])
}
