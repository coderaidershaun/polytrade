//! A fault of one table does not stop the other tables from sealing.

use std::fs::File;
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use engine::msg::ids::{InstrumentId, Price, Qty, Side};
use engine::msg::persist::{FeatureId, FeatureRow, PersistRecord, TradeRow};
use engine::msg::time::{LocalTs, VenueTs};
use engine::runtime::config::{RecordedTables, TableKind};
use engine::tape::PersistError;

use crate::engine_support::ONE;
use crate::parquet_readback::{
    BASE_TS_US, TempDir, drain_blocking, parquet_files, read_parquet_file, run_dir, spawn_writer,
};

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

fn feature_record(index: usize) -> PersistRecord {
    PersistRecord::Feature(FeatureRow {
        instrument: InstrumentId(0),
        feature: FeatureId(0),
        value: index as f64,
        event_ts_us: LocalTs::from_micros(BASE_TS_US + index as i64),
    })
}

/// Features rows then a trades row: the healthy table opens its file before the trades open
/// faults, so a naive abort-on-first-error would abandon the features footer.
fn records_features_then_trade() -> Vec<PersistRecord> {
    let mut records: Vec<PersistRecord> = (0..3).map(feature_record).collect();
    records.push(PersistRecord::Trade(TradeRow {
        instrument: InstrumentId(0),
        price: Price::from_mantissa(100 * ONE),
        qty: Qty::from_mantissa(ONE),
        side: Side::Buy,
        exchange_ts_us: VenueTs::from_micros(BASE_TS_US),
        received_ts_us: LocalTs::from_micros(BASE_TS_US),
    }));
    records
}

/// Spawn the actor, then drain it exactly as the runtime does on SIGTERM.
fn drive_drain(dir_root: &Path, records: Vec<PersistRecord>) -> Result<(), PersistError> {
    drain_blocking(spawn_writer(dir_root, features_and_trades(), records))
}

fn features_and_trades() -> RecordedTables {
    RecordedTables::new(&[TableKind::Features, TableKind::Trades])
}
