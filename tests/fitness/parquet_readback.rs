//! Parquet read-back + temp-dir helpers for the persistence fitness tests. Decodes any persisted
//! table's files back into comparable rows through the arrow reader, exposes their footer
//! key-values, and holds a scratch tree removed on drop — no `tempfile` dependency is worth adding
//! just to give a test a working directory. Also holds the writer-spawn fixtures those tests share.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rtrb::RingBuffer;
use tokio::sync::mpsc;

use polysim::config::{ExecutionMode, RecordedTables, StrategyId, TradingEngineId};
use polysim::ids::FIXED_SCALE;
use polysim::msg::persist::PersistRecord;
use polysim::persist::{PersistConfig, PersistError, PersistHandle, PersistWriter, RunMeta};

/// `f64` is stored as bits so cells compare exactly — deterministic replay is bit-for-bit equal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F64Bits(u64),
    Str(String),
    Bool(bool),
    /// A nullable column with nothing in it. Its own cell rather than a zero, because the execution
    /// tables use null for an id the venue never assigned and a zero there would read as a real id.
    Null,
}

pub struct FileData {
    pub field_names: Vec<String>,
    pub footer: Vec<(String, Option<String>)>,
    pub rows: Vec<Vec<Cell>>,
}

pub fn read_parquet_file(path: &Path) -> FileData {
    let file = File::open(path).expect("open parquet file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read parquet footer");
    let field_names = builder
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let footer = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kvs| {
            kvs.iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect()
        })
        .unwrap_or_default();
    let reader = builder.build().expect("build parquet reader");
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.expect("read record batch");
        for row in 0..batch.num_rows() {
            let cells = (0..batch.num_columns())
                .map(|col| cell_at(batch.column(col), row))
                .collect();
            rows.push(cells);
        }
    }
    FileData {
        field_names,
        footer,
        rows,
    }
}

fn cell_at(array: &ArrayRef, row: usize) -> Cell {
    if array.is_null(row) {
        return Cell::Null;
    }
    match array.data_type() {
        DataType::Int32 => Cell::I32(downcast::<Int32Array>(array).value(row)),
        DataType::Int64 => Cell::I64(downcast::<Int64Array>(array).value(row)),
        DataType::UInt8 => Cell::U8(downcast::<UInt8Array>(array).value(row)),
        DataType::UInt16 => Cell::U16(downcast::<UInt16Array>(array).value(row)),
        DataType::UInt32 => Cell::U32(downcast::<UInt32Array>(array).value(row)),
        DataType::UInt64 => Cell::U64(downcast::<UInt64Array>(array).value(row)),
        DataType::Float64 => Cell::F64Bits(downcast::<Float64Array>(array).value(row).to_bits()),
        DataType::Utf8 => Cell::Str(downcast::<StringArray>(array).value(row).to_owned()),
        DataType::Boolean => Cell::Bool(downcast::<BooleanArray>(array).value(row)),
        other => panic!("unexpected parquet column type {other:?}"),
    }
}

fn downcast<A: 'static>(array: &ArrayRef) -> &A {
    array
        .as_any()
        .downcast_ref::<A>()
        .expect("column type matches its arrow data type")
}

/// Every `.parquet` file under `dir` (recursing the `date=` partition dirs), path-sorted —
/// which within a single run is hour order, since one run shares one boot stamp in the name.
pub fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_parquet(dir, &mut files);
    files.sort();
    files
}

fn collect_parquet(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_parquet(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }
}

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "polysim-e2e-{}-{nanos}-{unique}-{tag}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Arbitrary in-range receipt time; the partition it picks doesn't affect the assertions.
pub(crate) const BASE_TS_US: i64 = 3_600_000_000;

/// The `{strategy-id}/{te-id}` pair the writer keys the tree by — a run's own root.
pub(crate) fn run_dir(dir_root: &Path) -> PathBuf {
    dir_root.join("recorder").join("te-recorder")
}

/// Footer metadata for a writer these tests spawn; its feature dictionary is not asserted on.
pub(crate) fn run_meta() -> RunMeta {
    RunMeta {
        strategy_id: StrategyId::new("recorder").expect("valid strategy id"),
        te_id: TradingEngineId::new("te-recorder").expect("valid trading engine id"),
        execution_mode: Some(ExecutionMode::Live),
        fixed_scale: FIXED_SCALE,
        engine_version: "test".into(),
        feature_names: vec!["ewma_vol".into()],
        instrument_symbols: vec!["btcusdt".into()],
        asset_symbols: vec!["btc".into(), "usdt".into()],
    }
}

/// Preloads the ring so the actor sees a producer that has already quiesced — the records are all
/// available the moment it starts pumping.
pub(crate) fn spawn_writer(
    dir_root: &Path,
    tables: RecordedTables,
    records: Vec<PersistRecord>,
) -> PersistHandle {
    let config = PersistConfig {
        dir: dir_root.to_path_buf(),
        tables,
        retention: Duration::from_hours(6),
    };
    spawn_writer_with(config, Some(ExecutionMode::Live), records)
}

/// Like [`spawn_writer`], but the test chooses the persistence config and the execution mode.
pub(crate) fn spawn_writer_with(
    config: PersistConfig,
    execution_mode: Option<ExecutionMode>,
    records: Vec<PersistRecord>,
) -> PersistHandle {
    let (mut producer, consumer) = RingBuffer::<PersistRecord>::new(records.len() + 1);
    for record in records {
        producer
            .push(record)
            .expect("persist test ring has capacity");
    }
    // No rotation lineage in these tests: the sender drops immediately, so the side-channel stays
    // empty.
    let (_, rotations_rx) = mpsc::channel(1);
    let meta = RunMeta {
        execution_mode,
        ..run_meta()
    };
    PersistWriter::spawn(config, meta, consumer, rotations_rx)
}

/// Drain exactly as the runtime does on SIGTERM.
pub(crate) fn drain_blocking(handle: PersistHandle) -> Result<(), PersistError> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build a current-thread runtime for the persistence drain")
        .block_on(handle.drain())
}
