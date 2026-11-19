//! The tape tests read files back as comparable rows and footers, and share one way to start a
//! writer and one scratch folder, removed on drop, so that no `tempfile` dependency is needed.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rtrb::{Consumer, RingBuffer};
use tokio::sync::mpsc;

use engine::msg::ids::FIXED_SCALE;
use engine::msg::persist::{PersistRecord, RotationRow};
use engine::runtime::artifacts::RunArtifacts;
use engine::runtime::config::{ExecutionMode, RecordedTables, RunIdentity, TableKind};
use engine::runtime::durable_file::FileSteps;
use engine::tape::{PersistConfig, PersistError, PersistHandle, PersistWriter, RunMeta};

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

/// One row of a features file with its id replaced by the column name the id stands for.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamedFeature {
    pub event_ts_us: i64,
    pub instrument_id: u16,
    pub name: String,
    pub value_bits: u64,
}

/// The rows of one features file, each id named through the feature dictionary in the footer of
/// that same file. Another file of the same folder may come from a build that declared its columns
/// in another order.
pub fn named_feature_rows(path: &Path) -> Vec<NamedFeature> {
    let file = read_parquet_file(path);
    let dictionary = feature_dictionary(&file);
    let column = |name: &str| {
        file.field_names
            .iter()
            .position(|field| field == name)
            .unwrap_or_else(|| panic!("{} has no column {name}", path.display()))
    };
    let (ts, instrument, feature, value) = (
        column("event_ts_us"),
        column("instrument_id"),
        column("feature_id"),
        column("value"),
    );
    file.rows
        .iter()
        .map(|row| {
            let (
                Cell::I64(event_ts_us),
                Cell::U16(instrument_id),
                Cell::U16(id),
                Cell::F64Bits(value_bits),
            ) = (&row[ts], &row[instrument], &row[feature], &row[value])
            else {
                panic!(
                    "{}: a features row of unexpected cells {row:?}",
                    path.display()
                )
            };
            NamedFeature {
                event_ts_us: *event_ts_us,
                instrument_id: *instrument_id,
                name: dictionary[usize::from(*id)].clone(),
                value_bits: *value_bits,
            }
        })
        .collect()
}

fn feature_dictionary(file: &FileData) -> Vec<String> {
    let json = file
        .footer
        .iter()
        .find_map(|(key, value)| (key == "feature_dictionary").then_some(value.as_deref()))
        .flatten()
        .expect("a tape file's footer holds a feature_dictionary");
    serde_json::from_str(json).expect("the feature_dictionary is a JSON list of names")
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
    let identity = recorder_identity();
    RunMeta {
        artifacts: RunArtifacts::new(Some(ExecutionMode::Live), &identity),
        strategy_id: identity.strategy_id,
        te_id: identity.te_id,
        fixed_scale: FIXED_SCALE,
        engine_version: "test".into(),
        feature_names: vec!["ewma_vol".into()],
        instrument_symbols: vec!["btcusdt".into()],
        asset_symbols: vec![
            ("binance".into(), "btc".into()),
            ("binance".into(), "usdt".into()),
        ],
    }
}

fn recorder_identity() -> RunIdentity {
    RunIdentity::new("recorder", "te-recorder").expect("valid identity")
}

/// Preloads the ring so the actor sees a producer that has already quiesced — the records are all
/// available the moment it starts pumping.
pub(crate) fn spawn_writer(
    dir_root: &Path,
    tables: RecordedTables,
    records: Vec<PersistRecord>,
) -> PersistHandle {
    spawn_writer_with(
        persist_config(dir_root, tables),
        Some(ExecutionMode::Live),
        records,
    )
}

/// Like [`spawn_writer`], but the test chooses the persistence config and the execution mode.
pub(crate) fn spawn_writer_with(
    config: PersistConfig,
    execution_mode: Option<ExecutionMode>,
    records: Vec<PersistRecord>,
) -> PersistHandle {
    PersistWriter::on_disk().spawn(
        config,
        meta_for(execution_mode),
        preloaded(records),
        no_rotations(),
    )
}

/// Like [`spawn_writer`] for the features table alone, but each file step goes through `steps`.
pub(crate) fn spawn_writer_through(
    steps: Arc<dyn FileSteps>,
    dir_root: &Path,
    records: Vec<PersistRecord>,
) -> PersistHandle {
    PersistWriter::through(steps).spawn(
        persist_config(dir_root, RecordedTables::new(&[TableKind::Features])),
        meta_for(Some(ExecutionMode::Live)),
        preloaded(records),
        no_rotations(),
    )
}

/// Like [`spawn_writer`] for the features table alone, with the column names its footer records.
pub(crate) fn spawn_feature_writer(
    dir_root: &Path,
    feature_names: &[&str],
    records: Vec<PersistRecord>,
) -> PersistHandle {
    let meta = RunMeta {
        feature_names: feature_names.iter().map(|name| (*name).into()).collect(),
        ..meta_for(Some(ExecutionMode::Live))
    };
    PersistWriter::on_disk().spawn(
        persist_config(dir_root, RecordedTables::new(&[TableKind::Features])),
        meta,
        preloaded(records),
        no_rotations(),
    )
}

fn persist_config(dir_root: &Path, tables: RecordedTables) -> PersistConfig {
    PersistConfig {
        dir: dir_root.to_path_buf(),
        tables,
        retention: Duration::from_hours(6),
    }
}

fn meta_for(execution_mode: Option<ExecutionMode>) -> RunMeta {
    RunMeta {
        artifacts: RunArtifacts::new(execution_mode, &recorder_identity()),
        ..run_meta()
    }
}

fn preloaded(records: Vec<PersistRecord>) -> Consumer<PersistRecord> {
    let (mut producer, consumer) = RingBuffer::<PersistRecord>::new(records.len() + 1);
    for record in records {
        producer
            .push(record)
            .expect("persist test ring has capacity");
    }
    consumer
}

/// No rotation lineage in these tests: the sender drops at once, so the side channel stays empty.
fn no_rotations() -> mpsc::Receiver<RotationRow> {
    mpsc::channel(1).1
}

/// Drain exactly as the runtime does on SIGTERM.
pub(crate) fn drain_blocking(handle: PersistHandle) -> Result<(), PersistError> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build a current-thread runtime for the persistence drain")
        .block_on(handle.drain())
}
