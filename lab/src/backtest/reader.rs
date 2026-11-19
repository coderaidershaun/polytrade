//! Reads the tape of a run back in its order, one row group at a time: a second thread decodes the
//! next group while the caller folds the current one, so a long tape costs two row groups of memory.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt8Type, UInt16Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use engine::msg::ids::QueueId;
use engine::msg::inbound::InboundMessage;
use engine::msg::persist::BootRecord;
use engine::msg::time::LocalTs;
use engine::tape::{TapePayload, decode_payload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::error::ReplayError;

const TAPE_TABLE: &str = "tape";
const FINAL_EXTENSION: &str = "parquet";

/// One row of the tape, decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct TapeRow {
    pub seq: u64,
    pub kind: u16,
    /// The queue that the message was popped from, and `None` for a row that no queue gave.
    pub queue: Option<QueueId>,
    pub received_ts_us: LocalTs,
    pub queued_ts_us: LocalTs,
    pub payload: TapePayload,
}

impl TapeRow {
    /// The message as the hot thread popped it, and `None` for a boot, snapshot or gap row.
    pub fn message(&self) -> Option<InboundMessage> {
        let TapePayload::Message(body) = &self.payload else {
            return None;
        };
        let mut message = InboundMessage::new(self.received_ts_us, *body);
        message.header.queued_ts_us = self.queued_ts_us;
        Some(message)
    }
}

/// The rows of one tape in seal order, the boot row taken out first.
pub struct TapeReader {
    boot: BootRecord,
    current: Option<HeldGroup>,
    next_groups: Receiver<Result<HeldGroup, ReplayError>>,
    held: Arc<GroupCount>,
}

impl TapeReader {
    /// # Errors
    /// [`ReplayError`] when the run has no tape, a file cannot be read, or the tape does not open
    /// with its boot row.
    pub fn open(run_dir: &Path) -> Result<TapeReader, ReplayError> {
        Self::of_files(tape_files(run_dir)?)
    }

    /// The reader of one listing of a tape, so a re-fold folds the files that it checked.
    ///
    /// # Errors
    /// [`ReplayError`] when a file cannot be read, or the tape does not open with its boot row.
    pub(super) fn of_files(files: Vec<PathBuf>) -> Result<TapeReader, ReplayError> {
        let Some(first) = files.first().cloned() else {
            return Err(ReplayError::NoTape {
                dir: PathBuf::new(),
            });
        };
        let held = Arc::new(GroupCount::default());
        let (sender, next_groups) = sync_channel(0);
        let decoder_count = Arc::clone(&held);
        thread::Builder::new()
            .name("tape-reader".to_owned())
            .spawn(move || decode_files(&files, &sender, &decoder_count))
            .expect("the replay can start one reader thread");
        let mut reader = TapeReader {
            boot: BootRecord::default(),
            current: None,
            next_groups,
            held,
        };
        reader.boot = match reader.next() {
            Some(Ok(TapeRow {
                payload: TapePayload::Boot(boot),
                ..
            })) => boot,
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) | None => return Err(ReplayError::BootMissing { file: first }),
        };
        Ok(reader)
    }

    /// The start state of the fold: the nonce of each venue that traded and each restored position.
    pub fn boot(&self) -> &BootRecord {
        &self.boot
    }

    /// The most row groups that were decoded and not yet dropped at one time.
    pub fn most_groups_held(&self) -> usize {
        self.held.most.load(Ordering::Acquire)
    }
}

impl Iterator for TapeReader {
    type Item = Result<TapeRow, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.current.as_mut().and_then(|group| group.rows.next()) {
                return Some(Ok(row));
            }
            // Dropped before the next one is taken, so the caller never holds two groups and the
            // thread a third.
            self.current = None;
            match self.next_groups.recv() {
                Ok(Ok(group)) => self.current = Some(group),
                Ok(Err(error)) => return Some(Err(error)),
                Err(_) => return None,
            }
        }
    }
}

/// How many row groups are alive at once, and the most there ever were.
#[derive(Default)]
struct GroupCount {
    alive: AtomicUsize,
    most: AtomicUsize,
}

/// The decoded rows of one row group. It counts itself alive from the start of its decode to its
/// drop.
struct HeldGroup {
    rows: std::vec::IntoIter<TapeRow>,
    count: Arc<GroupCount>,
}

impl HeldGroup {
    fn start(count: &Arc<GroupCount>) -> Arc<GroupCount> {
        let alive = count.alive.fetch_add(1, Ordering::AcqRel) + 1;
        count.most.fetch_max(alive, Ordering::AcqRel);
        Arc::clone(count)
    }
}

impl Drop for HeldGroup {
    fn drop(&mut self) {
        self.count.alive.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Decodes each row group of each file in turn, and stops at the first error or when the reader is
/// gone.
fn decode_files(
    files: &[PathBuf],
    sender: &SyncSender<Result<HeldGroup, ReplayError>>,
    count: &Arc<GroupCount>,
) {
    for file in files {
        let groups = match row_group_count(file) {
            Ok(groups) => groups,
            Err(error) => {
                // A failed send means the reader is gone, and then nobody waits for this error.
                sender.send(Err(error)).ok();
                return;
            }
        };
        for group in 0..groups {
            let held = HeldGroup::start(count);
            let decoded = decode_group(file, group).map(|rows| HeldGroup {
                rows: rows.into_iter(),
                count: held,
            });
            let is_error = decoded.is_err();
            if sender.send(decoded).is_err() || is_error {
                return;
            }
        }
    }
}

/// The final tape files of a run, in seal order: the number at the end of each name.
///
/// # Errors
/// [`ReplayError::NoTape`] when the run holds no sealed tape file.
pub(super) fn tape_files(run_dir: &Path) -> Result<Vec<PathBuf>, ReplayError> {
    let root = run_dir.join(TAPE_TABLE);
    let mut files = Vec::new();
    for date in read_dir(&root) {
        for path in read_dir(&date) {
            if path.extension().is_some_and(|ext| ext == FINAL_EXTENSION) {
                files.push(path);
            }
        }
    }
    files.sort_by_key(|path| seal_number(path));
    match files.is_empty() {
        true => Err(ReplayError::NoTape { dir: root }),
        false => Ok(files),
    }
}

fn read_dir(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| Some(entry.ok()?.path()))
        .collect();
    paths.sort();
    paths
}

fn seal_number(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.rsplit('-').next()?.parse().ok()
}

/// The reader of one file, its footer already read.
///
/// # Errors
/// [`ReplayError::Read`] when the file cannot be opened or its footer read.
pub(super) fn open_file(file: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>, ReplayError> {
    let read_error = |source| ReplayError::Read {
        file: file.to_path_buf(),
        source,
    };
    let handle = File::open(file).map_err(|error| read_error(error.into()))?;
    ParquetRecordBatchReaderBuilder::try_new(handle).map_err(read_error)
}

fn row_group_count(file: &Path) -> Result<usize, ReplayError> {
    Ok(open_file(file)?.metadata().num_row_groups())
}

fn decode_group(file: &Path, group: usize) -> Result<Vec<TapeRow>, ReplayError> {
    let reader = open_file(file)?
        .with_row_groups(vec![group])
        .build()
        .map_err(|source| ReplayError::Read {
            file: file.to_path_buf(),
            source,
        })?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|source| ReplayError::Read {
            file: file.to_path_buf(),
            source: source.into(),
        })?;
        decode_batch(file, &batch, &mut rows)?;
    }
    Ok(rows)
}

fn decode_batch(
    file: &Path,
    batch: &RecordBatch,
    rows: &mut Vec<TapeRow>,
) -> Result<(), ReplayError> {
    let wrong = |index: usize, wanted: &str| wrong_column(file, batch, index, wanted);
    let seqs = batch
        .column(0)
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| wrong(0, "uint64"))?;
    let kinds = batch
        .column(1)
        .as_primitive_opt::<UInt16Type>()
        .ok_or_else(|| wrong(1, "uint16"))?;
    let queues = batch
        .column(2)
        .as_primitive_opt::<UInt8Type>()
        .ok_or_else(|| wrong(2, "uint8"))?;
    let received = batch
        .column(3)
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| wrong(3, "int64"))?;
    let queued = batch
        .column(4)
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| wrong(4, "int64"))?;
    let payloads = batch
        .column(5)
        .as_binary_opt::<i32>()
        .ok_or_else(|| wrong(5, "binary"))?;
    for row in 0..batch.num_rows() {
        let seq = seqs.value(row);
        let kind = kinds.value(row);
        let payload = decode_payload(kind, payloads.value(row))
            .map_err(|source| ReplayError::Decode { seq, kind, source })?;
        let queue = match queues.is_null(row) {
            true => None,
            false => Some(
                QueueId::new(usize::from(queues.value(row))).map_err(|error| {
                    ReplayError::Column {
                        file: file.to_path_buf(),
                        detail: format!("row seq {seq}: {error}").into(),
                    }
                })?,
            ),
        };
        rows.push(TapeRow {
            seq,
            kind,
            queue,
            received_ts_us: LocalTs::from_micros(received.value(row)),
            queued_ts_us: LocalTs::from_micros(queued.value(row)),
            payload,
        });
    }
    Ok(())
}

fn wrong_column(file: &Path, batch: &RecordBatch, index: usize, wanted: &str) -> ReplayError {
    let field = batch.schema_ref().field(index).clone();
    ReplayError::Column {
        file: file.to_path_buf(),
        detail: format!(
            "column {index} `{}` is {}, not {wanted}",
            field.name(),
            field.data_type()
        )
        .into(),
    }
}
