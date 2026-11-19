//! A tape file takes its final name only once its footer is on disk, so a process that ends at any
//! write step leaves no file that a reader lists and cannot open.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine::msg::ids::InstrumentId;
use engine::msg::persist::{FeatureId, FeatureRow, ResearchRecord};
use engine::msg::time::LocalTs;
use engine::runtime::config::{RecordedTables, TableKind};
use engine::runtime::durable_file::{DiskFiles, FileSteps, StepFile};

use crate::parquet_readback::{
    BASE_TS_US, Cell, Record, TempDir, drain_blocking, parquet_files, read_parquet_file, run_dir,
    spawn_writer, spawn_writer_through, table_files,
};

/// Fewer rows than the row cap, all in one hour, so they make one file, and enough bytes that
/// they reach the file in more than one write.
const ONE_FILE_ROWS: usize = 5_000;
/// The writer's row cap, pinned here on purpose: a reader depends on it, so a change of the cap
/// must break this test.
const ROWS_PER_SEAL: usize = 10_000;
const MID_RUN_ROWS: usize = 2 * ROWS_PER_SEAL + 5_000;
const POLL_DEADLINE: Duration = Duration::from_secs(30);
const UNFINISHED_EXTENSION: &str = "part";

/// Silent corruption guarded: a file under its final name with no footer is listed by each reader
/// and cannot be opened, so a crash would hide every row of it behind a read error.
#[test]
fn kill_at_each_write_step_leaves_no_file_without_footer() {
    let steps = steps_of_one_file();
    let created = steps
        .iter()
        .position(|step| *step == Step::Create)
        .expect("the writer creates its file")
        + 1;
    for allowed in created..=steps.len() {
        assert_a_kill_leaves_no_file_without_footer(&steps, allowed);
    }
    assert_sealed_files_read_back_mid_run();
}

fn steps_of_one_file() -> Vec<Step> {
    let dir = TempDir::new("tape-steps");
    let full_run = EndsAfter::steps(usize::MAX);
    assert_eq!(
        write_one_file(dir.path(), &full_run),
        0,
        "a writer that is not killed stops no table"
    );
    let steps = full_run.taken();
    let writes = steps.iter().filter(|step| **step == Step::Write).count();
    assert!(
        writes > 1,
        "the rows reach the file in more than one write, so a kill can fall between the rows and \
         the footer: {steps:?}"
    );
    steps
}

fn assert_a_kill_leaves_no_file_without_footer(steps: &[Step], allowed: usize) {
    let dir = TempDir::new("tape-kill");
    // The only table of these runs, so each file below the runs is one of its files.
    let features = run_dir(dir.path());
    let stopped = write_one_file(dir.path(), &EndsAfter::steps(allowed));
    let taken = &steps[..allowed];
    let last = taken.last().expect("the kill comes after a step");
    assert_eq!(
        stopped == 0,
        allowed == steps.len(),
        "the table stops exactly when a step after {last:?} was refused: {stopped} stopped"
    );
    let complete = parquet_files(&features);
    let unfinished = unfinished_files(&features);
    if taken.contains(&Step::Rename) {
        assert!(
            unfinished.is_empty(),
            "after the rename no unfinished file is left: {unfinished:?}"
        );
        assert_eq!(
            complete.len(),
            1,
            "after the rename one file has its final name"
        );
        assert_eq!(
            feature_values(&complete[0]),
            expected_values(0..ONE_FILE_ROWS),
            "the renamed file has its footer and each row in order"
        );
    } else {
        assert!(
            complete.is_empty(),
            "a kill after step {} ({last:?}) leaves a file with its final name and no footer: \
             {complete:?}",
            taken.len()
        );
        assert_eq!(
            unfinished.len(),
            1,
            "a kill after step {} ({last:?}) leaves one unfinished file",
            taken.len()
        );
    }
    let next_start = spawn_writer(dir.path(), features_only(), Vec::new());
    drain_blocking(next_start).expect("a writer with no rows drains clean");
    assert!(
        unfinished_files(&features).is_empty(),
        "the next start removes each unfinished file of the killed run"
    );
}

/// The writer seals a file at the row cap, and a reader opens it while the run goes on.
fn assert_sealed_files_read_back_mid_run() {
    let dir = TempDir::new("tape-midrun");
    let features = run_dir(dir.path());
    let records = (0..MID_RUN_ROWS).map(feature_record).collect();
    let handle = spawn_writer(dir.path(), features_and_trades(), records);

    let sealed = wait_for_complete_files(&features, 2);
    assert_eq!(
        sealed.len(),
        2,
        "the third file is still open, so no reader lists it"
    );
    for (index, path) in sealed.iter().enumerate() {
        let first = index * ROWS_PER_SEAL;
        assert_eq!(
            feature_values(path),
            expected_values(first..first + ROWS_PER_SEAL),
            "a file sealed at the row cap reads back during the run, each row in order: {}",
            path.display()
        );
    }
    assert!(
        table_files(dir.path(), "trades").is_empty(),
        "a table with no rows never opens a file"
    );

    drain_blocking(handle).expect("clean drain");
    let values: Vec<u64> = parquet_files(&features)
        .iter()
        .flat_map(|path| feature_values(path))
        .collect();
    assert_eq!(
        values,
        expected_values(0..MID_RUN_ROWS),
        "path order is seal order, and no row is dropped or repeated across a seal"
    );
}

/// The count of tables that the writer stopped. The features table is research data, so its fault
/// stops it alone and the drain stays clean.
fn write_one_file(dir_root: &Path, steps: &EndsAfter) -> u32 {
    let records = (0..ONE_FILE_ROWS).map(feature_record).collect();
    let handle = spawn_writer_through(Arc::new(steps.clone()), dir_root, records);
    let stopped = handle.stopped_tables();
    drain_blocking(handle).expect("a fault of a research table leaves the drain clean");
    stopped.count()
}

/// Polls, because the claim is that files appear without a drain.
fn wait_for_complete_files(dir: &Path, want: usize) -> Vec<PathBuf> {
    let start = Instant::now();
    loop {
        let files = parquet_files(dir);
        if files.len() >= want {
            return files;
        }
        assert!(
            start.elapsed() < POLL_DEADLINE,
            "only {} of {want} complete files appeared under {} within {POLL_DEADLINE:?}",
            files.len(),
            dir.display()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn unfinished_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(unfinished_files(&path));
        } else if path
            .extension()
            .is_some_and(|extension| extension == UNFINISHED_EXTENSION)
        {
            found.push(path);
        }
    }
    found
}

fn feature_values(path: &Path) -> Vec<u64> {
    read_parquet_file(path)
        .rows
        .into_iter()
        .map(|row| match row[3] {
            Cell::F64Bits(bits) => bits,
            ref other => panic!("the features value column is f64, got {other:?}"),
        })
        .collect()
}

fn expected_values(indexes: std::ops::Range<usize>) -> Vec<u64> {
    indexes
        .map(|index| feature_value(index).to_bits())
        .collect()
}

/// A value whose bytes do not compress, so the rows of one file take more than one write.
fn feature_value(index: usize) -> f64 {
    (index as f64).sin()
}

fn feature_record(index: usize) -> Record {
    Record::from(ResearchRecord::Feature(FeatureRow {
        instrument: InstrumentId(0),
        feature: FeatureId(0),
        value: feature_value(index),
        event_ts_us: LocalTs::from_micros(BASE_TS_US + index as i64),
    }))
}

fn features_only() -> RecordedTables {
    RecordedTables::new(&[TableKind::Features])
}

fn features_and_trades() -> RecordedTables {
    RecordedTables::new(&[TableKind::Features, TableKind::Trades])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    CreateFolder,
    Create,
    Write,
    Sync,
    Rename,
    SyncFolder,
    Remove,
}

/// The real file system until the process ends: each step after the first `allowed` is refused,
/// as nothing reaches the disk after a kill.
#[derive(Clone)]
struct EndsAfter {
    allowed: usize,
    taken: Arc<Mutex<Vec<Step>>>,
}

impl EndsAfter {
    fn steps(allowed: usize) -> Self {
        Self {
            allowed,
            taken: Arc::default(),
        }
    }

    fn taken(&self) -> Vec<Step> {
        self.taken
            .lock()
            .expect("the step log is not poisoned")
            .clone()
    }

    fn take(&self, step: Step) -> io::Result<()> {
        let mut taken = self.taken.lock().expect("the step log is not poisoned");
        if taken.len() >= self.allowed {
            return Err(io::Error::other("the process has ended"));
        }
        taken.push(step);
        Ok(())
    }
}

impl FileSteps for EndsAfter {
    fn create_folder(&self, folder: &Path) -> io::Result<()> {
        self.take(Step::CreateFolder)?;
        DiskFiles.create_folder(folder)
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn StepFile>> {
        self.take(Step::Create)?;
        let file = DiskFiles.create(path)?;
        Ok(Box::new(EndingFile {
            file,
            steps: self.clone(),
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.take(Step::Rename)?;
        DiskFiles.rename(from, to)
    }

    fn sync_folder(&self, folder: &Path) -> io::Result<()> {
        self.take(Step::SyncFolder)?;
        DiskFiles.sync_folder(folder)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.take(Step::Remove)?;
        DiskFiles.remove(path)
    }
}

struct EndingFile {
    file: Box<dyn StepFile>,
    steps: EndsAfter,
}

impl Write for EndingFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.steps.take(Step::Write)?;
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl StepFile for EndingFile {
    fn sync(&mut self) -> io::Result<()> {
        self.steps.take(Step::Sync)?;
        self.file.sync()
    }
}
