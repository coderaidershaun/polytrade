//! The Parquet files of one table, sealed and rolled as rows arrive, so a reader never waits for
//! shutdown. A file gets its final name once its footer is synced, so each listed file opens.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use arrow_schema::SchemaRef;

use crate::info;
use crate::msg::time::LocalTs;
use crate::runtime::durable_file::{FileSteps, Staging, StepFile, WriteError};

use super::schema::TableRow;
use super::{PersistError, TapeTable};

const ROWS_PER_WRITE: usize = 1000;
const ROWS_PER_SEAL: u64 = 10_000;

const ZSTD_LEVEL: i32 = 3;
const US_PER_HOUR: i64 = 3_600_000_000;

pub(super) const DATE_FOLDER_PREFIX: &str = "date=";
pub(super) const FILE_EXTENSION: &str = "parquet";
/// Added to the name of a file that has no footer yet. Each reader keeps `.parquet` names only.
pub(super) const UNFINISHED_EXTENSION: &str = "part";

/// What each table writer of one run shares: where its files go, the boot stamp in their names,
/// their footer, and the steps that write them.
#[derive(Clone)]
pub(super) struct TableFiles {
    /// `{persistence.dir}/{strategy-id}/{te-id}`.
    pub(super) run_dir: PathBuf,
    pub(super) boot_ts_us: i64,
    pub(super) footer: Vec<(String, String)>,
    pub(super) steps: Arc<dyn FileSteps>,
}

struct OpenFile {
    writer: ArrowWriter<Box<dyn StepFile>>,
    hour_bucket: i64,
    staging: Staging,
    rows_written: u64,
}

pub(super) struct TableWriter<R> {
    files: TableFiles,
    schema: SchemaRef,
    pending: Vec<R>,
    open: Option<OpenFile>,
    next_sequence: u32,
}

impl<R: TableRow> TableWriter<R> {
    pub(super) fn new(files: TableFiles) -> Self {
        Self {
            files,
            schema: R::schema(),
            pending: Vec::new(),
            open: None,
            next_sequence: 0,
        }
    }

    /// Buffer a row, sealing on hour cross or [`ROWS_PER_SEAL`] rows. Records older than open hour land in current file.
    ///
    /// # Errors
    /// File, encode, or write failure.
    pub(super) fn push(&mut self, row: R) -> Result<(), PersistError> {
        let hour_bucket = hour_bucket(row.partition_ts());
        let needs_new_file = match &self.open {
            Some(open) => hour_bucket > open.hour_bucket,
            None => true,
        };
        if needs_new_file {
            self.seal()?;
            self.open_file(hour_bucket)?;
        }
        self.pending.push(row);
        if self.pending.len() >= ROWS_PER_WRITE {
            self.write_pending()?;
        }
        if self.rows_since_seal() >= ROWS_PER_SEAL {
            self.seal()?;
        }
        Ok(())
    }

    /// Writes the remaining rows and the footer, syncs the file, and gives it its final name, at
    /// which point a reader lists it.
    ///
    /// # Errors
    /// Encode, write, sync, or rename failure.
    pub(super) fn seal(&mut self) -> Result<(), PersistError> {
        self.write_pending()?;
        if let Some(open) = self.open.take() {
            let OpenFile {
                mut writer,
                staging,
                rows_written,
                hour_bucket: _,
            } = open;
            writer.finish().map_err(|source| PersistError::Parquet {
                table: R::TABLE,
                source,
            })?;
            staging
                .finish(&*self.files.steps, writer.inner_mut().as_mut())
                .map_err(|error| not_durable(R::TABLE, error))?;
            info!(
                "persist sealed {} {} ({} rows)",
                R::TABLE,
                staging.target.display(),
                rows_written
            );
        }
        Ok(())
    }

    /// Buffered + written rows since seal; cap fires on ACCEPTED rows only.
    fn rows_since_seal(&self) -> u64 {
        let written = self.open.as_ref().map_or(0, |open| open.rows_written);
        written + self.pending.len() as u64
    }

    fn write_pending(&mut self) -> Result<(), PersistError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = self.pending.len() as u64;
        let batch =
            R::to_batch(&self.pending, &self.schema).map_err(|source| PersistError::Batch {
                table: R::TABLE,
                source,
            })?;
        let Some(open) = self.open.as_mut() else {
            debug_assert!(false, "pending rows without an open file");
            return Ok(());
        };
        open.writer
            .write(&batch)
            .map_err(|source| PersistError::Parquet {
                table: R::TABLE,
                source,
            })?;
        open.rows_written += rows;
        self.pending.clear();
        Ok(())
    }

    fn open_file(&mut self, hour_bucket: i64) -> Result<(), PersistError> {
        let target = self.file_path(hour_bucket, self.next_sequence);
        self.next_sequence += 1;
        if let Some(parent) = target.parent() {
            self.files
                .steps
                .create_folder(parent)
                .map_err(PersistError::io_at(R::TABLE, parent))?;
        }
        let staging = Staging {
            temporary: unfinished_path(&target),
            target,
        };
        let file = self
            .files
            .steps
            .create(&staging.temporary)
            .map_err(PersistError::io_at(R::TABLE, &staging.temporary))?;
        let writer =
            ArrowWriter::try_new(file, self.schema.clone(), Some(self.writer_properties()))
                .map_err(|source| PersistError::Parquet {
                    table: R::TABLE,
                    source,
                })?;
        info!("persist open {} {}", R::TABLE, staging.temporary.display());
        self.open = Some(OpenFile {
            writer,
            hour_bucket,
            staging,
            rows_written: 0,
        });
        Ok(())
    }

    fn writer_properties(&self) -> WriterProperties {
        let metadata = self
            .files
            .footer
            .iter()
            .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
            .collect();
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(ZSTD_LEVEL).expect("zstd level 3 is valid"),
            ))
            .set_key_value_metadata(Some(metadata))
            .build()
    }

    /// Zero-padded sequence -> path sort = seal order.
    fn file_path(&self, hour_bucket: i64, sequence: u32) -> PathBuf {
        let at = LocalTs::from_micros(hour_bucket * US_PER_HOUR).civil();
        let mut path = self.files.run_dir.clone();
        path.push(R::TABLE.as_str());
        path.push(format!(
            "{DATE_FOLDER_PREFIX}{:04}-{:02}-{:02}",
            at.year, at.month, at.day
        ));
        path.push(format!(
            "{:02}-{}-{sequence:06}.{FILE_EXTENSION}",
            at.hour, self.files.boot_ts_us
        ));
        path
    }
}

fn unfinished_path(target: &Path) -> PathBuf {
    let mut name = target.as_os_str().to_os_string();
    name.push(".");
    name.push(UNFINISHED_EXTENSION);
    PathBuf::from(name)
}

fn not_durable(table: TapeTable, error: WriteError) -> PersistError {
    match error {
        WriteError::NotWritten { path, source }
        | WriteError::FolderNotSynced {
            directory: path,
            source,
        } => PersistError::Io {
            table,
            path,
            source,
        },
    }
}

fn hour_bucket(ts: LocalTs) -> i64 {
    ts.micros().div_euclid(US_PER_HOUR)
}
