//! One table's Parquet files: sealed and rolled as rows arrive, so a reader never waits for shutdown. Seals on row cap, parent interval, record hour, or drain.
//! Names carry boot stamp + per-writer sequence -> no overwrites within an hour.

use std::fs::File;
use std::path::{Path, PathBuf};

use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use arrow_schema::SchemaRef;

use crate::info;
use crate::time::TsUs;

use super::PersistError;
use super::schema::TableRow;

const ROWS_PER_WRITE: usize = 1000;
const ROWS_PER_SEAL: u64 = 10_000;

const ZSTD_LEVEL: i32 = 3;
const US_PER_HOUR: i64 = 3_600_000_000;

pub(super) const DATE_FOLDER_PREFIX: &str = "date=";
pub(super) const FILE_EXTENSION: &str = "parquet";

struct OpenFile {
    writer: ArrowWriter<File>,
    hour_bucket: i64,
    path: PathBuf,
    rows_written: u64,
}

pub(super) struct TableWriter<R> {
    run_dir: PathBuf,
    boot_ts_us: i64,
    schema: SchemaRef,
    footer: Vec<(String, String)>,
    pending: Vec<R>,
    open: Option<OpenFile>,
    next_sequence: u32,
}

impl<R: TableRow> TableWriter<R> {
    /// `run_dir` = `{persistence.dir}/{strategy-id}/{te-id}`.
    pub(super) fn new(run_dir: &Path, boot_ts_us: i64, footer: &[(String, String)]) -> Self {
        Self {
            run_dir: run_dir.to_path_buf(),
            boot_ts_us,
            schema: R::schema(),
            footer: footer.to_vec(),
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

    /// Write remaining rows + close file's footer. File becomes readable at this point.
    ///
    /// # Errors
    /// Encode, write, or close failure.
    pub(super) fn seal(&mut self) -> Result<(), PersistError> {
        self.write_pending()?;
        if let Some(open) = self.open.take() {
            let OpenFile {
                writer,
                path,
                rows_written,
                hour_bucket: _,
            } = open;
            writer.close().map_err(|source| PersistError::Parquet {
                table: R::NAME,
                source,
            })?;
            info!(
                "persist sealed {} {} ({} rows)",
                R::NAME,
                path.display(),
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
                table: R::NAME,
                source,
            })?;
        let Some(open) = self.open.as_mut() else {
            debug_assert!(false, "pending rows without an open file");
            return Ok(());
        };
        open.writer
            .write(&batch)
            .map_err(|source| PersistError::Parquet {
                table: R::NAME,
                source,
            })?;
        open.rows_written += rows;
        self.pending.clear();
        Ok(())
    }

    fn open_file(&mut self, hour_bucket: i64) -> Result<(), PersistError> {
        let path = self.file_path(hour_bucket, self.next_sequence);
        self.next_sequence += 1;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(PersistError::io_at(parent))?;
        }
        let file = File::create(&path).map_err(PersistError::io_at(&path))?;
        let writer =
            ArrowWriter::try_new(file, self.schema.clone(), Some(self.writer_properties()))
                .map_err(|source| PersistError::Parquet {
                    table: R::NAME,
                    source,
                })?;
        info!("persist open {} {}", R::NAME, path.display());
        self.open = Some(OpenFile {
            writer,
            hour_bucket,
            path,
            rows_written: 0,
        });
        Ok(())
    }

    fn writer_properties(&self) -> WriterProperties {
        let metadata = self
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
        let at = TsUs::from_micros(hour_bucket * US_PER_HOUR).civil();
        let mut path = self.run_dir.clone();
        path.push(R::NAME);
        path.push(format!(
            "{DATE_FOLDER_PREFIX}{:04}-{:02}-{:02}",
            at.year, at.month, at.day
        ));
        path.push(format!(
            "{:02}-{}-{sequence:06}.{FILE_EXTENSION}",
            at.hour, self.boot_ts_us
        ));
        path
    }
}

fn hour_bucket(ts: TsUs) -> i64 {
    ts.micros().div_euclid(US_PER_HOUR)
}
