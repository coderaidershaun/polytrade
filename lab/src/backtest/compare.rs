//! The re-fold check: each table that the fold writes holds the same rows, in the same order, in a
//! run and in its re-fold. Two exceptions, each a row that the run lost and its re-fold kept: a
//! research row dropped on a full ring, no breach while such rows are not more than the drops
//! counted; and each row past the last row of a table that a writer fault stopped, which
//! `stopped_tables` of the run's `run.json` names.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float64Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

use engine::runtime::manifest::{MANIFEST_FILE, RunManifest};

use super::error::ReplayError;
use super::reader::open_file;

/// The record of money: each row equal, none extra.
const AUDIT_TABLES: [&str; 4] = ["orders", "fills", "incidents", "ledger"];

/// Research tables, each with the sink that counts its drops in `metrics_drops`.
const RESEARCH_TABLES: [(&str, &str); 7] = [
    ("features", "research_features"),
    ("trades", "research_trades"),
    ("book_events", "research_book_events"),
    ("klines", "research_klines"),
    ("link_frames", "research_link_frames"),
    ("mids", "research_mids"),
    ("no_quotes", "research_no_quotes"),
];

/// Where a re-fold parted from its run.
#[derive(thiserror::Error, Debug)]
pub enum RefoldDiff {
    #[error("table {table}: row {index} of the run is {run}, and the re-fold holds {replay}")]
    RowDiffers {
        table: &'static str,
        index: usize,
        run: String,
        replay: String,
    },
    #[error("table {table}: the re-fold lacks row {index} of the run, {row}")]
    OnlyInRun {
        table: &'static str,
        index: usize,
        row: String,
    },
    #[error(
        "table {table}: the re-fold holds {extra} rows that the run lacks, and the run dropped only \
         {dropped}"
    )]
    OnlyInReplay {
        table: &'static str,
        extra: u64,
        dropped: u64,
    },
    #[error(transparent)]
    Read(#[from] ReplayError),
}

/// Compares each table that the fold writes, in the order of the contract.
///
/// # Errors
/// The [`RefoldDiff`] of the first table that parts.
pub fn check_refold(run: &Path, replay: &Path) -> Result<(), RefoldDiff> {
    let stopped = stopped_tables(run)?;
    for table in AUDIT_TABLES {
        let is_stopped = stopped.iter().any(|name| name == table);
        let (ours, theirs) = (table_rows(run, table)?, table_rows(replay, table)?);
        if let Some(index) = ours.iter().zip(&theirs).position(|(a, b)| a != b) {
            return Err(RefoldDiff::RowDiffers {
                table,
                index,
                run: ours[index].clone(),
                replay: theirs[index].clone(),
            });
        }
        if ours.len() > theirs.len() {
            return Err(RefoldDiff::OnlyInRun {
                table,
                index: theirs.len(),
                row: ours[theirs.len()].clone(),
            });
        }
        if theirs.len() > ours.len() && !is_stopped {
            return Err(RefoldDiff::OnlyInReplay {
                table,
                extra: (theirs.len() - ours.len()) as u64,
                dropped: 0,
            });
        }
    }
    let drops = research_drops(run)?;
    for (table, sink) in RESEARCH_TABLES {
        let (ours, theirs) = (table_rows(run, table)?, table_rows(replay, table)?);
        let dropped = drops.get(sink).copied().unwrap_or(0);
        let past_stop = match stopped.iter().any(|name| name == table) {
            true => PastStop::Taken,
            false => PastStop::Refused,
        };
        check_with_drops(table, &ours, &theirs, dropped, past_stop)?;
    }
    Ok(())
}

/// Whether the rows of the re-fold past the run's last row are rows the run lost at a stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PastStop {
    Taken,
    Refused,
}

/// The run's rows must come in the re-fold in the same order, with rows of the re-fold alone in
/// between, each one a row the run dropped, and, for a stopped table, any rows after the last one.
fn check_with_drops(
    table: &'static str,
    ours: &[String],
    theirs: &[String],
    dropped: u64,
    past_stop: PastStop,
) -> Result<(), RefoldDiff> {
    let mut matched = 0;
    let mut extra = 0_u64;
    for row in theirs {
        if ours.get(matched) == Some(row) {
            matched += 1;
        } else if matched < ours.len() || past_stop == PastStop::Refused {
            extra += 1;
        }
    }
    if let Some(row) = ours.get(matched) {
        return Err(RefoldDiff::OnlyInRun {
            table,
            index: matched,
            row: row.clone(),
        });
    }
    match extra > dropped {
        true => Err(RefoldDiff::OnlyInReplay {
            table,
            extra,
            dropped,
        }),
        false => Ok(()),
    }
}

/// The tables that a writer fault stopped in the run, from its `run.json`.
fn stopped_tables(run: &Path) -> Result<Vec<String>, ReplayError> {
    let file = run.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&file).map_err(|source| ReplayError::Manifest {
        file: file.clone(),
        source,
    })?;
    let manifest: RunManifest = serde_json::from_str(&text)
        .map_err(|source| ReplayError::ManifestParse { file, source })?;
    Ok(manifest.stopped_tables)
}

/// The last count of each sink in the run's `metrics_drops`: each row holds the total since the
/// start of the run.
fn research_drops(run: &Path) -> Result<HashMap<String, u64>, ReplayError> {
    let mut drops = HashMap::new();
    for file in table_files(run, "metrics_drops") {
        for batch in batches(&file)? {
            let sinks = batch.column(1).as_string_opt::<i32>();
            let totals = batch.column(2).as_primitive_opt::<UInt64Type>();
            let (Some(sinks), Some(totals)) = (sinks, totals) else {
                return Err(ReplayError::Column {
                    file,
                    detail: "metrics_drops is not (interval_end_ts_us, sink, dropped_total)".into(),
                });
            };
            for row in 0..batch.num_rows() {
                let total = drops.entry(sinks.value(row).to_owned()).or_insert(0);
                *total = (*total).max(totals.value(row));
            }
        }
    }
    Ok(drops)
}

/// Each row of a table of a run, as the text of its cells, file by file in seal order.
fn table_rows(run: &Path, table: &str) -> Result<Vec<String>, ReplayError> {
    let mut rows = Vec::new();
    for file in table_files(run, table) {
        for batch in batches(&file)? {
            for row in 0..batch.num_rows() {
                let mut text = String::new();
                for column in batch.columns() {
                    text.push_str(&cell(column.as_ref(), row));
                    text.push('|');
                }
                rows.push(text);
            }
        }
    }
    Ok(rows)
}

fn batches(file: &Path) -> Result<Vec<RecordBatch>, ReplayError> {
    let read_error = |source| ReplayError::Read {
        file: file.to_path_buf(),
        source,
    };
    open_file(file)?
        .build()
        .map_err(read_error)?
        .map(|batch| batch.map_err(|source| read_error(source.into())))
        .collect()
}

/// The sealed files of one table of a run, in seal order: the number at the end of each name.
fn table_files(run: &Path, table: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(dates) = std::fs::read_dir(run.join(table)) else {
        return files;
    };
    for date in dates.filter_map(Result::ok) {
        let Ok(entries) = std::fs::read_dir(date.path()) else {
            continue;
        };
        files.extend(
            entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "parquet")),
        );
    }
    files.sort_by_key(|path| {
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("");
        stem.rsplit('-')
            .next()
            .and_then(|seq| seq.parse::<u64>().ok())
    });
    files
}

/// One cell as text. A float is written by its bits, so two runs compare by value, not by print.
fn cell(column: &dyn Array, row: usize) -> String {
    if column.is_null(row) {
        return "null".to_owned();
    }
    let mut text = String::new();
    let written = match column.data_type() {
        DataType::Int64 => write!(text, "{}", column.as_primitive::<Int64Type>().value(row)),
        DataType::Int32 => write!(text, "{}", column.as_primitive::<Int32Type>().value(row)),
        DataType::UInt8 => write!(text, "{}", column.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => write!(text, "{}", column.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => write!(text, "{}", column.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => write!(text, "{}", column.as_primitive::<UInt64Type>().value(row)),
        DataType::Float64 => {
            let value = column.as_primitive::<Float64Type>().value(row);
            write!(text, "{value}/{:#018x}", value.to_bits())
        }
        DataType::Boolean => write!(text, "{}", column.as_boolean().value(row)),
        DataType::Utf8 => write!(text, "{:?}", column.as_string::<i32>().value(row)),
        DataType::Binary => write!(text, "{:02x?}", column.as_binary::<i32>().value(row)),
        _ => write!(text, "{:?}", column.slice(row, 1)),
    };
    written.expect("a string takes every write");
    text
}
