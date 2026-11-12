//! Parquet research output, off the hot path. Files seal (not just flush) so a reader never waits
//! for shutdown, and a truncated file is an error rather than a silently short table.

mod drain;
mod retention;
mod schema;
mod table;

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::{ExecutionMode, RecordedTables, StrategyId, TradingEngineId};

pub use drain::{PersistHandle, PersistWriter};

#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub dir: PathBuf,
    pub tables: RecordedTables,
    /// A file is deleted once it is older than this. Its age comes from its modified time on the
    /// wall clock, not from the event times inside it.
    pub retention: Duration,
}

#[derive(Debug, Clone)]
pub struct RunMeta {
    pub strategy_id: StrategyId,
    pub te_id: TradingEngineId,
    /// Records the execution mode and isolates simulated artifacts.
    pub execution_mode: Option<ExecutionMode>,
    pub fixed_scale: i64,
    pub engine_version: Box<str>,
    pub feature_names: Vec<Box<str>>,
    pub instrument_symbols: Vec<Box<str>>,
    // Wrong reconciliation if not decoded.
    pub asset_symbols: Vec<Box<str>>,
}

#[derive(thiserror::Error, Debug)]
pub enum PersistError {
    #[error("persistence io failed at {}", .path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet write failed for {table} table")]
    Parquet {
        table: &'static str,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("record batch build failed for {table} table")]
    Batch {
        table: &'static str,
        #[source]
        source: arrow_schema::ArrowError,
    },
    #[error("persistence writer thread panicked: {payload}")]
    WriterPanicked { payload: Box<str> },
    #[error("persistence drain interrupted: writer thread outcome not observed")]
    DrainInterrupted,
}

impl PersistError {
    fn io_at(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}
