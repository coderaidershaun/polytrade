//! The Parquet tape of a run, audit and research tables, off the hot path. Files seal (not just
//! flush) so a reader never waits for shutdown, and a truncated file is an error, not a short table.

mod drain;
mod fault;
mod retention;
mod schema;
mod table;

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::msg::failure::{FailureClass, Halt};
use crate::runtime::artifacts::RunArtifacts;
use crate::runtime::config::{RecordedTables, StrategyId, TradingEngineId};

pub use drain::{PersistHandle, PersistWriter};
pub use fault::{StoppedTables, TapeStatus};

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
    /// The tape folder of the run and the mode its footer records, so a simulated run writes
    /// below `sim/`.
    pub artifacts: RunArtifacts,
    pub fixed_scale: i64,
    pub engine_version: Box<str>,
    pub feature_names: Vec<Box<str>>,
    pub instrument_symbols: Vec<Box<str>>,
    // Wrong reconciliation if not decoded. The venue name and the asset name of each asset id.
    pub asset_symbols: Vec<(Box<str>, Box<str>)>,
}

/// Each table of the tape. The orders, the fills, the incidents and the link frames are the record
/// of what the engine did and why, which an audit reads; each other table is research data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TapeTable {
    Orders,
    Fills,
    Incidents,
    LinkFrames,
    Features,
    Trades,
    BookEvents,
    Klines,
    Rotations,
}

impl TapeTable {
    pub const fn as_str(self) -> &'static str {
        match self {
            TapeTable::Orders => "orders",
            TapeTable::Fills => "fills",
            TapeTable::Incidents => "incidents",
            TapeTable::LinkFrames => "link_frames",
            TapeTable::Features => "features",
            TapeTable::Trades => "trades",
            TapeTable::BookEvents => "book_events",
            TapeTable::Klines => "klines",
            TapeTable::Rotations => "rotations",
        }
    }

    /// A lost audit row leaves the engine trading with no record of it, so quoting halts. A lost
    /// research row costs that table alone, so only the table stops.
    pub const fn fault_class(self) -> FailureClass {
        match self {
            TapeTable::Orders | TapeTable::Fills | TapeTable::Incidents | TapeTable::LinkFrames => {
                FailureClass::Halt(Halt::Quoting)
            }
            TapeTable::Features
            | TapeTable::Trades
            | TapeTable::BookEvents
            | TapeTable::Klines
            | TapeTable::Rotations => FailureClass::Reject,
        }
    }
}

impl fmt::Display for TapeTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PersistError {
    #[error("persistence io failed for {table} table at {}", .path.display())]
    Io {
        table: TapeTable,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet write failed for {table} table")]
    Parquet {
        table: TapeTable,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("record batch build failed for {table} table")]
    Batch {
        table: TapeTable,
        #[source]
        source: arrow_schema::ArrowError,
    },
    #[error("persistence writer thread panicked: {payload}")]
    WriterPanicked { payload: Box<str> },
    #[error("persistence drain interrupted: writer thread outcome not observed")]
    DrainInterrupted,
}

impl PersistError {
    /// A fault of one table takes the class of that table. A writer thread that panicked or was
    /// never joined lost each table at once, so the engine halts.
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            PersistError::Io { table, .. }
            | PersistError::Parquet { table, .. }
            | PersistError::Batch { table, .. } => table.fault_class(),
            PersistError::WriterPanicked { .. } | PersistError::DrainInterrupted => {
                FailureClass::Halt(Halt::Engine)
            }
        }
    }

    fn io_at(table: TapeTable, path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::Io {
            table,
            path: path.to_path_buf(),
            source,
        }
    }
}
