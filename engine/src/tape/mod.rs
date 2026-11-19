//! The Parquet tape of a run, audit and research tables, off the hot path. Files seal (not just
//! flush) so a reader never waits for shutdown, and a truncated file is an error, not a short table.
//! Each table rides the input whose policy fits it: an audit row never drops while quoting runs,
//! and a research row drops and is counted.

mod contract;
mod drain;
mod fault;
mod payload;
mod retention;
mod rollups;
mod schema;
mod table;
mod tables;
mod tape_table;

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rtrb::Consumer;
use tokio::sync::mpsc;

use crate::msg::failure::{FailureClass, Halt};
use crate::msg::kinds::FeatureKind;
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::link::LinkHash;
use crate::msg::persist::{AuditRecord, ResearchRecord, RotationRow};
use crate::runtime::artifacts::RunArtifacts;
use crate::runtime::config::{RecordedTables, StrategyId, TableKind, TradingEngineId};
use crate::runtime::manifest::{RunId, RunKind};
use crate::runtime::metrics::RollupRow;

pub use contract::{HOLDOUT_ROOT, METRICS, MetricName, contract_json};
pub use drain::{InlineWriter, PersistHandle, PersistWriter};
pub use fault::{StoppedTables, TapeStatus};
pub use payload::{
    GapSpan, TAPE_KINDS, TapeDecodeError, TapeKindInfo, TapePayload, decode_payload,
};
pub use rollups::{ROLLUP_RING_CAPACITY, RollupRows, rollup_ring};
pub use tape_table::TapeInput;

#[derive(Debug, Clone)]
pub struct PersistConfig {
    pub dir: PathBuf,
    pub tables: RecordedTables,
    /// A file is deleted once it is older than this. Its age comes from its modified time on the
    /// wall clock, not from the event times inside it.
    pub retention: Duration,
}

/// Everything the footer of each file of one run records, and the folder of the run.
#[derive(Debug, Clone)]
pub struct RunMeta {
    pub strategy_id: StrategyId,
    pub te_id: TradingEngineId,
    /// The names of the files of the run and the mode its footer records.
    pub artifacts: RunArtifacts,
    pub run_kind: RunKind,
    /// The boot stamp: the folder of the run, the name of each file, and the footer.
    pub run_id: RunId,
    pub fixed_scale: i64,
    pub engine_version: Box<str>,
    pub feature_names: Vec<Box<str>>,
    /// The kind of each feature, by `feature_id`: whether it reads the run's own orders.
    pub feature_kinds: Vec<FeatureKind>,
    pub instrument_symbols: Vec<Box<str>>,
    // Wrong reconciliation if not decoded. The venue name and the asset name of each asset id.
    pub asset_symbols: Vec<(Box<str>, Box<str>)>,
    /// The strategy's link field names, by `slot` of the link frames.
    pub link_slot_names: Vec<Box<str>>,
    /// The strategy's link topic names, by `topic` less the first topic of a strategy.
    pub link_topic_names: Vec<Box<str>>,
    /// The engine of each sender hash a link frame can carry.
    pub senders: Vec<(LinkHash, Box<str>)>,
}

impl RunMeta {
    /// `{persistence.dir}/{strategy_id}/{te_id}/{kind}/{run_id}`.
    pub fn run_dir(&self, root: &Path) -> PathBuf {
        self.artifacts.run_dir(root, self.run_kind, self.run_id)
    }
}

/// The read ends of each input of the writer thread.
pub struct WriterInputs {
    pub audit: Consumer<AuditRecord>,
    pub research: Consumer<ResearchRecord>,
    /// `None` until the metrics thread writes its rows to the tape.
    pub rollups: Option<Consumer<RollupRow>>,
    pub rotations: mpsc::Receiver<RotationRow>,
}

impl WriterInputs {
    /// The inputs of a fold that no venue actor feeds: the two rings of the fold, no rollup rows,
    /// and a rotation channel that is closed from the start.
    pub fn of_rings(audit: Consumer<AuditRecord>, research: Consumer<ResearchRecord>) -> Self {
        let (_, rotations) = mpsc::channel(1);
        Self {
            audit,
            research,
            rollups: None,
            rotations,
        }
    }
}

labelled_enum! {
    /// Which input of the writer carries the rows of a table, and so what a full input costs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Ring {
        Audit = "audit",
        Research = "research",
        Rollup = "rollup",
        Rotations = "rotations",
        /// The messages that the hot thread popped, in pop order.
        Tape = "tape",
    }
    pub fn as_str;
}

labelled_enum! {
    /// Each table of the tape, in the order of the contract. The orders, the fills, the incidents,
    /// the ledger and the rotations are the record of what the engine did with money, which an
    /// audit reads; each other table is research data.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum TapeTable {
        Orders = "orders",
        Fills = "fills",
        Incidents = "incidents",
        Ledger = "ledger",
        Rotations = "rotations",
        Features = "features",
        Trades = "trades",
        BookEvents = "book_events",
        Klines = "klines",
        LinkFrames = "link_frames",
        Mids = "mids",
        NoQuotes = "no_quotes",
        Tape = "tape",
        MetricsLatency = "metrics_latency",
        MetricsDrops = "metrics_drops",
        MetricsSenderDrops = "metrics_sender_drops",
        MetricsBacklog = "metrics_backlog",
    }
    pub fn as_str;
    pub const ALL;
}

impl TapeTable {
    /// A lost audit row leaves the engine trading with no record of it, so quoting halts. A lost
    /// research row costs that table alone, so only the table stops. A rotation row is the only
    /// record of which market an instrument slot of an order stood for, so it is audit. The tape
    /// costs trading nothing: a fault stops the tape, and a replay refuses the run.
    pub const fn fault_class(self) -> FailureClass {
        match self {
            TapeTable::Orders
            | TapeTable::Fills
            | TapeTable::Incidents
            | TapeTable::Ledger
            | TapeTable::Rotations => FailureClass::Halt(Halt::Quoting),
            TapeTable::Features
            | TapeTable::Trades
            | TapeTable::BookEvents
            | TapeTable::Klines
            | TapeTable::LinkFrames
            | TapeTable::Mids
            | TapeTable::NoQuotes
            | TapeTable::Tape
            | TapeTable::MetricsLatency
            | TapeTable::MetricsDrops
            | TapeTable::MetricsSenderDrops
            | TapeTable::MetricsBacklog => FailureClass::Reject,
        }
    }

    pub const fn ring(self) -> Ring {
        match self {
            TapeTable::Orders | TapeTable::Fills | TapeTable::Incidents | TapeTable::Ledger => {
                Ring::Audit
            }
            TapeTable::Rotations => Ring::Rotations,
            TapeTable::Features
            | TapeTable::Trades
            | TapeTable::BookEvents
            | TapeTable::Klines
            | TapeTable::LinkFrames
            | TapeTable::Mids
            | TapeTable::NoQuotes => Ring::Research,
            TapeTable::Tape => Ring::Tape,
            TapeTable::MetricsLatency
            | TapeTable::MetricsDrops
            | TapeTable::MetricsSenderDrops
            | TapeTable::MetricsBacklog => Ring::Rollup,
        }
    }

    /// The name that `strategy.tables` switches the table by, and `None` for a table that is
    /// always written.
    pub const fn switch(self) -> Option<TableKind> {
        match self {
            TapeTable::Orders => Some(TableKind::Orders),
            TapeTable::Fills => Some(TableKind::Fills),
            TapeTable::Features => Some(TableKind::Features),
            TapeTable::Trades => Some(TableKind::Trades),
            TapeTable::BookEvents => Some(TableKind::BookEvents),
            TapeTable::Klines => Some(TableKind::Klines),
            TapeTable::LinkFrames => Some(TableKind::LinkFrames),
            TapeTable::Incidents
            | TapeTable::Ledger
            | TapeTable::Rotations
            | TapeTable::Mids
            | TapeTable::NoQuotes
            | TapeTable::Tape
            | TapeTable::MetricsLatency
            | TapeTable::MetricsDrops
            | TapeTable::MetricsSenderDrops
            | TapeTable::MetricsBacklog => None,
        }
    }

    /// Each table that a run with these switches writes, in the order of the contract.
    pub fn written(tables: RecordedTables) -> impl Iterator<Item = TapeTable> {
        TapeTable::ALL
            .into_iter()
            .filter(move |table| table.switch().is_none_or(|switch| tables.contains(switch)))
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

    /// The table that failed, and `None` for a fault of the whole writer.
    pub const fn table(&self) -> Option<TapeTable> {
        match self {
            PersistError::Io { table, .. }
            | PersistError::Parquet { table, .. }
            | PersistError::Batch { table, .. } => Some(*table),
            PersistError::WriterPanicked { .. } | PersistError::DrainInterrupted => None,
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
