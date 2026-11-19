//! The manifest of one run, `run.json`: what a reader needs to pair, judge and re-fold a run without
//! its process. It is written open at the start and again at the end, never by a drop, so a run
//! that a hard kill ended still reads `open`.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::msg::labelled_enum::labelled_enum;
use crate::msg::link::{FNV_OFFSET_BASIS, fnv_fold};
use crate::runtime::artifacts::RunArtifacts;
use crate::runtime::config::{Config, RunIdentity};
use crate::runtime::durable_file::{self, WriteError};
use crate::runtime::registry::{InstrumentRow, Registry};

/// The version of the contract that each table, footer and manifest of this build follows.
pub const CONTRACT_VERSION: u32 = 1;

/// The name of the manifest in the folder of its run.
pub const MANIFEST_FILE: &str = "run.json";

labelled_enum! {
    /// Which kind of run made a folder. A run of the engine with execution `sim` is `sim`; each
    /// other boot of an engine is `live`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum RunKind {
        Live = "live",
        Sim = "sim",
        Replay = "replay",
        Backtest = "backtest",
    }
    pub fn as_str;
}

labelled_enum! {
    /// How a run ended. `open` until the end is written, so a run cut by a hard kill stays `open`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum EndState {
        Open = "open",
        Graceful = "graceful",
        Halted = "halted",
        DrainFailed = "drain_failed",
        DeadlinePassed = "deadline_passed",
        StartRefused = "start_refused",
        /// A re-fold that failed after its folder opened.
        Refused = "refused",
    }
    pub fn as_str;
}

/// One run: the boot stamp of an engine in microseconds since the epoch, read once at its start.
/// It names the folder, each file and the footer of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RunId(i64);

impl RunId {
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }

    pub const fn micros(self) -> i64 {
        self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The FNV-1a digest of the exact bytes of a config, as `0x` and 16 lowercase hex digits. A
/// replay compares it with the config it is given.
pub fn config_digest(text: &str) -> String {
    format!("0x{:016x}", fnv_fold(FNV_OFFSET_BASIS, text.as_bytes()))
}

/// The source run of a replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRun {
    /// Below the root of the tape.
    pub run_dir: String,
    pub run_id: String,
    pub config_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetWindow {
    pub window_us: i64,
    pub max_places: u32,
}

/// What the venue does, as the run found it. A replay cannot probe a venue, so it reads this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueTerms {
    pub holds_reservations_until_settled: bool,
    pub fee_model: String,
    pub order_budget: Vec<BudgetWindow>,
    pub rotates_markets: bool,
    pub base_asset_is_position: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueEntry {
    pub venue_id: u8,
    pub name: String,
    /// `None` for a venue that does not trade in this run.
    pub run_nonce: Option<u32>,
    pub capabilities: Option<VenueTerms>,
}

/// The terms of one instrument that preflight read from its venue. Each mantissa is an integer at
/// the fixed scale, and an absent term is `null`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentTerms {
    pub instrument_id: u16,
    pub venue_id: u8,
    pub venue_symbol: String,
    pub display: String,
    pub base: String,
    pub quote: String,
    pub base_asset_id: u16,
    pub quote_asset_id: u16,
    pub tick_size: Option<i64>,
    pub lot_size: Option<i64>,
    pub min_qty: Option<i64>,
    pub min_notional: Option<i64>,
    pub max_num_orders: Option<u32>,
    pub max_num_order_amends: Option<u32>,
    pub max_price: Option<i64>,
    pub qty_scale: i64,
}

impl InstrumentTerms {
    /// The terms of each instrument of the registry, in id order.
    pub fn of(registry: &Registry) -> Vec<InstrumentTerms> {
        registry.instruments().iter().map(Self::of_row).collect()
    }

    fn of_row(row: &InstrumentRow) -> InstrumentTerms {
        InstrumentTerms {
            instrument_id: row.instrument_id.0,
            venue_id: row.venue.0,
            venue_symbol: row.venue_symbol.to_string(),
            display: row.display.to_string(),
            base: row.base.to_string(),
            quote: row.quote.to_string(),
            base_asset_id: row.base_asset.0,
            quote_asset_id: row.quote_asset.0,
            tick_size: row.tick_size.map(|price| price.mantissa()),
            lot_size: row.lot_size.map(|qty| qty.mantissa()),
            min_qty: row.min_qty.map(|qty| qty.mantissa()),
            min_notional: row.min_notional.map(|notional| notional.mantissa()),
            max_num_orders: row.max_num_orders,
            max_num_order_amends: row.max_num_order_amends,
            max_price: row.max_price.map(|price| price.mantissa()),
            qty_scale: row.qty_scale,
        }
    }
}

/// `run.json`. The fields keep this order in the file, and [`MANIFEST_FIELDS`] lists them for the
/// contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunManifest {
    pub contract_version: u32,
    pub run_id: String,
    pub kind: RunKind,
    pub strategy_id: String,
    pub te_id: String,
    pub execution_mode: String,
    pub boot_ts_us: i64,
    pub engine_version: String,
    pub config_digest: String,
    pub seed: Option<u64>,
    pub spin_interval_us: u64,
    pub seal_interval_spins: u64,
    pub warmup_us: i64,
    pub fixed_scale: i64,
    pub tables: Vec<String>,
    pub identity_lock: Option<String>,
    pub source: Option<SourceRun>,
    pub venues: Vec<VenueEntry>,
    pub instruments: Vec<InstrumentTerms>,
    pub end_state: EndState,
    pub end_reason: Option<String>,
    pub stopped_tables: Vec<String>,
}

/// The name, JSON type and null flag of each field of `run.json`, in file order.
pub const MANIFEST_FIELDS: [(&str, &str, bool); 22] = [
    ("contract_version", "integer", false),
    ("run_id", "string", false),
    ("kind", "string", false),
    ("strategy_id", "string", false),
    ("te_id", "string", false),
    ("execution_mode", "string", false),
    ("boot_ts_us", "integer", false),
    ("engine_version", "string", false),
    ("config_digest", "string", false),
    ("seed", "integer", true),
    ("spin_interval_us", "integer", false),
    ("seal_interval_spins", "integer", false),
    ("warmup_us", "integer", false),
    ("fixed_scale", "integer", false),
    ("tables", "array", false),
    ("identity_lock", "string", true),
    ("source", "object", true),
    ("venues", "array", false),
    ("instruments", "array", false),
    ("end_state", "string", false),
    ("end_reason", "string", true),
    ("stopped_tables", "array", false),
];

/// The tables of a run are those of the tape, which only a native build has.
#[cfg(not(target_arch = "wasm32"))]
mod of_start {
    use crate::msg::ids::FIXED_SCALE;
    use crate::runtime::config::RecordedTables;
    use crate::tape::TapeTable;

    use super::{CONTRACT_VERSION, EndState, InstrumentTerms, ManifestOf, RunManifest};

    impl RunManifest {
        /// The manifest of one boot, `open`, with the values that the fold of the run uses.
        pub fn of<P>(of: ManifestOf<'_, P>) -> RunManifest {
            let ManifestOf {
                config,
                identity,
                artifacts,
                registry,
                run_id,
                venues,
                identity_lock,
            } = of;
            let tables = RecordedTables::new(&config.strategy.tables);
            RunManifest {
                contract_version: CONTRACT_VERSION,
                run_id: run_id.to_string(),
                kind: artifacts.kind(),
                strategy_id: identity.strategy_id.as_str().to_owned(),
                te_id: identity.te_id.as_str().to_owned(),
                execution_mode: artifacts.footer_mode().to_owned(),
                boot_ts_us: run_id.micros(),
                engine_version: env!("CARGO_PKG_VERSION").to_owned(),
                config_digest: config.source.digest.clone(),
                seed: None,
                spin_interval_us: config.engine.spin_interval_us,
                seal_interval_spins: config.seal_interval_spins().get(),
                warmup_us: config.engine.warmup_secs as i64 * 1_000_000,
                fixed_scale: FIXED_SCALE,
                tables: TapeTable::written(tables)
                    .map(|table| table.as_str().to_owned())
                    .collect(),
                identity_lock: identity_lock.map(|path| path.display().to_string()),
                source: None,
                venues,
                instruments: InstrumentTerms::of(registry),
                end_state: EndState::Open,
                end_reason: None,
                stopped_tables: Vec::new(),
            }
        }
    }
}

/// How a run ended, for the last write of its manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEnd {
    pub state: EndState,
    pub reason: Option<String>,
    pub stopped_tables: Vec<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum ManifestError {
    #[error("failed to write the run manifest {}", .path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl From<WriteError> for ManifestError {
    fn from(error: WriteError) -> Self {
        match error {
            WriteError::NotWritten { path, source }
            | WriteError::FolderNotSynced {
                directory: path,
                source,
            } => ManifestError::Write { path, source },
        }
    }
}

/// The manifest of a run that has not ended. Only [`OpenManifest::close`] writes the end: no drop
/// does, so a process that dies leaves `open` on disk.
#[derive(Debug)]
pub struct OpenManifest {
    path: PathBuf,
    manifest: RunManifest,
}

/// What the manifest of one boot of an engine is made from.
pub struct ManifestOf<'a, P> {
    pub config: &'a Config<P>,
    pub identity: &'a RunIdentity,
    pub artifacts: &'a RunArtifacts,
    pub registry: &'a Registry,
    pub run_id: RunId,
    /// Each venue, in venue order, with the nonce and the terms of the one that trades.
    pub venues: Vec<VenueEntry>,
    pub identity_lock: Option<&'a Path>,
}

impl RunManifest {
    /// Writes the manifest into `dir` with the end state `open`, through a temporary file, a sync
    /// and a rename.
    ///
    /// # Errors
    /// [`ManifestError::Write`] when the file cannot be written durably.
    pub fn open(dir: &Path, manifest: &RunManifest) -> Result<OpenManifest, ManifestError> {
        let open = OpenManifest {
            path: dir.join(MANIFEST_FILE),
            manifest: RunManifest {
                end_state: EndState::Open,
                end_reason: None,
                ..manifest.clone()
            },
        };
        open.write()?;
        Ok(open)
    }
}

impl OpenManifest {
    pub fn manifest(&self) -> &RunManifest {
        &self.manifest
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// # Errors
    /// [`ManifestError::Write`] when the file cannot be written durably. The file then still holds
    /// the manifest as it was opened.
    pub fn close(mut self, end: RunEnd) -> Result<(), ManifestError> {
        self.manifest.end_state = end.state;
        self.manifest.end_reason = end.reason;
        self.manifest.stopped_tables = end.stopped_tables;
        self.write()
    }

    fn write(&self) -> Result<(), ManifestError> {
        let mut body = serde_json::to_vec_pretty(&self.manifest)
            .expect("a manifest of strings, integers and lists always serialises to json");
        body.push(b'\n');
        durable_file::write(&self.path, &body).map_err(ManifestError::from)
    }
}
