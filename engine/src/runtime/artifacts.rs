//! The names of the artifacts of one run, made once from its mode and identity, so no writer can
//! name a file of a simulated run as if the run were live.

use std::path::{Path, PathBuf};

use crate::runtime::config::{ExecutionMode, RunIdentity};
use crate::runtime::manifest::{RunId, RunKind};

/// Where the console log of an engine that the fleet starts lives, below the root of the tree.
const CONSOLE_LOG_DIR: &str = "logs";

/// Each path and footer value of a run. A simulated run carries `-sim` on each name and is of kind
/// `sim`; a run in any other mode, or with no execution block, keeps the plain names and is of kind
/// `live`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArtifacts {
    mode: Option<ExecutionMode>,
    stem: Box<str>,
    /// Below the root of the tape: the strategy, then the trading engine.
    identity: PathBuf,
}

impl RunArtifacts {
    pub fn new(mode: Option<ExecutionMode>, identity: &RunIdentity) -> Self {
        let stem = if mode.is_some_and(ExecutionMode::is_simulated) {
            format!("{identity}-sim")
        } else {
            identity.to_string()
        };
        Self {
            mode,
            stem: stem.into_boxed_str(),
            identity: Path::new(identity.strategy_id.as_str()).join(identity.te_id.as_str()),
        }
    }

    pub fn mode(&self) -> Option<ExecutionMode> {
        self.mode
    }

    /// The start of the name of each file of the run, and the service name of its export.
    pub fn stem(&self) -> &str {
        &self.stem
    }

    pub fn exposure_path(&self, dir: &Path) -> PathBuf {
        dir.join(format!("{}.json", self.stem))
    }

    /// The kind of a boot of the engine: `sim` in mode `sim`, `live` in each other mode.
    pub fn kind(&self) -> RunKind {
        if self.mode.is_some_and(ExecutionMode::is_simulated) {
            RunKind::Sim
        } else {
            RunKind::Live
        }
    }

    /// The folder of each run of `kind` of this identity, below the root of the tape.
    pub fn kind_dir(&self, root: &Path, kind: RunKind) -> PathBuf {
        root.join(&self.identity).join(kind.as_str())
    }

    /// `{root}/{strategy_id}/{te_id}/{kind}/{run_id}`: one run, with its `run.json` and one folder
    /// for each table.
    pub fn run_dir(&self, root: &Path, kind: RunKind, run_id: RunId) -> PathBuf {
        self.kind_dir(root, kind).join(run_id.to_string())
    }

    /// The value of the footer key `execution_mode`, which keeps an absent execution block apart
    /// from `off`.
    pub fn footer_mode(&self) -> &'static str {
        ExecutionMode::footer_value(self.mode)
    }

    pub fn console_log_path(&self, root: &Path) -> PathBuf {
        root.join(CONSOLE_LOG_DIR)
            .join(format!("{}-console.log", self.stem))
    }
}
