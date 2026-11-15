//! The names of the artifacts of one run, made once from its mode and identity, so no writer can
//! name a file of a simulated run as if the run were live.

use std::path::{Path, PathBuf};

use crate::runtime::config::{ExecutionMode, RunIdentity};

/// Where the console log of an engine that the fleet starts lives, below the root of the tree.
const CONSOLE_LOG_DIR: &str = "logs";

/// Each path and footer value of a run. A simulated run carries `-sim` on each name and `sim/` in
/// its tape; a run in any other mode, or with no execution block, keeps the plain names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArtifacts {
    mode: Option<ExecutionMode>,
    stem: Box<str>,
    /// Below the root of the tape: the strategy, the trading engine, then `sim` for a simulated
    /// run.
    tape: PathBuf,
}

impl RunArtifacts {
    pub fn new(mode: Option<ExecutionMode>, identity: &RunIdentity) -> Self {
        let mut tape = Path::new(identity.strategy_id.as_str()).join(identity.te_id.as_str());
        let stem = if mode.is_some_and(ExecutionMode::is_simulated) {
            tape.push("sim");
            format!("{identity}-sim")
        } else {
            identity.to_string()
        };
        Self {
            mode,
            stem: stem.into_boxed_str(),
            tape,
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

    pub fn tape_dir(&self, root: &Path) -> PathBuf {
        root.join(&self.tape)
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
