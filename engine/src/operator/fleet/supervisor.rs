//! What is running: the engines this server started, each one a child process it alone can
//! signal, and the engines that another process runs, which their identity locks show.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::IdentityLocks;
use crate::runtime::config::{ExecutionMode, RunIdentity};
use crate::runtime::durable_file::{DiskFiles, FileSteps};

use super::catalog::{self, ConsoleLog, Engine, ProbedConfig};
use super::child::{ChildSlot, SlotState};
use super::error::FleetError;
use super::reply::{EngineListing, EngineSummary, ModeChange, Started, Stopped};

pub(super) const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FleetTiming {
    /// A child that exits inside this window is reported as a failed start, with its output.
    pub start_grace: Duration,
    /// How long SIGTERM gets before SIGKILL. One shared deadline covers every engine in a stop.
    pub stop_deadline: Duration,
}

impl Default for FleetTiming {
    fn default() -> Self {
        FleetTiming {
            start_grace: Duration::from_secs(2),
            stop_deadline: Duration::from_secs(20),
        }
    }
}

/// The operator's supervisor: it starts engines as child processes, stops them by signal and
/// edits their configs, so any front end can drive the engines through it.
pub struct Fleet {
    root: PathBuf,
    bin_dir: PathBuf,
    pub(super) timing: FleetTiming,
    pub(super) children: HashMap<RunIdentity, ChildSlot>,
    pub(super) is_closed: bool,
    files: Arc<dyn FileSteps>,
    locks: IdentityLocks,
}

impl std::fmt::Debug for Fleet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Fleet")
            .field("root", &self.root)
            .field("bin_dir", &self.bin_dir)
            .field("timing", &self.timing)
            .field("children", &self.children)
            .field("is_closed", &self.is_closed)
            .field("locks", &self.locks)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct StopOutcome {
    pub identity: RunIdentity,
    pub result: Result<Stopped, FleetError>,
}

impl Fleet {
    pub fn new(root: PathBuf, bin_dir: PathBuf, timing: FleetTiming) -> Fleet {
        Fleet {
            root,
            bin_dir,
            timing,
            children: HashMap::new(),
            is_closed: false,
            files: Arc::new(DiskFiles),
            locks: IdentityLocks::of_host(),
        }
    }

    /// The folder where the engines hold their identity locks, which is the one of the host unless
    /// a test gives its own.
    #[must_use]
    pub fn with_identity_locks(mut self, locks: IdentityLocks) -> Fleet {
        self.locks = locks;
        self
    }

    /// The config edit writes through these steps, so a test can fail it at any one of them.
    #[must_use]
    pub fn with_file_steps(mut self, files: Arc<dyn FileSteps>) -> Fleet {
        self.files = files;
        self
    }

    /// # Errors
    /// [`FleetError::ReadDir`] when `strategies/` or a strategy folder inside it cannot be read. An
    /// unreadable `strategies/` usually means the root is wrong. [`FleetError::Wait`] when a
    /// child's exit status cannot be read, and [`FleetError::LockProbe`] when an identity lock
    /// cannot be read. One unreadable config never fails the listing: its row says why instead.
    pub fn list(&mut self) -> Result<EngineListing, FleetError> {
        let engines = catalog::discover(&self.root)?;
        let tracked: Vec<RunIdentity> = self.children.keys().cloned().collect();
        for identity in &tracked {
            self.reap(identity)?;
        }
        let mut rows: Vec<EngineSummary> = engines
            .iter()
            .map(|engine| self.summarize(engine))
            .collect::<Result<_, _>>()?;
        // A running engine whose folder was renamed or removed keeps its row, so a client never
        // loses sight of an engine it still has to stop.
        let moved = self
            .children
            .iter()
            .filter(|(identity, _)| !engines.iter().any(|engine| engine.identity == **identity));
        rows.extend(moved.filter_map(|(identity, slot)| slot.running_row(identity)));
        rows.sort_by(|left, right| {
            (&left.strategy_id, &left.te_id).cmp(&(&right.strategy_id, &right.te_id))
        });
        Ok(EngineListing { engines: rows })
    }

    /// Waits out the start grace, so an engine that refuses its config fails this call with its
    /// own last words instead of looking started.
    ///
    /// # Errors
    /// [`FleetError::ShuttingDown`], [`FleetError::UnknownEngine`], [`FleetError::AlreadyRunning`],
    /// [`FleetError::Stopping`], [`FleetError::RunningElsewhere`],
    /// [`FleetError::ConfigUnreadable`], [`FleetError::BinaryMissing`],
    /// [`FleetError::ExitedEarly`], or a failure to read the strategies tree, probe the identity
    /// lock, open the console log, spawn the binary or read its exit status.
    pub async fn start(&mut self, identity: &RunIdentity) -> Result<Started, FleetError> {
        if self.is_closed {
            return Err(FleetError::ShuttingDown);
        }
        let engine = self.find_engine(identity)?;
        if let Some(pid) = self.live_pid(identity)? {
            let is_stopping = self
                .children
                .get(identity)
                .is_some_and(ChildSlot::is_stopping);
            let identity = identity.clone();
            return Err(if is_stopping {
                FleetError::Stopping { identity, pid }
            } else {
                FleetError::AlreadyRunning { identity, pid }
            });
        }
        if self.is_held_elsewhere(identity)? {
            return Err(FleetError::RunningElsewhere {
                identity: identity.clone(),
                lock: self.locks.path_of(identity),
            });
        }
        let boot = match engine.probe() {
            ProbedConfig::Parsed(parsed) => parsed,
            ProbedConfig::Unreadable(refusal) => return Err(refusal),
        };
        let mode = boot.mode;
        let bin = identity.to_string();
        let bin_path = self.bin_dir.join(&bin);
        if !bin_path.is_file() {
            return Err(FleetError::BinaryMissing {
                path: bin_path,
                bin,
            });
        }
        let console_log = engine.console_log_path(&self.root, mode);
        let log = ConsoleLog::open(&console_log)?;
        let start_offset = log.start_offset;
        let child = self.spawn(bin_path, &engine.config_path, log)?;
        let pid = child.id();
        // Registered before the grace wait: dropping a `Child` never kills it, so a start that is
        // abandoned mid-wait must still leave the child where the final stop finds it.
        self.children.insert(
            identity.clone(),
            ChildSlot::running(child, console_log.clone(), boot),
        );
        self.await_start_grace(identity, &console_log, start_offset)
            .await?;
        Ok(Started {
            strategy_id: identity.strategy_id.as_str().to_owned(),
            te_id: identity.te_id.as_str().to_owned(),
            pid,
            mode: ExecutionMode::footer_value(mode),
            console_log: console_log.display().to_string(),
        })
    }

    /// # Errors
    /// [`FleetError::UnknownEngine`], [`FleetError::ModeLockedWhileRunning`],
    /// [`FleetError::ConfigUnreadable`], [`FleetError::NoModeLine`],
    /// [`FleetError::RewriteNotVerified`], or a failure to read the strategies tree, read or write
    /// the config, or read a child's exit status.
    pub fn set_mode(
        &mut self,
        identity: &RunIdentity,
        mode: ExecutionMode,
    ) -> Result<ModeChange, FleetError> {
        let engine = self.find_engine(identity)?;
        if let Some(pid) = self.live_pid(identity)? {
            return Err(FleetError::ModeLockedWhileRunning {
                identity: identity.clone(),
                pid,
            });
        }
        let previous = engine.rewrite_mode(mode, self.files.as_ref())?;
        Ok(ModeChange {
            strategy_id: identity.strategy_id.as_str().to_owned(),
            te_id: identity.te_id.as_str().to_owned(),
            previous: ExecutionMode::footer_value(previous),
            mode: mode.as_str(),
            config_path: engine.config_path.display().to_string(),
        })
    }

    /// A null stdin, because an inherited one would read the bytes of the agent's requests. The
    /// child's output goes to its console log, because this server's stdout carries the protocol.
    fn spawn(
        &self,
        bin_path: PathBuf,
        config_path: &Path,
        log: ConsoleLog,
    ) -> Result<Child, FleetError> {
        Command::new(&bin_path)
            .arg("--config")
            .arg(config_path)
            .current_dir(&self.root)
            .stdin(Stdio::null())
            .stdout(log.stdout)
            .stderr(log.stderr)
            .spawn()
            .map_err(|source| FleetError::Spawn {
                path: bin_path,
                source,
            })
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the operator fleet times the starts and stops of its children and holds no engine state"
    )]
    async fn await_start_grace(
        &mut self,
        identity: &RunIdentity,
        console_log: &Path,
        start_offset: u64,
    ) -> Result<(), FleetError> {
        let deadline = Instant::now() + self.timing.start_grace;
        loop {
            if let Some(status) = self.reap(identity)? {
                return Err(FleetError::ExitedEarly {
                    identity: identity.clone(),
                    status,
                    tail: ConsoleLog::tail(console_log, start_offset),
                });
            }
            if Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    fn is_held_elsewhere(&self, identity: &RunIdentity) -> Result<bool, FleetError> {
        self.locks
            .is_held(identity)
            .map_err(|source| FleetError::LockProbe {
                path: self.locks.path_of(identity),
                source,
            })
    }

    fn find_engine(&self, identity: &RunIdentity) -> Result<Engine, FleetError> {
        catalog::discover(&self.root)?
            .into_iter()
            .find(|engine| engine.identity == *identity)
            .ok_or_else(|| FleetError::UnknownEngine {
                identity: identity.clone(),
                strategies: catalog::strategies_dir(&self.root),
            })
    }

    /// The pid of a child that is still alive, after reaping it.
    fn live_pid(&mut self, identity: &RunIdentity) -> Result<Option<u32>, FleetError> {
        self.reap(identity)?;
        Ok(self.children.get(identity).and_then(ChildSlot::running_pid))
    }

    /// Records a child that has gone as exited. The status comes back only from the call that
    /// noticed the exit.
    pub(super) fn reap(
        &mut self,
        identity: &RunIdentity,
    ) -> Result<Option<ExitStatus>, FleetError> {
        let Some(slot) = self.children.get_mut(identity) else {
            return Ok(None);
        };
        slot.reap().map_err(|source| FleetError::Wait {
            identity: identity.clone(),
            source,
        })
    }

    /// A row of an engine that this server does not run reads the config file, because that is
    /// what the next start boots from. An engine whose identity lock is held runs in another
    /// process.
    fn summarize(&self, engine: &Engine) -> Result<EngineSummary, FleetError> {
        let slot = self.children.get(&engine.identity);
        if let Some(row) = slot.and_then(|slot| slot.running_row(&engine.identity)) {
            return Ok(row);
        }
        let probed = engine.probe();
        let is_held_elsewhere = self.is_held_elsewhere(&engine.identity)?;
        let (state, exit_code, console_log) = match slot {
            _ if is_held_elsewhere => (
                "running elsewhere",
                None,
                engine.console_log_path(&self.root, probed.mode()),
            ),
            Some(ChildSlot {
                console_log,
                state: SlotState::Exited { exit_code },
            }) => ("exited", *exit_code, console_log.clone()),
            _ => (
                "stopped",
                None,
                engine.console_log_path(&self.root, probed.mode()),
            ),
        };
        let mode = probed.label();
        let (link_bind, config_error) = match probed {
            ProbedConfig::Parsed(parsed) => (parsed.link_bind, None),
            ProbedConfig::Unreadable(refusal) => (None, Some(refusal.to_string())),
        };
        Ok(EngineSummary {
            strategy_id: engine.identity.strategy_id.as_str().to_owned(),
            te_id: engine.identity.te_id.as_str().to_owned(),
            bin: engine.identity.to_string(),
            mode,
            state,
            pid: None,
            started_at: None,
            exit_code,
            console_log: console_log.display().to_string(),
            link_bind,
            config_error,
        })
    }
}
