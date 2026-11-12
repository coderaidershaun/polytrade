//! What is running: the engines this server started, each one a child process it alone can
//! signal.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::config::{ExecutionMode, RunIdentity};

use super::catalog::{self, ConsoleLog, Engine, ProbedConfig};
use super::child::{ChildSlot, Settled, SlotState, Stopping};
use super::error::FleetError;
use super::reply::{EngineListing, EngineSummary, ModeChange, Started, Stopped};

const POLL_INTERVAL: Duration = Duration::from_millis(50);

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
#[derive(Debug)]
pub struct Fleet {
    root: PathBuf,
    bin_dir: PathBuf,
    timing: FleetTiming,
    children: HashMap<RunIdentity, ChildSlot>,
    is_closed: bool,
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
        }
    }

    /// # Errors
    /// [`FleetError::ReadDir`] when `strategies/` or a strategy folder inside it cannot be read. An
    /// unreadable `strategies/` usually means the root is wrong. [`FleetError::Wait`] when a
    /// child's exit status cannot be read. One unreadable config never fails the listing: its row
    /// says why instead.
    pub fn list(&mut self) -> Result<EngineListing, FleetError> {
        let engines = catalog::discover(&self.root)?;
        let tracked: Vec<RunIdentity> = self.children.keys().cloned().collect();
        for identity in &tracked {
            self.reap(identity)?;
        }
        let mut rows: Vec<EngineSummary> = engines
            .iter()
            .map(|engine| self.summarize(engine))
            .collect();
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
    /// [`FleetError::ConfigUnreadable`], [`FleetError::BinaryMissing`],
    /// [`FleetError::ExitedEarly`], or a failure to read the strategies tree, open the console
    /// log, spawn the binary or read its exit status.
    pub async fn start(&mut self, identity: &RunIdentity) -> Result<Started, FleetError> {
        if self.is_closed {
            return Err(FleetError::ShuttingDown);
        }
        let engine = self.find_engine(identity)?;
        if let Some(pid) = self.live_pid(identity)? {
            return Err(FleetError::AlreadyRunning {
                identity: identity.clone(),
                pid,
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
    /// [`FleetError::NotRunning`], or the signal, kill and wait failures.
    pub async fn stop(&mut self, identity: &RunIdentity) -> Result<Stopped, FleetError> {
        let outcomes = self.stop_together(vec![identity.clone()]).await;
        outcomes.into_iter().next().map_or_else(
            || {
                Err(FleetError::NotRunning {
                    identity: identity.clone(),
                })
            },
            |outcome| outcome.result,
        )
    }

    /// Stops every running child under one shared deadline, and refuses every later start.
    pub async fn stop_all(&mut self) -> Vec<StopOutcome> {
        self.is_closed = true;
        let mut running: Vec<RunIdentity> = self
            .children
            .iter()
            .filter(|(_, slot)| matches!(slot.state, SlotState::Running(_)))
            .map(|(identity, _)| identity.clone())
            .collect();
        running.sort_by_cached_key(ToString::to_string);
        let outcomes = self.stop_together(running).await;
        outcomes
            .into_iter()
            .filter(|outcome| !matches!(outcome.result, Err(FleetError::NotRunning { .. })))
            .collect()
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
        let previous = engine.rewrite_mode(mode)?;
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

    /// Signals every target first and only then waits, so N engines share one deadline.
    async fn stop_together(&mut self, identities: Vec<RunIdentity>) -> Vec<StopOutcome> {
        let began = Instant::now();
        let mut outcomes = Vec::with_capacity(identities.len());
        let mut stopping = Vec::with_capacity(identities.len());
        for identity in identities {
            match self.begin_stop(&identity) {
                Ok(child) => stopping.push(child),
                Err(error) => outcomes.push(StopOutcome {
                    identity,
                    result: Err(error),
                }),
            }
        }
        let deadline = began + self.timing.stop_deadline;
        while !stopping.is_empty() {
            let is_past_deadline = Instant::now() >= deadline;
            let mut still_running = Vec::with_capacity(stopping.len());
            for mut child in stopping {
                match child.settle(is_past_deadline) {
                    Ok(Some(settled)) => outcomes.push(stopped(child.identity, settled, began)),
                    Ok(None) => still_running.push(child),
                    Err(error) => {
                        let (identity, slot) = child.into_slot();
                        self.children.insert(identity.clone(), slot);
                        outcomes.push(StopOutcome {
                            identity,
                            result: Err(error),
                        });
                    }
                }
            }
            stopping = still_running;
            if !stopping.is_empty() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        outcomes
    }

    /// Takes a live child out of the registry and sends it SIGTERM. Reaping first means the pid
    /// signalled is one this server still owns.
    fn begin_stop(&mut self, identity: &RunIdentity) -> Result<Stopping, FleetError> {
        let not_running = || FleetError::NotRunning {
            identity: identity.clone(),
        };
        self.reap(identity)?;
        let slot = self.children.remove(identity).ok_or_else(not_running)?;
        let stopping = match slot.state {
            SlotState::Running(live) => Stopping {
                identity: identity.clone(),
                console_log: slot.console_log,
                live,
            },
            SlotState::Exited { .. } => {
                self.children.insert(identity.clone(), slot);
                return Err(not_running());
            }
        };
        match stopping.send_terminate() {
            Ok(()) => Ok(stopping),
            Err(error) => {
                let (identity, slot) = stopping.into_slot();
                self.children.insert(identity, slot);
                Err(error)
            }
        }
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
    fn reap(&mut self, identity: &RunIdentity) -> Result<Option<ExitStatus>, FleetError> {
        let Some(slot) = self.children.get_mut(identity) else {
            return Ok(None);
        };
        slot.reap().map_err(|source| FleetError::Wait {
            identity: identity.clone(),
            source,
        })
    }

    /// A stopped or exited row reads the config file, because that is what the next start boots
    /// from.
    fn summarize(&self, engine: &Engine) -> EngineSummary {
        let slot = self.children.get(&engine.identity);
        if let Some(row) = slot.and_then(|slot| slot.running_row(&engine.identity)) {
            return row;
        }
        let probed = engine.probe();
        let (state, exit_code, console_log) = match slot {
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
        EngineSummary {
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
        }
    }
}

fn stopped(identity: RunIdentity, settled: Settled, began: Instant) -> StopOutcome {
    let elapsed_ms = began.elapsed().as_millis();
    let result = Ok(Stopped {
        strategy_id: identity.strategy_id.as_str().to_owned(),
        te_id: identity.te_id.as_str().to_owned(),
        exit_code: settled.status.code(),
        forced: settled.is_forced,
        duration_ms: u64::try_from(elapsed_ms).unwrap_or(u64::MAX),
    });
    StopOutcome { identity, result }
}
