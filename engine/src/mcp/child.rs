//! One engine process this server spawned: how it is recorded, how a client sees it while it runs,
//! how its exit is noticed, and how it is stopped by signal.

use std::io;
use std::path::PathBuf;
use std::process::{Child, ExitStatus};

use rustix::process::{Pid, Signal, kill_process};

use crate::config::{ExecutionMode, RunIdentity};
use crate::time::{TsUs, boot_stamp_us};

use super::catalog::ParsedConfig;
use super::error::FleetError;
use super::reply::EngineSummary;

#[derive(Debug)]
pub(super) struct ChildSlot {
    pub(super) console_log: PathBuf,
    pub(super) state: SlotState,
}

#[derive(Debug)]
pub(super) enum SlotState {
    Running(LiveChild),
    Exited { exit_code: Option<i32> },
}

#[derive(Debug)]
pub(super) struct LiveChild {
    pub(super) child: Child,
    pub(super) started_ts_us: TsUs,
    pub(super) boot: ParsedConfig,
}

impl ChildSlot {
    /// Reads the wall clock, because a client shows this moment as the engine's start time.
    pub(super) fn running(child: Child, console_log: PathBuf, boot: ParsedConfig) -> ChildSlot {
        ChildSlot {
            console_log,
            state: SlotState::Running(LiveChild {
                child,
                started_ts_us: boot_stamp_us(),
                boot,
            }),
        }
    }

    pub(super) fn running_pid(&self) -> Option<u32> {
        match &self.state {
            SlotState::Running(live) => Some(live.child.id()),
            SlotState::Exited { .. } => None,
        }
    }

    /// Built from what the engine booted with and never from its config file, which may have
    /// been edited or moved since. `None` unless the child is running.
    pub(super) fn running_row(&self, identity: &RunIdentity) -> Option<EngineSummary> {
        let SlotState::Running(live) = &self.state else {
            return None;
        };
        Some(EngineSummary {
            strategy_id: identity.strategy_id.as_str().to_owned(),
            te_id: identity.te_id.as_str().to_owned(),
            bin: identity.to_string(),
            mode: ExecutionMode::footer_value(live.boot.mode),
            state: "running",
            pid: Some(live.child.id()),
            started_at: Some(format_rfc3339(live.started_ts_us)),
            exit_code: None,
            console_log: self.console_log.display().to_string(),
            link_bind: live.boot.link_bind.clone(),
            config_error: None,
        })
    }

    /// Records a child that has gone as exited, and returns its status only on that transition.
    pub(super) fn reap(&mut self) -> io::Result<Option<ExitStatus>> {
        let SlotState::Running(live) = &mut self.state else {
            return Ok(None);
        };
        let status = live.child.try_wait()?;
        if let Some(status) = status {
            self.state = SlotState::Exited {
                exit_code: status.code(),
            };
        }
        Ok(status)
    }
}

/// A live child taken out of the registry for the length of a stop. It goes back into the registry
/// if the stop fails, so a child this server could not stop is never forgotten.
#[derive(Debug)]
pub(super) struct Stopping {
    pub(super) identity: RunIdentity,
    pub(super) console_log: PathBuf,
    pub(super) live: LiveChild,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Settled {
    pub(super) status: ExitStatus,
    pub(super) is_forced: bool,
}

impl Stopping {
    pub(super) fn send_terminate(&self) -> Result<(), FleetError> {
        let pid = self.live.child.id();
        let Some(target) = i32::try_from(pid).ok().and_then(Pid::from_raw) else {
            unreachable!("a spawned child's pid {pid} is a positive i32");
        };
        // A child that `try_wait` has not reaped still owns its pid, so this cannot hit a stranger.
        kill_process(target, Signal::TERM).map_err(|errno| FleetError::Signal {
            identity: self.identity.clone(),
            pid,
            source: io::Error::from_raw_os_error(errno.raw_os_error()),
        })
    }

    /// `None` while the child is still draining. Past the deadline a child still alive is killed.
    pub(super) fn settle(&mut self, is_past_deadline: bool) -> Result<Option<Settled>, FleetError> {
        let child = &mut self.live.child;
        let identity = &self.identity;
        let wait_failed = |source| FleetError::Wait {
            identity: identity.clone(),
            source,
        };
        if let Some(status) = child.try_wait().map_err(wait_failed)? {
            return Ok(Some(Settled {
                status,
                is_forced: false,
            }));
        }
        if !is_past_deadline {
            return Ok(None);
        }
        let pid = child.id();
        child.kill().map_err(|source| FleetError::Kill {
            identity: identity.clone(),
            pid,
            source,
        })?;
        let status = child.wait().map_err(wait_failed)?;
        Ok(Some(Settled {
            status,
            is_forced: true,
        }))
    }

    pub(super) fn into_slot(self) -> (RunIdentity, ChildSlot) {
        let slot = ChildSlot {
            console_log: self.console_log,
            state: SlotState::Running(self.live),
        };
        (self.identity, slot)
    }
}

/// UTC with a microsecond fraction, the shape a JSON client parses without a calendar library.
fn format_rfc3339(at: TsUs) -> String {
    let civil = at.civil();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        civil.year, civil.month, civil.day, civil.hour, civil.minute, civil.second, civil.micros
    )
}
