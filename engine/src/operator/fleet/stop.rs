//! How the fleet stops its engines: SIGTERM to each, one shared deadline, and SIGKILL only past it.

use std::time::Instant;

use crate::runtime::config::RunIdentity;

use super::child::{Settled, SlotState, Stopping};
use super::error::FleetError;
use super::reply::Stopped;
use super::supervisor::{Fleet, POLL_INTERVAL, StopOutcome};

impl Fleet {
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

    /// Signals every target first and only then waits, so N engines share one deadline.
    #[expect(
        clippy::disallowed_methods,
        reason = "the operator fleet times the starts and stops of its children and holds no engine state"
    )]
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
}

#[expect(
    clippy::disallowed_methods,
    reason = "the operator fleet times the starts and stops of its children and holds no engine state"
)]
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
