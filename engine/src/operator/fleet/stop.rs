//! How the fleet stops its engines: SIGTERM to each, one shared deadline, and SIGKILL only past it.
//! A stop holds the fleet only to signal and to record, and waits for its engines without it.

use std::time::{Duration, Instant};

use crate::runtime::config::RunIdentity;

use super::child::{Settled, SlotState, Stopping};
use super::error::FleetError;
use super::reply::Stopped;
use super::supervisor::{Fleet, POLL_INTERVAL, StopOutcome};

impl Fleet {
    /// # Errors
    /// [`FleetError::NotRunning`], [`FleetError::Stopping`], or the signal, kill and wait failures.
    pub async fn stop(&mut self, identity: &RunIdentity) -> Result<Stopped, FleetError> {
        let stops = self.begin_stops(vec![identity.clone()]);
        let settled = stops.settle().await;
        stop_result(identity, self.finish_stops(settled))
    }

    /// Stops every running child under one shared deadline, and refuses every later start.
    pub async fn stop_all(&mut self) -> Vec<StopOutcome> {
        let stops = self.begin_stop_all();
        let settled = stops.settle().await;
        self.finish_all(settled)
    }

    /// Closes the fleet to starts, and signals every running child.
    pub(super) fn begin_stop_all(&mut self) -> StopBatch {
        self.is_closed = true;
        let mut running: Vec<RunIdentity> = self
            .children
            .iter()
            .filter(|(_, slot)| matches!(slot.state, SlotState::Running(_)))
            .map(|(identity, _)| identity.clone())
            .collect();
        running.sort_by_cached_key(ToString::to_string);
        self.begin_stops(running)
    }

    pub(super) fn finish_all(&mut self, settled: SettledStops) -> Vec<StopOutcome> {
        self.finish_stops(settled)
            .into_iter()
            .filter(|outcome| !matches!(outcome.result, Err(FleetError::NotRunning { .. })))
            .collect()
    }

    /// Signals every target first and only then waits, so N engines share one deadline. Each slot
    /// stays as stopping, and the wait needs no borrow of the fleet.
    #[expect(
        clippy::disallowed_methods,
        reason = "the operator fleet times the starts and stops of its children and holds no engine state"
    )]
    pub(super) fn begin_stops(&mut self, identities: Vec<RunIdentity>) -> StopBatch {
        let mut batch = StopBatch {
            began: Instant::now(),
            deadline: self.timing.stop_deadline,
            stopping: Vec::with_capacity(identities.len()),
            refused: Vec::new(),
        };
        for identity in identities {
            match self.begin_stop(&identity) {
                Ok(child) => batch.stopping.push(child),
                Err(error) => batch.refused.push(StopOutcome {
                    identity,
                    result: Err(error),
                }),
            }
        }
        batch
    }

    /// Records each outcome: a stopped child leaves its slot, and a child that could not be
    /// stopped goes back into it.
    pub(super) fn finish_stops(&mut self, settled: SettledStops) -> Vec<StopOutcome> {
        let SettledStops {
            began,
            settled,
            mut outcomes,
        } = settled;
        for (child, result) in settled {
            let identity = child.identity.clone();
            match result {
                Ok(settled) => {
                    self.children.remove(&identity);
                    outcomes.push(stopped(identity, settled, began));
                }
                Err(error) => {
                    if let Some(slot) = self.children.get_mut(&identity) {
                        slot.give_back(child);
                    }
                    outcomes.push(StopOutcome {
                        identity,
                        result: Err(error),
                    });
                }
            }
        }
        outcomes
    }

    /// Sends a live child SIGTERM and takes it out of its slot, which stays as stopping. Reaping
    /// first means the pid signalled is one this server still owns.
    fn begin_stop(&mut self, identity: &RunIdentity) -> Result<Stopping, FleetError> {
        let not_running = || FleetError::NotRunning {
            identity: identity.clone(),
        };
        self.reap(identity)?;
        let slot = self.children.get_mut(identity).ok_or_else(not_running)?;
        if let (true, Some(pid)) = (slot.is_stopping(), slot.running_pid()) {
            return Err(FleetError::Stopping {
                identity: identity.clone(),
                pid,
            });
        }
        let stopping = slot.take_for_stop(identity).ok_or_else(not_running)?;
        match stopping.send_terminate() {
            Ok(()) => Ok(stopping),
            Err(error) => {
                slot.give_back(stopping);
                Err(error)
            }
        }
    }
}

/// Engines that were signalled, to be waited for without the fleet. One deadline covers them all.
#[derive(Debug)]
pub(super) struct StopBatch {
    began: Instant,
    deadline: Duration,
    stopping: Vec<Stopping>,
    refused: Vec<StopOutcome>,
}

#[derive(Debug)]
pub(super) struct SettledStops {
    began: Instant,
    settled: Vec<(Stopping, Result<Settled, FleetError>)>,
    outcomes: Vec<StopOutcome>,
}

impl StopBatch {
    /// Polls each child until it exits, and kills each one still alive at the deadline.
    #[expect(
        clippy::disallowed_methods,
        reason = "the operator fleet times the starts and stops of its children and holds no engine state"
    )]
    pub(super) async fn settle(self) -> SettledStops {
        let StopBatch {
            began,
            deadline,
            mut stopping,
            refused,
        } = self;
        let deadline = began + deadline;
        let mut settled = Vec::with_capacity(stopping.len());
        while !stopping.is_empty() {
            let is_past_deadline = Instant::now() >= deadline;
            let mut still_running = Vec::with_capacity(stopping.len());
            for mut child in stopping {
                match child.settle(is_past_deadline) {
                    Ok(Some(exit)) => settled.push((child, Ok(exit))),
                    Ok(None) => still_running.push(child),
                    Err(error) => settled.push((child, Err(error))),
                }
            }
            stopping = still_running;
            if !stopping.is_empty() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
        SettledStops {
            began,
            settled,
            outcomes: refused,
        }
    }
}

/// The outcome of a stop of one engine.
pub(super) fn stop_result(
    identity: &RunIdentity,
    outcomes: Vec<StopOutcome>,
) -> Result<Stopped, FleetError> {
    outcomes.into_iter().next().map_or_else(
        || {
            Err(FleetError::NotRunning {
                identity: identity.clone(),
            })
        },
        |outcome| outcome.result,
    )
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
