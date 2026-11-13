//! Exit lifecycle shared by all venue edges: the handle the runtime holds, the latches that handle
//! trips, and the plan a sweep executes under. Ordering and pacing are safety rules that must not
//! drift between implementations.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::msg::exec::CancelReason;
use crate::msg::ids::ClientOrderId;
use crate::msg::time::{DurationUs, TsUs};
use crate::{error, warn};

use super::MirroredOrder;

// The sole mechanism for pacing cancel retries after the hot path stops.
const SWEEP_RETRY: DurationUs = DurationUs::from_micros(500_000);

#[derive(Clone, Default)]
pub(crate) struct ExecStop {
    pub(crate) requested: Arc<AtomicBool>,
    settled: Arc<Notify>,
    failure: Arc<OnceLock<SweepFailure>>,
}

impl ExecStop {
    /// Raises the settled signal, with the failure of its sweep when the sweep did not end clean.
    pub(crate) fn settle(&self, failure: Option<SweepFailure>) {
        if let Some(failure) = failure {
            self.failure.get_or_init(|| failure);
        }
        self.settled.notify_one();
    }
}

/// What the runtime holds after a venue edge is spawned. Dropping it detaches; shutting it down
/// cancels first and stops second, so no order is left resting with nothing watching it — a
/// market-data adapter can abort instantly, an execution edge cannot.
pub(crate) struct EdgeHandle {
    pub(crate) join: JoinHandle<()>,
    pub(crate) stop: ExecStop,
    pub(crate) sweep_deadline: Duration,
    /// Which edge an operator is reading about when a sweep misses its deadline.
    pub(crate) venue: &'static str,
    /// What that missed sweep costs here. Venue-specific because it genuinely differs: orders left
    /// on a real venue outlive the process, while a simulator's die with it and take the run's
    /// results down instead.
    pub(crate) missed_sweep_cost: &'static str,
}

impl EdgeHandle {
    /// For a live venue only: the cost it reports for a missed sweep is orders left resting there.
    pub(crate) fn spawn(
        venue: &'static str,
        sweep_deadline: Duration,
        start: impl FnOnce(ExecStop) -> JoinHandle<()>,
    ) -> Self {
        let stop = ExecStop::default();
        let join = start(stop.clone());
        Self {
            join,
            stop,
            sweep_deadline,
            venue,
            missed_sweep_cost: "orders may still rest on the venue",
        }
    }

    /// # Errors
    /// [`SweepFailure`] when the sweep was not confirmed within its deadline, or when the edge
    /// settled with orders still resting.
    pub(crate) async fn shutdown(self) -> Result<(), SweepFailure> {
        self.stop.requested.store(true, Ordering::Release);
        let settled = self.stop.settled.notified();
        let is_settled = tokio::time::timeout(self.sweep_deadline, settled)
            .await
            .is_ok();
        if !is_settled {
            warn!(
                "{} did not confirm its cancel sweep within {}ms — {}",
                self.venue,
                self.sweep_deadline.as_millis(),
                self.missed_sweep_cost
            );
        }
        crate::runtime::shutdown::abort_and_warn(self.join, self.venue).await;
        if !is_settled {
            return Err(SweepFailure::Unconfirmed {
                venue: self.venue,
                deadline_ms: self.sweep_deadline.as_millis(),
            });
        }
        self.stop.failure.get().cloned().map_or(Ok(()), Err)
    }
}

pub struct ExitPlan {
    pub(crate) reason: CancelReason,
    pub(crate) deadline: TsUs,
    retry_at: TsUs,
}

impl ExitPlan {
    pub fn new(reason: CancelReason, now: TsUs, sweep_deadline: Duration) -> Self {
        Self {
            reason,
            deadline: now + DurationUs::from_micros(sweep_deadline.as_micros() as i64),
            retry_at: now + SWEEP_RETRY,
        }
    }

    pub(crate) fn is_final(&self) -> bool {
        self.reason.is_terminal()
    }

    /// Returns the reason to sweep once the retry interval allows. Pacing prevents wasting the venue's cancel budget.
    pub(crate) fn claim_retry(&mut self, now: TsUs) -> Option<CancelReason> {
        if now < self.retry_at {
            return None;
        }
        self.retry_at = now + SWEEP_RETRY;
        Some(self.reason)
    }

    /// How the exit ends once `now` is past its deadline, given the orders still mirrored as
    /// resting; `None` while the deadline holds. Giving up writes one ERROR line naming each order.
    pub fn past_deadline(
        &self,
        now: TsUs,
        venue: &'static str,
        resting: &[MirroredOrder],
    ) -> Option<SessionOutcome> {
        if now <= self.deadline {
            return None;
        }
        if resting.is_empty() {
            return Some(SessionOutcome::Swept);
        }
        let client_ids: Vec<String> = resting
            .iter()
            .map(|order| format!("{:016x}", order.client_id.0))
            .collect();
        error!(
            "{venue} execution gave up waiting for its {:?} sweep — orders still mirrored as resting: {}",
            self.reason,
            client_ids.join(", ")
        );
        let resting = resting.iter().map(|order| order.client_id).collect();
        Some(SessionOutcome::SweepPastDeadline(
            SweepFailure::PastDeadline { venue, resting },
        ))
    }
}

/// How a served connection ended: reconnect and carry on, or the exit sweep finished under it, or
/// the sweep gave up at its deadline with orders still resting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionOutcome {
    Reconnect,
    Swept,
    SweepPastDeadline(SweepFailure),
}

/// Why the exit sweep of an execution edge did not end clean. Orders may still rest on the venue, so
/// the drain counts it as a failure and the exit is not graceful.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SweepFailure {
    #[error(
        "{venue} gave up its exit sweep at the deadline with {} orders still mirrored as resting",
        resting.len()
    )]
    PastDeadline {
        venue: &'static str,
        resting: Box<[ClientOrderId]>,
    },
    /// The edge never confirmed its sweep: its socket stayed down, or nothing answered in time.
    #[error("{venue} did not confirm its exit sweep within {deadline_ms} ms")]
    Unconfirmed {
        venue: &'static str,
        deadline_ms: u128,
    },
}
