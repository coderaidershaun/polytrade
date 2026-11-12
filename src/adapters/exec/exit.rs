//! Exit lifecycle shared by all venue edges: the handle the runtime holds, the latches that handle
//! trips, and the plan a sweep executes under. Ordering and pacing are safety rules that must not
//! drift between implementations.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::msg::exec::CancelReason;
use crate::time::{DurationUs, TsUs};
use crate::warn;

// The sole mechanism for pacing cancel retries after the hot path stops.
const SWEEP_RETRY: DurationUs = DurationUs::from_micros(500_000);

#[derive(Clone, Default)]
pub(crate) struct ExecStop {
    pub(crate) requested: Arc<AtomicBool>,
    pub(crate) settled: Arc<Notify>,
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

    pub(crate) async fn shutdown(self) {
        self.stop.requested.store(true, Ordering::Release);
        let settled = self.stop.settled.notified();
        if tokio::time::timeout(self.sweep_deadline, settled)
            .await
            .is_err()
        {
            warn!(
                "{} did not confirm its cancel sweep within {}ms — {}",
                self.venue,
                self.sweep_deadline.as_millis(),
                self.missed_sweep_cost
            );
        }
        crate::shutdown::abort_and_warn(self.join, self.venue).await;
    }
}

pub(crate) struct ExitPlan {
    pub(crate) reason: CancelReason,
    pub(crate) deadline: TsUs,
    retry_at: TsUs,
}

impl ExitPlan {
    pub(crate) fn new(reason: CancelReason, now: TsUs, sweep_deadline: Duration) -> Self {
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
}

/// How a served connection ended: reconnect and carry on, or the exit sweep finished under it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionOutcome {
    Reconnect,
    Swept,
}
