//! The time-driven arms: the exit latches, the request deadlines and the periodic reconcile reads.
//! A timeout goes to reconcile rather than to a retry, because the command may already have
//! executed and the safe move is to ask. A refusal is resent instead, because it is proof that
//! nothing happened.

use crate::adapters::edge::EdgeDriver;
use crate::adapters::exec::{SessionOutcome, TimeoutFallout};
use crate::time::TsUs;
use crate::warn;

use super::rest::RestJob;
use super::{Actor, Writer};

impl Actor {
    pub(super) async fn on_housekeeping(&mut self, writer: &mut Writer) -> Option<SessionOutcome> {
        if let Some(reason) = self.control.wanted_new_exit() {
            self.plan_exit(reason, Some(writer)).await;
        }
        let now = self.control.clock.now();
        if let Some(ended) = self.expire_requests(now, writer).await {
            return Some(ended);
        }
        if let Some(ended) = self.poll_resync(now) {
            return Some(ended);
        }
        self.retry_sweep(Some(writer)).await;
        self.control
            .finished_exit(now, self.venue(), self.session.core.mirror().len())
    }

    async fn expire_requests(&mut self, now: TsUs, writer: &mut Writer) -> Option<SessionOutcome> {
        let expired = self.session.expire(now);
        let mut ended = None;
        for fallout in expired.unanswered_reads {
            ended = ended.or(self.on_unanswered_read(fallout));
        }
        self.dispatch(expired.outgoing, Some(writer))
            .await
            .or(ended)
    }

    #[cold]
    fn on_unanswered_read(&self, fallout: TimeoutFallout) -> Option<SessionOutcome> {
        match fallout {
            TimeoutFallout::StreamUnusable => {
                warn!("binance execution subscribe went unanswered — reconnecting");
                Some(SessionOutcome::Reconnect)
            }
            // The hot pass re-asks on its own cadence, so no reconnect is needed here.
            TimeoutFallout::ReadAbandoned => {
                warn!(
                    "binance execution open-orders read went unanswered — waiting for the next pass rather than dropping the connection"
                );
                None
            }
            TimeoutFallout::OrderInDoubt { .. } => None,
        }
    }

    // Runs the reconcile pass and piggybacks the periodic clock resync onto the same tick.
    pub(super) fn on_reconcile_tick(&mut self) -> Option<SessionOutcome> {
        self.reconcile_ticks = self.reconcile_ticks.wrapping_add(1);
        if self
            .reconcile_ticks
            .is_multiple_of(super::CLOCK_RESYNC_TICKS)
        {
            // A refused resync asks again on the next multiple, and the offset already in hand
            // stays usable meanwhile.
            let _dropped = self.submit(RestJob::SyncClock);
        }
        if self.is_balance_snapshot_due {
            self.request_balance_snapshot();
        }
        for cursor in &self.cursors {
            // The next tick walks from the same cursor, so a refused trade read loses no fill.
            let _dropped = self.submit(RestJob::MyTrades {
                instrument: cursor.instrument,
                from_id: cursor.from_id,
            });
        }
        None
    }
}
