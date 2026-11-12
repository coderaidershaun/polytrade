//! Time-driven arms: the exit latches, request expiry, the correlation buffer's TTL, the
//! delayed-cancel release, the heartbeat, and the rotation bindings arriving from the market-data
//! actor.

use std::sync::Arc;

use crate::adapters::edge::EdgeDriver;
use crate::adapters::exec::SessionOutcome;
use crate::adapters::polymarket::rotation::WindowAssignment;
use crate::msg::exec::CancelReason;
use crate::time::TsUs;
use crate::{info, warn};

use super::super::binding::{BindingStep, EnrichmentRead, TokenOfInstrument};
use super::super::codec::{
    cancel_market_orders, clob_market_request, heartbeat, neg_risk_request, trades_page,
};
use super::rest::{Lane, RestJob, RestPurpose};
use super::{Actor, CLOSE_MARGIN, Writer};

impl Actor {
    pub(super) async fn on_housekeeping(&mut self, writer: &mut Writer) -> Option<SessionOutcome> {
        if let Some(reason) = self.control.wanted_new_exit() {
            self.plan_exit(reason, Some(writer)).await;
        }
        let now = self.control.clock.now();
        self.clear_park(now);
        self.abandon_expired_frames();
        if let Some(ended) = self.release_withheld_cancels(Some(writer)).await {
            return Some(ended);
        }
        if let Some(ended) = self.expire_requests(now, writer).await {
            return Some(ended);
        }
        if let Some(ended) = self.poll_resync(now) {
            return Some(ended);
        }
        self.send_heartbeat(now);
        self.retry_binding_reads(now);
        if let Some(ended) = self.sweep_expiring_windows(writer).await {
            return Some(ended);
        }
        self.retry_sweep(Some(writer)).await;
        self.control
            .finished_exit(now, self.venue(), self.session.core.mirror().len())
    }

    fn clear_park(&mut self, now: TsUs) {
        let Some(parked) = self.parked else {
            return;
        };
        if now < parked.until {
            return;
        }
        info!(
            "polymarket execution resuming after venue state {:?}",
            parked.availability
        );
        self.parked = None;
    }

    /// Unanswered requests past deadline are reconciled, never retried (venue may have accepted).
    async fn expire_requests(&mut self, now: TsUs, writer: &mut Writer) -> Option<SessionOutcome> {
        let expired = self.session.expire(now);
        for _read in &expired.unanswered_reads {
            warn!(
                "polymarket execution read went unanswered — waiting for the next pass rather than dropping the stream"
            );
        }
        // An unanswered placement may rest under an id this run never learned; the resync adopts it.
        if expired.has_lost_placement {
            self.nudge_resync();
        }
        self.dispatch(expired.outgoing, Some(writer)).await
    }

    /// Reads fills to watch for settlement, paced within the venue's read budget.
    pub(super) fn on_reconcile_tick(&mut self) -> Option<SessionOutcome> {
        let purpose = RestPurpose::Trades {
            resync_seq: None,
            page: 0,
        };
        // A dropped poll costs one settlement look; the next tick is five seconds away.
        let _dropped = self.submit(Lane::Control, RestJob::signed(purpose, trades_page(None)));
        None
    }

    /// Arms once orders become possible; once started, the venue cancels the book after 10s
    /// of silence.
    fn send_heartbeat(&mut self, now: TsUs) {
        if !self.heartbeat.is_started {
            if !self.session.core.phase().admits_new_orders() {
                return;
            }
            self.heartbeat.is_started = true;
            info!(
                "polymarket execution starting its dead-man's-switch heartbeat — the venue will cancel this book if the process stops"
            );
        }
        if now.diff(self.heartbeat.sent_at) < super::HEARTBEAT_PERIOD {
            return;
        }
        let id = self.heartbeat.id.clone().unwrap_or_default();
        let Ok(request) = heartbeat(&id) else {
            return;
        };
        // Stamped only once the beat is on its way. Stamping the attempt would hold the next one off
        // for five seconds against a ten-second deadline, and the venue cancels the whole book when
        // that deadline passes.
        if self
            .submit(
                Lane::Heartbeat,
                RestJob::signed(RestPurpose::Heartbeat, request),
            )
            .is_queued()
        {
            self.heartbeat.sent_at = now;
        }
    }

    /// A backstop that cancels resting orders on windows closing within CLOSE_MARGIN.
    async fn sweep_expiring_windows(&mut self, writer: &mut Writer) -> Option<SessionOutcome> {
        let expiring = self
            .bindings
            .close_margin_reached(self.control.clock.now(), CLOSE_MARGIN);
        if expiring.is_empty() {
            return None;
        }
        let mut ended = None;
        for TokenOfInstrument {
            instrument,
            token_id,
        } in expiring
        {
            // In the normal case, the hot engine has already withdrawn on its own margin;
            // this check avoids sweeping an instrument that has nothing left resting.
            let is_resting = self
                .session
                .core
                .mirror()
                .iter()
                .any(|order| order.instrument == instrument);
            if !is_resting {
                continue;
            }
            if let Ok(request) = cancel_market_orders(&token_id) {
                warn!(
                    "polymarket execution sweeping instrument {} — its window closes within {}ms and {} order(s) are still mirrored",
                    instrument.0,
                    CLOSE_MARGIN.micros() / 1_000,
                    self.session.core.mirror().len()
                );
                // The per-order sweep below reaches the same orders, so a dropped bulk cancel costs
                // the shortcut, not the sweep.
                let purpose = RestPurpose::MarketCancel { instrument };
                let _dropped = self.submit(Lane::Control, RestJob::signed(purpose, request));
            }
            // The mirror must also learn the orders are gone, so both sides are swept.
            let outgoing = self.session.sweep(CancelReason::Halt, Some(instrument));
            ended = ended.or(self.dispatch(outgoing, Some(writer)).await);
        }
        ended
    }

    /// A window assignment from the market-data actor; the binding completes only once its
    /// enrichment reads land.
    pub(super) fn on_assignment(
        &mut self,
        assignment: Option<WindowAssignment>,
    ) -> Option<SessionOutcome> {
        let assignment = assignment?;
        info!(
            "polymarket execution binding window {} on condition {}",
            assignment.window_open_ts_us.micros(),
            assignment.condition_id
        );
        let step = self.bindings.on_assignment(&assignment);
        let BindingStep::Enrich {
            condition_id,
            tokens,
        } = step
        else {
            return None;
        };
        self.submit_enrichment(EnrichmentRead::Market {
            condition_id: Arc::clone(&condition_id),
        });
        for TokenOfInstrument {
            instrument,
            token_id,
        } in tokens
        {
            self.submit_enrichment(EnrichmentRead::NegRisk {
                condition_id: Arc::clone(&condition_id),
                instrument,
                token_id,
            });
        }
        None
    }

    /// Retries the enrichment reads for bindings still waiting, recovering from transient failures.
    fn retry_binding_reads(&mut self, now: TsUs) {
        for read in self.bindings.due_enrichment_reads(now) {
            self.submit_enrichment(read);
        }
    }

    /// A dropped read is re-issued by the retry until the binding completes or gives up, so a drop
    /// costs one retry interval.
    fn submit_enrichment(&mut self, read: EnrichmentRead) {
        let job = match read {
            EnrichmentRead::Market { condition_id } => {
                let request = clob_market_request(&condition_id);
                RestJob::public(RestPurpose::Market { condition_id }, request)
            }
            EnrichmentRead::NegRisk {
                condition_id,
                instrument,
                token_id,
            } => {
                let purpose = RestPurpose::NegRisk {
                    condition_id,
                    instrument,
                };
                RestJob::public(purpose, neg_risk_request(&token_id))
            }
        };
        let _dropped = self.submit(Lane::Control, job);
    }
}
