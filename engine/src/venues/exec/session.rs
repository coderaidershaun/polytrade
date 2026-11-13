//! The rules that each execution edge applies to its core, its in-flight table, its resync pass and
//! its event sink, written once. Each instant is a parameter, so nothing here reads a clock.

use crate::msg::exec::{CancelReason, ExecEvent};
use crate::msg::ids::{ClientOrderId, InstrumentId, Side};
use crate::msg::inbound::{InboundMessage, MessageBody};
use crate::msg::time::{DurationUs, LocalTs};
use crate::{error, warn};

use super::{
    ExecCore, ExecEffect, ExecRequest, ExecSessionError, InFlightTable, MAX_RESYNC_ATTEMPTS,
    Outgoing, PlaceNotSentReason, ResyncPass, ResyncStep, TimeoutFallout, amend_not_sent,
    open_orders_snapshot_end, place_not_sent, request_timed_out, stream_reset,
};

pub trait EventSink {
    fn send_exec(&mut self, received_ts_us: LocalTs, event: ExecEvent);
}

impl EventSink for Vec<InboundMessage> {
    fn send_exec(&mut self, received_ts_us: LocalTs, event: ExecEvent) {
        self.push(InboundMessage::new(
            received_ts_us,
            MessageBody::Exec(event),
        ));
    }
}

pub struct SessionSetup<S> {
    pub venue: &'static str,
    pub core: ExecCore,
    pub instruments: Vec<InstrumentId>,
    pub events: S,
    /// `execution.inflight_timeout_ms`, the deadline that the hot slot of a request also reads, so
    /// the edge and the fold give up on an unanswered request at the same time.
    pub inflight_timeout: DurationUs,
}

/// What a deadline pass leaves for the venue: status probes to send, reads nobody answered, and
/// whether a placement was among the lost.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Expired {
    pub outgoing: Vec<Outgoing>,
    pub unanswered_reads: Vec<TimeoutFallout>,
    pub has_lost_placement: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResyncPoll {
    Wait,
    Reread { resync_seq: u64 },
    GiveUp,
}

pub struct ExecSession<S> {
    pub core: ExecCore,
    pub inflight: InFlightTable,
    pub resync: ResyncPass,
    pub events: S,
    venue: &'static str,
    instruments: Vec<InstrumentId>,
    is_readiness_pending: bool,
}

impl<S: EventSink> ExecSession<S> {
    pub fn new(setup: SessionSetup<S>) -> Self {
        Self {
            core: setup.core,
            inflight: InFlightTable::new(setup.inflight_timeout),
            resync: ResyncPass::default(),
            events: setup.events,
            venue: setup.venue,
            instruments: setup.instruments,
            is_readiness_pending: false,
        }
    }

    pub fn instruments(&self) -> &[InstrumentId] {
        &self.instruments
    }

    #[must_use]
    pub fn connect(&mut self) -> Vec<Outgoing> {
        let mut outgoing = Vec::new();
        self.core
            .on_connected(&mut Outgoing::sink(&mut outgoing, 0));
        outgoing
    }

    #[must_use]
    pub fn sweep(
        &mut self,
        reason: CancelReason,
        instrument: Option<InstrumentId>,
    ) -> Vec<Outgoing> {
        let mut outgoing = Vec::new();
        self.core
            .begin_sweep(reason, instrument, &mut Outgoing::sink(&mut outgoing, 0));
        outgoing
    }

    /// # Errors
    /// [`ExecSessionError::MirrorRetainFailed`] when the event reveals an order that the mirror
    /// cannot hold. The rest of the event is still applied.
    pub fn fold(
        &mut self,
        event: &ExecEvent,
        recon_seq: u64,
        outgoing: &mut Vec<Outgoing>,
    ) -> Result<(), ExecSessionError> {
        self.core
            .on_event(event, &mut Outgoing::sink(outgoing, recon_seq))
            .map_err(|source| ExecSessionError::MirrorRetainFailed {
                venue: self.venue,
                source,
            })
    }

    /// An open order that the codec cannot read leaves the mirror incomplete, and quoting against
    /// an incomplete mirror is what strands somebody's order, so quoting stops.
    #[must_use]
    pub fn undecodable_open_order(&mut self, venue_order_id: i64) -> ExecSessionError {
        self.core.stop_quoting();
        ExecSessionError::UndecodableOpenOrder {
            venue: self.venue,
            venue_order_id,
        }
    }

    /// Answers the hot slot for an effect the core decided must not be sent. Without the answer the
    /// slot waits out its whole in-flight timeout before the side can quote again.
    ///
    /// # Errors
    /// [`ExecSessionError::PlacementFailedClosed`] when the core refused a placement because its
    /// own state is corrupt. The slot is still answered.
    pub fn on_unsent(&mut self, effect: &ExecEffect, now: LocalTs) -> Result<(), ExecSessionError> {
        match *effect {
            ExecEffect::PlaceNotSent {
                instrument,
                client_id,
                side,
                reason,
            } => {
                warn!(
                    "{} execution proved placement {:016x} was not sent ({reason:?})",
                    self.venue, client_id.0
                );
                self.events
                    .send_exec(now, place_not_sent(instrument, client_id, side, now));
                if reason.is_fatal() {
                    return Err(ExecSessionError::PlacementFailedClosed {
                        venue: self.venue,
                        client_id,
                        reason,
                    });
                }
            }
            ExecEffect::AmendNotSent {
                instrument,
                client_id,
            } => {
                warn!(
                    "{} execution proved amend of {:016x} was not sent — the order rests unchanged",
                    self.venue, client_id.0
                );
                self.events
                    .send_exec(now, amend_not_sent(instrument, client_id, now));
            }
            ExecEffect::Send { .. }
            | ExecEffect::Skipped { .. }
            | ExecEffect::SweepComplete { .. } => {}
        }
        Ok(())
    }

    /// The edge could not put this request on the wire, so nothing reached the venue.
    ///
    /// # Errors
    /// The error of [`Self::on_unsent`] for a placement that the core refused on corrupt state.
    #[cold]
    pub fn refuse(
        &mut self,
        request: ExecRequest,
        reason: PlaceNotSentReason,
        now: LocalTs,
    ) -> Result<(), ExecSessionError> {
        match request {
            ExecRequest::Place {
                instrument,
                client_id,
                side,
                ..
            } => {
                self.core.on_place_not_sent(client_id);
                let effect = ExecEffect::PlaceNotSent {
                    instrument,
                    client_id,
                    side,
                    reason,
                };
                self.on_unsent(&effect, now)
            }
            ExecRequest::AmendQty {
                instrument,
                client_id,
                ..
            } => self.on_unsent(
                &ExecEffect::AmendNotSent {
                    instrument,
                    client_id,
                },
                now,
            ),
            ExecRequest::Cancel { client_id, .. } => {
                // Re-arming the latch lets the next cancel command or sweep pass send it again.
                self.core.re_arm_cancel(client_id);
                self.warn_refused(request, reason);
                Ok(())
            }
            ExecRequest::OrderStatus { .. }
            | ExecRequest::OpenOrders { .. }
            | ExecRequest::SubscribeUserStream => {
                self.warn_refused(request, reason);
                Ok(())
            }
        }
    }

    /// The bytes may have reached the venue, so the order's fate stays unknown until a read
    /// settles it.
    pub fn mark_in_doubt(&mut self, request: ExecRequest, now: LocalTs) {
        let TimeoutFallout::OrderInDoubt {
            instrument,
            client_id,
        } = request.timeout_fallout()
        else {
            return;
        };
        let side = self.mirrored_side(client_id);
        self.core.mark_ambiguous(client_id);
        self.events
            .send_exec(now, request_timed_out(instrument, client_id, side, now));
    }

    pub fn lose_connection(&mut self, now: LocalTs) {
        let outstanding = self.inflight.take_all();
        if !outstanding.is_empty() {
            warn!(
                "{} execution lost its connection with {} requests unanswered — their orders are unresolved until the resync",
                self.venue,
                outstanding.len()
            );
        }
        for entry in outstanding {
            self.mark_in_doubt(entry.request, now);
        }
        self.core.on_disconnected();
        self.events.send_exec(now, stream_reset(now));
    }

    /// An expired order request is reconciled with a status probe and never resent, because the
    /// first one may have executed and a second would double the order.
    #[must_use]
    pub fn expire(&mut self, now: LocalTs) -> Expired {
        let mut expired = Expired::default();
        for entry in self.inflight.take_expired(now) {
            let fallout = entry.request.timeout_fallout();
            let TimeoutFallout::OrderInDoubt {
                instrument,
                client_id,
            } = fallout
            else {
                expired.unanswered_reads.push(fallout);
                continue;
            };
            warn!(
                "{} execution request for order {:016x} went unanswered — reconciling, never retrying",
                self.venue, client_id.0
            );
            let side = self.mirrored_side(client_id);
            self.events
                .send_exec(now, request_timed_out(instrument, client_id, side, now));
            let mut probe = Outgoing::sink(&mut expired.outgoing, entry.recon_seq);
            self.core
                .on_request_timeout(instrument, client_id, &mut probe);
            expired.has_lost_placement |= matches!(entry.request, ExecRequest::Place { .. });
        }
        expired
    }

    #[must_use]
    pub fn begin_pass(&mut self, reads: usize) -> u64 {
        self.is_readiness_pending = false;
        self.resync.begin(reads)
    }

    pub fn finish_pass(&mut self) {
        self.is_readiness_pending = true;
    }

    #[must_use]
    pub fn poll_resync(&mut self, now: LocalTs, reads: usize) -> ResyncPoll {
        match self.resync.due(now) {
            ResyncStep::Wait => ResyncPoll::Wait,
            ResyncStep::Retry => {
                let resync_seq = self.resync.begin_retry(reads);
                warn!(
                    "{} execution re-reading its state — attempt {} of {MAX_RESYNC_ATTEMPTS}",
                    self.venue,
                    self.resync.attempts() + 1
                );
                ResyncPoll::Reread { resync_seq }
            }
            ResyncStep::GiveUp => {
                error!(
                    "{} execution could not read its state in {MAX_RESYNC_ATTEMPTS} attempts — dropping the connection rather than quoting against a mirror it never verified",
                    self.venue
                );
                ResyncPoll::GiveUp
            }
        }
    }

    /// A prior run's order still resting keeps readiness pending, so the pass that finished arms
    /// once that order is gone.
    pub fn arm_readiness(&mut self, now: LocalTs) -> bool {
        if !self.is_readiness_pending || !self.core.on_stream_ready() {
            return false;
        }
        self.is_readiness_pending = false;
        for &instrument in &self.instruments {
            self.events
                .send_exec(now, open_orders_snapshot_end(instrument, now));
        }
        true
    }

    fn mirrored_side(&self, client_id: ClientOrderId) -> Side {
        self.core
            .mirrored(client_id)
            .map_or(Side::Buy, |order| order.side)
    }

    fn warn_refused(&self, request: ExecRequest, reason: PlaceNotSentReason) {
        warn!(
            "{} execution could not send {request:?} ({reason:?}) — refusing rather than guessing",
            self.venue
        );
    }
}
