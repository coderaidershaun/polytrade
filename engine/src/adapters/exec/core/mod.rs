//! Core order state machine: decides phases and order lifecycle, mirrors known orders, and
//! coordinates fatal shutdown. Drivers provide I/O (socket/clock) and inbound events for replay.

mod answer;
mod command;

use crate::ids::{ClientOrderId, InstrumentId, Side};
use crate::msg::exec::{CancelReason, Provenance};
use crate::time::DurationUs;

use super::effect::{ExecEffect, ExecRequest, PlaceNotSentReason, RequestId, SkipReason};
use super::mirror::{MirroredOrder, OrderMirror};

// After this interval, treat unanswered requests as unknown (reconcile instead of retry).
// Retrying risks two live orders at the same price.
pub const REQUEST_TIMEOUT: DurationUs = DurationUs::from_micros(5_000_000);

// Lifecycle gate. Only Quoting accepts new orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Phase {
    Down,
    Resyncing,
    Quoting,
    Cancelling,
    Settled,
}

impl Phase {
    pub const fn admits_new_orders(self) -> bool {
        matches!(self, Phase::Quoting)
    }

    pub const fn admits_cancels(self) -> bool {
        matches!(self, Phase::Resyncing | Phase::Quoting | Phase::Cancelling)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Phase::Settled)
    }
}

pub struct ExecCore {
    phase: Phase,
    next_request_id: u64,
    mirror: OrderMirror,
    max_orders_per_side: usize,
    sweep: Option<CancelReason>,
    has_opened_quoting: bool,
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObserveOrderError {
    #[error("execution mirror is full at {capacity} orders and cannot retain another")]
    MirrorStorageExhausted { capacity: usize },
    #[error(
        "instrument {} holds {count} possibly-live {side:?} orders, over the limit of {limit}",
        instrument.0
    )]
    OwnedSideOverLimit {
        instrument: InstrumentId,
        side: Side,
        count: usize,
        limit: usize,
    },
}

impl ExecCore {
    pub fn with_limits(max_orders_per_side: usize, mirror_capacity: usize) -> Self {
        Self {
            phase: Phase::Down,
            next_request_id: 1,
            mirror: OrderMirror::with_capacity(mirror_capacity),
            max_orders_per_side,
            sweep: None,
            has_opened_quoting: false,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn mirror(&self) -> &[MirroredOrder] {
        self.mirror.as_slice()
    }

    // Move to Resyncing until stream proves itself. Quoting against stale state is a guess.
    pub fn on_connected(&mut self, emit: &mut dyn FnMut(ExecEffect)) {
        if self.phase.is_terminal() {
            return;
        }
        if !matches!(self.phase, Phase::Cancelling) {
            self.phase = Phase::Resyncing;
        }
        self.send(ExecRequest::SubscribeUserStream, emit);
    }

    // Marks mirror stale but does not change phase if mid-sweep; orders stay resting on venue.
    pub fn on_disconnected(&mut self) {
        if self.phase.is_terminal() {
            return;
        }
        if !matches!(self.phase, Phase::Cancelling) {
            self.phase = Phase::Down;
        }
        self.mirror.mark_all_stale();
    }

    pub fn on_stream_ready(&mut self) -> bool {
        if matches!(self.phase, Phase::Resyncing) && !self.mirror.has_prior_run() {
            self.phase = Phase::Quoting;
            self.has_opened_quoting = true;
            return true;
        }
        false
    }

    /// Never resets. Before this run first quotes, any order found resting cannot be one it placed,
    /// so the fold treats it as a prior run's.
    pub fn has_opened_quoting(&self) -> bool {
        self.has_opened_quoting
    }

    pub fn possibly_live_count(&self, instrument: InstrumentId, side: Side) -> usize {
        self.mirror.possibly_live_count(instrument, side)
    }

    pub fn has_prior_run(&self) -> bool {
        self.mirror.has_prior_run()
    }

    pub fn is_mirrored(&self, client_id: ClientOrderId) -> bool {
        self.mirror.find(client_id).is_some()
    }

    pub fn mirrored(&self, client_id: ClientOrderId) -> Option<&MirroredOrder> {
        self.mirror.find(client_id)
    }

    /// Only current-run placement released; prior-run identity with same id stays.
    pub fn on_place_not_sent(&mut self, client_id: ClientOrderId) {
        let is_current_place = self
            .mirror
            .find(client_id)
            .is_some_and(|order| order.provenance == Provenance::Mine);
        if is_current_place {
            self.mirror.remove(client_id);
        }
    }

    /// Transport may have accepted bytes -> reservation uncertain until terminal event or proof.
    pub fn mark_ambiguous(&mut self, client_id: ClientOrderId) {
        if let Some(order) = self.mirror.find_mut(client_id) {
            order.is_ambiguous = true;
        }
    }

    pub fn stop_quoting(&mut self) {
        if !self.phase.is_terminal() {
            self.phase = Phase::Cancelling;
        }
    }

    fn refuse_place(
        &self,
        instrument: InstrumentId,
        client_id: ClientOrderId,
        side: Side,
        reason: PlaceNotSentReason,
        emit: &mut dyn FnMut(ExecEffect),
    ) {
        emit(ExecEffect::PlaceNotSent {
            instrument,
            client_id,
            side,
            reason,
        });
    }

    /// Exit path entry: pull every order this run owns (reason recorded for tape).
    pub fn begin_sweep(
        &mut self,
        reason: CancelReason,
        instrument: Option<InstrumentId>,
        emit: &mut dyn FnMut(ExecEffect),
    ) {
        if self.phase.is_terminal() {
            return;
        }
        self.phase = Phase::Cancelling;
        self.sweep = Some(reason);
        self.cancel_matching(instrument, Provenance::Mine, emit);
        self.cancel_matching(instrument, Provenance::PriorRun, emit);
        self.probe_unresolved(instrument, emit);
        self.settle_if_swept(emit);
    }

    /// Re-ask unresolved orders (cancel sent, answer left state unknown). Repeats on each pass
    /// (probe can be lost mid-flight, no hot thread to re-derive by sweep time).
    fn probe_unresolved(
        &mut self,
        instrument: Option<InstrumentId>,
        emit: &mut dyn FnMut(ExecEffect),
    ) {
        for (instrument, client_id) in self.mirror.unresolved(instrument) {
            self.send(
                ExecRequest::OrderStatus {
                    instrument,
                    client_id,
                },
                emit,
            );
        }
    }

    fn cancel_one(
        &mut self,
        instrument: InstrumentId,
        client_id: ClientOrderId,
        emit: &mut dyn FnMut(ExecEffect),
    ) {
        if !self.phase.admits_cancels() {
            return;
        }
        let Some(order) = self.mirror.find_mut(client_id) else {
            self.send(
                ExecRequest::Cancel {
                    instrument,
                    client_id,
                },
                emit,
            );
            return;
        };
        if matches!(order.provenance, Provenance::Foreign) {
            emit(ExecEffect::Skipped {
                client_id: Some(client_id),
                reason: SkipReason::ForeignOrder,
            });
            return;
        }
        if order.has_sent_cancel {
            emit(ExecEffect::Skipped {
                client_id: Some(client_id),
                reason: SkipReason::AlreadyCancelling,
            });
            return;
        }
        order.has_sent_cancel = true;
        self.send(
            ExecRequest::Cancel {
                instrument,
                client_id,
            },
            emit,
        );
    }

    fn cancel_matching(
        &mut self,
        instrument: Option<InstrumentId>,
        provenance: Provenance,
        emit: &mut dyn FnMut(ExecEffect),
    ) {
        let targets = self.mirror.cancellable(instrument, provenance);
        for (instrument, client_id) in targets {
            self.cancel_one(instrument, client_id, emit);
        }
    }

    fn settle_if_swept(&mut self, emit: &mut dyn FnMut(ExecEffect)) {
        let Some(reason) = self.sweep else {
            return;
        };
        if self.mirror.has_ours() {
            return;
        }
        self.sweep = None;
        self.phase = if reason.is_terminal() { Phase::Settled } else { Phase::Down };
        emit(ExecEffect::SweepComplete { reason });
    }

    fn send(&mut self, request: ExecRequest, emit: &mut dyn FnMut(ExecEffect)) {
        let request_id = RequestId::new(self.next_request_id);
        self.next_request_id += 1;
        emit(ExecEffect::Send {
            request_id,
            request,
        });
    }
}
