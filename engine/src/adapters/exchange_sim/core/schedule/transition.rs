//! Wire-visible lifecycle transitions and reconciliation answers for simulated orders.

use crate::ids::ClientOrderId;
use crate::msg::exec::VenueOrderStatus;

use super::super::resting::{OrderPhase, OrderSnapshot};
use super::super::wallet::FillSettlement;
use super::voice::refusal;
use super::{
    AnswerKind, AnswerSubject, DeliverySchedule, GenerationKey, Landing, Rejection,
    SynthesisedEvent, VenueAnswer, VenueReport, VenueVoice,
};

impl DeliverySchedule {
    pub(super) fn announce(&mut self, snapshot: OrderSnapshot, at: Landing) {
        let key = GenerationKey::of(snapshot);
        let order = snapshot.order;
        self.open_barrier(key, at.due_ts_us);
        self.push(
            Some(key),
            AnswerKind::PlaceAck,
            at,
            VenueVoice::Response(VenueAnswer::PlaceAccepted(order)),
        );
        self.push(
            Some(key),
            AnswerKind::ReportNew,
            at,
            VenueVoice::Report(VenueReport::New(order)),
        );
    }

    pub(super) fn refuse_place(
        &mut self,
        snapshot: OrderSnapshot,
        rejection: Rejection,
        at: Landing,
    ) {
        let key = GenerationKey::of(snapshot);
        let order = snapshot.order;
        self.push(
            Some(key),
            AnswerKind::Refusal,
            at,
            refusal(order.client_id, rejection, AnswerSubject::Place),
        );
        if rejection.has_rejected_report() {
            self.push(
                Some(key),
                AnswerKind::Refusal,
                at,
                VenueVoice::Report(VenueReport::Rejected(order)),
            );
        }
    }

    pub(super) fn fill(
        &mut self,
        snapshot: OrderSnapshot,
        settlement: FillSettlement,
        trade_id: crate::ids::TradeId,
        at: Landing,
    ) {
        let key = GenerationKey::of(snapshot);
        let order = snapshot.order;
        self.push(
            Some(key),
            AnswerKind::Transition,
            at,
            VenueVoice::Report(VenueReport::Trade {
                order,
                trade_id,
                settlement,
            }),
        );
    }

    pub(super) fn cancel(&mut self, snapshot: OrderSnapshot, at: Landing) {
        let key = GenerationKey::of(snapshot);
        let order = snapshot.order;
        self.push(
            Some(key),
            AnswerKind::Transition,
            at,
            VenueVoice::Response(VenueAnswer::CancelAccepted(order)),
        );
        self.push(
            Some(key),
            AnswerKind::Transition,
            at,
            VenueVoice::Report(VenueReport::Canceled(order)),
        );
    }

    pub(super) fn amend(&mut self, snapshot: OrderSnapshot, at: Landing) {
        let key = GenerationKey::of(snapshot);
        self.push(
            Some(key),
            AnswerKind::Transition,
            at,
            VenueVoice::Response(VenueAnswer::AmendAccepted(snapshot.order)),
        );
    }

    pub(super) fn refuse(
        &mut self,
        client_id: ClientOrderId,
        rejection: Rejection,
        about: AnswerSubject,
        at: Landing,
    ) {
        self.push(
            None,
            AnswerKind::Observation,
            at,
            refusal(client_id, rejection, about),
        );
    }

    pub(super) fn observe(&mut self, event: SynthesisedEvent, at: Landing) {
        self.push(
            None,
            AnswerKind::Observation,
            at,
            VenueVoice::Synthesised(event),
        );
    }

    pub(super) fn withdraw(&mut self, snapshot: OrderSnapshot, at: Landing) {
        let key = GenerationKey::of(snapshot);
        self.pending.retain(|entry| entry.key != Some(key));
        if let Some(spoken) = self.spoken_mut(key) {
            spoken.new_barrier_ts_us = None;
        }
        self.push(
            Some(key),
            AnswerKind::NotSent,
            at,
            VenueVoice::Synthesised(SynthesisedEvent::PlaceNotSent(snapshot.order)),
        );
    }

    pub(super) fn pull(&mut self, snapshot: OrderSnapshot, at: Landing) {
        let key = GenerationKey::of(snapshot);
        if self.has_pending_terminal(key) {
            return;
        }
        self.push(
            Some(key),
            AnswerKind::Transition,
            at,
            VenueVoice::Report(VenueReport::Canceled(snapshot.order)),
        );
    }

    pub(super) fn status(&mut self, snapshot: OrderSnapshot, at: Landing) {
        self.push(
            None,
            AnswerKind::Observation,
            at,
            VenueVoice::Response(VenueAnswer::Status {
                order: snapshot.order,
                status: status_of(snapshot),
            }),
        );
    }

    pub(super) fn no_such_order(&mut self, client_id: ClientOrderId, at: Landing) {
        self.push(
            None,
            AnswerKind::Observation,
            at,
            refusal(client_id, Rejection::NoSuchOrder, AnswerSubject::Query),
        );
    }

    pub(super) fn snapshot(&mut self, rows: &[OrderSnapshot], at: Landing) {
        let listed = rows.iter().map(|row| row.order).collect();
        self.push(
            None,
            AnswerKind::Observation,
            at,
            VenueVoice::Response(VenueAnswer::OpenOrders(listed)),
        );
    }

    fn has_pending_terminal(&self, key: GenerationKey) -> bool {
        self.pending
            .iter()
            .any(|entry| entry.key == Some(key) && entry.voice.terminal_half().is_some())
    }
}

fn status_of(snapshot: OrderSnapshot) -> VenueOrderStatus {
    match snapshot.phase {
        OrderPhase::Closed(reason) => reason.venue_status(),
        _ if snapshot.order.filled.0 > 0 => VenueOrderStatus::PartiallyFilled,
        _ => VenueOrderStatus::New,
    }
}
