//! The transition table: the one pure function every venue event folds through, and the money
//! decisions encoded in it.
//!
//! Split from the store beside it because these are two different questions. `order.rs` answers
//! WHERE an order lives — which slot, under which id, retired when. This file answers what an event
//! MEANS, and it is the half where a wrong answer costs money rather than tidiness.
//!
//! Two "in-flight" notions run through it and must never merge. EXISTENCE in-flight
//! ([`OrderState::PendingNew`]) is resolved by ANY event naming the client id — the order either
//! reached the venue or it did not. COMMAND in-flight (cancel/amend) is resolved only by that
//! command's own ack or by a terminal report: a fill arriving mid-cancel does not mean the cancel
//! landed, and treating it that way emits a second cancel.

use crate::msg::exec::{ExecEvent, ExecKind, RejectClass, VenueOrderStatus};

use super::order::{CloseReason, FillDelta, OrderSlot, OrderState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub fill: FillDelta,
    pub state: OrderState,
    pub is_resurrection: bool,
}

pub fn apply_exec_event(slot: &mut OrderSlot, event: &ExecEvent) -> Applied {
    if slot.state == OrderState::Free {
        return Applied {
            fill: FillDelta::NONE,
            state: OrderState::Free,
            is_resurrection: false,
        };
    }
    // Bound before anything mutates the slot, because the tail below has to ask what the state was
    // BEFORE this event to know whether a command was outstanding. A read of `slot.state` there
    // would answer that only for as long as the assignment stayed at the very bottom.
    let previous_state = slot.state;
    let fill = slot.fold_fill(event);
    let mut is_resurrection = false;
    let next = match event.kind {
        ExecKind::AckPlaced | ExecKind::ReportNew => match previous_state {
            OrderState::Closed(reason) => OrderState::Closed(reason),
            OrderState::CancelInFlight | OrderState::AmendInFlight => previous_state,
            _ => OrderState::Live,
        },

        ExecKind::PlaceNotSent => match previous_state {
            OrderState::PendingNew => OrderState::Closed(CloseReason::Rejected),
            other => other,
        },

        // Only the command is unwound; the order itself never moved. Every other state stands,
        // including a terminal one that raced the refusal — an amend that never left says nothing
        // about whether the order survived.
        ExecKind::AmendNotSent => match previous_state {
            OrderState::AmendInFlight => OrderState::Live,
            other => other,
        },

        ExecKind::AckFailed => match event.reject {
            Some(RejectClass::StillLive) => match previous_state {
                OrderState::PendingNew => OrderState::Closed(CloseReason::Rejected),
                OrderState::Closed(reason) => OrderState::Closed(reason),
                OrderState::Unknown => OrderState::Unknown,
                _ => OrderState::Live,
            },
            Some(RejectClass::Refused) => close_or_keep(previous_state, CloseReason::Rejected),
            Some(RejectClass::Gone) => close_or_keep(previous_state, CloseReason::ReconciledGone),
            _ => match previous_state {
                OrderState::Closed(reason) => OrderState::Closed(reason),
                _ => OrderState::Unknown,
            },
        },

        ExecKind::ReportTrade => {
            let is_complete = event.status == Some(VenueOrderStatus::Filled)
                || (slot.qty.0 > 0 && slot.filled_base.0 >= slot.qty.0);
            if is_complete {
                close_or_keep(previous_state, CloseReason::Filled)
            } else {
                keep_working(previous_state)
            }
        }

        ExecKind::ReportCanceled | ExecKind::AckCanceled => {
            close_or_keep(previous_state, CloseReason::Canceled)
        }

        ExecKind::ReportExpired => close_or_keep(previous_state, CloseReason::Expired),
        ExecKind::ReportRejected => close_or_keep(previous_state, CloseReason::Rejected),

        ExecKind::AckAmended | ExecKind::ReportAmended => match previous_state {
            OrderState::Closed(reason) => OrderState::Closed(reason),
            _ => OrderState::Live,
        },

        ExecKind::SnapshotOrder => snapshot_state(previous_state, event, &mut is_resurrection),

        ExecKind::SnapshotEnd | ExecKind::StreamReset | ExecKind::StreamReady => previous_state,
    };
    slot.state = next;
    if matches!(event.kind, ExecKind::AckAmended | ExecKind::ReportAmended) {
        // Only an amend WE asked for spends budget: the same report arriving against a Live slot is
        // the venue restating an order, and counting it would cancel the ladder early.
        if previous_state == OrderState::AmendInFlight {
            slot.amends_used = slot.amends_used.saturating_add(1);
        }
        if event.qty.0 > 0 {
            slot.qty = slot.qty.min(event.qty);
        }
    }
    if event.amends_remaining == ExecEvent::AMENDS_EXHAUSTED {
        slot.amends_used = u8::MAX;
    }
    if event.kind == ExecKind::SnapshotOrder {
        slot.seen_recon_seq = slot.seen_recon_seq.max(event.recon_seq);
        if event.qty.0 > 0 {
            slot.price = event.price;
            slot.qty = event.qty;
        }
    }
    if let Some(id) = event.venue_order_id {
        slot.venue_order_id = Some(id);
    }
    slot.last_event_ts_us = event.received_ts_us;
    if matches!(next, OrderState::Closed(_)) && !matches!(previous_state, OrderState::Closed(_)) {
        slot.closed_ts_us = event.received_ts_us;
    }
    Applied {
        fill,
        state: next,
        is_resurrection,
    }
}

#[inline]
fn snapshot_state(state: OrderState, event: &ExecEvent, is_resurrection: &mut bool) -> OrderState {
    if let Some(reason) = terminal_reason(event.status) {
        return close_or_keep(state, reason);
    }
    match state {
        OrderState::Closed(_) => {
            *is_resurrection = true;
            OrderState::Unknown
        }
        OrderState::CancelInFlight | OrderState::AmendInFlight => state,
        _ => OrderState::Live,
    }
}

/// The close reason a venue status implies, or `None` while the order is still workable. Absent
/// status means the answer carried none, which cannot be read as terminal.
#[inline]
fn terminal_reason(status: Option<VenueOrderStatus>) -> Option<CloseReason> {
    match status? {
        VenueOrderStatus::Filled => Some(CloseReason::Filled),
        VenueOrderStatus::Canceled => Some(CloseReason::Canceled),
        VenueOrderStatus::Expired | VenueOrderStatus::ExpiredInMatch => Some(CloseReason::Expired),
        VenueOrderStatus::Rejected => Some(CloseReason::Rejected),
        VenueOrderStatus::New
        | VenueOrderStatus::PartiallyFilled
        | VenueOrderStatus::PendingCancel => None,
    }
}

/// A slot already `Closed` keeps its original reason: the first terminal answer is the true one, and
/// a later duplicate must not rewrite why it ended.
#[inline]
fn close_or_keep(state: OrderState, reason: CloseReason) -> OrderState {
    match state {
        OrderState::Closed(existing) => OrderState::Closed(existing),
        _ => OrderState::Closed(reason),
    }
}

/// A partial fill leaves a command outstanding exactly as it found it — the whole point of keeping
/// the two in-flight notions apart.
#[inline]
fn keep_working(state: OrderState) -> OrderState {
    match state {
        OrderState::PendingNew | OrderState::Unknown => OrderState::Live,
        other => other,
    }
}
