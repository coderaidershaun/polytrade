//! Slot transitions -> orders/fills rows. Banks all transitions incl. engine-driven (send/timeout/recon). Gate + drop counter via strategy bank.

use crate::hot::strategy::Actions;
use crate::msg::exec::{ExecEvent, ExecKind, Provenance, RejectClass};
use crate::msg::persist::{FillRow, OrderLifecycle, OrderRow, OrderTransition, PersistRecord};
use crate::time::TsUs;

use super::order::{FillDelta, OrderSlot};

struct OrderAudit<'a> {
    slot: &'a OrderSlot,
    transition: OrderTransition,
    previous_state: OrderLifecycle,
    provenance: Provenance,
    reject: Option<RejectClass>,
    reject_code: i32,
    /// 0 on engine-driven (venue never heard of it).
    exchange_ts_us: TsUs,
    received_ts_us: TsUs,
}

/// A transition the engine made on its own, so there is no venue answer, no reject and no exchange
/// stamp.
pub(super) fn bank_engine_driven(
    bank: &mut Actions,
    slot: &OrderSlot,
    transition: OrderTransition,
    previous_state: OrderLifecycle,
    at: TsUs,
) {
    bank_order(
        bank,
        OrderAudit {
            slot,
            transition,
            previous_state,
            provenance: Provenance::Mine,
            reject: None,
            reject_code: 0,
            exchange_ts_us: TsUs::from_micros(0),
            received_ts_us: at,
        },
    );
}

/// Chain by client_id, start at Free (dropped HEAD visible; slot recoverable from id bits).
fn bank_order(bank: &mut Actions, audit: OrderAudit<'_>) {
    let slot = audit.slot;
    bank.push_persist(PersistRecord::Order(OrderRow {
        instrument: slot.instrument,
        client_id: slot.client_id,
        venue_order_id: slot.venue_order_id,
        transition: audit.transition,
        state: slot.state.into(),
        previous_state: audit.previous_state,
        provenance: audit.provenance,
        side: slot.side,
        quote_level: (audit.provenance == Provenance::Mine).then_some(slot.level),
        style: slot.style,
        price: slot.price,
        qty: slot.qty,
        filled_qty: slot.filled_base,
        filled_quote: slot.filled_quote,
        reject: audit.reject,
        reject_code: audit.reject_code,
        exchange_ts_us: audit.exchange_ts_us,
        received_ts_us: audit.received_ts_us,
    }));
}

/// Delta = booked cumulative (diverges from event on lost report).
pub(super) fn bank_fill(bank: &mut Actions, slot: &OrderSlot, event: &ExecEvent, delta: FillDelta) {
    bank.push_persist(PersistRecord::Fill(FillRow {
        instrument: slot.instrument,
        trade_id: event.trade_id,
        venue_order_id: slot.venue_order_id,
        client_id: slot.client_id,
        provenance: event.provenance,
        side: slot.side,
        quote_level: (event.provenance == Provenance::Mine).then_some(slot.level),
        liquidity: event.liquidity,
        last_price: event.last_price,
        last_qty: event.last_qty,
        booked_qty: delta.base,
        booked_quote: delta.quote,
        commission: delta.commission,
        commission_asset: event.commission_asset,
        exchange_ts_us: event.exchange_ts_us,
        received_ts_us: event.received_ts_us,
    }));
}

/// Order row on state/fill move, fill paired (partial fill live = no state change).
pub(super) fn bank_event_rows(
    bank: &mut Actions,
    slot: &OrderSlot,
    event: &ExecEvent,
    previous_state: OrderLifecycle,
    fill: FillDelta,
) {
    let Some(transition) = transition_of(event.kind) else {
        return;
    };
    if !is_worth_recording(previous_state, slot, fill) {
        return;
    }
    bank_order(
        bank,
        OrderAudit {
            slot,
            transition,
            previous_state,
            provenance: event.provenance,
            reject: event.reject,
            reject_code: event.reject_code,
            exchange_ts_us: event.exchange_ts_us,
            received_ts_us: event.received_ts_us,
        },
    );
    if !fill.is_empty() {
        bank_fill(bank, slot, event, fill);
    }
}

/// None for stream events (not per-order); they bank rows elsewhere.
fn transition_of(kind: ExecKind) -> Option<OrderTransition> {
    match kind {
        ExecKind::AckPlaced => Some(OrderTransition::AckPlaced),
        // Both abandonments are the same event on the tape — a request the engine gave up on before
        // the wire — and the row's own previous/current states say which request it was.
        ExecKind::PlaceNotSent | ExecKind::AmendNotSent => Some(OrderTransition::SendAbandoned),
        ExecKind::AckCanceled => Some(OrderTransition::AckCanceled),
        ExecKind::AckAmended => Some(OrderTransition::AckAmended),
        ExecKind::AckFailed => Some(OrderTransition::AckFailed),
        ExecKind::ReportNew => Some(OrderTransition::ReportNew),
        ExecKind::ReportTrade => Some(OrderTransition::ReportTrade),
        ExecKind::ReportCanceled => Some(OrderTransition::ReportCanceled),
        ExecKind::ReportExpired => Some(OrderTransition::ReportExpired),
        ExecKind::ReportRejected => Some(OrderTransition::ReportRejected),
        ExecKind::ReportAmended => Some(OrderTransition::ReportAmended),
        ExecKind::SnapshotOrder => Some(OrderTransition::SnapshotOrder),
        ExecKind::SnapshotEnd | ExecKind::StreamReset | ExecKind::StreamReady => None,
    }
}

/// State or fill moved (duplicates already absorbed).
#[inline]
fn is_worth_recording(previous_state: OrderLifecycle, slot: &OrderSlot, fill: FillDelta) -> bool {
    OrderLifecycle::from(slot.state) != previous_state || !fill.is_empty()
}
