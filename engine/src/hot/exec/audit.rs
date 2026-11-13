//! The audit trail of the execution engine: a row for each transition of a slot, the engine's own
//! included, a row for each fill, and a counted row for each incident.

use crate::hot::digest::digest_by_hash;
use crate::hot::strategy::Actions;
use crate::msg::exec::{ExecEvent, ExecKind, Provenance, RejectClass};
use crate::msg::persist::{
    FillRow, IncidentKind, IncidentRow, OrderLifecycle, OrderRow, OrderTransition, PersistRecord,
};
use crate::msg::time::{LocalTs, VenueTs};

use super::order::{FillDelta, OrderSlot};

struct OrderAudit<'a> {
    slot: &'a OrderSlot,
    transition: OrderTransition,
    previous_state: OrderLifecycle,
    provenance: Provenance,
    reject: Option<RejectClass>,
    reject_code: i32,
    /// Zero for a transition that the engine made on its own: the venue never heard of it.
    exchange_ts_us: VenueTs,
    received_ts_us: LocalTs,
}

/// The incidents of each kind that the fold recorded, and the events of its own orders that held no
/// slot and no fill, which are refused with a count and no record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct IncidentCounts {
    pub(crate) fee_gaps: u64,
    pub(crate) stranded_positions: u64,
    pub(crate) orphan_fills: u64,
    pub(crate) unbooked_fees: u64,
    pub(crate) orphan_events_without_fill: u64,
}

digest_by_hash!(IncidentCounts);

/// Counts the incident whether or not the tape has room for its row.
pub(super) fn bank_incident(bank: &mut Actions, counts: &mut IncidentCounts, row: IncidentRow) {
    let count = match row.kind {
        IncidentKind::FeeGap => &mut counts.fee_gaps,
        IncidentKind::StrandedPosition => &mut counts.stranded_positions,
        IncidentKind::OrphanFill => &mut counts.orphan_fills,
        IncidentKind::UnbookedFee => &mut counts.unbooked_fees,
    };
    *count += 1;
    bank.push_persist(PersistRecord::Incident(row));
}

/// A transition the engine made on its own, so there is no venue answer, no reject and no exchange
/// stamp.
pub(super) fn bank_engine_driven(
    bank: &mut Actions,
    slot: &OrderSlot,
    transition: OrderTransition,
    previous_state: OrderLifecycle,
    at: LocalTs,
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
            exchange_ts_us: VenueTs::from_micros(0),
            received_ts_us: at,
        },
    );
}

/// The rows of one order chain by client id from `Free`, so a lost first row shows, and the id
/// names the slot.
fn bank_order(bank: &mut Actions, audit: OrderAudit<'_>) {
    let slot = audit.slot;
    bank.push_persist(PersistRecord::Order(OrderRow {
        instrument: slot.instrument(),
        client_id: slot.client_id(),
        venue_order_id: slot.venue_order_id(),
        transition: audit.transition,
        state: slot.state().into(),
        previous_state: audit.previous_state,
        provenance: audit.provenance,
        side: slot.side(),
        quote_level: (audit.provenance == Provenance::Mine).then_some(slot.level()),
        style: slot.style(),
        price: slot.price(),
        qty: slot.qty(),
        filled_qty: slot.filled_base(),
        filled_quote: slot.filled_quote().mantissa(),
        reject: audit.reject,
        reject_code: audit.reject_code,
        exchange_ts_us: audit.exchange_ts_us,
        received_ts_us: audit.received_ts_us,
    }));
}

/// The booked quantity is the step of the venue's running total, which differs from the report's
/// own quantity when a report was lost.
pub(super) fn bank_fill(
    bank: &mut Actions,
    slot: &OrderSlot,
    event: &ExecEvent,
    delta: FillDelta,
    received_ts_us: LocalTs,
) {
    bank.push_persist(PersistRecord::Fill(FillRow {
        instrument: slot.instrument(),
        trade_id: event.trade_id,
        venue_order_id: slot.venue_order_id(),
        client_id: slot.client_id(),
        provenance: event.provenance,
        side: slot.side(),
        quote_level: (event.provenance == Provenance::Mine).then_some(slot.level()),
        liquidity: event.liquidity,
        last_price: event.last_price,
        last_qty: event.last_qty,
        booked_qty: delta.base,
        booked_quote: delta.quote.mantissa(),
        commission: event.commission,
        commission_asset: event.commission_asset,
        exchange_ts_us: event.exchange_ts_us,
        received_ts_us,
    }));
}

/// An order row when the state or the fill moved, with a fill row beside it. A partial fill of a
/// live order moves only the fill.
pub(super) fn bank_event_rows(
    bank: &mut Actions,
    slot: &OrderSlot,
    event: &ExecEvent,
    previous_state: OrderLifecycle,
    fill: FillDelta,
    received_ts_us: LocalTs,
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
            received_ts_us,
        },
    );
    if !fill.is_empty() {
        bank_fill(bank, slot, event, fill, received_ts_us);
    }
}

/// `None` for the events of the stream itself, which name no order and bank their rows elsewhere.
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

/// A repeat of a report moves neither, so it leaves no row.
#[inline]
fn is_worth_recording(previous_state: OrderLifecycle, slot: &OrderSlot, fill: FillDelta) -> bool {
    OrderLifecycle::from(slot.state()) != previous_state || !fill.is_empty()
}
