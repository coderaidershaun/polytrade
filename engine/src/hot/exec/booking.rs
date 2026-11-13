//! What a venue report books: its quantity and its fee in the ledger, and a counted incident record
//! for a fill or a fee that the ledger cannot book.

use crate::hot::ledger::{FillFee, LedgerFill, PositionLedger};
use crate::hot::strategy::Actions;
use crate::log::{hot_error, hot_warn};
use crate::msg::exec::ExecEvent;
use crate::msg::ids::{Notional, Qty};
use crate::msg::persist::{IncidentKind, IncidentRow};
use crate::msg::time::LocalTs;

use super::audit::bank_incident;
use super::engine::{ExecEngine, FoldStamps};
use super::gates::HaltReason;
use super::order::{Applied, FeeFold, OrderSlot};

impl ExecEngine {
    /// Ours by tag and nonce, but no slot holds it: the reap window was too short, or the order
    /// table leaked a slot. With a fill, the venue moved money that the ledger cannot place without
    /// a guess, so quoting halts and the tape keeps the fill. With none, the event is refused and
    /// counted.
    #[cold]
    pub(super) fn on_orphan(&mut self, event: &ExecEvent, stamps: FoldStamps, bank: &mut Actions) {
        if event.cumulative_qty.mantissa() <= 0 {
            orphan_warning(event);
            self.incidents.orphan_events_without_fill += 1;
            return;
        }
        orphan_fill_error(event);
        self.record_orphan_fill(event, stamps.received, bank);
        self.halt(HaltReason::OrphanFill, stamps.fold);
    }

    /// The fill of an own order that holds no slot, as the venue's running total.
    #[cold]
    pub(super) fn record_orphan_fill(
        &mut self,
        event: &ExecEvent,
        received: LocalTs,
        bank: &mut Actions,
    ) {
        let Some(instrument) = event.instrument else { return };
        if event.cumulative_qty.mantissa() <= 0 {
            return;
        }
        let orphan_fill = IncidentRow {
            kind: IncidentKind::OrphanFill,
            instrument,
            client_id: Some(event.client_id),
            trade_id: event.trade_id,
            qty: event.cumulative_qty,
            amount: 0,
            window_close_ts_us: None,
            exchange_ts_us: event.exchange_ts_us,
            received_ts_us: received,
        };
        bank_incident(bank, &mut self.incidents, orphan_fill);
    }

    /// The quantity of a report moves by the venue's running totals, and its fee by the fee memory
    /// of its order. A step that the report's own trade does not explain, and a fee that the ledger
    /// cannot book, each leave a record.
    pub(super) fn book_report(
        &mut self,
        slot: &OrderSlot,
        event: &ExecEvent,
        applied: Applied,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
        received: LocalTs,
    ) {
        if !applied.fill.is_empty() {
            ledger.apply_fill(&LedgerFill {
                instrument: slot.instrument(),
                side: slot.side(),
                base: applied.fill.base,
                notional_quote: applied.fill.quote,
            });
        }
        let unexplained = applied.fee.unexplained;
        if unexplained.mantissa() > 0 {
            fee_gap_warning(slot, unexplained);
            let row = report_incident(IncidentKind::FeeGap, slot, event, received);
            bank_incident(
                bank,
                &mut self.incidents,
                IncidentRow {
                    qty: unexplained,
                    ..row
                },
            );
        }
        if event.commission == 0 {
            return;
        }
        let is_booked = match applied.fee.fold {
            FeeFold::Book => ledger
                .book_fee(slot.instrument(), self.fee_of(slot, event))
                .is_ok(),
            FeeFold::AlreadyBooked => return,
            FeeFold::Unbookable => false,
        };
        if is_booked {
            return;
        }
        unbooked_fee_warning(slot, event);
        let row = report_incident(IncidentKind::UnbookedFee, slot, event, received);
        let unbooked = IncidentRow {
            qty: event.last_qty,
            amount: event.commission,
            ..row
        };
        bank_incident(bank, &mut self.incidents, unbooked);
    }

    fn fee_of(&self, slot: &OrderSlot, event: &ExecEvent) -> FillFee {
        let instrument = &self.instruments[usize::from(slot.instrument().0)];
        let (asset, amount) = (event.commission_asset, event.commission);
        if asset == instrument.quote_asset {
            FillFee::Quote(Notional::from_mantissa(amount))
        } else if asset == instrument.base_asset {
            FillFee::Base {
                asset,
                qty: Qty::from_mantissa(amount),
            }
        } else {
            FillFee::Other { asset, amount }
        }
    }
}

/// The incident of one report of an order, with no quantity and no amount yet.
fn report_incident(
    kind: IncidentKind,
    slot: &OrderSlot,
    event: &ExecEvent,
    received_ts_us: LocalTs,
) -> IncidentRow {
    IncidentRow {
        kind,
        instrument: slot.instrument(),
        client_id: Some(slot.client_id()),
        trade_id: event.trade_id,
        qty: Qty::from_mantissa(0),
        amount: 0,
        window_close_ts_us: None,
        exchange_ts_us: event.exchange_ts_us,
        received_ts_us,
    }
}

#[cold]
fn orphan_warning(event: &ExecEvent) {
    hot_warn!(
        "execution event {:?} for client id {:x} matches no slot and carries no fill — refused and counted",
        event.kind,
        event.client_id.0
    );
}

#[cold]
fn orphan_fill_error(event: &ExecEvent) {
    hot_error!(
        "order {:x} filled {} and no slot holds it — reaped too early, or a slot leaked; the ledger does not book the fill, and quoting halts",
        event.client_id.0,
        event.cumulative_qty.mantissa()
    );
}

#[cold]
fn fee_gap_warning(slot: &OrderSlot, unexplained: Qty) {
    hot_warn!(
        "order {:x} filled {} more than its report's own trade explains, so the fees of the trades in between are unknown — recorded as a fee gap",
        slot.client_id().0,
        unexplained.mantissa()
    );
}

#[cold]
fn unbooked_fee_warning(slot: &OrderSlot, event: &ExecEvent) {
    hot_warn!(
        "a fee of {} in asset {} on order {:x} is not booked: its asset has no row, or its trade cannot be told from one booked before — recorded with its amount",
        event.commission,
        event.commission_asset.0,
        slot.client_id().0
    );
}
