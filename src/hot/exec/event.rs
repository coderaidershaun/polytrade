//! Folding a venue event: the slot transition, the money it moves, the reservation it releases and
//! the audit rows it earns, for this run's orders and for those an earlier run left resting.

use crate::config::MAX_QUOTE_LEVELS;
use crate::error;
use crate::hot::ledger::{LedgerFill, PositionLedger};
use crate::hot::strategy::Actions;
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Side};
use crate::msg::exec::{ExecEvent, ExecKind, RejectClass};
use crate::msg::persist::OrderLifecycle;

use super::account::ReleaseOutcome;
use super::audit::bank_event_rows;
use super::engine::{ExecEngine, view_of};
use super::gates::HaltReason;
use super::order::{
    FillDelta, MAX_ORDER_INSTRUMENTS, MAX_ORDER_SLOTS, OrderSlot, OrderState, free_or_oldest_closed,
};
use super::transition::apply_exec_event;
use super::view::{ExecCallback, Fill, OrderReject, RejectOrigin};

impl ExecEngine {
    /// Retry releases that beat account update. Fixed-slot scan, no alloc.
    pub(super) fn retry_reservation_releases(&mut self) {
        for index in 0..MAX_ORDER_SLOTS {
            let slot = self.orders.slot(index);
            if slot.reserved_amount == 0 || slot.state == OrderState::PendingNew {
                continue;
            }
            self.release_reservation(index);
        }
    }

    pub(super) fn apply_to_slot(
        &mut self,
        index: usize,
        event: &ExecEvent,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) -> ExecCallback {
        let previous_state = OrderLifecycle::from(self.orders.slot(index).state);
        let applied = apply_exec_event(self.orders.slot_mut(index), event);
        if applied.is_resurrection {
            self.resurrection_error(event);
        }
        let slot = *self.orders.slot(index);

        self.book_fill(&slot, event, applied.fill, ledger);
        if event.kind == ExecKind::PlaceNotSent {
            self.release_unsent_reservation(index);
        } else {
            self.release_reservation(index);
        }
        self.record_outcome(event, applied.state);
        bank_event_rows(bank, &slot, event, previous_state, applied.fill);

        if !applied.fill.is_empty() {
            return ExecCallback::Fill(Fill {
                instrument: slot.instrument,
                client_id: slot.client_id,
                side: slot.side,
                level: slot.level,
                price: event.last_price,
                qty: applied.fill.base,
                notional: applied.fill.quote,
                commission: event.commission,
                commission_asset: event.commission_asset,
                liquidity: event.liquidity,
                trade_id: event.trade_id,
                state: applied.state,
                event_ts_us: event.received_ts_us,
            });
        }
        if let Some(class) = event.reject {
            return ExecCallback::Reject(OrderReject {
                instrument: slot.instrument,
                client_id: Some(slot.client_id),
                side: slot.side,
                level: Some(slot.level),
                origin: RejectOrigin::Venue {
                    class,
                    code: event.reject_code,
                },
                event_ts_us: event.received_ts_us,
            });
        }
        ExecCallback::Update(view_of(&slot).updated_at(event.received_ts_us))
    }

    /// Prior-run order event: no reservation release, no reject count, no callback (position already in ledger).
    #[cold]
    pub(super) fn apply_to_prior_run(
        &mut self,
        event: &ExecEvent,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) {
        let Some(index) = self.prior_orders.find_or_seat(event) else {
            self.prior_run_overflow_error(event);
            return;
        };
        let previous_state = OrderLifecycle::from(self.prior_orders.slot(index).state);
        let applied = apply_exec_event(self.prior_orders.slot_mut(index), event);
        let slot = *self.prior_orders.slot(index);
        self.book_fill(&slot, event, applied.fill, ledger);
        bank_event_rows(bank, &slot, event, previous_state, applied.fill);
    }

    /// Fee + fill. No-op on empty (duplicates already folded).
    fn book_fill(
        &mut self,
        slot: &OrderSlot,
        event: &ExecEvent,
        fill: FillDelta,
        ledger: &mut PositionLedger,
    ) {
        if fill.is_empty() {
            return;
        }
        // Only commission in own quote_asset counts (foreign asset = wrong PnL forever).
        let quote_asset = self.instruments[usize::from(slot.instrument.0)].quote_asset;
        let commission_quote =
            if event.commission_asset == quote_asset { event.commission } else { 0 };
        ledger.apply_fill(&LedgerFill {
            instrument: slot.instrument,
            side: slot.side,
            base: fill.base,
            notional_quote: fill.quote,
            commission_quote,
        });
    }

    #[cold]
    fn prior_run_overflow_error(&self, event: &ExecEvent) {
        error!(
            "prior-run order {:x} has nowhere to fold — all {PRIOR_RUN_SLOTS} entries are still working, so its fills cannot reach the ledger",
            event.client_id.0
        );
    }

    #[cold]
    fn resurrection_error(&self, event: &ExecEvent) {
        error!(
            "order {:x} was believed closed and the venue still holds it — resurrected; an order believed dead is unbounded risk",
            event.client_id.0
        );
    }

    /// The release POLICY is a venue capability, [`ExecSettings::holds_reservations_until_settled`].
    ///
    /// A venue that locks funds on placement (Binance) folds the lock into every balance update, so
    /// the release waits for an update stamped later than the reservation — the watermark gate — on
    /// any slot past `PendingNew`, resting orders included. Ack alone would double-spend.
    ///
    /// A venue that does NOT lock (Polymarket) never moves balances for an open order, so `free`
    /// never subtracts a resting order: releasing while the slot still WORKS would over-admit against
    /// money it has committed, and a fill anywhere would then free a live order's reservation. There
    /// the reservation stands until the slot terminates — a zero-fill terminal moved no money and
    /// releases UNGATED; a filled terminal keeps the gate, because its post-fill restatement is what
    /// drops `free` and releasing before it lands would double-spend.
    fn release_reservation(&mut self, index: usize) {
        let slot = self.orders.slot(index);
        if slot.reserved_amount == 0 {
            return;
        }
        let asset = self.instruments[usize::from(slot.instrument.0)].spend_asset(slot.side);
        if self.settings.holds_reservations_until_settled {
            if slot.state != OrderState::PendingNew {
                self.gated_release(index, asset);
            }
            return;
        }
        if slot.state.is_working() {
            return;
        }
        if slot.filled_base.0 == 0 {
            self.release_unsent_reservation(index);
            return;
        }
        self.gated_release(index, asset);
    }

    /// Release once the venue reports a balance stamped later than the reservation, else leave it
    /// held for the next spin to retry.
    fn gated_release(&mut self, index: usize, asset: AssetId) {
        let slot = self.orders.slot(index);
        let outcome = self
            .account
            .release(asset, slot.reserved_amount, slot.reserved_at);
        if outcome == ReleaseOutcome::Released {
            self.orders.slot_mut(index).reserved_amount = 0;
        }
    }

    pub(super) fn release_unsent_reservation(&mut self, index: usize) {
        let slot = self.orders.slot(index);
        if slot.reserved_amount == 0 {
            return;
        }
        let asset = self.instruments[usize::from(slot.instrument.0)].spend_asset(slot.side);
        self.account.release_unsent(asset, slot.reserved_amount);
        self.orders.slot_mut(index).reserved_amount = 0;
    }

    /// A venue answer ends the reject streak, extends it, or leaves it alone. `Refused` leaves it
    /// alone: a post-only quote the book moved through is the venue doing exactly what post-only
    /// asks, and counting it toward the kill switch parks the engine precisely when the market is
    /// moving.
    ///
    /// This relies on the Binance edge mapping `INSUFFICIENT_BALANCES` to `Fatal` and not to
    /// `Refused`: the two share venue code `-2010` and only the reject REASON separates them. Both
    /// Binance decoders split on the message, and an unrecognised `-2010` classifies `Fatal`, which
    /// parks the engine loudly rather than letting it place orders it cannot fund.
    fn record_outcome(&mut self, event: &ExecEvent, state: OrderState) {
        let Some(class) = event.reject else {
            // One order the venue accepts shows the requests are not wrong in kind, so it ends the
            // streak, and only the venue can do that. The kinds the EDGE synthesises for a request
            // that never left resolve a slot without the venue accepting anything, and an amend
            // refused for want of a socket leaves its order reading `Live`. Read here as acceptance,
            // that would reset the count exactly when the venue is least reachable.
            let is_venue_answer =
                !matches!(event.kind, ExecKind::PlaceNotSent | ExecKind::AmendNotSent);
            if is_venue_answer && matches!(state, OrderState::Live) {
                self.reject_streak = 0;
            }
            return;
        };
        match class {
            RejectClass::Fatal => {
                self.fatal_reject_error(event);
                self.halt(HaltReason::FatalReject, event.received_ts_us);
            }
            // The ordinary cost of quoting post-only. It must never touch the hard-reject streak: a
            // maker earns these all day, and counting them parks the engine exactly when the market
            // is interesting.
            RejectClass::Refused => {}
            // A reconciliation answering that the order never existed is not the venue refusing a
            // request, so it must not count toward the kill switch.
            RejectClass::Gone => {}
            RejectClass::StillLive | RejectClass::Ambiguous => {
                self.reject_streak += 1;
                if self.reject_streak >= self.settings.max_consecutive_rejects {
                    self.halt(HaltReason::RejectStreak, event.received_ts_us);
                }
            }
        }
    }

    /// Class determines branch. Same fatal class for different causes -> error message carries the code.
    #[cold]
    fn fatal_reject_error(&self, event: &ExecEvent) {
        error!(
            "venue refused order {:x} fatally with code {} — halting rather than retrying",
            event.client_id.0, event.reject_code
        );
    }
}

const PRIOR_RUN_SLOTS: usize = MAX_QUOTE_LEVELS * 2 * MAX_ORDER_INSTRUMENTS;

/// Orders an EARLIER run of this trading engine left resting on the venue. They are cancelled on
/// sight, but the cancel takes hundreds of milliseconds and whatever rests fills in the meantime.
/// Those are real executions on this account, so they move the ledger and earn audit rows exactly
/// like any other.
///
/// They must never join THIS run's quoting, which is why they sit in a table of their own rather
/// than in [`OrderTable`](super::order::OrderTable). A prior-run client id decodes to a slot index
/// in that table, so seating one there would either evict an order this run is about to place or
/// lose the fill to the orphan path, decided by nothing but arrival order. Keeping them apart also
/// keeps a cancel answered "unknown order", this path's ordinary outcome, out of the reject streak.
pub(super) struct PriorRunOrders {
    slots: Box<[OrderSlot; PRIOR_RUN_SLOTS]>,
}

impl PriorRunOrders {
    pub(super) fn new() -> Self {
        Self {
            slots: Box::new([OrderSlot::EMPTY; PRIOR_RUN_SLOTS]),
        }
    }

    #[inline]
    pub(super) fn slot(&self, index: usize) -> &OrderSlot {
        &self.slots[index]
    }

    #[inline]
    pub(super) fn slot_mut(&mut self, index: usize) -> &mut OrderSlot {
        &mut self.slots[index]
    }

    pub(super) fn working(
        &self,
        instrument: InstrumentId,
        side: Side,
    ) -> impl Iterator<Item = &OrderSlot> {
        self.slots.iter().filter(move |slot| {
            slot.instrument == instrument && slot.side == side && slot.state.is_working()
        })
    }

    pub(super) fn find_or_seat(&mut self, event: &ExecEvent) -> Option<usize> {
        if let Some(index) = self.find(event.client_id) {
            return Some(index);
        }
        let index = free_or_oldest_closed(&self.slots[..])?;
        self.slots[index] = OrderSlot {
            client_id: event.client_id,
            venue_order_id: event.venue_order_id,
            state: OrderState::Unknown,
            side: event.side,
            instrument: event.instrument,
            price: event.price,
            qty: event.qty,
            last_event_ts_us: event.received_ts_us,
            ..OrderSlot::EMPTY
        };
        Some(index)
    }

    fn find(&self, id: ClientOrderId) -> Option<usize> {
        self.slots
            .iter()
            .position(|slot| slot.client_id == id && slot.state != OrderState::Free)
    }
}
