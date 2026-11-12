//! Turning a decision into a command: claim the slot, reserve the money, bank the command, record
//! the transition. Split from the spin loop because deciding WHAT to do and committing to it are
//! separate jobs — everything here runs after the argument is over, and every path through it must
//! leave the slot, the reservation and the audit trail agreeing.

use crate::hot::strategy::Actions;
use crate::ids::{ClientOrderId, InstrumentId, Side};
use crate::msg::exec::ExecCommand;
use crate::msg::persist::{OrderLifecycle, OrderTransition};
use crate::time::TsUs;

use super::audit::bank_engine_driven;
use super::desired::QuoteLevel;
use super::engine::ExecEngine;
use super::gates::HaltReason;
use super::order::{CloseReason, OrderClaim, OrderState};
use super::reconcile::{PlaceIntent, ReconcileOutcome};
use super::spin::SpinInput;

impl ExecEngine {
    pub(super) fn act_on(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        outcome: ReconcileOutcome,
        input: &mut SpinInput<'_>,
    ) {
        let at = input.tick.received_ts_us;
        match outcome {
            ReconcileOutcome::Nothing | ReconcileOutcome::Reject(_) => {}
            ReconcileOutcome::Place(intent) => {
                self.place(instrument, side, level, intent, at, input.bank)
            }
            ReconcileOutcome::Cancel => {
                if let Some(index) = self.resting_index(instrument, side, level) {
                    self.cancel_slot(index, at, input.bank);
                }
            }
            ReconcileOutcome::AmendQty(qty) => {
                let Some(index) = self.resting_index(instrument, side, level) else {
                    return;
                };
                let client_id = self.orders.slot(index).client_id;
                let command = ExecCommand::AmendQty {
                    instrument,
                    client_id,
                    qty,
                };
                if self.bank_or_halt(command, at) {
                    let next = OrderState::AmendInFlight;
                    self.move_slot(index, next, OrderTransition::AmendSent, at, input.bank);
                }
            }
        }
    }

    #[inline]
    fn resting_index(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
    ) -> Option<usize> {
        let client_id = self.orders.resting(instrument, side, level)?.client_id;
        self.orders.find(client_id)
    }

    /// The slot moves to cancel-in-flight only once the cancel is banked: a slot waiting on an answer
    /// is one the engine stops asking about, so a cancel that never left must leave it as it was.
    pub(super) fn cancel_slot(&mut self, index: usize, at: TsUs, bank: &mut Actions) -> bool {
        let slot = self.orders.slot(index);
        let command = ExecCommand::Cancel {
            instrument: slot.instrument,
            client_id: slot.client_id,
        };
        let is_sent = self.bank_or_halt(command, at);
        if is_sent {
            let next = OrderState::CancelInFlight;
            self.move_slot(index, next, OrderTransition::CancelSent, at, bank);
        }
        is_sent
    }

    pub(super) fn move_slot(
        &mut self,
        index: usize,
        next: OrderState,
        transition: OrderTransition,
        at: TsUs,
        bank: &mut Actions,
    ) {
        let slot = self.orders.slot_mut(index);
        let previous = OrderLifecycle::from(slot.state);
        slot.state = next;
        bank_engine_driven(bank, slot, transition, previous, at);
    }

    /// Every placement the engine mints passes through here, so every one of them spends the venue
    /// budget — a flatten is exempt from being REFUSED by it, never from consuming it. Counting the
    /// exits is what makes the quote gate refuse early enough to leave room for the next one.
    pub(super) fn place(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        intent: PlaceIntent,
        at: TsUs,
        bank: &mut Actions,
    ) {
        let Some((index, client_id)) = self.claim(instrument, side, level, intent, at) else {
            return;
        };
        let is_banked = self.pending.bank(ExecCommand::Place {
            instrument,
            client_id,
            side,
            price: intent.price,
            qty: intent.qty,
            style: intent.style,
        });
        if !is_banked {
            self.unwind(index, at, bank);
            return;
        }
        self.budget.record_place();
        bank_engine_driven(
            bank,
            self.orders.slot(index),
            OrderTransition::Placed,
            OrderLifecycle::Free,
            at,
        );
    }

    fn claim(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        intent: PlaceIntent,
        at: TsUs,
    ) -> Option<(usize, ClientOrderId)> {
        let Some((index, client_id)) = self.orders.claim(OrderClaim {
            instrument,
            side,
            level,
            price: intent.price,
            qty: intent.qty,
            style: intent.style,
            claimed_ts_us: at,
            recon_seq: self.recon_seq,
        }) else {
            self.halt(HaltReason::SlotLeak, at);
            return None;
        };
        let asset = self.instruments[usize::from(instrument.0)].spend_asset(side);
        let amount = side.spend(intent.price, intent.qty);
        let reserved_at = self.account.reserve(asset, amount);
        let slot = self.orders.slot_mut(index);
        slot.reserved_amount = amount;
        slot.reserved_at = reserved_at;
        Some((index, client_id))
    }

    #[cold]
    fn unwind(&mut self, index: usize, at: TsUs, bank: &mut Actions) {
        self.release_unsent_reservation(index);
        self.orders.slot_mut(index).closed_ts_us = at;
        let next = OrderState::Closed(CloseReason::Rejected);
        self.move_slot(index, next, OrderTransition::SendAbandoned, at, bank);
        self.halt(HaltReason::CommandBankOverflow, at);
    }

    /// `false` means the bank refused the command and nothing was sent. The bank holds a whole
    /// spin's worth of work, so a refusal means the edge has stopped draining and the engine can no
    /// longer act at all, and it halts. Two paths bank directly instead: a placement, which must
    /// unwind its claim before the halt, and the sweep the halt itself issues, which has nothing left
    /// to escalate to and says so out loud.
    #[inline]
    pub(super) fn bank_or_halt(&mut self, command: ExecCommand, at: TsUs) -> bool {
        let is_banked = self.pending.bank(command);
        if !is_banked {
            self.halt(HaltReason::CommandBankOverflow, at);
        }
        is_banked
    }
}
