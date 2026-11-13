//! Turns a decision into a command: claim the slot, reserve the money, bank the command, and record
//! the change, so that each path leaves the slot, the reservation and the audit rows agreeing.

use crate::hot::strategy::Actions;
use crate::msg::exec::ExecCommand;
use crate::msg::ids::{ClientOrderId, InstrumentId, Side};
use crate::msg::persist::{OrderLifecycle, OrderTransition};
use crate::msg::time::LocalTs;

use super::audit::bank_engine_driven;
use super::desired::QuoteLevel;
use super::engine::{ExecEngine, FoldStamps};
use super::gates::HaltReason;
use super::order::{ClaimRefusal, OrderClaim, OrderState};
use super::reconcile::{PlaceIntent, ReconcileOutcome, RejectReason};
use super::spin::SpinInput;

impl ExecEngine {
    pub(super) fn act_on(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        outcome: ReconcileOutcome,
        input: &mut SpinInput<'_>,
    ) -> Option<RejectReason> {
        let stamps = input.stamps();
        let at = stamps.fold;
        match outcome {
            ReconcileOutcome::Nothing | ReconcileOutcome::Reject(_) => {}
            ReconcileOutcome::Place(intent) => {
                return self.place(instrument, side, level, intent, stamps, input.bank);
            }
            ReconcileOutcome::Cancel => {
                if let Some(index) = self.resting_index(instrument, side, level) {
                    self.cancel_slot(index, stamps, input.bank);
                }
            }
            ReconcileOutcome::AmendQty(qty) => {
                let index = self.resting_index(instrument, side, level)?;
                let client_id = self.orders.slot(index).client_id();
                let command = ExecCommand::AmendQty {
                    instrument,
                    client_id,
                    qty,
                };
                if self.bank_or_halt(command, at) {
                    let previous = self.orders.mark_amend_sent(index);
                    let transition = OrderTransition::AmendSent;
                    self.bank_moved(index, previous, transition, stamps.received, input.bank);
                }
            }
        }
        None
    }

    #[inline]
    fn resting_index(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
    ) -> Option<usize> {
        let client_id = self.orders.resting(instrument, side, level)?.client_id();
        self.orders.find(client_id)
    }

    /// The slot moves to cancel-in-flight only once the cancel is banked: a slot waiting on an answer
    /// is one the engine stops asking about, so a cancel that never left must leave it as it was.
    pub(super) fn cancel_slot(
        &mut self,
        index: usize,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) -> bool {
        let at = stamps.fold;
        let slot = self.orders.slot(index);
        let command = ExecCommand::Cancel {
            instrument: slot.instrument(),
            client_id: slot.client_id(),
        };
        let is_sent = self.bank_or_halt(command, at);
        if is_sent {
            let previous = self.orders.mark_cancel_sent(index);
            let transition = OrderTransition::CancelSent;
            self.bank_moved(index, previous, transition, stamps.received, bank);
        }
        is_sent
    }

    /// Banks the row of a transition that the engine drove and the order table has made.
    pub(super) fn bank_moved(
        &self,
        index: usize,
        previous: OrderState,
        transition: OrderTransition,
        received: LocalTs,
        bank: &mut Actions,
    ) {
        let slot = self.orders.slot(index);
        bank_engine_driven(bank, slot, transition, previous.into(), received);
    }

    /// Every placement the engine mints passes through here, so every one of them spends the venue
    /// budget — a flatten is exempt from being REFUSED by it, never from consuming it. Counting the
    /// exits is what makes the quote gate refuse early enough to leave room for the next one.
    ///
    /// Returns the refusal of a level whose client ids are spent. A level with no slot at all halts
    /// instead, and the halt is what the next spin reports.
    pub(super) fn place(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        intent: PlaceIntent,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) -> Option<RejectReason> {
        let at = stamps.fold;
        let (index, client_id) = match self.claim(instrument, side, level, intent, at) {
            Ok(claimed) => claimed,
            Err(ClaimRefusal::IdsSpent) => return Some(RejectReason::IdsSpent),
            Err(ClaimRefusal::LevelFull) => {
                self.halt(HaltReason::SlotLeak, at);
                return None;
            }
        };
        let is_banked = self.pending.bank(
            ExecCommand::Place {
                instrument,
                client_id,
                side,
                price: intent.price,
                qty: intent.qty,
                style: intent.style,
            },
            at,
        );
        if !is_banked {
            self.unwind(index, stamps, bank);
            return None;
        }
        self.budget.record_place();
        bank_engine_driven(
            bank,
            self.orders.slot(index),
            OrderTransition::Placed,
            OrderLifecycle::Free,
            stamps.received,
        );
        None
    }

    fn claim(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        intent: PlaceIntent,
        at: LocalTs,
    ) -> Result<(usize, ClientOrderId), ClaimRefusal> {
        let (index, client_id) = self.orders.claim(OrderClaim {
            instrument,
            side,
            level,
            price: intent.price,
            qty: intent.qty,
            style: intent.style,
            claimed_ts_us: at,
            recon_seq: self.recon_seq,
        })?;
        let asset = self.instruments[usize::from(instrument.0)].spend_asset(side);
        let amount = side.spend(intent.price, intent.qty);
        let reserved_at = self.account.reserve(asset, amount);
        self.orders.hold_reservation(index, amount, reserved_at);
        Ok((index, client_id))
    }

    #[cold]
    fn unwind(&mut self, index: usize, stamps: FoldStamps, bank: &mut Actions) {
        let at = stamps.fold;
        self.release_unsent_reservation(index);
        let previous = self.orders.abandon(index, at);
        let transition = OrderTransition::SendAbandoned;
        self.bank_moved(index, previous, transition, stamps.received, bank);
        self.halt(HaltReason::CommandBankOverflow, at);
    }

    /// `false` means the bank refused the command and nothing was sent. The bank holds a whole
    /// spin's worth of work, so a refusal means the edge has stopped draining and the engine can no
    /// longer act at all, and it halts. Two paths bank directly instead: a placement, which must
    /// unwind its claim before the halt, and the sweep the halt itself issues, which has nothing left
    /// to escalate to and says so out loud.
    #[inline]
    pub(super) fn bank_or_halt(&mut self, command: ExecCommand, at: LocalTs) -> bool {
        let is_banked = self.pending.bank(command, at);
        if !is_banked {
            self.halt(HaltReason::CommandBankOverflow, at);
        }
        is_banked
    }
}
