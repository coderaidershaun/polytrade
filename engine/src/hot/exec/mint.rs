//! Turns a decision into a command: claim the slot, reserve the money, bank the command, and record
//! the change, so that each path leaves the slot, the reservation and the audit rows agreeing.

use crate::hot::strategy::Actions;
use crate::msg::exec::ExecCommand;
use crate::msg::ids::{ClientOrderId, InstrumentId, Side};
use crate::msg::persist::{OrderLifecycle, OrderTransition};
use crate::msg::time::LocalTs;

use super::audit::bank_engine_driven;
use super::budget::QuotePermit;
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
                let permit = match self.admit_quote_placement(instrument, side) {
                    Ok(permit) => permit,
                    Err(reason) => return Some(reason),
                };
                let kind = PlacementKind::Quote { level, permit };
                return self.place(instrument, side, intent, kind, stamps, input.bank);
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
                if self.bank_or_halt(self.venue_of(instrument), command, at) {
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
        let is_sent = self.bank_or_halt(self.venue_of(slot.instrument()), command, at);
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

    /// Every placement the engine mints passes through here, so each one spends the venue budget,
    /// the order that gets a market flat included: the budget never refuses it, but it still counts
    /// it, so the quotes after it are refused early enough to leave room for the next one.
    ///
    /// Returns the refusal of a level whose client ids are spent. A level with no slot at all halts
    /// instead, and the halt is what the next spin reports.
    pub(super) fn place(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        intent: PlaceIntent,
        kind: PlacementKind,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) -> Option<RejectReason> {
        let at = stamps.fold;
        let level = match kind {
            PlacementKind::Quote { level, .. } => level,
            PlacementKind::Flatten => QuoteLevel::ZERO,
        };
        let (index, client_id) = match self.claim(instrument, side, level, intent, at) {
            Ok(claimed) => claimed,
            Err(ClaimRefusal::IdsSpent) => return Some(RejectReason::IdsSpent),
            Err(ClaimRefusal::LevelFull) => {
                self.halt(HaltReason::SlotLeak, at);
                return None;
            }
        };
        let is_banked = self.venue_mut(instrument).pending.bank(
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
        match kind {
            PlacementKind::Quote { permit, .. } => {
                self.venue_mut(instrument).budget.spend_quote(permit)
            }
            PlacementKind::Flatten => self.venue_mut(instrument).budget.spend_flatten(),
        }
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
            recon_seq: self.venue(instrument).recon_seq,
            run_nonce: self.venue(instrument).run_nonce,
        })?;
        let asset = self.instruments[usize::from(instrument.0)].spend_asset(side);
        let amount = side.spend(intent.price, intent.qty);
        let reserved_at = self.venue_mut(instrument).account.reserve(asset, amount);
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
    pub(super) fn bank_or_halt(&mut self, venue: usize, command: ExecCommand, at: LocalTs) -> bool {
        let is_banked = self.venues[venue].pending.bank(command, at);
        if !is_banked {
            self.halt(HaltReason::CommandBankOverflow, at);
        }
        is_banked
    }
}

/// What a placement is, for the slot it takes and the budget it spends. A quote rests at its level
/// and spends the permit that the meter gave it. The order that gets a market flat takes level zero
/// and needs no permit, because the budget never refuses it. It holds no rung of the ladder:
/// `is_resting_quote` is what keeps the reconciler from reading it as a quote.
pub(super) enum PlacementKind {
    Quote {
        level: QuoteLevel,
        permit: QuotePermit,
    },
    Flatten,
}
