//! The engine's own record of its orders: a fixed table of slots, the client id that names a slot,
//! and the rules that hand slots out and take them back.
//!
//! The table never reads what an event means: it finds the slot, and a separate transition table
//! decides what happens to it.

mod prior_run;
mod slot;
mod transition;

use std::ops::Range;

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::msg::exec::{ExecEvent, OrderStyle};
use crate::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, Side, Spend};
use crate::msg::time::{DurationUs, LocalTs};
use crate::runtime::config::MAX_QUOTE_LEVELS;

use super::account::AccountWatermark;
use super::capacity::ExecCapacity;
use super::desired::QuoteLevel;

pub(super) use prior_run::{PriorRunOrders, Unseated};
pub use slot::{CloseReason, FillDelta, OrderSlot, OrderState};
pub use transition::{Applied, apply_exec_event};

const SLOTS_PER_LEVEL: usize = 4;
const SLOTS_PER_SIDE: usize = MAX_QUOTE_LEVELS * SLOTS_PER_LEVEL;
pub(super) const SLOTS_PER_INSTRUMENT: usize = 2 * SLOTS_PER_SIDE;

/// The most order slots a client order id can address: the id holds 16 bits of slot index.
pub const MAX_ORDER_SLOTS: usize = 1 << 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientIdLayout {
    pub run_nonce: u32,
}

impl ClientIdLayout {
    /// # Panics
    /// When `slot_index` is past the slots that a client order id addresses. The start refuses an
    /// order table that large, so this is a bug.
    #[inline]
    pub fn encode(self, slot_index: usize, generation: u16) -> ClientOrderId {
        assert!(
            slot_index < MAX_ORDER_SLOTS,
            "slot index {slot_index} beyond the {MAX_ORDER_SLOTS} slots a client order id addresses"
        );
        ClientOrderId(
            (u64::from(self.run_nonce) << 32)
                | ((slot_index as u64 & 0xFFFF) << 16)
                | u64::from(generation),
        )
    }

    #[inline]
    pub fn slot_of(id: ClientOrderId) -> usize {
        ((id.0 >> 16) & 0xFFFF) as usize
    }

    #[inline]
    pub fn nonce_of(id: ClientOrderId) -> u32 {
        (id.0 >> 32) as u32
    }

    #[inline]
    pub fn generation_of(id: ClientOrderId) -> u16 {
        (id.0 & 0xFFFF) as u16
    }
}

#[inline]
pub fn side_base(instrument: InstrumentId, side: Side) -> usize {
    (usize::from(instrument.0) * 2 + side.index()) * SLOTS_PER_SIDE
}

#[inline]
fn level_base(instrument: InstrumentId, side: Side, level: QuoteLevel) -> usize {
    side_base(instrument, side) + level.index() * SLOTS_PER_LEVEL
}

#[inline]
pub fn level_of_slot(slot_index: usize) -> QuoteLevel {
    let within_side = slot_index % SLOTS_PER_SIDE;
    QuoteLevel::new((within_side / SLOTS_PER_LEVEL) as u8)
        .expect("slot geography always names a valid quote level")
}

#[inline]
fn instrument_base(instrument: InstrumentId) -> usize {
    usize::from(instrument.0) * SLOTS_PER_INSTRUMENT
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderClaim {
    pub instrument: InstrumentId,
    pub side: Side,
    pub level: QuoteLevel,
    pub price: Price,
    pub qty: Qty,
    pub style: OrderStyle,
    pub claimed_ts_us: LocalTs,
    pub recon_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReconcilePass {
    pub instrument: InstrumentId,
    pub recon_seq: u64,
    pub recon_ts_us: LocalTs,
}

pub struct OrderTable {
    slots: Box<[OrderSlot]>,
    layout: ClientIdLayout,
}

digest_by_hash!(ClientIdLayout, OrderSlot);
digest_fields! { OrderTable { slots, layout } }

impl OrderTable {
    pub fn new(capacity: &ExecCapacity, run_nonce: u32) -> Self {
        Self {
            slots: vec![OrderSlot::EMPTY; capacity.order_slots()].into_boxed_slice(),
            layout: ClientIdLayout { run_nonce },
        }
    }

    #[inline]
    pub fn slot(&self, index: usize) -> &OrderSlot {
        &self.slots[index]
    }

    /// Each slot index of the table, for a pass that visits every slot by index.
    #[inline]
    pub(super) fn slot_indices(&self) -> Range<usize> {
        0..self.slots.len()
    }

    #[inline]
    pub fn find(&self, id: ClientOrderId) -> Option<usize> {
        let index = ClientIdLayout::slot_of(id);
        let slot = self.slots.get(index)?;
        (slot.client_id == id && slot.state != OrderState::Free).then_some(index)
    }

    #[inline]
    pub fn side_slots(&self, instrument: InstrumentId, side: Side) -> &[OrderSlot] {
        &self.slots[self.side_slot_range(instrument, side)]
    }

    /// Slot indices this side owns, for a caller that needs to address slots by index rather than
    /// read them through a borrow of the table.
    #[inline]
    pub fn side_slot_range(&self, instrument: InstrumentId, side: Side) -> Range<usize> {
        let base = side_base(instrument, side);
        base..base + SLOTS_PER_SIDE
    }

    #[inline]
    pub fn level_slots(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
    ) -> &[OrderSlot] {
        let base = level_base(instrument, side, level);
        &self.slots[base..base + SLOTS_PER_LEVEL]
    }

    /// The working order OCCUPYING a ladder level. A marketable order is stored in the same slots
    /// but occupies no level: it never rests, so the reconciler must not see it as the quote it
    /// asked for and cancel it — on a venue that holds taker orders for a moment before matching,
    /// that cancel is one the venue would refuse anyway.
    pub fn resting(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
    ) -> Option<&OrderSlot> {
        self.level_slots(instrument, side, level)
            .iter()
            .find(|slot| slot.state.is_working() && slot.style != Some(OrderStyle::Immediate))
    }

    #[inline]
    pub fn is_awaiting_answer(&self, instrument: InstrumentId, side: Side) -> bool {
        self.side_slots(instrument, side)
            .iter()
            .any(|slot| slot.state.is_in_flight())
    }

    #[inline]
    pub fn possibly_live_count(&self, instrument: InstrumentId, side: Side) -> usize {
        self.side_slots(instrument, side)
            .iter()
            .filter(|slot| slot.state.is_working())
            .count()
    }

    pub fn claim(&mut self, claim: OrderClaim) -> Option<(usize, ClientOrderId)> {
        let base = level_base(claim.instrument, claim.side, claim.level);
        let index = base + free_or_oldest_closed(&self.slots[base..base + SLOTS_PER_LEVEL])?;
        let slot = &mut self.slots[index];
        let generation = slot.generation.wrapping_add(1);
        let client_id = self.layout.encode(index, generation);
        *slot = OrderSlot {
            client_id,
            state: OrderState::PendingNew,
            side: claim.side,
            level: claim.level,
            style: Some(claim.style),
            instrument: claim.instrument,
            price: claim.price,
            qty: claim.qty,
            generation,
            last_event_ts_us: claim.claimed_ts_us,
            placed_recon_seq: claim.recon_seq,
            ..OrderSlot::EMPTY
        };
        Some((index, client_id))
    }

    pub fn adopt(&mut self, event: &ExecEvent, now: LocalTs) -> Option<usize> {
        let index = ClientIdLayout::slot_of(event.client_id);
        let slot = self.slots.get_mut(index)?;
        if slot.state != OrderState::Free {
            return None;
        }
        *slot = OrderSlot {
            client_id: event.client_id,
            venue_order_id: event.venue_order_id,
            state: OrderState::Unknown,
            side: event.side,
            level: level_of_slot(index),
            instrument: event.instrument?,
            price: event.price,
            qty: event.qty,
            generation: ClientIdLayout::generation_of(event.client_id),
            last_event_ts_us: now,
            placed_recon_seq: event.recon_seq,
            ..OrderSlot::EMPTY
        };
        Some(index)
    }

    #[inline]
    pub fn is_timed_out(&self, index: usize, now: LocalTs, timeout: DurationUs) -> bool {
        let slot = &self.slots[index];
        slot.state.is_in_flight() && now.diff(slot.last_event_ts_us) > timeout
    }

    #[cold]
    pub fn sweep_unseen(
        &mut self,
        pass: ReconcilePass,
        closed: &mut impl FnMut(&OrderSlot, OrderState),
    ) {
        let base = instrument_base(pass.instrument);
        for slot in self.slots[base..base + SLOTS_PER_INSTRUMENT].iter_mut() {
            let is_unseen = slot.state.is_working()
                && slot.seen_recon_seq < pass.recon_seq
                && slot.placed_recon_seq < pass.recon_seq;
            if !is_unseen {
                continue;
            }
            let previous = slot.state;
            slot.state = OrderState::Closed(CloseReason::ReconciledGone);
            slot.closed_ts_us = pass.recon_ts_us;
            closed(slot, previous);
        }
    }

    #[cold]
    pub fn invalidate_all(&mut self, invalidated: &mut impl FnMut(&OrderSlot, OrderState)) {
        for slot in self.slots.iter_mut() {
            if !slot.state.is_working() {
                continue;
            }
            let previous = slot.state;
            slot.state = OrderState::Unknown;
            invalidated(slot, previous);
        }
    }

    /// Folds one venue event into the slot at `index`.
    #[inline]
    pub fn apply(&mut self, index: usize, event: &ExecEvent, now: LocalTs) -> Applied {
        apply_exec_event(&mut self.slots[index], event, now)
    }

    /// The cancel of the order at `index` is banked. Returns the state it left.
    #[inline]
    pub fn mark_cancel_sent(&mut self, index: usize) -> OrderState {
        self.move_to(index, OrderState::CancelInFlight)
    }

    /// The amend of the order at `index` is banked. Returns the state it left.
    #[inline]
    pub fn mark_amend_sent(&mut self, index: usize) -> OrderState {
        self.move_to(index, OrderState::AmendInFlight)
    }

    /// The venue gave no answer in time, so the order is unknown until the venue is asked about it.
    /// Returns the state it left.
    #[cold]
    pub fn mark_timed_out(&mut self, index: usize, now: LocalTs) -> OrderState {
        self.slots[index].last_event_ts_us = now;
        self.move_to(index, OrderState::Unknown)
    }

    /// A claim whose placement the bank refused: the venue never heard of it. Returns the state it
    /// left.
    #[cold]
    pub(crate) fn abandon(&mut self, index: usize, at: LocalTs) -> OrderState {
        self.slots[index].closed_ts_us = at;
        self.move_to(index, OrderState::Closed(CloseReason::Rejected))
    }

    /// A zero amount holds nothing, so it is kept as `None`.
    #[inline]
    pub(crate) fn hold_reservation(
        &mut self,
        index: usize,
        amount: Spend,
        reserved_at: AccountWatermark,
    ) {
        let slot = &mut self.slots[index];
        slot.reserved_amount = Some(amount).filter(|amount| amount.mantissa() != 0);
        slot.reserved_at = reserved_at;
    }

    #[inline]
    pub(crate) fn clear_reservation(&mut self, index: usize) {
        self.slots[index].reserved_amount = None;
    }

    #[inline]
    fn move_to(&mut self, index: usize, next: OrderState) -> OrderState {
        let slot = &mut self.slots[index];
        let previous = slot.state;
        slot.state = next;
        previous
    }

    #[inline]
    pub fn slot_mut(&mut self, index: usize) -> &mut OrderSlot {
        &mut self.slots[index]
    }

    pub fn reap(&mut self, now: LocalTs, window: DurationUs) {
        for slot in self.slots.iter_mut() {
            let is_reapable = matches!(slot.state, OrderState::Closed(_))
                && now.diff(slot.closed_ts_us) >= window;
            if is_reapable {
                *slot = OrderSlot {
                    generation: slot.generation,
                    ..OrderSlot::EMPTY
                };
            }
        }
    }
}

pub(super) fn free_or_oldest_closed(slots: &[OrderSlot]) -> Option<usize> {
    if let Some(index) = slots.iter().position(|slot| slot.state == OrderState::Free) {
        return Some(index);
    }
    slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| matches!(slot.state, OrderState::Closed(_)))
        .min_by_key(|(_, slot)| slot.closed_ts_us)
        .map(|(index, _)| index)
}
