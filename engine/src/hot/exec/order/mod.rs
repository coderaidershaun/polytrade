//! The engine's own record of its orders: a fixed table of slots, the client id that names a slot,
//! and the rules that hand slots out and take them back.
//!
//! The table never reads what an event means: it finds the slot, and a separate transition table
//! decides what happens to it.

mod fees;
mod occupancy;
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

pub(crate) use fees::FeeFold;
use fees::FeeMemory;
use occupancy::SlotsInUse;
pub(super) use prior_run::{PriorRunOrders, PriorRunSeat, Unseated};
pub use slot::{CloseReason, FillDelta, OrderSlot, OrderState};
pub use transition::Applied;
use transition::apply_exec_event;

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
fn level_range(instrument: InstrumentId, side: Side, level: QuoteLevel) -> Range<usize> {
    let base = level_base(instrument, side, level);
    base..base + SLOTS_PER_LEVEL
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
    /// The run nonce of the venue of the instrument: each venue has its own lease and nonce.
    pub run_nonce: u32,
}

/// Why a level of the ladder has no slot for a new order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimRefusal {
    /// An idle slot of the level is retired: the next order is refused, and the engine quotes on.
    IdsSpent,
    /// Each slot of the level holds a working order, so a slot leaked: the engine halts.
    LevelFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReconcilePass {
    pub instrument: InstrumentId,
    pub recon_seq: u64,
    pub recon_ts_us: LocalTs,
}

pub struct OrderTable {
    slots: Box<[OrderSlot]>,
    /// One for each slot, beside the slots rather than in them, so a copy of a slot stays small.
    fees: Box<[FeeMemory]>,
    in_use: SlotsInUse,
}

digest_by_hash!(ClientIdLayout, OrderSlot, FeeMemory);
digest_fields! {
    OrderTable { slots, fees }
    not_state {
        // Which slots are not free: a function of the slots, which the digest holds.
        in_use,
    }
}

impl OrderTable {
    pub fn new(capacity: &ExecCapacity) -> Self {
        let slots = capacity.order_slots();
        Self {
            slots: vec![OrderSlot::EMPTY; slots].into_boxed_slice(),
            fees: vec![FeeMemory::EMPTY; slots].into_boxed_slice(),
            in_use: SlotsInUse::with_slots(slots),
        }
    }

    #[inline]
    pub fn slot(&self, index: usize) -> &OrderSlot {
        &self.slots[index]
    }

    /// Each slot that is not free, in ascending order.
    #[inline]
    pub fn slots_in_use(&self) -> impl Iterator<Item = usize> + '_ {
        self.in_use.iter()
    }

    /// The first slot that is not free at or after `from`, for a pass that changes slots as it
    /// walks.
    #[inline]
    pub(super) fn next_in_use(&self, from: usize) -> Option<usize> {
        self.in_use.next_from(from)
    }

    /// The slots of one side that are not free, in ascending order.
    #[inline]
    pub(super) fn side_in_use(
        &self,
        instrument: InstrumentId,
        side: Side,
    ) -> impl Iterator<Item = &OrderSlot> + '_ {
        let range = self.side_slot_range(instrument, side);
        self.in_use.within(range).map(|index| &self.slots[index])
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
        &self.slots[level_range(instrument, side, level)]
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
        self.in_use
            .within(level_range(instrument, side, level))
            .map(|index| &self.slots[index])
            .find(|slot| slot.state.is_working() && slot.style != Some(OrderStyle::Immediate))
    }

    #[inline]
    pub fn is_awaiting_answer(&self, instrument: InstrumentId, side: Side) -> bool {
        self.side_in_use(instrument, side)
            .any(|slot| slot.state.is_in_flight())
    }

    #[inline]
    pub fn possibly_live_count(&self, instrument: InstrumentId, side: Side) -> usize {
        self.side_in_use(instrument, side)
            .filter(|slot| slot.state.is_working())
            .count()
    }

    /// # Errors
    /// [`ClaimRefusal::IdsSpent`] when an idle slot of the level has spent its client ids, and
    /// [`ClaimRefusal::LevelFull`] when each slot of the level holds a working order.
    pub fn claim(&mut self, claim: OrderClaim) -> Result<(usize, ClientOrderId), ClaimRefusal> {
        let base = level_base(claim.instrument, claim.side, claim.level);
        let level = &self.slots[base..base + SLOTS_PER_LEVEL];
        let Some(offset) = free_or_oldest_closed(level) else {
            let is_any_idle = level.iter().any(|slot| slot.state.is_terminal());
            return Err(if is_any_idle { ClaimRefusal::IdsSpent } else { ClaimRefusal::LevelFull });
        };
        let index = base + offset;
        let slot = &mut self.slots[index];
        let generation = slot.generation + 1;
        let layout = ClientIdLayout {
            run_nonce: claim.run_nonce,
        };
        let client_id = layout.encode(index, generation);
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
        self.fees[index] = FeeMemory::EMPTY;
        self.in_use.insert(index);
        Ok((index, client_id))
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
        self.fees[index] = FeeMemory::EMPTY;
        self.in_use.insert(index);
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
        self.invalidate_within(0..self.slots.len(), invalidated);
    }

    /// A stream reset of one venue leaves the orders of each other venue as they were.
    pub(crate) fn invalidate_within(
        &mut self,
        slots: Range<usize>,
        invalidated: &mut impl FnMut(&OrderSlot, OrderState),
    ) {
        let mut next = self.in_use.next_from(slots.start);
        while let Some(index) = next.filter(|index| *index < slots.end) {
            next = self.in_use.next_from(index + 1);
            let slot = &mut self.slots[index];
            if !slot.state.is_working() {
                continue;
            }
            let previous = slot.state;
            slot.state = OrderState::Unknown;
            invalidated(slot, previous);
        }
    }

    #[inline]
    pub fn apply(&mut self, index: usize, event: &ExecEvent, now: LocalTs) -> Applied {
        apply_exec_event(&mut self.slots[index], &mut self.fees[index], event, now)
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
        let slot = &mut self.slots[index];
        slot.last_event_ts_us = now;
        slot.status_probes = slot.status_probes.saturating_add(1);
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

    pub fn reap(&mut self, now: LocalTs, window: DurationUs) {
        self.reap_within(0..self.slots.len(), now, window);
    }

    /// Each venue reaps its own slots with its own window.
    pub(crate) fn reap_within(&mut self, slots: Range<usize>, now: LocalTs, window: DurationUs) {
        let mut next = self.in_use.next_from(slots.start);
        while let Some(index) = next.filter(|index| *index < slots.end) {
            next = self.in_use.next_from(index + 1);
            let slot = &mut self.slots[index];
            let is_reapable = matches!(slot.state, OrderState::Closed(_))
                && now.diff(slot.closed_ts_us) >= window;
            if is_reapable {
                *slot = OrderSlot {
                    generation: slot.generation,
                    ..OrderSlot::EMPTY
                };
                self.fees[index] = FeeMemory::EMPTY;
                self.in_use.remove(index);
            }
        }
    }
}

/// The slot a new order may take: a free one first, else the one that closed longest ago. A slot at
/// its last generation is retired for the run, because the next id it minted would wrap to an id
/// the venue has already seen.
pub(super) fn free_or_oldest_closed(slots: &[OrderSlot]) -> Option<usize> {
    let is_reusable = |slot: &OrderSlot| slot.generation < u16::MAX;
    let free = slots
        .iter()
        .position(|slot| slot.state == OrderState::Free && is_reusable(slot));
    if let Some(index) = free {
        return Some(index);
    }
    slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| matches!(slot.state, OrderState::Closed(_)) && is_reusable(slot))
        .min_by_key(|(_, slot)| slot.closed_ts_us)
        .map(|(index, _)| index)
}
