//! Engine-owned order truth: the fixed slot array, the geography that addresses a slot, and the
//! lifecycle that hands slots out and takes them back.
//!
//! What an event MEANS lives next door in `transition.rs`. The two are separable because the store
//! never interprets: it locates a slot, and the transition table decides what happens to it.

use std::ops::Range;

use crate::config::MAX_QUOTE_LEVELS;
use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, Side, VenueOrderId};
use crate::msg::exec::{ExecEvent, OrderStyle};
use crate::msg::persist::OrderLifecycle;
use crate::time::{DurationUs, TsUs};

use super::account::AccountWatermark;
use super::desired::QuoteLevel;

const SLOTS_PER_LEVEL: usize = 4;
const SLOTS_PER_SIDE: usize = MAX_QUOTE_LEVELS * SLOTS_PER_LEVEL;
const SLOTS_PER_INSTRUMENT: usize = 2 * SLOTS_PER_SIDE;
pub const MAX_ORDER_INSTRUMENTS: usize = 16;
pub const MAX_ORDER_SLOTS: usize = MAX_ORDER_INSTRUMENTS * SLOTS_PER_INSTRUMENT;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientIdLayout {
    pub run_nonce: u32,
}

impl ClientIdLayout {
    #[inline]
    pub fn encode(self, slot_index: usize, generation: u16) -> ClientOrderId {
        debug_assert!(
            slot_index < MAX_ORDER_SLOTS,
            "slot index {slot_index} beyond the table"
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
    pub claimed_ts_us: TsUs,
    pub recon_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseReason {
    Filled,
    Canceled,
    Rejected,
    Expired,
    ReconciledGone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderState {
    Free,
    PendingNew,
    Live,
    CancelInFlight,
    AmendInFlight,
    Unknown,
    Closed(CloseReason),
}

impl OrderState {
    #[inline]
    pub fn is_in_flight(self) -> bool {
        matches!(
            self,
            OrderState::PendingNew
                | OrderState::CancelInFlight
                | OrderState::AmendInFlight
                | OrderState::Unknown
        )
    }

    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(self, OrderState::Free | OrderState::Closed(_))
    }

    #[inline]
    pub fn is_working(self) -> bool {
        !self.is_terminal()
    }
}

impl From<OrderState> for OrderLifecycle {
    fn from(state: OrderState) -> Self {
        match state {
            OrderState::Free => OrderLifecycle::Free,
            OrderState::PendingNew => OrderLifecycle::PendingNew,
            OrderState::Live => OrderLifecycle::Live,
            OrderState::CancelInFlight => OrderLifecycle::CancelInFlight,
            OrderState::AmendInFlight => OrderLifecycle::AmendInFlight,
            OrderState::Unknown => OrderLifecycle::Unknown,
            OrderState::Closed(CloseReason::Filled) => OrderLifecycle::ClosedFilled,
            OrderState::Closed(CloseReason::Canceled) => OrderLifecycle::ClosedCanceled,
            OrderState::Closed(CloseReason::Rejected) => OrderLifecycle::ClosedRejected,
            OrderState::Closed(CloseReason::Expired) => OrderLifecycle::ClosedExpired,
            OrderState::Closed(CloseReason::ReconciledGone) => OrderLifecycle::ClosedReconciledGone,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReconcilePass {
    pub instrument: InstrumentId,
    pub recon_seq: u64,
    pub recon_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderSlot {
    pub client_id: ClientOrderId,
    pub venue_order_id: Option<VenueOrderId>,
    pub state: OrderState,
    pub side: Side,
    pub level: QuoteLevel,
    pub style: Option<OrderStyle>,
    pub instrument: InstrumentId,
    pub price: Price,
    pub qty: Qty,
    pub filled_base: Qty,
    pub filled_quote: i64,
    pub amends_used: u8,
    pub generation: u16,
    pub reserved_amount: i64,
    pub reserved_at: AccountWatermark,
    pub placed_recon_seq: u64,
    pub seen_recon_seq: u64,
    pub last_event_ts_us: TsUs,
    pub closed_ts_us: TsUs,
}

impl OrderSlot {
    pub const EMPTY: OrderSlot = OrderSlot {
        client_id: ClientOrderId(0),
        venue_order_id: None,
        state: OrderState::Free,
        side: Side::Buy,
        level: QuoteLevel::ZERO,
        style: None,
        instrument: InstrumentId(0),
        price: Price(0),
        qty: Qty(0),
        filled_base: Qty(0),
        filled_quote: 0,
        amends_used: 0,
        generation: 0,
        reserved_amount: 0,
        reserved_at: AccountWatermark::ZERO,
        placed_recon_seq: 0,
        seen_recon_seq: 0,
        last_event_ts_us: TsUs::from_micros(0),
        closed_ts_us: TsUs::from_micros(0),
    };

    /// Whether the venue is holding this order PASSIVELY — the shape a cancel can actually reach.
    ///
    /// A marketable order is excluded even while it reads Live. It never rests, so there is nothing
    /// for a cancel to retrieve, and a venue that holds taker orders for a moment before matching
    /// refuses the cancel outright — a refusal the engine would then have to explain away.
    #[inline]
    pub fn is_resting_quote(&self) -> bool {
        self.state == OrderState::Live && self.style != Some(OrderStyle::Immediate)
    }

    #[inline]
    pub fn fold_fill(&mut self, event: &ExecEvent) -> FillDelta {
        let delta_base = event.cumulative_qty.0 - self.filled_base.0;
        let delta_quote = event.cumulative_quote - self.filled_quote;
        if delta_base <= 0 {
            return FillDelta::NONE;
        }
        self.filled_base = event.cumulative_qty;
        self.filled_quote = event.cumulative_quote;
        FillDelta {
            base: Qty(delta_base),
            quote: delta_quote,
            commission: event.commission,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillDelta {
    pub base: Qty,
    pub quote: i64,
    pub commission: i64,
}

impl FillDelta {
    pub const NONE: FillDelta = FillDelta {
        base: Qty(0),
        quote: 0,
        commission: 0,
    };

    #[inline]
    pub fn is_empty(self) -> bool {
        self.base.0 == 0
    }
}

pub struct OrderTable {
    slots: Box<[OrderSlot; MAX_ORDER_SLOTS]>,
    layout: ClientIdLayout,
}

impl OrderTable {
    pub fn new(run_nonce: u32) -> Self {
        Self {
            slots: Box::new([OrderSlot::EMPTY; MAX_ORDER_SLOTS]),
            layout: ClientIdLayout { run_nonce },
        }
    }

    #[inline]
    pub fn slot(&self, index: usize) -> &OrderSlot {
        &self.slots[index]
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

    pub fn adopt(&mut self, event: &ExecEvent) -> Option<usize> {
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
            instrument: event.instrument,
            price: event.price,
            qty: event.qty,
            generation: ClientIdLayout::generation_of(event.client_id),
            last_event_ts_us: event.received_ts_us,
            placed_recon_seq: event.recon_seq,
            ..OrderSlot::EMPTY
        };
        Some(index)
    }

    #[inline]
    pub fn is_timed_out(&self, index: usize, now: TsUs, timeout: DurationUs) -> bool {
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

    #[inline]
    pub fn slot_mut(&mut self, index: usize) -> &mut OrderSlot {
        &mut self.slots[index]
    }

    pub fn reap(&mut self, now: TsUs, window: DurationUs) {
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
