//! The orders that an earlier run of this trading engine left resting, in a table of their own.

use crate::hot::digest::digest_fields;
use crate::hot::exec::ExecCapacity;
use crate::msg::exec::ExecEvent;
use crate::msg::ids::{ClientOrderId, InstrumentId, Qty, Side};
use crate::msg::time::LocalTs;

use super::fees::FeeMemory;
use super::occupancy::SlotsInUse;
use super::{Applied, OrderSlot, OrderState, apply_exec_event, free_or_oldest_closed};

/// Orders an EARLIER run of this trading engine left resting on the venue. They are cancelled on
/// sight, but the cancel takes hundreds of milliseconds and whatever rests fills in the meantime.
/// Those are real executions on this account, so they move the ledger and earn audit rows exactly
/// like any other. Only the total that an order shows at its first report stays out of the ledger:
/// the earlier run may have booked any part of it, and the restored position holds what it booked.
///
/// They must never join THIS run's quoting, which is why they sit in a table of their own rather
/// than in [`OrderTable`](super::OrderTable). A prior-run client id decodes to a slot index
/// in that table, so seating one there would either evict an order this run is about to place or
/// lose the fill to the orphan path, decided by nothing but arrival order. Keeping them apart also
/// keeps a cancel answered "unknown order", this path's ordinary outcome, out of the reject streak.
pub(in crate::hot::exec) struct PriorRunOrders {
    slots: Box<[OrderSlot]>,
    fees: Box<[FeeMemory]>,
    /// A seat reuses a closed entry and nothing reaps one, so its bits only rise.
    in_use: SlotsInUse,
}

digest_fields! {
    PriorRunOrders { slots, fees }
    not_state {
        // Which entries are not free: a function of the slots, which the digest holds.
        in_use,
    }
}

/// The entry of an order of an earlier run, and the filled total at which the report seated it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::hot::exec) struct PriorRunSeat {
    pub(in crate::hot::exec) index: usize,
    /// Zero for an order that already had its entry.
    pub(in crate::hot::exec) unbooked: Qty,
}

/// Why an order of an earlier run has no slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::hot::exec) enum Unseated {
    TableFull,
    NoInstrument,
}

impl PriorRunOrders {
    pub(in crate::hot::exec) fn new(capacity: &ExecCapacity) -> Self {
        let slots = capacity.prior_run_slots();
        Self {
            slots: vec![OrderSlot::EMPTY; slots].into_boxed_slice(),
            fees: vec![FeeMemory::EMPTY; slots].into_boxed_slice(),
            in_use: SlotsInUse::with_slots(slots),
        }
    }

    #[inline]
    pub(in crate::hot::exec) fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub(in crate::hot::exec) fn slot(&self, index: usize) -> &OrderSlot {
        &self.slots[index]
    }

    #[inline]
    pub(in crate::hot::exec) fn apply(
        &mut self,
        index: usize,
        event: &ExecEvent,
        now: LocalTs,
    ) -> Applied {
        apply_exec_event(&mut self.slots[index], &mut self.fees[index], event, now)
    }

    pub(in crate::hot::exec) fn working(
        &self,
        instrument: InstrumentId,
        side: Side,
    ) -> impl Iterator<Item = &OrderSlot> {
        self.in_use
            .iter()
            .map(|index| &self.slots[index])
            .filter(move |slot| {
                slot.instrument == instrument && slot.side == side && slot.state.is_working()
            })
    }

    /// A report that seats an order sets its filled totals to the ones that it shows, so only what
    /// later reports add is a step that the ledger books.
    pub(in crate::hot::exec) fn find_or_seat(
        &mut self,
        event: &ExecEvent,
        now: LocalTs,
    ) -> Result<PriorRunSeat, Unseated> {
        if let Some(index) = self.find(event.client_id) {
            let unbooked = Qty::from_mantissa(0);
            return Ok(PriorRunSeat { index, unbooked });
        }
        let instrument = event.instrument.ok_or(Unseated::NoInstrument)?;
        let index = free_or_oldest_closed(&self.slots[..]).ok_or(Unseated::TableFull)?;
        self.slots[index] = OrderSlot {
            client_id: event.client_id,
            venue_order_id: event.venue_order_id,
            state: OrderState::Unknown,
            side: event.side,
            instrument,
            price: event.price,
            qty: event.qty,
            filled_base: event.cumulative_qty,
            filled_quote: event.cumulative_quote,
            last_event_ts_us: now,
            ..OrderSlot::EMPTY
        };
        self.fees[index] = FeeMemory::EMPTY;
        self.in_use.insert(index);
        let unbooked = event.cumulative_qty;
        Ok(PriorRunSeat { index, unbooked })
    }

    fn find(&self, id: ClientOrderId) -> Option<usize> {
        self.in_use.iter().find(|index| {
            let slot = &self.slots[*index];
            slot.client_id == id && slot.state != OrderState::Free
        })
    }
}
