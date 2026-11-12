//! Stable storage and limits for simulated orders.

use super::orders::{CORPUS_VENUE_ORDER_ID, SimOrder};
use super::queue::SimOrderIndex;
use super::wallet::Reservation;
use crate::ids::{ClientOrderId, Price, Qty, Side, TradeId, VenueOrderId};
use crate::msg::exec::VenueOrderStatus;
use crate::time::{DurationUs, TsUs};

const FIRST_TRADE_ID: TradeId = TradeId(778_291);

pub const ORDER_TABLE_CAPACITY: usize = u16::MAX as usize + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderPhase {
    Pending,
    Resting,
    Closed(ClosedReason),
    Reaped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClosedReason {
    Filled,
    Canceled,
    CrossedPostOnly,
    Refused(RefusalReason),
}

impl ClosedReason {
    pub const fn venue_status(self) -> VenueOrderStatus {
        match self {
            ClosedReason::Filled => VenueOrderStatus::Filled,
            ClosedReason::Canceled => VenueOrderStatus::Canceled,
            ClosedReason::CrossedPostOnly | ClosedReason::Refused(_) => VenueOrderStatus::Rejected,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefusalReason {
    TickGrid,
    StepGrid,
    MinQty,
    MinNotional,
    MaxOrders,
    StyleNotPermitted,
    NoSuchOrder,
    OrderGone,
    AmendBudgetSpent,
    AmendQuantityIncrease,
    AmendFilterFailure,
    InsufficientFunds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RestingOrder {
    pub order: SimOrder,
    pub phase: OrderPhase,
    pub generation: u32,
    pub amends_used: u8,
    pub effective_ts_us: TsUs,
    pub reservation: Option<Reservation>,
    pub joined_ts_us: TsUs,
    pub closed_ts_us: Option<TsUs>,
    pub prints_seen: u32,
    pub resyncs_while_resting: u32,
}

impl RestingOrder {
    pub fn pending(order: SimOrder, effective_ts_us: TsUs) -> Self {
        Self {
            order,
            phase: OrderPhase::Pending,
            generation: 0,
            amends_used: 0,
            effective_ts_us,
            reservation: None,
            joined_ts_us: effective_ts_us,
            closed_ts_us: None,
            prints_seen: 0,
            resyncs_while_resting: 0,
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(self.phase, OrderPhase::Pending | OrderPhase::Resting)
    }

    pub fn is_reapable(&self, at_ts_us: TsUs) -> bool {
        !self.is_open() && self.closed_ts_us.is_some_and(|closed| closed < at_ts_us)
    }

    pub fn snapshot(&self) -> OrderSnapshot {
        OrderSnapshot {
            order: self.order,
            phase: self.phase,
            generation: self.generation,
            joined_ts_us: self.joined_ts_us,
            prints_seen: self.prints_seen,
            resyncs_while_resting: self.resyncs_while_resting,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderSnapshot {
    pub order: SimOrder,
    pub phase: OrderPhase,
    pub generation: u32,
    pub joined_ts_us: TsUs,
    pub prints_seen: u32,
    pub resyncs_while_resting: u32,
}

#[derive(Debug, Clone)]
pub struct RestingOrders {
    records: Vec<RestingOrder>,
    latest_venue_order_id: VenueOrderId,
    next_trade_id: TradeId,
}

impl RestingOrders {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            latest_venue_order_id: CORPUS_VENUE_ORDER_ID,
            next_trade_id: FIRST_TRADE_ID,
        }
    }

    /// # Panics
    /// If the table is exhausted.
    pub fn admit(&mut self, mut record: RestingOrder) -> SimOrderIndex {
        assert!(
            self.records.len() < ORDER_TABLE_CAPACITY,
            "simulated order table exhausted after {ORDER_TABLE_CAPACITY} admissions — reaping \
             clears a verdict but never frees a row, so this bounds the orders one RUN may place, \
             not the ones it may hold at once"
        );
        record.generation = self
            .records
            .iter()
            .rev()
            .find(|previous| previous.order.client_id == record.order.client_id)
            .map_or(0, |previous| {
                previous
                    .generation
                    .checked_add(1)
                    .expect("simulated order generations exhausted")
            });
        let slot = self.records.len() as u16;
        self.records.push(record);
        SimOrderIndex(slot)
    }

    pub fn reap_through(&mut self, at_ts_us: TsUs, retention: DurationUs) {
        for record in &mut self.records {
            let is_expired = record.closed_ts_us.is_some_and(|closed| {
                i128::from(at_ts_us.micros()) - i128::from(closed.micros())
                    > i128::from(retention.micros())
            });
            if is_expired && record.is_reapable(at_ts_us) {
                record.phase = OrderPhase::Reaped;
            }
        }
    }

    pub fn get(&self, index: SimOrderIndex) -> Option<&RestingOrder> {
        self.records.get(index.0 as usize)
    }

    pub fn get_mut(&mut self, index: SimOrderIndex) -> Option<&mut RestingOrder> {
        self.records.get_mut(index.0 as usize)
    }

    pub fn snapshot(&self, index: SimOrderIndex) -> Option<OrderSnapshot> {
        self.get(index).map(RestingOrder::snapshot)
    }

    pub fn iter(&self) -> impl Iterator<Item = (SimOrderIndex, &RestingOrder)> {
        self.records
            .iter()
            .enumerate()
            .map(|(slot, record)| (SimOrderIndex(slot as u16), record))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut RestingOrder> {
        self.records.iter_mut()
    }

    pub fn find(&self, client_id: ClientOrderId) -> Option<SimOrderIndex> {
        self.records
            .iter()
            .rposition(|record| {
                record.order.client_id == client_id && record.phase != OrderPhase::Reaped
            })
            .map(|slot| SimOrderIndex(slot as u16))
    }

    pub fn count_resting_on(&self, side: Side) -> usize {
        self.records
            .iter()
            .filter(|record| record.phase == OrderPhase::Resting && record.order.side == side)
            .count()
    }

    pub fn open_indices(&self) -> impl Iterator<Item = SimOrderIndex> {
        self.iter()
            .filter(|(_, record)| record.is_open())
            .map(|(index, _)| index)
    }

    /// # Panics
    /// If venue order IDs are exhausted.
    pub fn mint_venue_order_id(&mut self) -> VenueOrderId {
        let next = self
            .latest_venue_order_id
            .0
            .checked_add(1)
            .expect("simulated venue order ids exhausted");
        self.latest_venue_order_id = VenueOrderId(next);
        self.latest_venue_order_id
    }

    /// # Panics
    /// As [`RestingOrders::mint_venue_order_id`].
    pub fn mint_trade_id(&mut self) -> TradeId {
        let minted = self.next_trade_id;
        self.next_trade_id = TradeId(
            minted
                .0
                .checked_add(1)
                .expect("simulated trade ids exhausted"),
        );
        minted
    }
}

impl Default for RestingOrders {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InstrumentLimits {
    pub tick: Price,
    pub step: Qty,
    pub min_qty: Qty,
    pub min_notional: i64,
    pub max_orders_per_side: u16,
    pub max_amends: u8,
}

impl InstrumentLimits {
    pub fn refuse(&self, price: Price, qty: Qty) -> Option<RefusalReason> {
        if self.tick.0 > 0 && price.0 % self.tick.0 != 0 {
            return Some(RefusalReason::TickGrid);
        }
        if self.step.0 > 0 && qty.0 % self.step.0 != 0 {
            return Some(RefusalReason::StepGrid);
        }
        if qty.0 < self.min_qty.0 {
            return Some(RefusalReason::MinQty);
        }
        if price.notional(qty) < self.min_notional {
            return Some(RefusalReason::MinNotional);
        }
        None
    }
}
