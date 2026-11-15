//! What the simulated venue emits, held in time order until the horizon releases it.

use super::market::ResetReason;
use super::queue;
use super::resting::{OrderSnapshot, RefusalReason};
use super::wallet::FillSettlement;

use crate::msg::ids::{ClientOrderId, Price, Qty, TradeId};
use crate::msg::time::LocalTs;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SimEmission {
    pub at_ts_us: LocalTs,
    pub sequence: u64,
    pub event: VenueEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VenueEvent {
    Rested {
        snapshot: OrderSnapshot,
        queue_ahead: queue::QueueAhead,
    },
    PostOnlyCrossed {
        snapshot: OrderSnapshot,
    },
    PlaceRefused {
        snapshot: OrderSnapshot,
        reason: RefusalReason,
    },
    Filled {
        snapshot: OrderSnapshot,
        last_qty: Qty,
        last_price: Price,
        trade_id: TradeId,
        settlement: FillSettlement,
    },
    Canceled {
        snapshot: OrderSnapshot,
    },
    CancelRefused {
        client_id: ClientOrderId,
        reason: RefusalReason,
    },
    Amended {
        snapshot: OrderSnapshot,
        total_qty: Qty,
    },
    AmendRefused {
        client_id: ClientOrderId,
        reason: RefusalReason,
    },
    OrderStatus {
        snapshot: OrderSnapshot,
    },
    NoSuchOrder {
        client_id: ClientOrderId,
    },
    OpenOrders {
        rows: Vec<OrderSnapshot>,
    },
    StreamSubscribed,
    MarketReset {
        reason: ResetReason,
    },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct EmissionBook {
    pending: Vec<SimEmission>,
    sequence: u64,
}

impl EmissionBook {
    pub fn push(&mut self, at_ts_us: LocalTs, event: VenueEvent) {
        self.pending.push(SimEmission {
            at_ts_us,
            sequence: self.sequence,
            event,
        });
        self.sequence += 1;
    }

    pub(super) fn drain_through(&mut self, horizon: LocalTs, out: &mut Vec<SimEmission>) {
        self.pending
            .sort_by_key(|emission| (emission.at_ts_us, emission.sequence));
        let split = self
            .pending
            .partition_point(|emission| emission.at_ts_us <= horizon);
        out.extend(self.pending.drain(..split));
    }
}
