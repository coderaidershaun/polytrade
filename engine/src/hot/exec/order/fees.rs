//! The trades whose fee an order has booked, so each fee is booked once however the venue repeats,
//! reorders or loses the reports of its trades.

use crate::msg::exec::ExecEvent;
use crate::msg::ids::{Qty, TradeId};

/// An order with more trades than this still books each new fee: a report that raises the filled
/// quantity is a new trade, so only a report that raises nothing needs the memory.
const REMEMBERED_TRADES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct FeeMemory {
    /// Oldest first. Once full it stays full, so the repeat of a trade that was pushed out is never
    /// taken for a new one.
    trade_ids: [Option<TradeId>; REMEMBERED_TRADES],
    /// Filled quantity that no report's own quantity has explained yet.
    open_gap: Qty,
    /// The filled total that an order of an earlier run showed when this run seated it, and zero
    /// for each other order. The earlier run may have booked the fee of any trade up to it.
    seated_total: Qty,
}

/// What one report owes the ledger in fees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FeeVerdict {
    pub(crate) fold: FeeFold,
    /// How much of the report's step its own quantity does not explain.
    pub(crate) unexplained: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeeFold {
    Book,
    /// The fee of this trade was booked when the trade was first reported.
    AlreadyBooked,
    /// The engine cannot tell whether the fee of this trade was booked, so it books nothing.
    Unbookable,
}

impl FeeVerdict {
    /// For an event that reaches no order.
    pub(super) const UNBOOKABLE: FeeVerdict = FeeVerdict {
        fold: FeeFold::Unbookable,
        unexplained: Qty::from_mantissa(0),
    };
}

impl FeeMemory {
    pub(super) const EMPTY: FeeMemory = FeeMemory {
        trade_ids: [None; REMEMBERED_TRADES],
        open_gap: Qty::from_mantissa(0),
        seated_total: Qty::from_mantissa(0),
    };

    pub(super) const fn seated_at(total: Qty) -> FeeMemory {
        FeeMemory {
            seated_total: total,
            ..FeeMemory::EMPTY
        }
    }

    /// `step` is how far `event` raised the order's filled quantity.
    pub(super) fn fold(&mut self, event: &ExecEvent, step: Qty) -> FeeVerdict {
        if step.mantissa() > 0 {
            if let Some(trade) = event.trade_id {
                self.remember(trade);
            }
            let unexplained = step.remaining_after(event.last_qty);
            self.open_gap = Qty::from_mantissa(self.open_gap.mantissa() + unexplained.mantissa());
            return FeeVerdict {
                fold: FeeFold::Book,
                unexplained,
            };
        }
        let fold = match event.trade_id {
            Some(trade) if self.trade_ids.contains(&Some(trade)) => FeeFold::AlreadyBooked,
            // Remembered, so that a repeat of the report leaves no second record of its fee.
            Some(trade) if self.is_at_or_below_the_seat(event) => {
                self.remember(trade);
                FeeFold::Unbookable
            }
            // A trade whose quantity an earlier report's step already held: the report came late.
            Some(trade) if !self.is_full() => {
                self.remember(trade);
                self.open_gap = self.open_gap.remaining_after(event.last_qty);
                FeeFold::Book
            }
            _ => FeeFold::Unbookable,
        };
        FeeVerdict {
            fold,
            unexplained: Qty::from_mantissa(0),
        }
    }

    #[inline]
    fn is_at_or_below_the_seat(&self, event: &ExecEvent) -> bool {
        self.seated_total.mantissa() > 0 && event.cumulative_qty <= self.seated_total
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.trade_ids[REMEMBERED_TRADES - 1].is_some()
    }

    fn remember(&mut self, trade: TradeId) {
        if self.trade_ids.contains(&Some(trade)) {
            return;
        }
        if self.is_full() {
            self.trade_ids.rotate_left(1);
            self.trade_ids[REMEMBERED_TRADES - 1] = Some(trade);
            return;
        }
        let free = self
            .trade_ids
            .iter()
            .position(Option::is_none)
            .expect("a memory that is not full has a free entry");
        self.trade_ids[free] = Some(trade);
    }
}
