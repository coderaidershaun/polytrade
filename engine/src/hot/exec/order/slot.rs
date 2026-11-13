//! An order slot: the state of one order of this run, and what a venue event moved in it.

use crate::hot::exec::{AccountWatermark, QuoteLevel};
use crate::msg::exec::{ExecEvent, OrderStyle};
use crate::msg::ids::{
    ClientOrderId, InstrumentId, Notional, Price, Qty, Side, Spend, VenueOrderId,
};
use crate::msg::persist::OrderLifecycle;
use crate::msg::time::LocalTs;

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
pub struct OrderSlot {
    pub(super) client_id: ClientOrderId,
    pub(super) venue_order_id: Option<VenueOrderId>,
    pub(super) state: OrderState,
    pub(super) side: Side,
    pub(super) level: QuoteLevel,
    pub(super) style: Option<OrderStyle>,
    pub(super) instrument: InstrumentId,
    pub(super) price: Price,
    pub(super) qty: Qty,
    pub(super) filled_base: Qty,
    pub(super) filled_quote: Notional,
    pub(super) amends_used: u8,
    pub(super) generation: u16,
    /// `None` while nothing is held for the order, a zero amount included.
    pub(super) reserved_amount: Option<Spend>,
    pub(super) reserved_at: AccountWatermark,
    pub(super) placed_recon_seq: u64,
    pub(super) seen_recon_seq: u64,
    pub(super) last_event_ts_us: LocalTs,
    pub(super) closed_ts_us: LocalTs,
}

/// What a reader outside the order table sees of a slot. Only a named transition of the table writes
/// one.
impl OrderSlot {
    #[inline]
    pub fn client_id(&self) -> ClientOrderId {
        self.client_id
    }

    #[inline]
    pub fn venue_order_id(&self) -> Option<VenueOrderId> {
        self.venue_order_id
    }

    #[inline]
    pub fn state(&self) -> OrderState {
        self.state
    }

    #[inline]
    pub fn side(&self) -> Side {
        self.side
    }

    #[inline]
    pub fn level(&self) -> QuoteLevel {
        self.level
    }

    /// `None` for an order adopted from a venue snapshot, whose style the engine never chose.
    #[inline]
    pub fn style(&self) -> Option<OrderStyle> {
        self.style
    }

    #[inline]
    pub fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    #[inline]
    pub fn price(&self) -> Price {
        self.price
    }

    #[inline]
    pub fn qty(&self) -> Qty {
        self.qty
    }

    #[inline]
    pub fn filled_base(&self) -> Qty {
        self.filled_base
    }

    #[inline]
    pub fn filled_quote(&self) -> Notional {
        self.filled_quote
    }

    #[inline]
    pub fn amends_used(&self) -> u8 {
        self.amends_used
    }

    #[inline]
    pub fn reserved_amount(&self) -> Option<Spend> {
        self.reserved_amount
    }

    #[inline]
    pub fn reserved_at(&self) -> AccountWatermark {
        self.reserved_at
    }

    #[inline]
    pub fn last_event_ts_us(&self) -> LocalTs {
        self.last_event_ts_us
    }
}

impl OrderSlot {
    pub(super) const EMPTY: OrderSlot = OrderSlot {
        client_id: ClientOrderId(0),
        venue_order_id: None,
        state: OrderState::Free,
        side: Side::Buy,
        level: QuoteLevel::ZERO,
        style: None,
        instrument: InstrumentId(0),
        price: Price::from_mantissa(0),
        qty: Qty::from_mantissa(0),
        filled_base: Qty::from_mantissa(0),
        filled_quote: Notional::ZERO,
        amends_used: 0,
        generation: 0,
        reserved_amount: None,
        reserved_at: AccountWatermark::ZERO,
        placed_recon_seq: 0,
        seen_recon_seq: 0,
        last_event_ts_us: LocalTs::from_micros(0),
        closed_ts_us: LocalTs::from_micros(0),
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
    pub(super) fn fold_fill(&mut self, event: &ExecEvent) -> FillDelta {
        let delta_base = event.cumulative_qty.mantissa() - self.filled_base.mantissa();
        let delta_quote = event.cumulative_quote - self.filled_quote;
        if delta_base <= 0 {
            return FillDelta::NONE;
        }
        self.filled_base = event.cumulative_qty;
        self.filled_quote = event.cumulative_quote;
        FillDelta {
            base: Qty::from_mantissa(delta_base),
            quote: delta_quote,
            // SMELL: the quantity above comes from the venue's running totals, but the fee comes
            // from this one report. A report that carries the fee of only one of its trades, or a
            // fee paid in another asset, which counts as zero, leaves the fee total wrong. The fee
            // needs to be folded once for each trade id.
            commission: event.commission,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillDelta {
    pub base: Qty,
    pub quote: Notional,
    pub commission: i64,
}

impl FillDelta {
    pub const NONE: FillDelta = FillDelta {
        base: Qty::from_mantissa(0),
        quote: Notional::ZERO,

        commission: 0,
    };

    #[inline]
    pub fn is_empty(self) -> bool {
        self.base.mantissa() == 0
    }
}
