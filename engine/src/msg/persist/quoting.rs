//! The rows that let a reader judge a run's money and its quoting without a re-fold: the ledger at
//! each point that moves it, the mid of each book at each spin, and each side that did not quote.

use crate::msg::ids::{ClientOrderId, InstrumentId, Notional, Price, Qty, Side, TradeId};
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::order::{ReadinessGap, RejectReason};
use crate::msg::time::LocalTs;

labelled_enum! {
    /// Why the ledger wrote a row.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum LedgerKind {
        /// The position that the run restored at its start, written before any other row.
        Boot = "boot",
        Fill = "fill",
        /// A regular row while a position is open, so a reader sees the mark move between fills.
        Interval = "interval",
        /// The row as it stood just before a rotation cleared it.
        Rotation = "rotation",
    }
    pub fn as_str;
    pub const ALL;
}

/// One state of the ledger of one instrument. Each amount is an exact quote mantissa, so a reader
/// adds them up with no rounding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerEntry {
    pub kind: LedgerKind,
    pub instrument: InstrumentId,
    pub position_base: Qty,
    pub cash_quote: Notional,
    pub basis_quote: Notional,
    /// Cash plus basis.
    pub realised_quote: Notional,
    pub mark: Option<Price>,
    /// Mark times position, and zero with no mark.
    pub exposure_quote: Notional,
    /// Cash plus exposure.
    pub pnl_quote: Notional,
    /// Set on the rows of kind `fill` only.
    pub client_id: Option<ClientOrderId>,
    pub trade_id: Option<TradeId>,
    pub received_ts_us: LocalTs,
}

/// The touch and the mid of one book at one spin. Each price is absent when the book lacks that
/// side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MidRow {
    pub instrument: InstrumentId,
    pub spin_seq: u64,
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    pub mid: Option<Price>,
    pub event_ts_us: LocalTs,
}

/// One side of one instrument that held no working quote after the execution pass of a spin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoQuoteRow {
    pub instrument: InstrumentId,
    pub spin_seq: u64,
    pub side: Side,
    pub reason: NoQuoteReason,
    pub event_ts_us: LocalTs,
}

labelled_enum! {
    /// Why a side did not quote: one word for each refusal of the execution pass, in the order of
    /// the refusals, so a reader can count the time not quoting by cause.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum NoQuoteReason {
        QtyBelowMin = "qty_below_min",
        NotionalBelowMin = "notional_below_min",
        NotionalAboveMax = "notional_above_max",
        WouldCross = "would_cross",
        OutsideBand = "outside_band",
        Underfunded = "underfunded",
        NotReadyStream = "not_ready_stream",
        NotReadyBalances = "not_ready_balances",
        NotReadyOpenOrders = "not_ready_open_orders",
        Halted = "halted",
        SessionReducingOnly = "session_reducing_only",
        ExposureCeiling = "exposure_ceiling",
        NoQuoteDeclared = "no_quote_declared",
        BookNotQuotable = "book_not_quotable",
        DuplicatePrice = "duplicate_price",
        OrderLimit = "order_limit",
        RateBudget = "rate_budget",
        OutsideWindow = "outside_window",
        Warming = "warming",
        IdsSpent = "ids_spent",
    }
    pub fn as_str;
    pub const ALL;
}

impl From<RejectReason> for NoQuoteReason {
    fn from(reason: RejectReason) -> Self {
        match reason {
            RejectReason::QtyBelowMin => NoQuoteReason::QtyBelowMin,
            RejectReason::NotionalBelowMin => NoQuoteReason::NotionalBelowMin,
            RejectReason::NotionalAboveMax => NoQuoteReason::NotionalAboveMax,
            RejectReason::WouldCross => NoQuoteReason::WouldCross,
            RejectReason::OutsideBand => NoQuoteReason::OutsideBand,
            RejectReason::Underfunded => NoQuoteReason::Underfunded,
            RejectReason::NotReady(ReadinessGap::Stream) => NoQuoteReason::NotReadyStream,
            RejectReason::NotReady(ReadinessGap::Balances) => NoQuoteReason::NotReadyBalances,
            RejectReason::NotReady(ReadinessGap::OpenOrders) => NoQuoteReason::NotReadyOpenOrders,
            RejectReason::Halted => NoQuoteReason::Halted,
            RejectReason::SessionReducingOnly => NoQuoteReason::SessionReducingOnly,
            RejectReason::ExposureCeiling => NoQuoteReason::ExposureCeiling,
            RejectReason::NoQuoteDeclared => NoQuoteReason::NoQuoteDeclared,
            RejectReason::BookNotQuotable => NoQuoteReason::BookNotQuotable,
            RejectReason::DuplicatePrice => NoQuoteReason::DuplicatePrice,
            RejectReason::OrderLimit => NoQuoteReason::OrderLimit,
            RejectReason::RateBudget => NoQuoteReason::RateBudget,
            RejectReason::OutsideWindow => NoQuoteReason::OutsideWindow,
            RejectReason::Warming => NoQuoteReason::Warming,
            RejectReason::IdsSpent => NoQuoteReason::IdsSpent,
        }
    }
}
