//! The records that the hot thread hands the tape writer: fixed-size copies with exact mantissas and
//! stamps, so a push never allocates. A rotation row carries text, so it travels apart.

use crate::hot::exec::QuoteLevel;
use crate::msg::exec::{Liquidity, OrderStyle, Provenance, RejectClass};
use crate::msg::ids::{
    AssetId, ClientOrderId, InstrumentId, Price, Qty, Side, TradeId, VenueOrderId,
};
use crate::msg::inbound::BookChunkKind;
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::{LocalTs, VenueTs};
use crate::runtime::config::{KlineInterval, TableKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureId(pub u16);

/// A copy of fixed size, because the ring to the tape writer holds its records by value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PersistRecord {
    Feature(FeatureRow),
    Trade(TradeRow),
    BookEvent(BookEventRow),
    Kline(KlineRow),
    LinkFrame(LinkFrameRow),
    Order(OrderRow),
    Fill(FillRow),
    /// Written whenever the tape exists: no config can switch off the record of money that the
    /// engine could not place in its state.
    Incident(IncidentRow),
    /// Seal each file. It travels in the ring, so each record banked before it is in the files.
    SealAll,
}

impl PersistRecord {
    /// The table that `strategy.tables` names for this record, and `None` for a record that no
    /// config can switch off.
    #[inline]
    pub fn table(&self) -> Option<TableKind> {
        match self {
            PersistRecord::Feature(_) => Some(TableKind::Features),
            PersistRecord::Trade(_) => Some(TableKind::Trades),
            PersistRecord::BookEvent(_) => Some(TableKind::BookEvents),
            PersistRecord::Kline(_) => Some(TableKind::Klines),
            PersistRecord::LinkFrame(_) => Some(TableKind::LinkFrames),
            PersistRecord::Order(_) => Some(TableKind::Orders),
            PersistRecord::Fill(_) => Some(TableKind::Fills),
            PersistRecord::Incident(_) | PersistRecord::SealAll => None,
        }
    }
}

/// Stamped with the time of its message, never a clock read, so a replay writes the same row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeatureRow {
    pub instrument: InstrumentId,
    pub feature: FeatureId,
    pub value: f64,
    pub event_ts_us: LocalTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeRow {
    pub instrument: InstrumentId,
    pub price: Price,
    pub qty: Qty,
    pub side: Side,
    pub exchange_ts_us: VenueTs,
    pub received_ts_us: LocalTs,
}

/// What the book applied, and its reset, rather than the venue's wire kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookEventKind {
    Delta,
    Snapshot,
    Reset,
}

impl From<BookChunkKind> for BookEventKind {
    #[inline]
    fn from(kind: BookChunkKind) -> Self {
        match kind {
            BookChunkKind::Delta => BookEventKind::Delta,
            BookChunkKind::Snapshot => BookEventKind::Snapshot,
        }
    }
}

/// One price level, or a reset, whose price, quantity and update id are zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookEventRow {
    pub instrument: InstrumentId,
    pub kind: BookEventKind,
    pub side: Option<Side>,
    pub price: Price,
    pub qty: Qty,
    pub update_id: u64,
    pub received_ts_us: LocalTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineRow {
    pub instrument: InstrumentId,
    pub interval: KlineInterval,
    pub open_ts_us: VenueTs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub base_volume: Qty,
    pub quote_volume: i64,
    pub trade_count: u32,
    pub is_closed: bool,
    pub exchange_ts_us: VenueTs,
    pub received_ts_us: LocalTs,
}

labelled_enum! {
    /// The run state sits in the kind rather than in the value, so its Parquet column reads as
    /// words.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum LinkRowKind {
        Payload = "payload",
        RunRunning = "run_running",
        RunIdle = "run_idle",
    }
    pub fn as_str;
}

/// A link frame or a run state, written by the fold itself so the tape cannot differ from what the
/// fold saw. One row for each value, so no row carries a wide array of floats.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkFrameRow {
    pub kind: LinkRowKind,
    pub sender_te_hash: u64,
    pub topic: u16,
    pub seq: u64,
    pub slot: u16,
    pub count: u16,
    pub value: f64,
    pub event_ts_us: VenueTs,
    pub received_ts_us: LocalTs,
}

labelled_enum! {
    /// What moved an order. It holds more than the venue's event kinds, because a tape with no row
    /// for an order that the venue never acknowledged would lose that order.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum OrderTransition {
        Placed = "placed",
        CancelSent = "cancel_sent",
        AmendSent = "amend_sent",
        SendAbandoned = "send_abandoned",
        Timeout = "timeout",
        SweepClosed = "sweep_closed",
        StreamReset = "stream_reset",
        AckPlaced = "ack_placed",
        AckCanceled = "ack_canceled",
        AckAmended = "ack_amended",
        AckFailed = "ack_failed",
        ReportNew = "report_new",
        ReportTrade = "report_trade",
        ReportCanceled = "report_canceled",
        ReportExpired = "report_expired",
        ReportRejected = "report_rejected",
        ReportAmended = "report_amended",
        SnapshotOrder = "snapshot_order",
    }
    pub fn as_str;
}

labelled_enum! {
    /// What the engine believes of an order, as one flat label for the tape.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum OrderLifecycle {
        Free = "free",
        PendingNew = "pending_new",
        Live = "live",
        CancelInFlight = "cancel_in_flight",
        AmendInFlight = "amend_in_flight",
        /// The engine no longer knows what the venue holds. The side does not quote until a read
        /// of open orders answers.
        Unknown = "unknown",
        ClosedFilled = "closed_filled",
        ClosedCanceled = "closed_canceled",
        ClosedRejected = "closed_rejected",
        ClosedExpired = "closed_expired",
        ClosedReconciledGone = "closed_reconciled_gone",
    }
    pub fn as_str;
}

/// One row for each transition of an order. A previous state that differs from the state of the row
/// before it shows a lost row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRow {
    pub instrument: InstrumentId,
    pub client_id: ClientOrderId,
    pub quote_level: Option<QuoteLevel>,
    // The key that joins the venue's records. Its absence says the order was sent and never
    // acknowledged.
    pub venue_order_id: Option<VenueOrderId>,
    pub transition: OrderTransition,
    pub state: OrderLifecycle,
    pub previous_state: OrderLifecycle,
    pub provenance: Provenance,
    pub side: Side,
    pub style: Option<OrderStyle>,
    pub price: Price,
    pub qty: Qty,
    // Beside the report's own quantity, a total that jumps shows a lost report.
    pub filled_qty: Qty,
    pub filled_quote: i64,
    pub reject: Option<RejectClass>,
    pub reject_code: i32,
    pub exchange_ts_us: VenueTs,
    pub received_ts_us: LocalTs,
}

/// One fill: the line that a venue statement is reconciled against, with its quantity, price and
/// fee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillRow {
    pub instrument: InstrumentId,
    pub trade_id: Option<TradeId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub client_id: ClientOrderId,
    pub quote_level: Option<QuoteLevel>,
    pub provenance: Provenance,
    pub side: Side,
    pub liquidity: Option<Liquidity>,
    pub last_price: Price,
    pub last_qty: Qty,
    // Beside the report's own quantity and price, what was booked shows a lost report.
    pub booked_qty: Qty,
    pub booked_quote: i64,
    // In the asset that `commission_asset` names, which need not be the quote asset. A wrong
    // asset here makes the fee wrong for good.
    pub commission: i64,
    pub commission_asset: AssetId,
    pub exchange_ts_us: VenueTs,
    pub received_ts_us: LocalTs,
}

labelled_enum! {
    /// Why money that the engine could not place in its state left a record.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum IncidentKind {
        /// The filled quantity of an order rose by more than its report's own quantity, so the fees
        /// of the trades in between are unknown.
        FeeGap = "fee_gap",
        StrandedPosition = "stranded_position",
        /// A fill of an own order that holds no slot, of this run or of an earlier one. The ledger
        /// does not book it.
        OrphanFill = "orphan_fill",
        /// A fee that the ledger did not book: its asset has no row, or the engine cannot tell
        /// whether the fee of its trade was booked before.
        UnbookedFee = "unbooked_fee",
    }
    pub fn as_str;
}

/// One money event that the engine could not place in its state, with the amounts that a person
/// needs to book it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncidentRow {
    pub kind: IncidentKind,
    pub instrument: InstrumentId,
    /// `None` for a stranded position, which no one order holds.
    pub client_id: Option<ClientOrderId>,
    pub trade_id: Option<TradeId>,
    /// The unexplained step of a fee gap, the filled quantity of an orphan fill, the position of a
    /// stranded position, and the report's own quantity of an unbooked fee.
    pub qty: Qty,
    /// The basis of a stranded position, as a quote mantissa, and an unbooked fee, as a mantissa of
    /// the asset that the fill row of its trade names. Zero for the other kinds.
    pub amount: i64,
    pub window_close_ts_us: Option<VenueTs>,
    pub exchange_ts_us: VenueTs,
    pub received_ts_us: LocalTs,
}

/// Which market an instrument stood for over which window. It travels to the tape writer by a
/// channel of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationRow {
    pub instrument: InstrumentId,
    pub window_open_ts_us: VenueTs,
    pub window_close_ts_us: VenueTs,
    pub token_id_up: Box<str>,
    pub token_id_down: Box<str>,
    pub condition_id: Box<str>,
    pub received_ts_us: LocalTs,
}
