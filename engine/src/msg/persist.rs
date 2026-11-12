//! Persistence records: POD Copy union (i64 mantissas, _ts_us stamps, no strings). Plus RotationRow (owned, strings, off hot path).

use crate::config::{KlineInterval, TableKind};
use crate::hot::exec::QuoteLevel;
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side, TradeId, VenueOrderId};
use crate::labelled_enum::labelled_enum;
use crate::msg::exec::{Liquidity, OrderStyle, Provenance, RejectClass};
use crate::msg::inbound::BookChunkKind;
use crate::time::TsUs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureId(pub u16);

/// Copy enum (output ring = fixed-layout SPSC like input).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PersistRecord {
    Feature(FeatureRow),
    Trade(TradeRow),
    BookEvent(BookEventRow),
    Kline(KlineRow),
    LinkFrame(LinkFrameRow),
    Order(OrderRow),
    Fill(FillRow),
    /// Seal all files (ring-ordered).
    SealAll,
}

impl PersistRecord {
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
            PersistRecord::SealAll => None,
        }
    }
}

// Event timestamp, not wall clock.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeatureRow {
    pub instrument: InstrumentId,
    pub feature: FeatureId,
    pub value: f64,
    pub event_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeRow {
    pub instrument: InstrumentId,
    pub price: Price,
    pub qty: Qty,
    pub side: Side,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

// Applied chunk kinds + reset (not wire).
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

/// One price level or (Reset kind) control event (zero price/qty/update_id).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookEventRow {
    pub instrument: InstrumentId,
    pub kind: BookEventKind,
    pub side: Option<Side>,
    pub price: Price,
    pub qty: Qty,
    pub update_id: u64,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineRow {
    pub instrument: InstrumentId,
    pub interval: KlineInterval,
    pub open_ts_us: TsUs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub base_volume: Qty,
    pub quote_volume: i64,
    pub trade_count: u32,
    pub is_closed: bool,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

labelled_enum! {
    /// LinkFrameRow kinds (run-state in kind, not value, for readable parquet column).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum LinkRowKind {
        Payload = "payload",
        RunRunning = "run_running",
        RunIdle = "run_idle",
    }
    pub fn as_str;
}

/// Link frame/run-state. Teed by dispatch (no actor -> no divergence). Long format (no wide f64).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkFrameRow {
    pub kind: LinkRowKind,
    pub sender_te_hash: u64,
    pub topic: u16,
    pub seq: u64,
    pub slot: u16,
    pub count: u16,
    pub value: f64,
    pub event_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

labelled_enum! {
    /// Transition cause (superset of venue ExecKind; tape missing pre-ack orders loses them).
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
    /// Engine's belief (flat, not nested).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum OrderLifecycle {
        Free = "free",
        PendingNew = "pending_new",
        Live = "live",
        CancelInFlight = "cancel_in_flight",
        AmendInFlight = "amend_in_flight",
        /// Venue truth lost (blocks side until reconciliation).
        Unknown = "unknown",
        ClosedFilled = "closed_filled",
        ClosedCanceled = "closed_canceled",
        ClosedRejected = "closed_rejected",
        ClosedExpired = "closed_expired",
        ClosedReconciledGone = "closed_reconciled_gone",
    }
    pub fn as_str;
}

/// Lifecycle transition (one row/transition). previous_state detects drops via mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRow {
    pub instrument: InstrumentId,
    pub client_id: ClientOrderId,
    pub quote_level: Option<QuoteLevel>,
    // Join key; valuable when absent (sent, never acked).
    pub venue_order_id: Option<VenueOrderId>,
    pub transition: OrderTransition,
    pub state: OrderLifecycle,
    pub previous_state: OrderLifecycle,
    pub provenance: Provenance,
    pub side: Side,
    pub style: Option<OrderStyle>,
    pub price: Price,
    pub qty: Qty,
    // Detects lost reports when paired with last_qty.
    pub filled_qty: Qty,
    pub filled_quote: i64,
    pub reject: Option<RejectClass>,
    pub reject_code: i32,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

/// One fill (line item a statement reconciles against, durable record of quantity/price/fee).
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
    // Paired with last_* detects lost reports.
    pub booked_qty: Qty,
    pub booked_quote: i64,
    // Not quote asset; wrong forever if incorrect.
    pub commission: i64,
    pub commission_asset: AssetId,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

// Venue lineage (side-channel).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationRow {
    pub instrument: InstrumentId,
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
    pub token_id_up: Box<str>,
    pub token_id_down: Box<str>,
    pub condition_id: Box<str>,
    pub received_ts_us: TsUs,
}
