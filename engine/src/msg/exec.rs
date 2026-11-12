//! The execution vocabulary: order-lifecycle events, inbound account balances, and outbound
//! commands. All fixed-size plain data. Venue-neutral, since the edge normalizes wire
//! spellings before anything reaches here. Absence is spelled two ways: an identity or
//! classification that may be missing is an `Option`; a missing number is zero.

use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side, TradeId, VenueOrderId};
use crate::labelled_enum::labelled_enum;
use crate::time::TsUs;

/// A normalised order-lifecycle transition. Stream-lifecycle kinds carry only their kind
/// and their stamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecEvent {
    /// A configured instrument; the edge drops any symbol that isn't configured. Balances
    /// instead carry an UNKNOWN asset when this happens.
    pub instrument: InstrumentId,
    pub client_id: ClientOrderId,
    pub venue_order_id: Option<VenueOrderId>,
    pub trade_id: Option<TradeId>,
    pub kind: ExecKind,
    pub status: Option<VenueOrderStatus>,
    pub reject: Option<RejectClass>,
    pub provenance: Provenance,
    pub side: Side,
    pub liquidity: Option<Liquidity>,
    pub price: Price,
    pub qty: Qty,
    /// This fill alone.
    pub last_price: Price,
    pub last_qty: Qty,
    /// The venue's absolute cumulative total, never a delta, so a dropped event can never
    /// corrupt the ledger.
    pub cumulative_qty: Qty,
    pub cumulative_quote: i64,
    /// The fee for `last_qty`, as a 1e-8 mantissa, negative for a rebate. The asset is UNKNOWN when
    /// this configured instrument is not charged a fee at all.
    pub commission: i64,
    pub commission_asset: AssetId,
    /// The venue's own error code, kept for the tape and for operators; the hot path
    /// branches on `reject`, never on this raw code.
    pub reject_code: i32,
    /// The amend budget remaining: 0 means exhausted, and `u8::MAX` means unknown, since
    /// Binance publishes no count.
    pub amends_remaining: u8,
    /// The reconciler pass this event belongs to; zero means it arrived on the stream
    /// instead. Lets the fold discard answers that have gone stale.
    pub recon_seq: u64,
    /// The venue's own transaction time, never interchanged with a local stamp.
    pub exchange_ts_us: TsUs,
    /// This process's wire-send stamp for the request this event answers. `None` for a
    /// stream event, a plain REST read, or a synthesised event, since none of those answer
    /// a timed request.
    pub request_sent_ts_us: Option<TsUs>,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

impl ExecEvent {
    /// No amend claim was made, because the venue publishes no count. Set to the max value
    /// because zero is itself meaningful — it means the budget is spent.
    pub const AMENDS_UNKNOWN: u8 = u8::MAX;

    /// The venue's strongest claim: this order gets no more amends. Named beside its counterpart so
    /// neither half of the encoding can be written as a bare literal somewhere a change would miss.
    pub const AMENDS_EXHAUSTED: u8 = 0;
}

/// An `Ack*` variant answers a request this engine made; a `Report*` variant arrives
/// unsolicited on the stream; everything else covers reconciliation and stream lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecKind {
    AckPlaced,
    /// The edge refused this placement before it was ever sent. Closes the pending slot
    /// and releases its reservation; this is not a venue rejection.
    PlaceNotSent,
    /// Edge refused an amend before send; returns the slot to resting at its unchanged size. Its
    /// counterpart above CLOSES a slot because a placement that never left never existed; an amend
    /// that never left leaves an order still working on the venue.
    AmendNotSent,
    AckCanceled,
    AckAmended,
    AckFailed,
    ReportNew,
    ReportTrade,
    ReportCanceled,
    ReportExpired,
    ReportRejected,
    ReportAmended,
    SnapshotOrder,
    /// The last SnapshotOrder of a reconciliation pass has been delivered; any live order
    /// the pass never named is GONE.
    SnapshotEnd,
    /// The account stream dropped; every order is stale until a reconciliation pass lands.
    StreamReset,
    StreamReady,
}

labelled_enum! {
    /// The rejection classification for state; the hot path branches on nothing else.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum RejectClass {
        StillLive = "still_live",
        Refused = "refused",
        Gone = "gone",
        /// State is indeterminate: Binance's -2011 "unknown order" can mean FILLED, so it
        /// can never be treated as Gone.
        Ambiguous = "ambiguous",
        Fatal = "fatal",
    }
    pub fn as_str;
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Provenance {
        Mine = "mine",
        /// From a prior run; swept at startup.
        PriorRun = "prior_run",
        /// Another trader's order; this engine never cancels it.
        Foreign = "foreign",
    }
    pub fn as_str;
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Liquidity {
        Maker = "maker",
        Taker = "taker",
    }
    pub fn as_str;
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum OrderStyle {
        PostOnly = "post_only",
        Immediate = "immediate",
    }
    pub fn as_str;
}

/// A normalised venue order status; the edge rejects any spelling it does not recognise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueOrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    PendingCancel,
    Rejected,
    Expired,
    /// A self-trade-prevention kill, distinct from a plain Expired: the account crossed
    /// itself, which is a strategy bug.
    ExpiredInMatch,
}

impl VenueOrderStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            VenueOrderStatus::Filled
                | VenueOrderStatus::Canceled
                | VenueOrderStatus::Rejected
                | VenueOrderStatus::Expired
                | VenueOrderStatus::ExpiredInMatch
        )
    }
}

/// Sized so [`AccountChunk`] stays far inside the `InboundMessage` byte budget.
pub const ACCOUNT_CHUNK_ASSETS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssetBalance {
    pub asset: AssetId,
    pub free: i64,
    pub locked: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountChunkKind {
    Snapshot,
    Update,
}

/// An account balance chunk, carrying absolute totals. Binance's stream sends deltas
/// instead, so this is filled from a REST snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccountChunk {
    pub kind: AccountChunkKind,
    pub balances: [AssetBalance; ACCOUNT_CHUNK_ASSETS],
    pub len: u8,
    pub is_last_chunk: bool,
    /// The venue's own stamp on the evidence that these balances moved, in whole milliseconds —
    /// never a sequence, an id, or a reading of a local clock. The hot side holds a balance
    /// reservation until this passes the value it read when the reservation was taken, so it must
    /// be monotone and must advance only when money actually moved. A venue that publishes an
    /// account-update clock supplies it directly; one that does not stamps whatever settlement its
    /// balances follow, and stays at zero until the first such evidence lands. Holding a
    /// reservation for want of evidence is the safe direction — a local clock advances on every
    /// read and frees one before the money moved.
    pub venue_update_ts_ms: u64,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

impl AccountChunk {
    /// The filled prefix of the balances array; the `len` invariant is asserted here.
    #[inline]
    pub fn active_balances(&self) -> &[AssetBalance] {
        debug_assert!(
            self.len as usize <= ACCOUNT_CHUNK_ASSETS,
            "chunk len {} exceeds capacity {ACCOUNT_CHUNK_ASSETS}",
            self.len
        );
        &self.balances[..self.len as usize]
    }
}

/// Commands from the hot thread to an edge; these are intents, not venue methods. There is
/// no cancel-all variant, since it could not distinguish a Foreign order from this engine's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecCommand {
    Place {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        side: Side,
        price: Price,
        qty: Qty,
        style: OrderStyle,
    },
    Cancel {
        instrument: InstrumentId,
        client_id: ClientOrderId,
    },
    /// Reduces a resting order's quantity. Shrink only; growing an order requires a cancel
    /// followed by a place.
    AmendQty {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        qty: Qty,
    },
    /// Queries one order's state; used to answer an Ambiguous reject.
    ReconcileOrder {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        recon_seq: u64,
    },
    /// Queries every venue order on an instrument; answered by a SnapshotOrder run
    /// terminated by a SnapshotEnd.
    ReconcileOpenOrders {
        instrument: InstrumentId,
        recon_seq: u64,
    },
    /// Cancels every order this engine owns. The reason is recorded for the tape; the edge
    /// itself ignores it.
    CancelOurs {
        instrument: InstrumentId,
        reason: CancelReason,
    },
    /// Cancel prior-run orders.
    CancelPriorRun { instrument: InstrumentId },
}

impl ExecCommand {
    /// Zero for every command that opens no reconcile pass — the same value a stream-born event
    /// carries.
    pub fn recon_seq(&self) -> u64 {
        match *self {
            ExecCommand::ReconcileOrder { recon_seq, .. }
            | ExecCommand::ReconcileOpenOrders { recon_seq, .. } => recon_seq,
            _ => 0,
        }
    }
}

/// Stamped when the hot bank drains for deterministic replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StampedExecCommand {
    pub command: ExecCommand,
    pub issued_ts_us: TsUs,
}

/// Transport envelope carrying lane progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecLaneItem {
    Command(StampedExecCommand),
    Watermark(TsUs),
}

/// Why an exit is cancelling. Every exit path must cancel, since there is no bound on
/// what an order outliving this engine could cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelReason {
    Startup,
    Shutdown,
    Park,
    Fatal,
    /// The venue disconnected long enough that resting orders can no longer be trusted.
    Disconnect,
    /// A gate stopped trading, from a reject streak or a loss threshold.
    Halt,
}

impl CancelReason {
    /// Whether the sweep this reason opens is the run's last: the execution edge settles for good
    /// instead of re-arming, and its exit plan finalises instead of retrying.
    pub const fn is_terminal(self) -> bool {
        matches!(self, CancelReason::Shutdown | CancelReason::Fatal)
    }
}
