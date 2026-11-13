//! Normalised inbound messages, the only data into the hot path: a header of the two local stamps,
//! so each kind is ordered by one received stamp, and a fixed-size body of one kind.

// SMELL: the plain message data imports the link and the runtime config here, and other message
// files import the hot thread, its sinks, and the runtime registry, so the vocabulary depends on
// the code it should sit beneath. These types need a home that imports nothing above it.
use crate::link::LinkFrame;
use crate::msg::exec::{AccountChunk, ExecEvent};
use crate::msg::ids::{AggregateTradeId, InstrumentId, Price, Qty, RawTradeId, Side, StreamEpoch};
use crate::msg::run_state::RunAssertion;
use crate::msg::time::{LocalTs, VenueTs};
use crate::runtime::config::KlineInterval;

pub const BOOK_CHUNK_LEVELS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Level {
    pub price: Price,
    pub qty: Qty,
}

/// The two stamps this process takes of every message. The received stamp is the key that orders
/// the fold across every input queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageHeader {
    pub received_ts_us: LocalTs,
    /// Set by the producer as it pushes the message onto its input queue.
    pub queued_ts_us: LocalTs,
}

/// Aligned to a cache line, so each slot of an input ring starts a line and no two slots share one.
#[repr(align(64))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InboundMessage {
    pub header: MessageHeader,
    pub body: MessageBody,
}

// Each slot of an input ring is this size. The link frame and the book chunk fill the body, so a
// field added to either makes the message 448 bytes.
const _: () = assert!(size_of::<InboundMessage>() == 384 && align_of::<InboundMessage>() == 64);

// Large variant by design (heap indirection forbidden).
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageBody {
    Trade(TradeEvent),
    Book(BookChunk),
    BookReset(BookReset),
    MarketRotation(MarketRotation),
    Kline(KlineEvent),
    SpinTick(SpinTick),
    Link(LinkFrame),
    RunControl(RunControl),
    Exec(ExecEvent),
    Account(AccountChunk),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageKind {
    Trade,
    Book,
    BookReset,
    MarketRotation,
    Kline,
    SpinTick,
    Link,
    RunControl,
    Exec,
    Account,
}

impl InboundMessage {
    /// The queued stamp starts at the received stamp, and the producer sets it again as it pushes.
    #[inline]
    pub fn new(received_ts_us: LocalTs, body: MessageBody) -> Self {
        Self {
            header: MessageHeader {
                received_ts_us,
                queued_ts_us: received_ts_us,
            },
            body,
        }
    }

    #[inline]
    pub fn kind(&self) -> MessageKind {
        match self.body {
            MessageBody::Trade(_) => MessageKind::Trade,
            MessageBody::Book(_) => MessageKind::Book,
            MessageBody::BookReset(_) => MessageKind::BookReset,
            MessageBody::MarketRotation(_) => MessageKind::MarketRotation,
            MessageBody::Kline(_) => MessageKind::Kline,
            MessageBody::SpinTick(_) => MessageKind::SpinTick,
            MessageBody::Link(_) => MessageKind::Link,
            MessageBody::RunControl(_) => MessageKind::RunControl,
            MessageBody::Exec(_) => MessageKind::Exec,
            MessageBody::Account(_) => MessageKind::Account,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookChunkKind {
    Delta,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeEvent {
    pub instrument: InstrumentId,
    pub price: Price,
    pub qty: Qty,
    pub side: Side,
    pub exchange_ts_us: VenueTs,
    /// Venue SEND stamp vs `exchange_ts_us`'s MATCH stamp; `None` when the venue publishes only one.
    pub exchange_sent_ts_us: Option<VenueTs>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookChunk {
    pub instrument: InstrumentId,
    pub kind: BookChunkKind,
    pub side: Side,
    pub levels: [Level; BOOK_CHUNK_LEVELS],
    pub len: u8,
    pub is_last_chunk: bool,
    pub update_id: u64,
    /// The moment this stamps differs by venue: the send, the transaction, or the publication. It is
    /// `None` for a REST snapshot. The message has room for only one such field.
    pub exchange_ts_us: Option<VenueTs>,
}

impl BookChunk {
    #[inline]
    pub fn active_levels(&self) -> &[Level] {
        debug_assert!(
            self.len as usize <= BOOK_CHUNK_LEVELS,
            "chunk len {} exceeds capacity {BOOK_CHUNK_LEVELS}",
            self.len
        );
        &self.levels[..self.len as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookReset {
    pub instrument: InstrumentId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketRotation {
    pub instrument: InstrumentId,
    pub window_open_ts_us: VenueTs,
    pub window_close_ts_us: VenueTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineEvent {
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
    /// Candle time: the frame's event time live, the candle's own CLOSE on REST backfill. Hours old
    /// there by design, so it is never a transport measurement — [`Self::exchange_sent_ts_us`] is.
    pub exchange_ts_us: VenueTs,
    /// Venue SEND stamp; `None` on REST backfill rows, which carry no send time of their own.
    pub exchange_sent_ts_us: Option<VenueTs>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpinTick {
    pub seq: u64,
}

// Run-state marker (level-triggered, dedups on epoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunControl {
    pub desired: RunAssertion,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TappedMessage {
    pub message: InboundMessage,
    pub venue_meta: VenueMeta,
}

#[allow(clippy::large_enum_variant)] // Same reason as MessageBody: heap indirection forbidden.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketTapItem {
    Event(TappedMessage),
    Watermark { received_ts_us: LocalTs },
}

/// Venue continuity metadata used only by the simulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueMeta {
    Trade {
        aggregate_id: AggregateTradeId,
        first_trade_id: RawTradeId,
        last_trade_id: RawTradeId,
        stream_epoch: StreamEpoch,
    },
    DepthDelta {
        exchange_ts_us: VenueTs,
    },
    DepthReset {
        exchange_ts_us: VenueTs,
    },
    /// Snapshot chunks and messages without venue sequence data.
    None,
}
