//! Normalised inbound messages: fixed-size POD Copy (only currency into hot path).
//! Book payloads chunk [Level; BOOK_CHUNK_LEVELS]. Every variant carries received_ts_us (uniform key).

use crate::config::KlineInterval;
use crate::ids::{AggregateTradeId, InstrumentId, Price, Qty, RawTradeId, Side, StreamEpoch};
use crate::link::InboundLink;
use crate::msg::exec::{AccountChunk, ExecEvent};
use crate::shutdown::RunAssertion;
use crate::time::TsUs;

pub const BOOK_CHUNK_LEVELS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Level {
    pub price: Price,
    pub qty: Qty,
}

// Large variant by design (heap indirection forbidden).
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InboundMessage {
    Trade(TradeEvent),
    Book(BookChunk),
    BookReset(BookReset),
    MarketRotation(MarketRotation),
    Kline(KlineEvent),
    SpinTick(SpinTick),
    Link(InboundLink),
    RunControl(RunControl),
    Exec(ExecEvent),
    Account(AccountChunk),
}

impl InboundMessage {
    #[inline]
    pub fn received_ts_us(&self) -> TsUs {
        match self {
            InboundMessage::Trade(event) => event.received_ts_us,
            InboundMessage::Book(chunk) => chunk.received_ts_us,
            InboundMessage::BookReset(reset) => reset.received_ts_us,
            InboundMessage::MarketRotation(rotation) => rotation.received_ts_us,
            InboundMessage::Kline(event) => event.received_ts_us,
            InboundMessage::SpinTick(tick) => tick.received_ts_us,
            InboundMessage::Link(link) => link.received_ts_us,
            InboundMessage::RunControl(control) => control.received_ts_us,
            InboundMessage::Exec(event) => event.received_ts_us,
            InboundMessage::Account(chunk) => chunk.received_ts_us,
        }
    }

    #[inline]
    pub fn set_queued_ts_us(&mut self, ts: TsUs) {
        match self {
            InboundMessage::Trade(event) => event.queued_ts_us = ts,
            InboundMessage::Book(chunk) => chunk.queued_ts_us = ts,
            InboundMessage::BookReset(reset) => reset.queued_ts_us = ts,
            InboundMessage::MarketRotation(rotation) => rotation.queued_ts_us = ts,
            InboundMessage::Kline(event) => event.queued_ts_us = ts,
            InboundMessage::SpinTick(tick) => tick.queued_ts_us = ts,
            InboundMessage::Link(link) => link.queued_ts_us = ts,
            InboundMessage::RunControl(control) => control.queued_ts_us = ts,
            InboundMessage::Exec(event) => event.queued_ts_us = ts,
            InboundMessage::Account(chunk) => chunk.queued_ts_us = ts,
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
    pub exchange_ts_us: TsUs,
    /// Venue SEND stamp vs `exchange_ts_us`'s MATCH stamp; `None` when the venue publishes only one.
    pub exchange_sent_ts_us: Option<TsUs>,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
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
    /// Anchor varies by venue (send/transact/publish); `None` for REST. 384-byte budget affords only one field.
    pub exchange_ts_us: Option<TsUs>,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
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
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketRotation {
    pub instrument: InstrumentId,
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineEvent {
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
    /// Candle time: the frame's event time live, the candle's own CLOSE on REST backfill. Hours old
    /// there by design, so it is never a transport measurement — [`Self::exchange_sent_ts_us`] is.
    pub exchange_ts_us: TsUs,
    /// Venue SEND stamp; `None` on REST backfill rows, which carry no send time of their own.
    pub exchange_sent_ts_us: Option<TsUs>,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpinTick {
    pub seq: u64,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

// Run-state marker (level-triggered, dedups on epoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunControl {
    pub desired: RunAssertion,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

const _: () = assert!(size_of::<InboundMessage>() <= 384);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TappedMessage {
    pub message: InboundMessage,
    pub venue_meta: VenueMeta,
}

#[allow(clippy::large_enum_variant)] // Same reason as InboundMessage: heap indirection forbidden.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketTapItem {
    Event(TappedMessage),
    Watermark { received_ts_us: TsUs },
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
        exchange_ts_us: TsUs,
    },
    DepthReset {
        exchange_ts_us: TsUs,
    },
    /// Snapshot chunks and messages without venue sequence data.
    None,
}
