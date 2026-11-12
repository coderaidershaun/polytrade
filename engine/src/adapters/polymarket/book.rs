//! Emit book/delta → normalized chunks; FSM controls Snapshot + shadow.

use std::borrow::Borrow;

use crate::adapters::chunk::{ChunkEmitter, ChunkPlan};
use crate::ids::{InstrumentId, Side};
use crate::msg::inbound::{BookChunkKind, InboundMessage, Level};
use crate::time::TsUs;

use super::parse::{PolyBook, PolyDelta};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkStamps {
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

/// Per-instrument chunk emitter; `update_id = 0` (no venue seq nums), continuity via snapshots.
#[derive(Debug)]
pub struct BookNormaliser {
    chunks: ChunkEmitter,
}

impl BookNormaliser {
    pub fn new(instrument: InstrumentId) -> Self {
        Self {
            chunks: ChunkEmitter::new(instrument),
        }
    }

    pub fn note_emit_floor(&mut self, floor: TsUs) {
        self.chunks.note_emit_floor(floor);
    }

    pub fn emit_snapshot(&mut self, book: &PolyBook, emit: &mut impl FnMut(InboundMessage)) {
        self.emit_event(
            BookChunkKind::Snapshot,
            ChunkStamps {
                exchange_ts_us: book.exchange_ts_us,
                received_ts_us: book.received_ts_us,
            },
            &book.bids,
            &book.asks,
            emit,
        );
    }

    pub fn emit_price_change(
        &mut self,
        changes: &[impl Borrow<PolyDelta>],
        stamps: ChunkStamps,
        emit: &mut impl FnMut(InboundMessage),
    ) {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for delta in changes {
            let delta = delta.borrow();
            match delta.side {
                Side::Buy => bids.push(delta.level),
                Side::Sell => asks.push(delta.level),
            }
        }
        self.emit_delta(&bids, &asks, stamps, emit);
    }

    pub fn emit_delta(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        stamps: ChunkStamps,
        emit: &mut impl FnMut(InboundMessage),
    ) {
        self.emit_event(BookChunkKind::Delta, stamps, bids, asks, emit);
    }

    fn emit_event(
        &mut self,
        kind: BookChunkKind,
        stamps: ChunkStamps,
        bids: &[Level],
        asks: &[Level],
        emit: &mut impl FnMut(InboundMessage),
    ) {
        let plan = ChunkPlan {
            kind,
            update_id: 0,
            exchange_ts_us: Some(stamps.exchange_ts_us),
            received_ts_us: self.chunks.clamp_emit_ts(stamps.received_ts_us),
        };
        self.chunks.emit_book(plan, bids, asks, &mut |chunk| {
            emit(InboundMessage::Book(chunk));
        });
    }
}
