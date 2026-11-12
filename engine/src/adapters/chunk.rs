//! One cutting rule for every venue's book. A `BookChunk` carries a fixed-width level array, so
//! where a side splits and which chunk closes it are decisions the hot book depends on — they must
//! not be taken twice. Venue metadata never reaches here: the caller's sink attaches it, which is
//! where the continuity evidence is known.

use crate::ids::{InstrumentId, Price, Qty, Side};
use crate::msg::inbound::{BOOK_CHUNK_LEVELS, BookChunk, BookChunkKind, Level};
use crate::time::TsUs;

// A chunk reports its own length in a `u8`, and raising the level count lives in another module.
const _: () = assert!(
    BOOK_CHUNK_LEVELS <= u8::MAX as usize,
    "a chunk's level count no longer fits the byte that reports it"
);

/// Everything a chunk carries beyond its levels. `update_id` is 0 on a venue with no sequence
/// numbers; `exchange_ts_us` is absent when the payload carried no venue stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkPlan {
    pub(crate) kind: BookChunkKind,
    pub(crate) update_id: u64,
    pub(crate) exchange_ts_us: Option<TsUs>,
    pub(crate) received_ts_us: TsUs,
}

/// Per-instrument cutter, holding the stamp floor that keeps a resync from queueing behind itself.
#[derive(Debug)]
pub(crate) struct ChunkEmitter {
    instrument: InstrumentId,
    last_emitted_ts_us: TsUs,
}

impl ChunkEmitter {
    pub(crate) fn new(instrument: InstrumentId) -> Self {
        Self {
            instrument,
            last_emitted_ts_us: TsUs::from_micros(i64::MIN),
        }
    }

    pub(crate) fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub(crate) fn note_emit_floor(&mut self, floor_ts_us: TsUs) {
        self.last_emitted_ts_us = self.last_emitted_ts_us.max(floor_ts_us);
    }

    /// The stamp to put on the next emission, never below one already emitted. Advances the floor,
    /// so the returned value is the only correct one to carry.
    #[must_use]
    pub(crate) fn clamp_emit_ts(&mut self, receipt: TsUs) -> TsUs {
        self.note_emit_floor(receipt);
        self.last_emitted_ts_us
    }

    pub(crate) fn emit_book(
        &self,
        plan: ChunkPlan,
        bids: &[Level],
        asks: &[Level],
        emit: &mut impl FnMut(BookChunk),
    ) {
        if bids.is_empty() && asks.is_empty() {
            // A snapshot replaces the book, so "no levels" is a statement the hot side has to
            // receive: it leaves AwaitingSnapshot only on a terminating chunk, and with none it
            // stays wedged there dropping deltas while the adapter believes it went live. A delta
            // that changes nothing genuinely is nothing, so it stays unemitted.
            if plan.kind == BookChunkKind::Snapshot {
                emit(self.one_chunk(plan, Side::Buy, &[], true));
            }
            return;
        }
        let bid_side_is_last = asks.is_empty();
        self.emit_side(plan, Side::Buy, bids, bid_side_is_last, emit);
        self.emit_side(plan, Side::Sell, asks, true, emit);
    }

    fn emit_side(
        &self,
        plan: ChunkPlan,
        side: Side,
        levels: &[Level],
        side_is_last: bool,
        emit: &mut impl FnMut(BookChunk),
    ) {
        let chunk_count = levels.len().div_ceil(BOOK_CHUNK_LEVELS);
        for (index, group) in levels.chunks(BOOK_CHUNK_LEVELS).enumerate() {
            let is_last_chunk = side_is_last && index + 1 == chunk_count;
            emit(self.one_chunk(plan, side, group, is_last_chunk));
        }
    }

    fn one_chunk(
        &self,
        plan: ChunkPlan,
        side: Side,
        group: &[Level],
        is_last_chunk: bool,
    ) -> BookChunk {
        let mut filled = [Level {
            price: Price(0),
            qty: Qty(0),
        }; BOOK_CHUNK_LEVELS];
        filled[..group.len()].copy_from_slice(group);
        BookChunk {
            instrument: self.instrument,
            kind: plan.kind,
            side,
            levels: filled,
            len: group.len() as u8,
            is_last_chunk,
            update_id: plan.update_id,
            exchange_ts_us: plan.exchange_ts_us,
            received_ts_us: plan.received_ts_us,
            // Restamped by the shell that pushes onto the ring; it alone knows when the message
            // was actually queued, and a chunk that never reaches a ring was never queued.
            queued_ts_us: plan.received_ts_us,
        }
    }
}
