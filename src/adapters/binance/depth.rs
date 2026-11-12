//! State machine; Spot: u seq, Perp: pu seq. Broken -> BookReset. Timestamp clamped to high-water (prevent backward queue).

use crate::adapters::chunk::{ChunkEmitter, ChunkPlan};
use crate::ids::InstrumentId;
use crate::msg::inbound::{BookChunkKind, BookReset, InboundMessage, VenueMeta};
use crate::time::TsUs;
use crate::warn;

use super::parse::{DepthDiff, DepthSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainRule {
    Spot,
    Perp,
}

// Resync/Overflow -> refetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffOutcome {
    Applied,
    /// Spot re-sends events the book already holds. Nothing was emitted and no anchor moved, so
    /// this is deliberately not [`DiffOutcome::Applied`].
    AlreadyApplied,
    Buffered,
    // Chain broke -> BookReset, await snapshot.
    Resync,
    // Buffer full before snapshot -> dropped, await.
    Overflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOutcome {
    Applied,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeqState {
    AwaitingSnapshot,
    Live,
}

pub struct DepthSequencer {
    rule: ChainRule,
    state: SeqState,
    buffer: Vec<DepthDiff>,
    capacity: usize,
    // Chain anchor; meaningful in Live only.
    book_update_id: u64,
    chunks: ChunkEmitter,
    overflow_warned: bool,
}

impl DepthSequencer {
    pub fn new(rule: ChainRule, instrument: InstrumentId, buffer_capacity: usize) -> Self {
        Self {
            rule,
            state: SeqState::AwaitingSnapshot,
            buffer: Vec::with_capacity(buffer_capacity),
            capacity: buffer_capacity,
            book_update_id: 0,
            chunks: ChunkEmitter::new(instrument),
            overflow_warned: false,
        }
    }

    pub fn is_live(&self) -> bool {
        self.state == SeqState::Live
    }

    // Prevent resync undercut.
    pub fn note_emit_floor(&mut self, floor_ts_us: TsUs) {
        self.chunks.note_emit_floor(floor_ts_us);
    }

    /// Emits each message with its originating venue metadata.
    pub fn on_diff(
        &mut self,
        diff: DepthDiff,
        emit: &mut impl FnMut(InboundMessage, VenueMeta),
    ) -> DiffOutcome {
        match self.state {
            SeqState::Live => self.on_diff_live(diff, emit),
            SeqState::AwaitingSnapshot => self.buffer_diff(diff),
        }
    }

    fn on_diff_live(
        &mut self,
        diff: DepthDiff,
        emit: &mut impl FnMut(InboundMessage, VenueMeta),
    ) -> DiffOutcome {
        if self.rule == ChainRule::Spot && self.book_update_id == u64::MAX {
            self.begin_resync(diff, emit);
            return DiffOutcome::Resync;
        }
        if self.chains_onto(self.book_update_id, &diff) {
            self.emit_delta(&diff, emit);
            self.book_update_id = diff.final_update_id;
            return DiffOutcome::Applied;
        }
        if self.rule == ChainRule::Spot && diff.final_update_id <= self.book_update_id {
            return DiffOutcome::AlreadyApplied;
        }
        self.begin_resync(diff, emit);
        DiffOutcome::Resync
    }

    fn chains_onto(&self, anchor: u64, diff: &DepthDiff) -> bool {
        match self.rule {
            ChainRule::Spot => anchor
                .checked_add(1)
                .is_some_and(|next| diff.first_update_id == next),
            ChainRule::Perp => diff.prev_final_update_id == Some(anchor),
        }
    }

    fn buffer_diff(&mut self, diff: DepthDiff) -> DiffOutcome {
        if self.buffer.len() >= self.capacity {
            if !self.overflow_warned {
                warn!(
                    "binance depth buffer overflow ({} deltas) instrument {} — resyncing",
                    self.capacity,
                    self.chunks.instrument().0
                );
                self.overflow_warned = true;
            }
            self.buffer.clear();
            self.buffer.push(diff);
            return DiffOutcome::Overflow;
        }
        self.buffer.push(diff);
        DiffOutcome::Buffered
    }

    fn begin_resync(&mut self, diff: DepthDiff, emit: &mut impl FnMut(InboundMessage, VenueMeta)) {
        let received_ts_us = self.chunks.clamp_emit_ts(diff.received_ts_us);
        emit(
            InboundMessage::BookReset(BookReset {
                instrument: self.chunks.instrument(),
                received_ts_us,
                queued_ts_us: received_ts_us,
            }),
            VenueMeta::DepthReset {
                exchange_ts_us: diff.exchange_ts_us,
            },
        );
        self.state = SeqState::AwaitingSnapshot;
        self.buffer.clear();
        self.overflow_warned = false;
        self.buffer.push(diff);
    }

    // Both-sides-empty -> Stale (would flip live with empty book, drop deltas). One-sided OK.
    pub fn apply_snapshot(
        &mut self,
        snapshot: DepthSnapshot,
        emit: &mut impl FnMut(InboundMessage, VenueMeta),
    ) -> SnapshotOutcome {
        if snapshot.bids.is_empty() && snapshot.asks.is_empty() {
            return SnapshotOutcome::Stale;
        }
        let last_update_id = snapshot.last_update_id;
        match self.rule {
            ChainRule::Spot => self
                .buffer
                .retain(|diff| diff.final_update_id > last_update_id),
            ChainRule::Perp => self
                .buffer
                .retain(|diff| diff.final_update_id >= last_update_id),
        }
        if !self.buffer_chains_from(last_update_id) {
            return SnapshotOutcome::Stale;
        }

        let plan = ChunkPlan {
            kind: BookChunkKind::Snapshot,
            update_id: last_update_id,
            // REST snapshots carry no venue stamp.
            exchange_ts_us: None,
            received_ts_us: self.chunks.clamp_emit_ts(snapshot.received_ts_us),
        };
        // A snapshot replaces the book, so it chains onto nothing.
        self.chunks
            .emit_book(plan, &snapshot.bids, &snapshot.asks, &mut |chunk| {
                emit(InboundMessage::Book(chunk), VenueMeta::None);
            });
        self.book_update_id = last_update_id;
        let buffered: Vec<DepthDiff> = self.buffer.drain(..).collect();
        for diff in &buffered {
            self.emit_delta(diff, emit);
            self.book_update_id = diff.final_update_id;
        }
        self.state = SeqState::Live;
        self.overflow_warned = false;
        SnapshotOutcome::Applied
    }

    // Chains unbroken.
    fn buffer_chains_from(&self, last_update_id: u64) -> bool {
        if self.rule == ChainRule::Spot && last_update_id == u64::MAX {
            return false;
        }
        let mut anchor = last_update_id;
        for (index, diff) in self.buffer.iter().enumerate() {
            let spans = if index == 0 {
                match self.rule {
                    // Spot: u>last; no past seam.
                    ChainRule::Spot => last_update_id
                        .checked_add(1)
                        .is_some_and(|next| diff.first_update_id <= next),
                    ChainRule::Perp => {
                        diff.first_update_id <= last_update_id
                            && diff.final_update_id >= last_update_id
                    }
                }
            } else {
                self.chains_onto(anchor, diff)
            };
            if !spans {
                return false;
            }
            anchor = diff.final_update_id;
        }
        true
    }

    fn emit_delta(&mut self, diff: &DepthDiff, emit: &mut impl FnMut(InboundMessage, VenueMeta)) {
        let plan = ChunkPlan {
            kind: BookChunkKind::Delta,
            update_id: diff.final_update_id,
            exchange_ts_us: Some(diff.exchange_ts_us),
            received_ts_us: self.chunks.clamp_emit_ts(diff.received_ts_us),
        };
        let venue_meta = VenueMeta::DepthDelta {
            exchange_ts_us: diff.exchange_ts_us,
        };
        self.chunks
            .emit_book(plan, &diff.bids, &diff.asks, &mut |chunk| {
                emit(InboundMessage::Book(chunk), venue_meta);
            });
    }
}
