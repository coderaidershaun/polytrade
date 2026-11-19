//! Orders the depth diffs of one Binance instrument into book chunks. A broken chain of update ids
//! resets the book, and it stays reset until a snapshot that the buffered diffs carry on from.

use crate::msg::ids::InstrumentId;
use crate::msg::inbound::{BookChunkKind, BookReset, InboundMessage, MessageBody};
use crate::msg::time::LocalTs;
use crate::venues::edge::chunk::{ChunkEmitter, ChunkPlan};
use crate::warn;

use super::parse::{DepthDiff, DepthSnapshot};

/// How one diff of a market follows the diff before it. The spot market numbers each update, so a
/// diff starts at the id after the last one; the perpetual market names the final id of the diff
/// before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainRule {
    Spot,
    Perp,
}

impl ChainRule {
    /// The spot market has no id after `u64::MAX`, so a book at that id cannot take another diff.
    fn has_successor(self, anchor: u64) -> bool {
        match self {
            ChainRule::Spot => anchor < u64::MAX,
            ChainRule::Perp => true,
        }
    }

    fn chains_onto(self, anchor: u64, diff: &DepthDiff) -> bool {
        match self {
            ChainRule::Spot => anchor
                .checked_add(1)
                .is_some_and(|next| diff.first_update_id == next),
            ChainRule::Perp => diff.prev_final_update_id == Some(anchor),
        }
    }

    /// Whether the first diff after a snapshot reaches past it with no gap. A spot diff can begin
    /// before the snapshot, and a perpetual diff must hold the snapshot's own id.
    fn spans_snapshot(self, snapshot_id: u64, diff: &DepthDiff) -> bool {
        match self {
            ChainRule::Spot => snapshot_id
                .checked_add(1)
                .is_some_and(|next| diff.first_update_id <= next),
            ChainRule::Perp => {
                diff.first_update_id <= snapshot_id && diff.final_update_id >= snapshot_id
            }
        }
    }

    /// Whether a buffered diff is one the snapshot already holds. The perpetual market applies the
    /// diff whose final id equals the snapshot's again, and the spot market drops it.
    fn drops_after_snapshot(self, snapshot_id: u64, diff: &DepthDiff) -> bool {
        match self {
            ChainRule::Spot => diff.final_update_id <= snapshot_id,
            ChainRule::Perp => diff.final_update_id < snapshot_id,
        }
    }

    /// Whether a live diff that does not chain is one the book already holds. Only the spot market
    /// sends such a diff again; on the perpetual market a diff that does not chain breaks the chain.
    fn drops_while_live(self, anchor: u64, diff: &DepthDiff) -> bool {
        match self {
            ChainRule::Spot => diff.final_update_id <= anchor,
            ChainRule::Perp => false,
        }
    }
}

/// `Resync` and `Overflow` ask the caller for a new snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffOutcome {
    Applied,
    /// Spot re-sends events the book already holds. Nothing was emitted and no anchor moved, so
    /// this is deliberately not [`DiffOutcome::Applied`].
    AlreadyApplied,
    Buffered,
    /// The chain broke: the book is reset, and the diff waits in the buffer for a snapshot.
    Resync,
    /// The buffer filled before a snapshot came: it keeps only the newest diff.
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
    /// The final id of the last diff in the book. It means something only while live.
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

    /// A held snapshot was received before the diff that lets it apply, so its chunks must not be
    /// stamped earlier than that diff.
    pub fn note_emit_floor(&mut self, floor_ts_us: LocalTs) {
        self.chunks.note_emit_floor(floor_ts_us);
    }

    /// Emits each message with the venue stamp of the diff that made it.
    pub fn on_diff(
        &mut self,
        diff: DepthDiff,
        emit: &mut impl FnMut(InboundMessage),
    ) -> DiffOutcome {
        match self.state {
            SeqState::Live => self.on_diff_live(diff, emit),
            SeqState::AwaitingSnapshot => self.buffer_diff(diff),
        }
    }

    fn on_diff_live(
        &mut self,
        diff: DepthDiff,
        emit: &mut impl FnMut(InboundMessage),
    ) -> DiffOutcome {
        if !self.rule.has_successor(self.book_update_id) {
            self.begin_resync(diff, emit);
            return DiffOutcome::Resync;
        }
        if self.rule.chains_onto(self.book_update_id, &diff) {
            self.emit_delta(&diff, emit);
            self.book_update_id = diff.final_update_id;
            return DiffOutcome::Applied;
        }
        if self.rule.drops_while_live(self.book_update_id, &diff) {
            return DiffOutcome::AlreadyApplied;
        }
        self.begin_resync(diff, emit);
        DiffOutcome::Resync
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

    fn begin_resync(&mut self, diff: DepthDiff, emit: &mut impl FnMut(InboundMessage)) {
        let received_ts_us = self.chunks.clamp_emit_ts(diff.received_ts_us);
        emit(InboundMessage::new(
            received_ts_us,
            MessageBody::BookReset(BookReset {
                instrument: self.chunks.instrument(),
                exchange_ts_us: diff.exchange_ts_us,
            }),
        ));
        self.state = SeqState::AwaitingSnapshot;
        self.buffer.clear();
        self.overflow_warned = false;
        self.buffer.push(diff);
    }

    /// A snapshot with no level on either side is `Stale`: going live on an empty book would drop
    /// the buffered diffs that it holds. A snapshot with one empty side applies.
    pub fn apply_snapshot(
        &mut self,
        snapshot: DepthSnapshot,
        emit: &mut impl FnMut(InboundMessage),
    ) -> SnapshotOutcome {
        if snapshot.bids.is_empty() && snapshot.asks.is_empty() {
            return SnapshotOutcome::Stale;
        }
        let last_update_id = snapshot.last_update_id;
        let rule = self.rule;
        self.buffer
            .retain(|diff| !rule.drops_after_snapshot(last_update_id, diff));
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
                emit(InboundMessage::new(
                    plan.received_ts_us,
                    MessageBody::Book(chunk),
                ));
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

    /// Whether the buffered diffs carry on from the snapshot with no gap.
    fn buffer_chains_from(&self, last_update_id: u64) -> bool {
        if !self.rule.has_successor(last_update_id) {
            return false;
        }
        let mut anchor = last_update_id;
        for (index, diff) in self.buffer.iter().enumerate() {
            let spans = if index == 0 {
                self.rule.spans_snapshot(last_update_id, diff)
            } else {
                self.rule.chains_onto(anchor, diff)
            };
            if !spans {
                return false;
            }
            anchor = diff.final_update_id;
        }
        true
    }

    fn emit_delta(&mut self, diff: &DepthDiff, emit: &mut impl FnMut(InboundMessage)) {
        let plan = ChunkPlan {
            kind: BookChunkKind::Delta,
            update_id: diff.final_update_id,
            exchange_ts_us: Some(diff.exchange_ts_us),
            received_ts_us: self.chunks.clamp_emit_ts(diff.received_ts_us),
        };
        self.chunks
            .emit_book(plan, &diff.bids, &diff.asks, &mut |chunk| {
                emit(InboundMessage::new(
                    plan.received_ts_us,
                    MessageBody::Book(chunk),
                ));
            });
    }
}
