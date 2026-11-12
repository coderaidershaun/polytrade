//! Per-instrument book: two sorted vecs (bids desc, asks asc), fixed capacity. Adapter sequencing; book counts/warns quirks.

use std::cmp::Ordering;

use crate::ids::{Price, Qty, Side};
use crate::msg::inbound::{BookChunk, Level};
use crate::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookState {
    AwaitingSnapshot,
    Valid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookVoice {
    Warn,
    Silent,
}

/// `BeyondDepth` distinguishes an empty level from an unobserved one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LevelState {
    Present(Qty),
    Absent,
    BeyondDepth,
}

/// Snapshot outcome (ImplicitReset -> book-derived state must reset too).
#[must_use = "an implicit reset must propagate to book-derived state"]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnapshotOutcome {
    Clean,
    ImplicitReset,
}

/// Which way a side's prices run: bids hold the highest first, asks the lowest. One comparator
/// serves both the binary search and the sort-invariant check, so the two cannot drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SortOrder {
    Descending,
    Ascending,
}

impl SortOrder {
    /// `earlier` against `later` as the side stores them: `Less` when they are in order.
    #[inline]
    fn compare(self, earlier: Price, later: Price) -> Ordering {
        match self {
            SortOrder::Descending => later.cmp(&earlier),
            SortOrder::Ascending => earlier.cmp(&later),
        }
    }
}

/// A single side's sorted levels, best first.
#[derive(Debug)]
struct BookSide {
    levels: Vec<Level>,
    capacity: usize,
    order: SortOrder,
    is_trim_warned: bool,
}

enum ApplyOutcome {
    Applied,
    RemoveMissing,
    Trimmed { first_of_episode: bool },
}

#[derive(Default)]
struct ApplyStats {
    trimmed: u64,
    remove_missing: u64,
    opened_trim_episode: bool,
}

impl BookSide {
    fn bids(capacity: usize) -> Self {
        Self::new(capacity, SortOrder::Descending)
    }

    fn asks(capacity: usize) -> Self {
        Self::new(capacity, SortOrder::Ascending)
    }

    fn new(capacity: usize, order: SortOrder) -> Self {
        Self {
            levels: Vec::with_capacity(capacity),
            capacity,
            order,
            is_trim_warned: false,
        }
    }

    #[inline]
    fn best(&self) -> Option<Level> {
        self.levels.first().copied()
    }

    #[inline]
    fn as_slice(&self) -> &[Level] {
        &self.levels
    }

    fn clear(&mut self) {
        self.levels.clear();
        self.is_trim_warned = false;
    }

    fn search(&self, price: Price) -> Result<usize, usize> {
        self.levels
            .binary_search_by(|probe| self.order.compare(probe.price, price))
    }

    fn level_state(&self, price: Price) -> LevelState {
        match self.search(price) {
            Ok(index) => LevelState::Present(self.levels[index].qty),
            Err(index) if index == self.levels.len() => LevelState::BeyondDepth,
            Err(_) => LevelState::Absent,
        }
    }

    fn apply(&mut self, level: Level) -> ApplyOutcome {
        match self.search(level.price) {
            Ok(index) => {
                if level.qty.0 == 0 {
                    self.levels.remove(index);
                    self.is_trim_warned = false;
                } else {
                    self.levels[index].qty = level.qty;
                }
                ApplyOutcome::Applied
            }
            Err(index) => {
                if level.qty.0 == 0 {
                    ApplyOutcome::RemoveMissing
                } else {
                    self.insert_new(index, level)
                }
            }
        }
    }

    fn insert_new(&mut self, index: usize, level: Level) -> ApplyOutcome {
        if self.levels.len() < self.capacity {
            self.levels.insert(index, level);
            return ApplyOutcome::Applied;
        }
        self.insert_at_capacity(index, level)
    }

    /// At capacity: deeper levels ignored, worst dropped to fit (no realloc).
    #[cold]
    fn insert_at_capacity(&mut self, index: usize, level: Level) -> ApplyOutcome {
        if index < self.capacity {
            self.levels.remove(self.capacity - 1);
            self.levels.insert(index, level);
        }
        let first_of_episode = !self.is_trim_warned;
        self.is_trim_warned = true;
        ApplyOutcome::Trimmed { first_of_episode }
    }

    fn apply_levels(&mut self, levels: &[Level]) -> ApplyStats {
        let mut stats = ApplyStats::default();
        for &level in levels {
            match self.apply(level) {
                ApplyOutcome::Applied => {}
                ApplyOutcome::RemoveMissing => stats.remove_missing += 1,
                ApplyOutcome::Trimmed { first_of_episode } => {
                    stats.trimmed += 1;
                    stats.opened_trim_episode |= first_of_episode;
                }
            }
        }
        stats
    }

    fn is_strictly_sorted(&self) -> bool {
        self.levels
            .windows(2)
            .all(|pair| self.order.compare(pair[0].price, pair[1].price) == Ordering::Less)
    }
}

#[derive(Debug)]
pub struct Book {
    bids: BookSide,
    asks: BookSide,
    state: BookState,
    voice: BookVoice,
    capacity_per_side: usize,
    trimmed_count: u64,
    locked_count: u64,
    crossed_count: u64,
    remove_missing_count: u64,
    is_crossed_warned: bool,
}

impl Book {
    /// Preallocate both sides (panics if capacity_per_side == 0).
    pub fn new(capacity_per_side: usize) -> Self {
        Self::with_voice(capacity_per_side, BookVoice::Warn)
    }

    pub fn new_silent(capacity_per_side: usize) -> Self {
        Self::with_voice(capacity_per_side, BookVoice::Silent)
    }

    fn with_voice(capacity_per_side: usize, voice: BookVoice) -> Self {
        assert!(
            capacity_per_side != 0,
            "book capacity_per_side must be non-zero"
        );
        Self {
            bids: BookSide::bids(capacity_per_side),
            asks: BookSide::asks(capacity_per_side),
            state: BookState::AwaitingSnapshot,
            voice,
            capacity_per_side,
            trimmed_count: 0,
            locked_count: 0,
            crossed_count: 0,
            remove_missing_count: 0,
            is_crossed_warned: false,
        }
    }

    pub fn level_state(&self, side: Side, price: Price) -> LevelState {
        match side {
            Side::Buy => self.bids.level_state(price),
            Side::Sell => self.asks.level_state(price),
        }
    }

    #[inline]
    fn is_silent(&self) -> bool {
        self.voice == BookVoice::Silent
    }

    /// Reset to AwaitingSnapshot (fires on loss/breach only).
    #[cold]
    pub fn apply_reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.state = BookState::AwaitingSnapshot;
        self.is_crossed_warned = false;
    }

    /// Apply snapshot chunk (last chunk -> Valid). Snapshot on valid = breach; implicit reset reported.
    pub fn apply_snapshot_chunk(&mut self, chunk: &BookChunk) -> SnapshotOutcome {
        let mut outcome = SnapshotOutcome::Clean;
        if self.state == BookState::Valid {
            self.warn_implicit_reset(chunk);
            self.apply_reset();
            outcome = SnapshotOutcome::ImplicitReset;
        }
        self.apply_levels(chunk);
        if chunk.is_last_chunk {
            self.state = BookState::Valid;
            self.check_crossed(chunk);
        }
        outcome
    }

    /// Apply delta chunk (qty==0 removes); pre-snapshot deltas dropped; crossed check only on last chunk.
    pub fn apply_delta_chunk(&mut self, chunk: &BookChunk) {
        if self.state == BookState::AwaitingSnapshot {
            self.warn_delta_before_snapshot(chunk);
            return;
        }
        self.apply_levels(chunk);
        if chunk.is_last_chunk {
            self.check_crossed(chunk);
        }
    }

    #[cold]
    fn warn_implicit_reset(&self, chunk: &BookChunk) {
        if self.is_silent() {
            return;
        }
        warn!(
            "snapshot chunk while valid: instrument {} implicitly reset (adapter sequencing breach)",
            chunk.instrument.0
        );
    }

    #[cold]
    fn warn_delta_before_snapshot(&self, chunk: &BookChunk) {
        if self.is_silent() {
            return;
        }
        warn!(
            "delta chunk dropped: instrument {} awaiting snapshot (adapter sequencing breach)",
            chunk.instrument.0
        );
    }

    #[inline]
    pub fn state(&self) -> BookState {
        self.state
    }

    #[inline]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.best()
    }

    #[inline]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.best()
    }

    /// Midpoint (stats only, None if incomplete book).
    #[inline]
    pub fn mid(&self) -> Option<f64> {
        self.best_bid()
            .zip(self.best_ask())
            .map(|(bid, ask)| (bid.price.to_f64() + ask.price.to_f64()) / 2.0)
    }

    #[inline]
    pub fn bids(&self) -> &[Level] {
        self.bids.as_slice()
    }

    #[inline]
    pub fn asks(&self) -> &[Level] {
        self.asks.as_slice()
    }

    /// Levels dropped: side at capacity.
    pub fn trimmed_count(&self) -> u64 {
        self.trimmed_count
    }

    /// Times best bid==ask after apply (benign, counted silently).
    pub fn locked_count(&self) -> u64 {
        self.locked_count
    }

    /// Times best bid>ask after apply (venue quirk, counted not rejected; distinct from locked).
    pub fn crossed_count(&self) -> u64 {
        self.crossed_count
    }

    /// No-op remove attempts (price not found).
    pub fn remove_missing_count(&self) -> u64 {
        self.remove_missing_count
    }

    fn apply_levels(&mut self, chunk: &BookChunk) {
        let side_name = match chunk.side {
            Side::Buy => "bid",
            Side::Sell => "ask",
        };
        let side = match chunk.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let stats = side.apply_levels(chunk.active_levels());
        debug_assert!(
            side.is_strictly_sorted(),
            "{side_name} side lost its sort invariant"
        );
        self.trimmed_count += stats.trimmed;
        self.remove_missing_count += stats.remove_missing;
        if stats.opened_trim_episode {
            self.warn_trim_episode(chunk, side_name);
        }
    }

    #[cold]
    fn warn_trim_episode(&self, chunk: &BookChunk, side_name: &str) {
        if self.is_silent() {
            return;
        }
        warn!(
            "book instrument {} {side_name} side at capacity {} — dropping deepest levels",
            chunk.instrument.0, self.capacity_per_side
        );
    }

    /// Classify top: locked = count only, crossed = count + warn/episode.
    fn check_crossed(&mut self, chunk: &BookChunk) {
        let (Some(bid), Some(ask)) = (self.bids.best(), self.asks.best()) else {
            self.is_crossed_warned = false;
            return;
        };
        if bid.price > ask.price {
            self.crossed_count += 1;
            if !self.is_crossed_warned {
                self.is_crossed_warned = true;
                self.warn_crossed(chunk, bid.price, ask.price);
            }
        } else if bid.price == ask.price {
            self.locked_count += 1;
        } else {
            self.is_crossed_warned = false;
        }
    }

    #[cold]
    fn warn_crossed(&self, chunk: &BookChunk, bid: Price, ask: Price) {
        if self.is_silent() {
            return;
        }
        warn!(
            "crossed book: instrument {} best bid {} > best ask {}",
            chunk.instrument.0, bid.0, ask.0
        );
    }
}
