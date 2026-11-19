//! Market continuity, recovery, and matching.

use std::cmp::Ordering;

use super::queue::{OwnFill, PriceLadder, PublicPolicy, QueueAhead, SimOrderIndex};
use crate::hot::book::{Book, LevelState, SnapshotOutcome};
use crate::msg::ids::{AggregateTradeId, InstrumentId, Price, Qty, RawTradeId, Side, StreamEpoch};
use crate::msg::inbound::{BookChunk, BookChunkKind, TradeEvent};
use crate::msg::time::VenueTs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResetReason {
    BookReset,
    TradeGap,
    TradeRangeOverlap,
    InvertedTradeRange,
    DeltaTransactionBroken,
    VenueParked,
}

/// The prints that walked no queue, by the reason each was set aside. With the prints that walked,
/// they add up to each print that reached the venue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct UnmatchedPrints {
    /// Matching was suspended, or the ids of the print broke continuity and suspended it.
    pub suspended: u64,
    /// The venue stamp of the print is at or before the last depth commit.
    pub before_last_commit: u64,
    /// The ids of the print are ones that its stream epoch has passed already.
    pub stale_id: u64,
    /// The first print of a stream epoch, which only seeds its ids.
    pub epoch_seed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchingState {
    Live,
    Suspended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketRecovery {
    pub generation: u64,
    pub snapshot_complete: bool,
    pub bridging_delta: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradeEvidence {
    pub aggregate_id: AggregateTradeId,
    pub first_trade_id: RawTradeId,
    pub last_trade_id: RawTradeId,
    pub stream_epoch: StreamEpoch,
}

impl TradeEvidence {
    pub fn of(trade: &TradeEvent) -> Self {
        Self {
            aggregate_id: trade.aggregate_id,
            first_trade_id: trade.first_trade_id,
            last_trade_id: trade.last_trade_id,
            stream_epoch: trade.stream_epoch,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContinuityVerdict {
    Accept,
    Ignore,
    Seed,
    Reset(ResetReason),
}

#[derive(Debug, Clone, Copy)]
struct TradeContinuity {
    epoch: Option<StreamEpoch>,
    high_water_aggregate_id: AggregateTradeId,
    high_water_last_trade_id: RawTradeId,
}

impl TradeContinuity {
    fn new() -> Self {
        Self {
            epoch: None,
            high_water_aggregate_id: AggregateTradeId(0),
            high_water_last_trade_id: RawTradeId(0),
        }
    }

    fn admit(&mut self, evidence: TradeEvidence) -> ContinuityVerdict {
        if evidence.first_trade_id > evidence.last_trade_id {
            return ContinuityVerdict::Reset(ResetReason::InvertedTradeRange);
        }
        let is_new_epoch = self.epoch != Some(evidence.stream_epoch);
        let is_stale = evidence.aggregate_id <= self.high_water_aggregate_id
            || evidence.last_trade_id <= self.high_water_last_trade_id;
        if !is_new_epoch && is_stale {
            return ContinuityVerdict::Ignore;
        }
        let verdict = if is_new_epoch {
            ContinuityVerdict::Seed
        } else if evidence.first_trade_id <= self.high_water_last_trade_id {
            ContinuityVerdict::Reset(ResetReason::TradeRangeOverlap)
        } else if evidence.first_trade_id.0 > self.high_water_last_trade_id.0 + 1 {
            ContinuityVerdict::Reset(ResetReason::TradeGap)
        } else {
            ContinuityVerdict::Accept
        };
        self.epoch = Some(evidence.stream_epoch);
        self.high_water_aggregate_id = evidence.aggregate_id;
        self.high_water_last_trade_id = evidence.last_trade_id;
        verdict
    }
}

#[derive(Debug, Clone)]
struct StagedDelta {
    update_id: u64,
    venue_ts_us: VenueTs,
    chunks: Vec<BookChunk>,
}

#[derive(Debug)]
pub struct MarketFold {
    instrument: InstrumentId,
    book: Book,
    bids: PriceLadder,
    asks: PriceLadder,
    last_commit_venue_ts_us: VenueTs,
    continuity: TradeContinuity,
    staged: Option<StagedDelta>,
    matching: MatchingState,
    recovery: MarketRecovery,
    unmatched: UnmatchedPrints,
}

impl MarketFold {
    pub fn new(instrument: InstrumentId, book_capacity: usize) -> Self {
        Self {
            instrument,
            book: Book::new_silent(book_capacity),
            bids: PriceLadder::new(Side::Buy),
            asks: PriceLadder::new(Side::Sell),
            last_commit_venue_ts_us: VenueTs::from_micros(i64::MIN),
            continuity: TradeContinuity::new(),
            staged: None,
            matching: MatchingState::Suspended,
            recovery: MarketRecovery {
                generation: 0,
                snapshot_complete: false,
                bridging_delta: false,
            },
            unmatched: UnmatchedPrints::default(),
        }
    }

    pub fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn is_matching_live(&self) -> bool {
        self.matching == MatchingState::Live
    }

    pub fn recovery(&self) -> MarketRecovery {
        self.recovery
    }

    pub fn unmatched_prints(&self) -> UnmatchedPrints {
        self.unmatched
    }

    pub fn ladder(&self, side: Side) -> &PriceLadder {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    pub fn ladder_mut(&mut self, side: Side) -> &mut PriceLadder {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    pub fn restore_matching(&mut self) {
        self.matching = MatchingState::Live;
    }

    #[cold]
    pub fn suspend_matching(&mut self, reason: ResetReason) -> ResetReason {
        self.matching = MatchingState::Suspended;
        self.recovery.generation = self
            .recovery
            .generation
            .checked_add(1)
            .expect("simulated market recovery generations exhausted");
        self.recovery.snapshot_complete = false;
        self.recovery.bridging_delta = false;
        self.mark_all_unobservable();
        reason
    }

    pub fn mark_all_unobservable(&mut self) {
        for (_, queue) in self.bids.iter_mut() {
            queue.mark_public_unobservable();
        }
        for (_, queue) in self.asks.iter_mut() {
            queue.mark_public_unobservable();
        }
    }

    pub fn public_at(&self, side: Side, price: Price) -> QueueAhead {
        match self.book.level_state(side, price) {
            LevelState::Present(qty) => QueueAhead::Known(qty),
            LevelState::Absent => QueueAhead::Known(Qty::from_mantissa(0)),
            LevelState::BeyondDepth => QueueAhead::Unobservable,
        }
    }

    /// A delta with no venue stamp cannot be placed among the prints, so it breaks the transaction.
    pub fn on_book_chunk(&mut self, chunk: &BookChunk) -> Option<ResetReason> {
        assert_eq!(
            chunk.instrument, self.instrument,
            "book chunk for instrument {} reached the venue for instrument {}",
            chunk.instrument.0, self.instrument.0
        );
        if chunk.kind == BookChunkKind::Snapshot {
            self.recovery.snapshot_complete = false;
            self.recovery.bridging_delta = false;
            return match self.book.apply_snapshot_chunk(chunk) {
                SnapshotOutcome::Clean => {
                    self.recovery.snapshot_complete = chunk.is_last_chunk;
                    None
                }
                SnapshotOutcome::ImplicitReset => {
                    self.staged = None;
                    let reason = self.suspend_matching(ResetReason::BookReset);
                    self.recovery.snapshot_complete = chunk.is_last_chunk;
                    Some(reason)
                }
            };
        }
        let Some(exchange_ts_us) = chunk.exchange_ts_us else {
            self.staged = None;
            return Some(self.suspend_matching(ResetReason::DeltaTransactionBroken));
        };
        self.stage_delta(chunk, exchange_ts_us)
    }

    fn stage_delta(&mut self, chunk: &BookChunk, venue_ts_us: VenueTs) -> Option<ResetReason> {
        if let Some(staged) = &self.staged
            && (staged.update_id != chunk.update_id || staged.venue_ts_us != venue_ts_us)
        {
            self.staged = None;
            return Some(self.suspend_matching(ResetReason::DeltaTransactionBroken));
        }
        self.staged
            .get_or_insert_with(|| StagedDelta {
                update_id: chunk.update_id,
                venue_ts_us,
                chunks: Vec::new(),
            })
            .chunks
            .push(*chunk);
        if chunk.is_last_chunk {
            self.commit_delta();
        }
        None
    }

    fn commit_delta(&mut self) {
        let Some(staged) = self.staged.take() else {
            panic!("commit reached with no staged depth transaction");
        };
        for chunk in &staged.chunks {
            self.book.apply_delta_chunk(chunk);
        }
        self.cap_queues_to_visible();
        self.last_commit_venue_ts_us = self.last_commit_venue_ts_us.max(staged.venue_ts_us);
        self.recovery.bridging_delta = self.recovery.snapshot_complete;
    }

    fn cap_queues_to_visible(&mut self) {
        let book = &self.book;
        for ladder in [&mut self.bids, &mut self.asks] {
            let side = ladder.side();
            for (price, queue) in ladder.iter_mut() {
                match book.level_state(side, price) {
                    LevelState::Present(qty) => queue.reconcile_known_public_to(qty),
                    LevelState::Absent => queue.reconcile_known_public_to(Qty::from_mantissa(0)),
                    LevelState::BeyondDepth => queue.mark_public_unobservable(),
                }
            }
        }
    }

    #[cold]
    pub fn on_book_reset(&mut self, venue_ts_us: VenueTs) -> ResetReason {
        self.staged = None;
        self.book.apply_reset();
        self.last_commit_venue_ts_us = self.last_commit_venue_ts_us.max(venue_ts_us);
        self.suspend_matching(ResetReason::BookReset)
    }

    /// # Panics
    /// If private fills exceed the public trade quantity.
    pub fn on_trade(
        &mut self,
        trade: &TradeEvent,
        evidence: TradeEvidence,
        take: &mut dyn FnMut(SimOrderIndex, Qty) -> OwnFill,
    ) -> Option<ResetReason> {
        assert_eq!(
            trade.instrument, self.instrument,
            "trade for instrument {} reached the venue for instrument {}",
            trade.instrument.0, self.instrument.0
        );
        match self.continuity.admit(evidence) {
            ContinuityVerdict::Seed => {
                self.unmatched.epoch_seed += 1;
                return None;
            }
            ContinuityVerdict::Ignore => {
                self.unmatched.stale_id += 1;
                return None;
            }
            ContinuityVerdict::Reset(reason) => {
                self.unmatched.suspended += 1;
                return Some(self.suspend_matching(reason));
            }
            ContinuityVerdict::Accept => {}
        }
        if !self.is_matching_live() {
            self.unmatched.suspended += 1;
            return None;
        }
        if trade.exchange_ts_us <= self.last_commit_venue_ts_us {
            self.unmatched.before_last_commit += 1;
            return None;
        }
        self.walk_aggressor(trade, take);
        None
    }

    fn walk_aggressor(
        &mut self,
        trade: &TradeEvent,
        take: &mut dyn FnMut(SimOrderIndex, Qty) -> OwnFill,
    ) {
        let own_side = trade.side.opposite();
        let mut budget = trade.qty;
        let mut filled_privately = Qty::from_mantissa(0);
        let mut counted = |index: SimOrderIndex, offered: Qty| {
            let fill = take(index, offered);
            filled_privately =
                Qty::from_mantissa(filled_privately.mantissa() + fill.taken.mantissa());
            fill
        };
        for (price, queue) in self.ladder_mut(own_side).iter_mut() {
            if budget.mantissa() == 0 {
                break;
            }
            match eligibility(own_side, price, trade.price) {
                Eligibility::Through => queue.walk(PublicPolicy::Skip, &mut budget, &mut counted),
                Eligibility::Equal => {
                    queue.walk(PublicPolicy::Consume, &mut budget, &mut counted);
                    break;
                }
                Eligibility::Away => break,
            }
        }
        assert!(
            filled_privately.mantissa() <= trade.qty.mantissa(),
            "one print of {} invented {} of private fill",
            trade.qty.mantissa(),
            filled_privately.mantissa()
        );
        self.ladder_mut(own_side).drop_vacant();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Eligibility {
    Through,
    Equal,
    Away,
}

fn eligibility(own_side: Side, own_price: Price, print: Price) -> Eligibility {
    match (own_price.mantissa().cmp(&print.mantissa()), own_side) {
        (Ordering::Equal, _) => Eligibility::Equal,
        (Ordering::Greater, Side::Buy) | (Ordering::Less, Side::Sell) => Eligibility::Through,
        (Ordering::Greater, Side::Sell) | (Ordering::Less, Side::Buy) => Eligibility::Away,
    }
}
