//! Order-book fitness: a chunked snapshot + delta stream reconstructs the same book a naive
//! capped `BTreeMap` model does, and applying never reallocates.

use std::collections::BTreeMap;

use polysim::adapters::polymarket::book::BookNormaliser;
use polysim::adapters::polymarket::parse::PolyBook;
use polysim::hot::book::{Book, BookState, SnapshotOutcome};
use polysim::ids::{InstrumentId, Side};
use polysim::msg::inbound::{BOOK_CHUNK_LEVELS, BookChunk, BookChunkKind, InboundMessage, Level};
use polysim::time::TsUs;
use proptest::prelude::*;

use crate::engine_support::{
    delta_chunk, partial_delta_chunk, partial_snapshot_chunk, snapshot_pair,
};

fn any_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn make_chunks(
    side: Side,
    kind: BookChunkKind,
    levels: &[(i64, i64)],
    chunk_size: usize,
) -> Vec<BookChunk> {
    levels
        .chunks(chunk_size.clamp(1, BOOK_CHUNK_LEVELS))
        .map(|group| {
            assert!(group.len() <= BOOK_CHUNK_LEVELS);
            match kind {
                BookChunkKind::Snapshot => partial_snapshot_chunk(0, side, group, 0),
                BookChunkKind::Delta => partial_delta_chunk(0, side, group, 0),
            }
        })
        .collect()
}

/// The oracle: a zero quantity removes a level, any other quantity inserts or replaces one, and at
/// capacity the worst level is evicted to make room unless the new level would be worse still.
#[derive(Default)]
struct BookModel {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl BookModel {
    fn apply(&mut self, side: Side, capacity: usize, price: i64, qty: i64) {
        let (levels, is_bid) = match side {
            Side::Buy => (&mut self.bids, true),
            Side::Sell => (&mut self.asks, false),
        };
        if qty == 0 {
            levels.remove(&price);
            return;
        }
        if levels.contains_key(&price) || levels.len() < capacity {
            levels.insert(price, qty);
            return;
        }
        let worst = if is_bid {
            *levels.keys().next().expect("non-empty at capacity")
        } else {
            *levels.keys().next_back().expect("non-empty at capacity")
        };
        let deeper_than_worst = if is_bid { price < worst } else { price > worst };
        if !deeper_than_worst {
            levels.remove(&worst);
            levels.insert(price, qty);
        }
    }

    fn bids_best_first(&self) -> Vec<(i64, i64)> {
        self.bids
            .iter()
            .rev()
            .map(|(&price, &qty)| (price, qty))
            .collect()
    }

    fn asks_best_first(&self) -> Vec<(i64, i64)> {
        self.asks
            .iter()
            .map(|(&price, &qty)| (price, qty))
            .collect()
    }
}

fn book_side(levels: &[Level]) -> Vec<(i64, i64)> {
    levels
        .iter()
        .map(|level| (level.price.0, level.qty.0))
        .collect()
}

proptest! {
    #[test]
    fn reconstructs_like_capped_model(
        capacity in 1usize..20,
        snapshot_bids in prop::collection::vec((0i64..1_000, 1i64..1_000), 0..40),
        snapshot_asks in prop::collection::vec((0i64..1_000, 1i64..1_000), 0..40),
        deltas in prop::collection::vec((any_side(), 0i64..1_000, 0i64..1_000), 0..120),
        chunk_size in 1usize..=BOOK_CHUNK_LEVELS,
    ) {
        let mut book = Book::new(capacity);
        let mut model = BookModel::default();

        let mut snapshot_chunks = make_chunks(Side::Buy, BookChunkKind::Snapshot, &snapshot_bids, chunk_size);
        snapshot_chunks.extend(make_chunks(Side::Sell, BookChunkKind::Snapshot, &snapshot_asks, chunk_size));
        if snapshot_chunks.is_empty() {
            snapshot_chunks.push(partial_snapshot_chunk(0, Side::Buy, &[], 0));
        }
        let last = snapshot_chunks.len() - 1;
        snapshot_chunks[last].is_last_chunk = true;

        for chunk in &snapshot_chunks {
            for &level in chunk.active_levels() {
                model.apply(chunk.side, capacity, level.price.0, level.qty.0);
            }
            let outcome = book.apply_snapshot_chunk(chunk);
            prop_assert_eq!(outcome, SnapshotOutcome::Clean);
        }
        prop_assert_eq!(book.state(), BookState::Valid);

        let delta_bids: Vec<(i64, i64)> =
            deltas.iter().filter(|(side, ..)| *side == Side::Buy).map(|&(_, p, q)| (p, q)).collect();
        let delta_asks: Vec<(i64, i64)> =
            deltas.iter().filter(|(side, ..)| *side == Side::Sell).map(|&(_, p, q)| (p, q)).collect();
        let mut delta_chunks = make_chunks(Side::Buy, BookChunkKind::Delta, &delta_bids, chunk_size);
        delta_chunks.extend(make_chunks(Side::Sell, BookChunkKind::Delta, &delta_asks, chunk_size));

        for chunk in &delta_chunks {
            for &level in chunk.active_levels() {
                model.apply(chunk.side, capacity, level.price.0, level.qty.0);
            }
            book.apply_delta_chunk(chunk);
        }

        prop_assert_eq!(book_side(book.bids()), model.bids_best_first());
        prop_assert_eq!(book_side(book.asks()), model.asks_best_first());
    }

    /// The book uses preallocated vectors and evicts then inserts in place, so applying never
    /// reallocates.
    #[test]
    fn apply_never_allocates(
        capacity in 1usize..16,
        ops in prop::collection::vec((any_side(), 0i64..500, 0i64..500), 0..400),
        chunk_size in 1usize..=BOOK_CHUNK_LEVELS,
    ) {
        let mut book = Book::new(capacity);

        let bids: Vec<(i64, i64)> =
            ops.iter().filter(|(side, ..)| *side == Side::Buy).map(|&(_, p, q)| (p, q)).collect();
        let asks: Vec<(i64, i64)> =
            ops.iter().filter(|(side, ..)| *side == Side::Sell).map(|&(_, p, q)| (p, q)).collect();

        let mut snapshot_chunks = make_chunks(Side::Buy, BookChunkKind::Snapshot, &bids, chunk_size);
        snapshot_chunks.extend(make_chunks(Side::Sell, BookChunkKind::Snapshot, &asks, chunk_size));
        if snapshot_chunks.is_empty() {
            snapshot_chunks.push(partial_snapshot_chunk(0, Side::Buy, &[], 0));
        }
        let last = snapshot_chunks.len() - 1;
        snapshot_chunks[last].is_last_chunk = true;

        let mut delta_chunks = make_chunks(Side::Buy, BookChunkKind::Delta, &bids, chunk_size);
        delta_chunks.extend(make_chunks(Side::Sell, BookChunkKind::Delta, &asks, chunk_size));

        let before = crate::alloc_count();
        for chunk in &snapshot_chunks {
            let outcome = book.apply_snapshot_chunk(chunk);
            prop_assert_eq!(outcome, SnapshotOutcome::Clean);
        }
        for chunk in &delta_chunks {
            book.apply_delta_chunk(chunk);
        }
        let after = crate::alloc_count();
        prop_assert_eq!(after, before, "book apply allocated");
    }
}

/// A snapshot with no levels on either side is still a snapshot, and the hot book leaves
/// `AwaitingSnapshot` only on a chunk carrying the terminator. An emitter that produced nothing for
/// an empty book would wedge the instrument there and drop every delta that followed, while the
/// adapter believed it had gone live. Driven through the real emitter rather than a test-local
/// cutter, because a hand-written cutter can carry the same terminator mistake as the emitter and
/// mask it.
#[test]
fn an_empty_snapshot_still_carries_the_terminator() {
    let mut normaliser = BookNormaliser::new(InstrumentId(0));
    let empty = PolyBook {
        asset_id: "token".into(),
        bids: Vec::new(),
        asks: Vec::new(),
        exchange_ts_us: TsUs::from_micros(1),
        received_ts_us: TsUs::from_micros(2),
    };

    let mut chunks = Vec::new();
    normaliser.emit_snapshot(&empty, &mut |message| {
        if let InboundMessage::Book(chunk) = message {
            chunks.push(chunk);
        }
    });

    assert_eq!(
        chunks.iter().filter(|chunk| chunk.is_last_chunk).count(),
        1,
        "an empty snapshot must close itself exactly once"
    );

    let mut book = Book::new(8);
    for chunk in &chunks {
        assert_eq!(book.apply_snapshot_chunk(chunk), SnapshotOutcome::Clean);
    }
    assert_eq!(
        book.state(),
        BookState::Valid,
        "the book must leave AwaitingSnapshot on an empty snapshot"
    );
}

/// A locked top (best bid == best ask) is a benign complementary-binary state —
/// counted, never warned; a crossed top (best bid > best ask) is counted per event with the WARN
/// episode-limited. The counters carry the classification; the WARN suppression mirrors the trim
/// episode and is review-checked, not log-asserted.
#[test]
fn locked_counts_silently_crossed_counts_each_event() {
    let mut book = Book::new(8);
    // Seed a proper book: best bid 50 < best ask 60.
    let (bids, asks) = snapshot_pair(0, &[(50, 1)], &[(60, 1)], 0);
    assert_eq!(book.apply_snapshot_chunk(&bids), SnapshotOutcome::Clean);
    assert_eq!(book.apply_snapshot_chunk(&asks), SnapshotOutcome::Clean);
    assert_eq!((book.locked_count(), book.crossed_count()), (0, 0));

    // A persistent lock at the touch (the 0.99/0.01 endgame shape): best ask down to 50, equal to
    // the best bid, re-quoted thrice. Each counts; none crosses, so the crossed/warn path never runs.
    for qty in [1, 2, 3] {
        book.apply_delta_chunk(&delta_chunk(0, Side::Sell, &[(50, qty)], 0));
    }
    assert_eq!(book.locked_count(), 3, "each lock counted");
    assert_eq!(
        book.crossed_count(),
        0,
        "a lock never enters the crossed/warn path"
    );

    // A crossing stampede within one episode: best bid 55 > best ask 50, re-quoted — both counted,
    // the WARN fires once.
    book.apply_delta_chunk(&delta_chunk(0, Side::Buy, &[(55, 1)], 0));
    book.apply_delta_chunk(&delta_chunk(0, Side::Buy, &[(55, 2)], 0));
    assert_eq!(
        book.crossed_count(),
        2,
        "each crossing counted; the WARN is episode-limited"
    );

    // Return to a proper spread (best ask back to 60) ends the episode; the next crossing opens a
    // fresh one.
    book.apply_delta_chunk(&delta_chunk(0, Side::Sell, &[(50, 0)], 0));
    book.apply_delta_chunk(&delta_chunk(0, Side::Buy, &[(65, 1)], 0));
    assert_eq!(
        book.crossed_count(),
        3,
        "the new episode counts the fresh crossing"
    );
    assert_eq!(book.locked_count(), 3, "no further locks");
}
