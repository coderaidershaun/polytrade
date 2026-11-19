//! The depth chain of each Binance market. Any split of a recorded stream at a snapshot gives the
//! same book, and a buffer that overflows before its snapshot keeps the newest diff.

use std::collections::BTreeMap;
use std::ops::Range;

use engine::hot::book::{Book, SnapshotOutcome as BookSnapshotOutcome};
use engine::msg::ids::{Price, Qty, Side};
use engine::msg::inbound::{BOOK_CHUNK_LEVELS, BookChunkKind, InboundMessage, Level, MessageBody};
use engine::msg::time::VenueTs;
use engine::venues::binance::depth::{ChainRule, DepthSequencer, DiffOutcome, SnapshotOutcome};
use engine::venues::binance::parse::{
    DepthDiff, DepthSnapshot, parse_combined_frame, parse_perp_depth_diff, parse_spot_depth_diff,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use crate::engine_support::level;
use crate::venues::binance::binance_depth_sequencing::{
    INSTRUMENT, RECEIVED, ctx, synthetic_diff, synthetic_snapshot,
};

const SPOT_DEPTH: &str = include_str!("../../../../fixtures/binance/spot_depth.jsonl");
const PERP_DEPTH: &str = include_str!("../../../../fixtures/binance/perp_depth.jsonl");
/// The buffer of each replay. A split buffers fewer diffs than this, so no replay overflows.
const CAPACITY: usize = 16;
/// Room for each price the recordings touch, so the book trims nothing.
const BOOK_CAPACITY: usize = 16_384;

type Emitted = Vec<InboundMessage>;

fn recorded(rule: ChainRule) -> Vec<DepthDiff> {
    let fixture = match rule {
        ChainRule::Spot => SPOT_DEPTH,
        ChainRule::Perp => PERP_DEPTH,
    };
    let diffs: Vec<DepthDiff> = fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let frame = parse_combined_frame(line).expect("combined envelope");
            match rule {
                ChainRule::Spot => parse_spot_depth_diff(&frame.data, ctx()),
                ChainRule::Perp => parse_perp_depth_diff(&frame.data, ctx()),
            }
            .expect("depth diff")
        })
        .collect();
    assert!(diffs.len() > 100, "the recording holds the full window");
    diffs
}

/// A book as the venue holds it: each price with its absolute quantity.
#[derive(Clone)]
struct Levels {
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
}

impl Levels {
    /// One level on each side, far from each recorded price, so no snapshot is empty.
    fn seeded() -> Self {
        let bid = level(100_000_000, 100_000_000);
        let ask = level(1_000_000_000_000_000, 100_000_000);
        Self {
            bids: BTreeMap::from([(bid.price, bid.qty)]),
            asks: BTreeMap::from([(ask.price, ask.qty)]),
        }
    }

    fn through(diffs: &[DepthDiff]) -> Self {
        let mut levels = Self::seeded();
        for diff in diffs {
            levels.apply(diff);
        }
        levels
    }

    fn apply(&mut self, diff: &DepthDiff) {
        for (side, changes) in [(&mut self.bids, &diff.bids), (&mut self.asks, &diff.asks)] {
            for change in changes {
                if change.qty.mantissa() == 0 {
                    side.remove(&change.price);
                } else {
                    side.insert(change.price, change.qty);
                }
            }
        }
    }

    fn best_first(side: &BTreeMap<Price, Qty>, order: Side) -> Vec<Level> {
        let levels = side.iter().map(|(&price, &qty)| Level { price, qty });
        match order {
            Side::Buy => levels.rev().collect(),
            Side::Sell => levels.collect(),
        }
    }

    fn snapshot(&self, last_update_id: u64) -> DepthSnapshot {
        DepthSnapshot {
            instrument: INSTRUMENT,
            last_update_id,
            bids: Self::best_first(&self.bids, Side::Buy),
            asks: Self::best_first(&self.asks, Side::Sell),
            received_ts_us: RECEIVED,
        }
    }
}

/// A replay of one recording: the sequencer buffers the diffs of `window`, gets `snapshot`, then
/// each later diff. `kept` names the buffered diffs that the snapshot does not hold.
struct Replay<'a> {
    rule: ChainRule,
    diffs: &'a [DepthDiff],
    window: Range<usize>,
    snapshot: DepthSnapshot,
    kept: Vec<usize>,
    reference: Levels,
}

impl Replay<'_> {
    fn run(self) -> Result<(), TestCaseError> {
        let mut sequencer = DepthSequencer::new(self.rule, INSTRUMENT, CAPACITY);
        for index in self.window.clone() {
            let mut emitted = Emitted::new();
            let outcome = sequencer.on_diff(self.diffs[index].clone(), &mut |message| {
                emitted.push(message)
            });
            prop_assert_eq!(outcome, DiffOutcome::Buffered, "diff {}", index);
            prop_assert!(emitted.is_empty(), "a buffered diff emits nothing");
        }

        let mut book = Book::new_silent(BOOK_CAPACITY);
        let snapshot_id = self.snapshot.last_update_id;
        let (bid_levels, ask_levels) = (self.snapshot.bids.len(), self.snapshot.asks.len());
        let mut emitted = Emitted::new();
        let outcome = sequencer.apply_snapshot(self.snapshot, &mut |message| emitted.push(message));
        prop_assert_eq!(outcome, SnapshotOutcome::Applied);
        prop_assert!(sequencer.is_live());
        let snapshot_chunks = emitted
            .iter()
            .take_while(|message| chunk_kind(message) == Some(BookChunkKind::Snapshot))
            .count();
        let (snapshot, mut deltas) = emitted.split_at(snapshot_chunks);
        check_event(
            snapshot,
            BookChunkKind::Snapshot,
            snapshot_id,
            (bid_levels, ask_levels),
            None,
        )?;
        for &index in &self.kept {
            let diff = &self.diffs[index];
            let chunks = deltas
                .iter()
                .take_while(|message| chunk_update_id(message) == Some(diff.final_update_id))
                .count();
            check_delta(&deltas[..chunks], diff)?;
            deltas = &deltas[chunks..];
        }
        prop_assert!(
            deltas.is_empty(),
            "the snapshot applies no buffered diff that it holds: {:?}",
            deltas
        );
        fold(&mut book, &emitted)?;

        for diff in &self.diffs[self.window.end..] {
            let mut emitted = Emitted::new();
            let outcome = sequencer.on_diff(diff.clone(), &mut |message| emitted.push(message));
            prop_assert_eq!(
                outcome,
                DiffOutcome::Applied,
                "chain broke at update id {}",
                diff.final_update_id
            );
            check_delta(&emitted, diff)?;
            fold(&mut book, &emitted)?;
        }

        prop_assert_eq!(book.trimmed_count(), 0, "the book trims nothing");
        let bids = Levels::best_first(&self.reference.bids, Side::Buy);
        let asks = Levels::best_first(&self.reference.asks, Side::Sell);
        prop_assert_eq!(book.bids(), bids.as_slice(), "bids");
        prop_assert_eq!(book.asks(), asks.as_slice(), "asks");
        Ok(())
    }
}

fn chunk_kind(message: &InboundMessage) -> Option<BookChunkKind> {
    match &message.body {
        MessageBody::Book(chunk) => Some(chunk.kind),
        _ => None,
    }
}

fn chunk_update_id(message: &InboundMessage) -> Option<u64> {
    match &message.body {
        MessageBody::Book(chunk) if chunk.kind == BookChunkKind::Delta => Some(chunk.update_id),
        _ => None,
    }
}

fn check_delta(emitted: &[InboundMessage], diff: &DepthDiff) -> Result<(), TestCaseError> {
    check_event(
        emitted,
        BookChunkKind::Delta,
        diff.final_update_id,
        (diff.bids.len(), diff.asks.len()),
        Some(diff.exchange_ts_us),
    )
}

/// One event of the sequencer: chunks of one kind and one update id that hold each level of the
/// event once, with one last-chunk flag on the final chunk, each with the venue stamp `stamp`.
fn check_event(
    emitted: &[InboundMessage],
    kind: BookChunkKind,
    update_id: u64,
    (bid_levels, ask_levels): (usize, usize),
    stamp: Option<VenueTs>,
) -> Result<(), TestCaseError> {
    let (mut buy, mut sell, mut last_flags) = (0usize, 0usize, 0usize);
    for message in emitted {
        let MessageBody::Book(chunk) = &message.body else {
            return Err(TestCaseError::fail(format!(
                "expected a book chunk, got {message:?}"
            )));
        };
        prop_assert_eq!(chunk.kind, kind);
        prop_assert_eq!(chunk.update_id, update_id);
        prop_assert!(chunk.active_levels().len() <= BOOK_CHUNK_LEVELS);
        prop_assert_eq!(chunk.exchange_ts_us, stamp, "update id {}", update_id);
        match chunk.side {
            Side::Buy => buy += chunk.active_levels().len(),
            Side::Sell => sell += chunk.active_levels().len(),
        }
        last_flags += usize::from(chunk.is_last_chunk);
    }
    prop_assert_eq!(
        buy,
        bid_levels,
        "bid levels conserved at update id {}",
        update_id
    );
    prop_assert_eq!(
        sell,
        ask_levels,
        "ask levels conserved at update id {}",
        update_id
    );
    if bid_levels + ask_levels == 0 {
        prop_assert!(emitted.is_empty(), "an empty event emits nothing");
        return Ok(());
    }
    prop_assert_eq!(
        last_flags,
        1,
        "exactly one last chunk at update id {}",
        update_id
    );
    let is_last = matches!(
        emitted.last(),
        Some(InboundMessage { body: MessageBody::Book(chunk), .. }) if chunk.is_last_chunk
    );
    prop_assert!(is_last, "the last-chunk flag sits on the final chunk");
    Ok(())
}

fn fold(book: &mut Book, emitted: &[InboundMessage]) -> Result<(), TestCaseError> {
    for message in emitted {
        match &message.body {
            MessageBody::Book(chunk) if chunk.kind == BookChunkKind::Snapshot => {
                let outcome = book.apply_snapshot_chunk(chunk);
                prop_assert_eq!(outcome, BookSnapshotOutcome::Clean);
            }
            MessageBody::Book(chunk) => book.apply_delta_chunk(chunk),
            MessageBody::BookReset(_) => book.apply_reset(),
            other => {
                return Err(TestCaseError::fail(format!(
                    "not a book message: {other:?}"
                )));
            }
        }
    }
    Ok(())
}

/// The venue's rule for a buffered diff when a snapshot holds each diff up to `snapshot_at`: the
/// spot market drops the diff whose final id equals the snapshot's, and the perpetual market
/// applies it again.
fn is_kept(rule: ChainRule, index: usize, snapshot_at: usize) -> bool {
    match rule {
        ChainRule::Spot => index > snapshot_at,
        ChainRule::Perp => index >= snapshot_at,
    }
}

/// The sequencer buffers the `buffered` diffs from `start`, and the snapshot holds each diff up to
/// `snapshot_at`, one of them.
#[derive(Debug, Clone, Copy)]
struct Split {
    start: usize,
    buffered: usize,
    snapshot_at: usize,
}

fn splits(count: usize) -> impl Strategy<Value = Split> {
    (1..CAPACITY).prop_flat_map(move |buffered| {
        (0..=count - buffered, 0..buffered).prop_map(move |(start, position)| Split {
            start,
            buffered,
            snapshot_at: start + position,
        })
    })
}

fn split_replay(rule: ChainRule, diffs: &[DepthDiff], split: Split) -> Replay<'_> {
    let window = split.start..split.start + split.buffered;
    let snapshot_at = split.snapshot_at;
    let held = Levels::through(&diffs[..=snapshot_at]);
    let mut reference = held.clone();
    for (index, diff) in diffs.iter().enumerate() {
        if is_kept(rule, index, snapshot_at) {
            reference.apply(diff);
        }
    }
    Replay {
        rule,
        diffs,
        kept: window
            .clone()
            .filter(|&index| is_kept(rule, index, snapshot_at))
            .collect(),
        window,
        snapshot: held.snapshot(diffs[snapshot_at].final_update_id),
        reference,
    }
}

/// The whole recording from its first diff, which a snapshot at the diff's own first id spans by
/// the rule of each market.
fn whole_stream(rule: ChainRule, diffs: &[DepthDiff]) -> Replay<'_> {
    Replay {
        rule,
        diffs,
        window: 0..1,
        snapshot: Levels::seeded().snapshot(diffs[0].first_update_id),
        kept: vec![0],
        reference: Levels::through(diffs),
    }
}

fn delta_chunks_at(emitted: &[InboundMessage], update_id: u64) -> usize {
    emitted
        .iter()
        .filter(|message| chunk_update_id(message) == Some(update_id))
        .count()
}

fn the_border_of_each_market_holds() {
    let boundary = 500;
    for (rule, prev, kept_chunks) in [(ChainRule::Spot, None, 0), (ChainRule::Perp, Some(400), 1)] {
        let mut sequencer = DepthSequencer::new(rule, INSTRUMENT, 8);
        sequencer.on_diff(synthetic_diff(490, boundary, prev), &mut |_| {});
        let mut emitted = Vec::new();
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(boundary), &mut |message| {
                emitted.push(message)
            }),
            SnapshotOutcome::Applied,
            "{rule:?}"
        );
        assert_eq!(
            delta_chunks_at(&emitted, boundary),
            kept_chunks,
            "{rule:?}: the diff whose final id equals the snapshot's"
        );
    }
}

fn the_highest_spot_id_resets() {
    let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
    sequencer.on_diff(synthetic_diff(u64::MAX, u64::MAX, None), &mut |_| {});
    assert_eq!(
        sequencer.apply_snapshot(synthetic_snapshot(u64::MAX - 1), &mut |_| {}),
        SnapshotOutcome::Applied,
        "the final representable diff can bridge a snapshot"
    );
    let mut emitted = Vec::new();
    assert_eq!(
        sequencer.on_diff(synthetic_diff(u64::MAX, u64::MAX, None), &mut |message| {
            emitted.push(message)
        }),
        DiffOutcome::Resync,
        "there is no sequence id after u64::MAX"
    );
    assert!(
        matches!(
            emitted.as_slice(),
            [InboundMessage {
                body: MessageBody::BookReset(_),
                ..
            }]
        ),
        "exhaustion is one reset, not a wrapped duplicate: {emitted:?}"
    );
}

fn a_snapshot_past_the_highest_spot_id_is_stale() {
    struct Case {
        name: &'static str,
        prime: fn(&mut DepthSequencer),
        snapshot_at: u64,
    }
    let cases = [
        Case {
            name: "no_prior_diff_at_the_ceiling",
            prime: |_| {},
            snapshot_at: u64::MAX,
        },
        Case {
            name: "buffered_chain_cannot_wrap_its_successor_to_zero",
            prime: |sequencer| {
                sequencer.on_diff(synthetic_diff(u64::MAX - 1, u64::MAX, None), &mut |_| {});
                sequencer.on_diff(synthetic_diff(u64::MAX, u64::MAX, None), &mut |_| {});
            },
            snapshot_at: u64::MAX - 2,
        },
    ];
    for case in cases {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        (case.prime)(&mut sequencer);
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(case.snapshot_at), &mut |_| {}),
            SnapshotOutcome::Stale,
            "{}: a snapshot with no successor id cannot start a live chain",
            case.name
        );
        assert!(!sequencer.is_live(), "{}", case.name);
    }
}

fn a_diff_that_comes_again_is_not_applied() {
    let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
    sequencer.on_diff(synthetic_diff(490, 500, None), &mut |_| {});
    assert_eq!(
        sequencer.apply_snapshot(synthetic_snapshot(490), &mut |_| {}),
        SnapshotOutcome::Applied
    );
    assert!(sequencer.is_live());
    let mut emitted = Vec::new();
    let outcome = sequencer.on_diff(synthetic_diff(495, 500, None), &mut |message| {
        emitted.push(message)
    });
    assert!(
        emitted.is_empty(),
        "a diff the book already holds changes no level"
    );
    assert_eq!(outcome, DiffOutcome::AlreadyApplied);
}

/// FITNESS: a book is the same for each point where a snapshot cuts into the stream, for both
/// markets of Binance. A sequencer that dropped a diff the snapshot lacks, or applied one in a
/// way the venue does not, would hand the engine a wrong book with no error anywhere.
#[test]
fn any_snapshot_split_gives_the_same_book() {
    for rule in [ChainRule::Spot, ChainRule::Perp] {
        let diffs = recorded(rule);
        whole_stream(rule, &diffs)
            .run()
            .unwrap_or_else(|error| panic!("{rule:?}, the whole stream: {error}"));
        // A runner counts its passed cases across runs, so each market gets its own.
        TestRunner::new(Config {
            cases: 128,
            failure_persistence: None,
            ..Config::default()
        })
        .run(&splits(diffs.len()), |split| {
            split_replay(rule, &diffs, split).run()
        })
        .unwrap_or_else(|error| panic!("{rule:?}: {error}"));
    }

    the_border_of_each_market_holds();
    the_highest_spot_id_resets();
    a_snapshot_past_the_highest_spot_id_is_stale();
    a_diff_that_comes_again_is_not_applied();
}

/// A diff of the overflow cases: one update id, and on the perpetual market the id before it as
/// its previous id.
fn numbered(rule: ChainRule, id: u64) -> DepthDiff {
    let prev = match rule {
        ChainRule::Spot => None,
        ChainRule::Perp => Some(id - 1),
    };
    synthetic_diff(id, id, prev)
}

/// The snapshot that the diff with `id` spans as the first diff after it: the spot market reads
/// the id before it, and the perpetual market applies the diff again.
fn spanned_by(rule: ChainRule, id: u64) -> DepthSnapshot {
    match rule {
        ChainRule::Spot => synthetic_snapshot(id - 1),
        ChainRule::Perp => synthetic_snapshot(id),
    }
}

fn feed(sequencer: &mut DepthSequencer, rule: ChainRule, ids: Range<u64>, expected: DiffOutcome) {
    for id in ids {
        let mut emitted = Vec::new();
        let outcome = sequencer.on_diff(numbered(rule, id), &mut |message| emitted.push(message));
        assert_eq!(outcome, expected, "{rule:?}: diff {id}");
        assert!(
            emitted.is_empty(),
            "{rule:?}: diff {id} emits nothing: {emitted:?}"
        );
    }
}

/// A sequencer of capacity 8 that was never live, after diffs 101 to 117: two overflow episodes.
fn overflowed_twice(rule: ChainRule) -> DepthSequencer {
    let mut sequencer = DepthSequencer::new(rule, INSTRUMENT, 8);
    feed(&mut sequencer, rule, 101..109, DiffOutcome::Buffered);
    feed(&mut sequencer, rule, 109..110, DiffOutcome::Overflow);
    feed(&mut sequencer, rule, 110..117, DiffOutcome::Buffered);
    feed(&mut sequencer, rule, 117..118, DiffOutcome::Overflow);
    sequencer
}

fn delta_ids(emitted: &[InboundMessage]) -> Vec<u64> {
    let mut ids: Vec<u64> = emitted.iter().filter_map(chunk_update_id).collect();
    ids.dedup();
    ids
}

/// FITNESS: a buffer that fills before its snapshot throws its diffs away and keeps only the
/// newest, so the next snapshot that the newest diff spans brings the book back whole. A buffer
/// that kept an old diff, or a reset that the book saw at each overflow, would apply diffs that
/// do not chain, and the book would be wrong with no error.
#[test]
fn depth_buffer_overflow_resets_and_keeps_newest_diff() {
    for rule in [ChainRule::Spot, ChainRule::Perp] {
        let mut sequencer = DepthSequencer::new(rule, INSTRUMENT, 8);
        feed(&mut sequencer, rule, 101..109, DiffOutcome::Buffered);
        feed(&mut sequencer, rule, 109..110, DiffOutcome::Overflow);
        let mut emitted = Vec::new();
        assert_eq!(
            sequencer.apply_snapshot(spanned_by(rule, 109), &mut |message| emitted.push(message)),
            SnapshotOutcome::Applied,
            "{rule:?}"
        );
        assert_eq!(
            delta_ids(&emitted),
            [109],
            "{rule:?}: after the first overflow the buffer holds diff 109 only"
        );

        let mut sequencer = overflowed_twice(rule);
        let mut emitted = Vec::new();
        assert_eq!(
            sequencer.apply_snapshot(spanned_by(rule, 117), &mut |message| emitted.push(message)),
            SnapshotOutcome::Applied,
            "{rule:?}"
        );
        assert!(sequencer.is_live(), "{rule:?}");
        let snapshot_chunks = emitted
            .iter()
            .take_while(|message| chunk_kind(message) == Some(BookChunkKind::Snapshot))
            .count();
        assert!(snapshot_chunks > 0, "{rule:?}: the snapshot comes first");
        assert_eq!(
            delta_ids(&emitted[snapshot_chunks..]),
            [117],
            "{rule:?}: then diff 117 only, and no diff of 101 to 116"
        );
        assert!(
            emitted
                .iter()
                .all(|message| !matches!(message.body, MessageBody::BookReset(_))),
            "{rule:?}: an overflow before the first snapshot sends no reset"
        );

        let mut sequencer = overflowed_twice(rule);
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(100), &mut |_| {}),
            SnapshotOutcome::Stale,
            "{rule:?}: diff 117 does not span a snapshot at 100"
        );
        assert!(!sequencer.is_live(), "{rule:?}");

        let mut sequencer = DepthSequencer::new(rule, INSTRUMENT, 8);
        feed(&mut sequencer, rule, 101..102, DiffOutcome::Buffered);
        assert_eq!(
            sequencer.apply_snapshot(spanned_by(rule, 101), &mut |_| {}),
            SnapshotOutcome::Applied,
            "{rule:?}"
        );
        let mut emitted = Vec::new();
        assert_eq!(
            sequencer.on_diff(numbered(rule, 200), &mut |message| emitted.push(message)),
            DiffOutcome::Resync,
            "{rule:?}: diff 200 breaks the chain"
        );
        assert!(
            matches!(
                emitted.as_slice(),
                [InboundMessage {
                    body: MessageBody::BookReset(_),
                    ..
                }]
            ),
            "{rule:?}: one reset at the break: {emitted:?}"
        );
        feed(&mut sequencer, rule, 201..208, DiffOutcome::Buffered);
        feed(&mut sequencer, rule, 208..209, DiffOutcome::Overflow);
    }
}
