//! Golden replay of the committed live capture. Two invariants on real venue bytes: every book
//! snapshot normalises so that best_bid is the highest bid and best_ask the lowest ask (the venue
//! sends prices ascending, so its best level is last), and the delta-reconstructed touch tracks the
//! venue's own inline best_bid and best_ask. The inline touch leads its removal deltas by 1-2
//! frames (there are no sequence numbers), so a handful of transient leads occur and converge,
//! which the shadow-book validator re-baselines. The collapse fixture is asserted to carry the
//! teardown burst.

use std::collections::HashMap;

use polysim::adapters::polymarket::book::{BookNormaliser, ChunkStamps};
use polysim::adapters::polymarket::parse::{
    PolyBook, PolyDelta, PolyFrame, is_frameless, parse_market_frame,
};
use polysim::adapters::polymarket::rest::parse_events;
use polysim::config::PolySeries;
use polysim::hot::book::{Book, BookState};
use polysim::ids::{InstrumentId, Price};
use polysim::time::TsUs;

use crate::polymarket_parse::apply;

const SERIES: PolySeries = PolySeries::BtcUpDown5m;

const HEAD: &str = include_str!("../../fixtures/polymarket/poly_market.jsonl");
const COLLAPSE: &str = include_str!("../../fixtures/polymarket/poly_market_collapse.jsonl");
const GAMMA_CURRENT: &str = include_str!("../../fixtures/polymarket/gamma_event_current.json");

const BOOK_CAPACITY: usize = 128;
// Any receipt stamp inside the capture window keeps the ms→µs clamp exact; the checks read prices.
const RECEIVED_US: i64 = 1_784_449_240_000_000;

fn received() -> TsUs {
    TsUs::from_micros(RECEIVED_US)
}

struct TokenBook {
    normaliser: BookNormaliser,
    book: Book,
}

impl TokenBook {
    fn new(id: u16) -> Self {
        Self {
            normaliser: BookNormaliser::new(InstrumentId(id)),
            book: Book::new(BOOK_CAPACITY),
        }
    }

    fn on_snapshot(&mut self, book: &PolyBook) {
        let mut messages = Vec::new();
        self.normaliser
            .emit_snapshot(book, &mut |m| messages.push(m));
        apply(&mut self.book, &messages);
    }

    fn on_delta(&mut self, delta: &PolyDelta, exchange_ts_us: TsUs) {
        let mut messages = Vec::new();
        let stamps = ChunkStamps {
            exchange_ts_us,
            received_ts_us: received(),
        };
        self.normaliser
            .emit_price_change(std::slice::from_ref(delta), stamps, &mut |m| {
                messages.push(m)
            });
        apply(&mut self.book, &messages);
    }
}

#[derive(Default)]
struct Replay {
    tokens: HashMap<Box<str>, TokenBook>,
    trades: u64,
    snapshots: u64,
    price_changes: u64,
    sort_violations: u64,
    tob_matches: u64,
    tob_mismatches: u64,
    zero_removals: u64,
    parse_errors: u64,
    fatal: u64,
}

impl Replay {
    fn of(fixture: &str) -> Self {
        let mut replay = Replay::default();
        for line in fixture.lines() {
            // These tapes are raw socket captures, so they hold the venue's keepalive answer and
            // its blank filler. The skip is the DRIVER'S OWN, not a rule of the harness: a copy
            // here could quietly grow more permissive than the socket path it stands in for, and
            // then a tape holding text the driver books as frame loss would still replay clean.
            if is_frameless(line) {
                continue;
            }
            match parse_market_frame(line, received()) {
                Ok(frame) => replay.dispatch(frame),
                Err(error) if error.is_fatal() => replay.fatal += 1,
                Err(_) => replay.parse_errors += 1,
            }
        }
        replay
    }

    fn dispatch(&mut self, frame: PolyFrame) {
        match frame {
            PolyFrame::Book(book) => {
                self.snapshots += 1;
                if !snapshot_best_is_extremal(&book) {
                    self.sort_violations += 1;
                }
                self.token(&book.asset_id).on_snapshot(&book);
            }
            PolyFrame::PriceChange(change) => {
                self.on_price_change(&change.changes, change.exchange_ts_us)
            }
            PolyFrame::Trade(_) => self.trades += 1,
            PolyFrame::TickSizeChange(_) => {}
            PolyFrame::Batch(frames) => frames.into_iter().for_each(|f| self.dispatch(f)),
            PolyFrame::Ignored => {}
        }
    }

    /// The venue stamps every element in a frame with the post-frame touch, so apply the whole frame
    /// then compare each affected token's book against that token's last-seen `best_*`.
    fn on_price_change(&mut self, changes: &[PolyDelta], exchange_ts_us: TsUs) {
        self.price_changes += 1;
        let mut last_best: HashMap<Box<str>, (Option<Price>, Option<Price>)> = HashMap::new();
        for delta in changes {
            if delta.level.qty.0 == 0 {
                self.zero_removals += 1;
            }
            self.token(&delta.asset_id).on_delta(delta, exchange_ts_us);
            last_best.insert(delta.asset_id.clone(), (delta.best_bid, delta.best_ask));
        }
        for (asset_id, (best_bid, best_ask)) in &last_best {
            let outcome = match self.tokens.get(asset_id) {
                Some(token) => top_of_book_outcome(&token.book, *best_bid, *best_ask),
                None => None,
            };
            match outcome {
                Some(true) => self.tob_matches += 1,
                Some(false) => self.tob_mismatches += 1,
                None => {}
            }
        }
    }

    fn token(&mut self, asset_id: &str) -> &mut TokenBook {
        let id = self.tokens.len() as u16;
        self.tokens
            .entry(asset_id.into())
            .or_insert_with(|| TokenBook::new(id))
    }
}

/// The ascending best=last pin: after normalisation the first bid is the highest-priced bid and
/// the first ask the lowest-priced ask, computed independently of the sort under test.
fn snapshot_best_is_extremal(book: &PolyBook) -> bool {
    let bid_ok =
        book.bids.first().copied().map(|l| l.price) == book.bids.iter().map(|l| l.price).max();
    let ask_ok =
        book.asks.first().copied().map(|l| l.price) == book.asks.iter().map(|l| l.price).min();
    bid_ok && ask_ok
}

/// `Some(matched)` when the frame carries a real touch and the book is Valid with both sides; `None`
/// when there is nothing meaningful to compare (book still awaiting, or a one-sided/collapse touch).
fn top_of_book_outcome(
    book: &Book,
    best_bid: Option<Price>,
    best_ask: Option<Price>,
) -> Option<bool> {
    if book.state() != BookState::Valid {
        return None;
    }
    let (best_bid, best_ask) = (best_bid?, best_ask?);
    let bid = book.best_bid()?.price;
    let ask = book.best_ask()?.price;
    Some(bid == best_bid && ask == best_ask)
}

mod head {
    use super::*;

    #[test]
    fn every_recorded_frame_parses_cleanly() {
        let replay = Replay::of(HEAD);
        assert_eq!(replay.fatal, 0, "no frame trips the fatal overflow signal");
        assert_eq!(replay.parse_errors, 0, "every data frame parses");
        assert!(
            replay.snapshots >= 2,
            "book snapshots present (incl. the initial array batch)"
        );
        assert!(replay.price_changes > 100, "price_change deltas present");
        assert!(replay.trades >= 1, "at least one trade present");
    }

    #[test]
    fn snapshots_normalise_best_to_the_touch() {
        let replay = Replay::of(HEAD);
        assert_eq!(
            replay.sort_violations, 0,
            "every snapshot's best_bid is the highest bid and best_ask the lowest ask (ascending best=last normalised)"
        );
        assert!(
            replay.snapshots >= 20,
            "the trap is pinned across many real snapshots"
        );
    }

    #[test]
    fn delta_top_of_book_tracks_venue_summary() {
        let replay = Replay::of(HEAD);
        assert!(
            replay.tob_matches > 400,
            "the capture exercises the top-of-book check heavily: {} matches",
            replay.tob_matches
        );
        // The venue's inline best_* leads its own removal deltas by 1-2 frames, so a
        // handful of transient leads are expected — they converge, and the shadow-book validator
        // re-baselines on divergence. Pin the tracking at >99%.
        assert!(
            replay.tob_mismatches * 100 <= replay.tob_matches,
            "reconstructed touch tracks the venue best on >99% of deltas: {} matches vs {} transient leads",
            replay.tob_matches,
            replay.tob_mismatches
        );
    }

    #[test]
    fn gamma_current_fixture_resolves_the_captured_tokens() {
        let market = parse_events(
            SERIES,
            GAMMA_CURRENT,
            TsUs::from_micros(1_784_449_200_000_000),
        )
        .expect("gamma current parses");
        let replay = Replay::of(HEAD);
        assert!(
            replay.tokens.contains_key(&*market.token_up),
            "the up token from Gamma appears in the WS capture"
        );
        assert!(
            replay.tokens.contains_key(&*market.token_down),
            "the down token from Gamma appears in the WS capture"
        );
    }
}

mod collapse {
    use super::*;
    use polysim::adapters::polymarket::teardown::{CollapseDetector, CollapseSignal, LevelUpdate};
    use polysim::ids::{Qty, Side};
    use polysim::msg::inbound::Level;

    const GAMMA_NEXT: &str = include_str!("../../fixtures/polymarket/gamma_event_next.json");

    /// Parse one capture line, flattening the venue's `[{},{}]` subscribe batch to its inner frames.
    fn frames_in(line: &str) -> Vec<PolyFrame> {
        match parse_market_frame(line, received()) {
            Ok(PolyFrame::Batch(frames)) => frames,
            Ok(frame) => vec![frame],
            Err(_) => Vec::new(),
        }
    }

    fn resting(side: Side, price: Price, exchange_ts_us: TsUs) -> LevelUpdate {
        LevelUpdate {
            side,
            price,
            qty: Qty(1),
            exchange_ts_us,
        }
    }

    /// Replay a resolving token's REAL captured teardown burst through the detector. WHY the seed:
    /// the grace tail is delta-only for ~357s (the token's last venue snapshot long predates its
    /// teardown), so the pre-collapse book cannot be frame-reconstructed — production leans on the
    /// definitive `/book` 404 probe when this fast path misses. The burst itself enumerates the live
    /// levels it wipes; seed those, plus the token's first real venue snapshot on the side it spares.
    fn captured_burst_collapses(token: &str) -> bool {
        let mut by_ts: HashMap<i64, Vec<(Side, Price)>> = HashMap::new();
        for line in COLLAPSE.lines() {
            for frame in frames_in(line) {
                let PolyFrame::PriceChange(change) = frame else {
                    continue;
                };
                for delta in &change.changes {
                    if &*delta.asset_id == token && delta.level.qty == Qty(0) {
                        by_ts
                            .entry(change.exchange_ts_us.micros())
                            .or_default()
                            .push((delta.side, delta.level.price));
                    }
                }
            }
        }
        let (burst_micros, removals) = by_ts
            .into_iter()
            .max_by_key(|(_, removals)| removals.len())
            .expect("the resolving token sheds levels in the capture");
        assert!(
            removals.len() >= 2,
            "a teardown is a mass removal, not one cancel"
        );
        let burst_ts = TsUs::from_micros(burst_micros);
        let seed_ts = TsUs::from_micros(burst_micros - 1);
        let persisting = match removals[0].0 {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let mut detector = CollapseDetector::default();
        for &(side, price) in &removals {
            detector.observe(resting(side, price, seed_ts));
        }
        for level in first_snapshot_levels(token, persisting) {
            detector.observe(resting(persisting, level.price, seed_ts));
        }
        removals.iter().any(|&(side, price)| {
            let removal = LevelUpdate {
                side,
                price,
                qty: Qty(0),
                exchange_ts_us: burst_ts,
            };
            detector.observe(removal) == CollapseSignal::Collapsed
        })
    }

    /// The `side` levels of the token's first venue book snapshot in the capture (the real book the
    /// burst leaves standing on the side it does not wipe).
    fn first_snapshot_levels(token: &str, side: Side) -> Vec<Level> {
        for line in HEAD.lines() {
            for frame in frames_in(line) {
                if let PolyFrame::Book(book) = frame
                    && &*book.asset_id == token
                {
                    return if side == Side::Buy { book.bids } else { book.asks };
                }
            }
        }
        Vec::new()
    }

    #[test]
    fn captured_teardown_burst_collapses_the_resolving_window_only() {
        let old = parse_events(
            SERIES,
            GAMMA_CURRENT,
            TsUs::from_micros(1_784_449_200_000_000),
        )
        .expect("current window parses");
        let new = parse_events(SERIES, GAMMA_NEXT, TsUs::from_micros(1_784_449_500_000_000))
            .expect("next window parses");

        // The resolving window's real teardown burst empties each token's book.
        assert!(
            captured_burst_collapses(&old.token_up),
            "the up token's captured teardown burst collapses its book"
        );
        assert!(
            captured_burst_collapses(&old.token_down),
            "the down token's captured teardown burst collapses its book"
        );

        // The concurrent live window, fed its real deltas, never collapses a fresh detector.
        for token in [&new.token_up, &new.token_down] {
            let mut detector = CollapseDetector::default();
            for line in COLLAPSE.lines() {
                for frame in frames_in(line) {
                    let PolyFrame::PriceChange(change) = frame else {
                        continue;
                    };
                    for delta in &change.changes {
                        if *delta.asset_id == **token {
                            let update = LevelUpdate {
                                side: delta.side,
                                price: delta.level.price,
                                qty: delta.level.qty,
                                exchange_ts_us: change.exchange_ts_us,
                            };
                            assert_ne!(
                                detector.observe(update),
                                CollapseSignal::Collapsed,
                                "the live window must not collapse"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn collapse_fixture_carries_the_teardown_burst() {
        let replay = Replay::of(COLLAPSE);
        assert_eq!(replay.fatal, 0);
        assert_eq!(
            replay.parse_errors, 0,
            "the collapse fixture parses cleanly"
        );
        assert!(
            replay.zero_removals >= 50,
            "the collapse burst (size-0 level removals) must be present: {} removals",
            replay.zero_removals
        );
    }
}
