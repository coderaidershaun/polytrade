//! The shadow book and the collapse of a Polymarket book. A mismatch resets the book only after
//! three in a row, and a collapse needs a burst of removals at one venue stamp.

use std::collections::HashMap;
use std::sync::Arc;

use engine::msg::ids::{InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{BookChunkKind, Level, MessageBody};
use engine::msg::time::{LocalTs, VenueTs};
use engine::runtime::config::PolySeries;
use engine::venues::polymarket::actor::{DriverEffect, PolyDriverCore, SlotLegs};
use engine::venues::polymarket::discovery::PolySchedule;
use engine::venues::polymarket::parse::{PolyBook, PolyFrame, is_frameless, parse_market_frame};
use engine::venues::polymarket::rest::parse_events;
use engine::venues::polymarket::rotation::{OutcomeLeg, TokenId, WindowAssignment};
use engine::venues::polymarket::shadow::{BookFrameOutcome, BookMismatch, ShadowValidator};
use engine::venues::polymarket::teardown::{CollapseDetector, CollapseSignal, LevelUpdate};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use crate::engine_support::level;

const HEAD: &str = include_str!("../../../../fixtures/polymarket/poly_market.jsonl");
const COLLAPSE: &str = include_str!("../../../../fixtures/polymarket/poly_market_collapse.jsonl");
const GAMMA_CURRENT: &str =
    include_str!("../../../../fixtures/polymarket/gamma_event_current.json");
const GAMMA_NEXT: &str = include_str!("../../../../fixtures/polymarket/gamma_event_next.json");
const SERIES: PolySeries = PolySeries::BtcUpDown5m;
/// Inside the window of `gamma_event_current.json`, which opens at 1,784,449,200 s.
const RECEIVED_US: i64 = 1_784_449_240_000_000;
const WINDOW_START_US: i64 = 1_784_449_200_000_000;
const NEXT_WINDOW_START_US: i64 = 1_784_449_500_000_000;
const UP: InstrumentId = InstrumentId(0);

fn received() -> LocalTs {
    LocalTs::from_micros(RECEIVED_US)
}

/// 0.40 at the price scale, and 100 at the size scale.
const PRICE_040: i64 = 40_000_000;
const PRICE_045: i64 = 45_000_000;
const SIZE_100: i64 = 10_000_000_000;

fn bid_at(size: i64) -> [Level; 1] {
    [level(PRICE_040, size)]
}

fn asks() -> [Level; 1] {
    [level(PRICE_045, SIZE_100)]
}

/// The window of slot A that the driver hosts, with the up and down tokens given.
fn window(up: &str, down: &str, open: VenueTs, close: VenueTs) -> WindowAssignment {
    WindowAssignment {
        up: OutcomeLeg {
            instrument: UP,
            token: TokenId::from(up),
        },
        down: OutcomeLeg {
            instrument: InstrumentId(1),
            token: TokenId::from(down),
        },
        window_open_ts_us: open,
        window_close_ts_us: close,
        condition_id: Arc::from("cond"),
    }
}

/// A driver whose slot A hosts `window`, subscribed at `received()`.
fn driver_with(window: WindowAssignment) -> PolyDriverCore {
    let mut core = PolyDriverCore::new(
        [
            SlotLegs {
                up: UP,
                down: InstrumentId(1),
            },
            SlotLegs {
                up: InstrumentId(2),
                down: InstrumentId(3),
            },
        ],
        PolySchedule::BTC_5M,
    );
    let mut effects = Vec::new();
    core.on_window_resolved(received(), window, &mut |effect| effects.push(effect));
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, DriverEffect::Subscribe(_))),
        "the window subscribes at once: {effects:?}"
    );
    core
}

fn book_of(token: &str, bid_size: i64) -> PolyFrame {
    PolyFrame::Book(PolyBook {
        asset_id: token.into(),
        bids: bid_at(bid_size).to_vec(),
        asks: asks().to_vec(),
        exchange_ts_us: received().as_venue_calendar(),
        received_ts_us: received(),
    })
}

fn effects_of(core: &mut PolyDriverCore, frame: &PolyFrame) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    core.on_frame(received(), frame, &mut |effect| effects.push(effect));
    effects
}

fn is_snapshot(effect: &DriverEffect) -> bool {
    matches!(effect, DriverEffect::Emit(message)
        if matches!(&message.body, MessageBody::Book(chunk) if chunk.kind == BookChunkKind::Snapshot))
}

fn is_reset(effect: &DriverEffect) -> bool {
    matches!(effect, DriverEffect::Emit(message) if matches!(message.body, MessageBody::BookReset(_)))
}

/// The count of `Diverged` over a run of matches (`false`) and mismatches (`true`) of books
/// against the shadow.
fn diverged_count(steps: &[bool]) -> usize {
    let mut validator = ShadowValidator::default();
    let mut shadow_size = SIZE_100;
    assert_eq!(
        validator.on_venue_book(&bid_at(shadow_size), &asks()),
        BookFrameOutcome::ForwardSnapshot
    );
    let mut diverged = 0;
    for (step, &is_mismatch) in steps.iter().enumerate() {
        let size = if is_mismatch { shadow_size + 1 + step as i64 } else { shadow_size };
        if let BookFrameOutcome::Diverged(_) = validator.on_venue_book(&bid_at(size), &asks()) {
            diverged += 1;
            shadow_size = size;
        }
    }
    diverged
}

fn the_validator_resets_only_after_three_in_a_row() {
    let mut validator = ShadowValidator::default();
    assert_eq!(
        validator.on_venue_book(&bid_at(SIZE_100), &asks()),
        BookFrameOutcome::ForwardSnapshot
    );
    let size_90 = SIZE_100 / 10 * 9;
    let size_80 = SIZE_100 / 10 * 8;
    assert_eq!(
        validator.on_venue_book(&bid_at(size_90), &asks()),
        BookFrameOutcome::Suspect { streak: 1 },
        "B1"
    );
    assert_eq!(
        validator.on_venue_book(&bid_at(size_90), &asks()),
        BookFrameOutcome::Suspect { streak: 2 },
        "B2"
    );
    assert_eq!(
        validator.on_venue_book(&bid_at(size_90), &asks()),
        BookFrameOutcome::Diverged(BookMismatch {
            side: Side::Buy,
            shadow: Some(level(PRICE_040, SIZE_100)),
            venue: Some(level(PRICE_040, size_90)),
            shadow_levels: 1,
            venue_levels: 1,
        }),
        "B3 confirms the desync with its first mismatch"
    );
    assert_eq!(
        validator.on_venue_book(&bid_at(size_90), &asks()),
        BookFrameOutcome::Validated,
        "B4: the shadow is rebuilt from B3"
    );
    assert_eq!(
        validator.on_venue_book(&bid_at(size_80), &asks()),
        BookFrameOutcome::Suspect { streak: 1 },
        "B5"
    );
    assert_eq!(
        validator.on_venue_book(&bid_at(size_90), &asks()),
        BookFrameOutcome::Validated,
        "B6: a match ends the streak"
    );

    let mut validator = ShadowValidator::default();
    validator.on_venue_book(&bid_at(SIZE_100), &asks());
    let with_empty_level = [level(39_000_000, 0), level(PRICE_040, SIZE_100)];
    assert_eq!(
        validator.on_venue_book(&with_empty_level, &asks()),
        BookFrameOutcome::Validated,
        "a venue level of size 0 is no level"
    );
}

/// One cut that disagrees and then agrees is a cut caught in flight; a desync disagrees at each
/// cut and confirms on the third; a venue level of size 0 is no level.
fn a_cut_caught_in_flight_clears_and_a_desync_confirms() {
    let bids = [level(50_000_000, 100)];
    let asks = [level(51_000_000, 150)];
    let stale_bids = [level(50_000_000, 80)];
    let fresh = || {
        let mut validator = ShadowValidator::default();
        assert_eq!(
            validator.on_venue_book(&bids, &asks),
            BookFrameOutcome::ForwardSnapshot
        );
        validator
    };

    let mut validator = fresh();
    assert_eq!(
        validator.on_venue_book(&stale_bids, &asks),
        BookFrameOutcome::Suspect { streak: 1 }
    );
    assert_eq!(
        validator.on_venue_book(&bids, &asks),
        BookFrameOutcome::Validated
    );

    let mut validator = fresh();
    for streak in 1..=2 {
        assert_eq!(
            validator.on_venue_book(&stale_bids, &asks),
            BookFrameOutcome::Suspect { streak }
        );
    }
    assert!(matches!(
        validator.on_venue_book(&stale_bids, &asks),
        BookFrameOutcome::Diverged(_)
    ));
    assert_eq!(
        validator.on_venue_book(&stale_bids, &asks),
        BookFrameOutcome::Validated,
        "the shadow is rebuilt from the desynced book"
    );

    let mut validator = fresh();
    let venue_asks = [level(51_000_000, 150), level(52_000_000, 0)];
    assert_eq!(
        validator.on_venue_book(&bids, &venue_asks),
        BookFrameOutcome::Validated
    );
}

fn the_driver_forwards_no_unconfirmed_mismatch() {
    let open = VenueTs::from_micros(WINDOW_START_US);
    let close = VenueTs::from_micros(WINDOW_START_US + 300_000_000);
    let mut core = driver_with(window("up", "down", open, close));
    let size_90 = SIZE_100 / 10 * 9;

    let first = effects_of(&mut core, &book_of("up", SIZE_100));
    assert!(
        first.iter().any(is_snapshot),
        "the first book forwards a snapshot: {first:?}"
    );
    for streak in 1..=2 {
        assert_eq!(
            effects_of(&mut core, &book_of("up", size_90)),
            [DriverEffect::Suspect {
                instrument: UP,
                streak
            }],
            "an unconfirmed mismatch is a suspect, and forwards nothing"
        );
    }
    let effects = effects_of(&mut core, &book_of("up", size_90));
    assert!(
        is_reset(&effects[0]),
        "the third resets the book first: {effects:?}"
    );
    assert!(effects[1..effects.len() - 1].iter().all(is_snapshot));
    assert!(
        matches!(
            effects.last(),
            Some(DriverEffect::Diverged { instrument: UP, .. })
        ),
        "then the snapshot, then the divergence: {effects:?}"
    );
    assert_eq!(effects.iter().filter(|effect| is_reset(effect)).count(), 1);
}

/// FITNESS: a book of the venue that differs from the shadow resets the shadow only on the third
/// mismatch in a row, and a match ends the streak. A reset at one mismatch would churn the book on
/// each timing artefact, and a mismatch that reset nothing would leave a desync in the book.
#[test]
fn three_mismatches_in_a_row_reset_once() {
    the_validator_resets_only_after_three_in_a_row();
    TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    })
    .run(&prop::collection::vec(any::<bool>(), 0..60), |steps| {
        let expected: usize = steps
            .split(|is_mismatch| !is_mismatch)
            .map(|run| run.len() / 3)
            .sum();
        prop_assert_eq!(diverged_count(&steps), expected, "{:?}", steps);
        Ok(())
    })
    .unwrap_or_else(|error| panic!("{error}"));
    a_cut_caught_in_flight_clears_and_a_desync_confirms();
    the_driver_forwards_no_unconfirmed_mismatch();
}

fn update(side: Side, price: i64, qty: i64, venue_ts: i64) -> LevelUpdate {
    LevelUpdate {
        side,
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        exchange_ts_us: VenueTs::from_micros(venue_ts),
    }
}

/// A detector of a two-sided book: two bids and two asks at venue time 1,000.
fn two_sided() -> CollapseDetector {
    let mut detector = CollapseDetector::default();
    for update in [
        update(Side::Buy, 50_000_000, 100, 1_000),
        update(Side::Buy, 49_000_000, 200, 1_000),
        update(Side::Sell, 51_000_000, 150, 1_000),
        update(Side::Sell, 52_000_000, 250, 1_000),
    ] {
        assert_eq!(detector.observe(update), CollapseSignal::Quiet);
    }
    detector
}

fn a_burst_of_one_stamp_that_empties_a_side_collapses() {
    let mut detector = two_sided();
    assert_eq!(
        detector.observe(update(Side::Buy, 50_000_000, 0, 2_000)),
        CollapseSignal::Quiet
    );
    assert_eq!(
        detector.observe(update(Side::Buy, 49_000_000, 0, 2_000)),
        CollapseSignal::Collapsed,
        "the second removal at one stamp empties the bids"
    );
    assert!(detector.has_collapsed());

    let mut detector = two_sided();
    detector.observe(update(Side::Buy, 50_000_000, 0, 2_000));
    assert_eq!(
        detector.observe(update(Side::Buy, 49_000_000, 0, 2_001)),
        CollapseSignal::Quiet,
        "removals at two stamps are two cancels, not a burst"
    );
    assert!(!detector.has_collapsed());
}

/// One capture line as frames, with the venue's subscribe batch flattened.
fn frames_in(line: &str) -> Vec<PolyFrame> {
    match parse_market_frame(line, received()) {
        Ok(PolyFrame::Batch(frames)) => frames,
        Ok(frame) => vec![frame],
        Err(_) => Vec::new(),
    }
}

fn resting(side: Side, price: Price, exchange_ts_us: VenueTs) -> LevelUpdate {
    LevelUpdate {
        side,
        price,
        qty: Qty::from_mantissa(1),
        exchange_ts_us,
    }
}

/// The levels of `side` in the first book of `token` in the market capture: the real book that
/// the burst leaves standing on the side it does not wipe.
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

/// Replays the real teardown burst of a resolving token through a detector. The grace tail
/// carries only deltas for about 357 s, so the book before the collapse cannot be rebuilt from
/// frames; the burst names each level it wipes, so those are seeded, with the token's first real
/// book on the side the burst spares.
fn captured_burst_collapses(token: &str) -> bool {
    let mut by_ts: HashMap<i64, Vec<(Side, Price)>> = HashMap::new();
    for line in COLLAPSE.lines() {
        for frame in frames_in(line) {
            let PolyFrame::PriceChange(change) = frame else {
                continue;
            };
            for delta in &change.changes {
                if &*delta.asset_id == token && delta.level.qty == Qty::from_mantissa(0) {
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
    let burst_ts = VenueTs::from_micros(burst_micros);
    let seed_ts = VenueTs::from_micros(burst_micros - 1);
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
            qty: Qty::from_mantissa(0),
            exchange_ts_us: burst_ts,
        };
        detector.observe(removal) == CollapseSignal::Collapsed
    })
}

fn the_captured_burst_collapses_the_resolving_window_only() {
    let old = parse_events(SERIES, GAMMA_CURRENT, VenueTs::from_micros(WINDOW_START_US))
        .expect("current window parses");
    let new = parse_events(
        SERIES,
        GAMMA_NEXT,
        VenueTs::from_micros(NEXT_WINDOW_START_US),
    )
    .expect("next window parses");
    assert!(
        captured_burst_collapses(&old.token_up),
        "the up token's captured teardown burst collapses its book"
    );
    assert!(
        captured_burst_collapses(&old.token_down),
        "the down token's captured teardown burst collapses its book"
    );
    for token in [&new.token_up, &new.token_down] {
        let mut detector = CollapseDetector::default();
        for line in COLLAPSE.lines() {
            for frame in frames_in(line) {
                let PolyFrame::PriceChange(change) = frame else {
                    continue;
                };
                for delta in change
                    .changes
                    .iter()
                    .filter(|delta| *delta.asset_id == **token)
                {
                    let update = LevelUpdate {
                        side: delta.side,
                        price: delta.level.price,
                        qty: delta.level.qty,
                        exchange_ts_us: change.exchange_ts_us,
                    };
                    assert_ne!(
                        detector.observe(update),
                        CollapseSignal::Collapsed,
                        "the live window does not collapse"
                    );
                }
            }
        }
    }
}

fn the_collapse_capture_carries_the_burst() {
    let (mut parse_errors, mut zero_removals) = (0, 0);
    for line in COLLAPSE.lines().filter(|line| !is_frameless(line)) {
        let Ok(frame) = parse_market_frame(line, received()) else {
            parse_errors += 1;
            continue;
        };
        let frames = match frame {
            PolyFrame::Batch(frames) => frames,
            frame => vec![frame],
        };
        for frame in frames {
            if let PolyFrame::PriceChange(change) = frame {
                zero_removals += change
                    .changes
                    .iter()
                    .filter(|delta| delta.level.qty.mantissa() == 0)
                    .count();
            }
        }
    }
    assert_eq!(parse_errors, 0, "the collapse capture parses cleanly");
    assert!(
        zero_removals >= 50,
        "the collapse burst of size-0 removals is present: {zero_removals} removals"
    );
}

/// FITNESS: a book collapses only when removals at one venue stamp empty a side of a two-sided
/// book, as the venue tears a resolved market down. A collapse on scattered cancels would tear
/// down a live window, and a missed burst would hold a resolved one until its 404.
#[test]
fn collapse_needs_a_burst_of_one_stamp() {
    a_burst_of_one_stamp_that_empties_a_side_collapses();
    the_collapse_capture_carries_the_burst();
    the_captured_burst_collapses_the_resolving_window_only();
}

/// The counts that the market capture gave before the driver named its suspects: no book of the
/// capture differs from the shadow, so neither a suspect nor a reset.
const RECORDED_SUSPECTS: usize = 0;
const RECORDED_DIVERGENCES: usize = 0;

/// FITNESS: the recorded market capture, through the driver with its window assigned, gives the
/// recorded counts of suspects and resets. A leg that compared the books in another order, or
/// lost a delta on the way to its shadow, would see mismatches in a capture that has none.
#[test]
fn capture_gives_recorded_counts_of_suspect_and_diverged() {
    let market = parse_events(SERIES, GAMMA_CURRENT, VenueTs::from_micros(WINDOW_START_US))
        .expect("current window parses");
    let mut core = driver_with(window(
        &market.token_up,
        &market.token_down,
        market.window_open_ts_us,
        market.window_close_ts_us,
    ));
    let (mut suspects, mut divergences) = (0, 0);
    let mut forwarded = [false, false];
    for line in HEAD.lines().filter(|line| !is_frameless(line)) {
        let frame = parse_market_frame(line, received()).expect("each frame of the capture parses");
        for effect in effects_of(&mut core, &frame) {
            match &effect {
                DriverEffect::Suspect { .. } => suspects += 1,
                DriverEffect::Diverged { .. } => divergences += 1,
                DriverEffect::Emit(message) => {
                    if let MessageBody::Book(chunk) = &message.body
                        && chunk.kind == BookChunkKind::Snapshot
                    {
                        forwarded[usize::from(chunk.instrument.0)] = true;
                    }
                }
                _ => {}
            }
        }
    }
    assert_eq!(
        forwarded,
        [true, true],
        "the capture reaches both legs of the window"
    );
    assert_eq!(suspects, RECORDED_SUSPECTS, "suspects");
    assert_eq!(divergences, RECORDED_DIVERGENCES, "resets");
}
