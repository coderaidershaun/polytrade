//! Guéant fill-intensity estimator (A, k) over the exponentially-decayed reach histogram. Driven
//! through `MicroTracker`, because the histogram is anchored on the tracker's PRE-trade top of book
//! and that anchoring is half the calculation: an estimator fed the post-trade book reports a fill
//! intensity that is wrong in one direction all day and errors nowhere.
//!
//! The reach histograms are private with no accessor, so only the two anomaly counters are pinned
//! directly, and buy-to-ask routing is pinned through `estimate.bid.is_none()`. The bucket
//! contents (per-depth fill, sweep grouping by `(side, exchange_ts)`, the shallower-continuation
//! rule, decay between groups, the clamp of an inside-spread print to bucket 0) and the closed-form
//! geometric MLE cross-check of the fit are not checked from outside the crate.

use super::Lcg;
use polysim::config::{IntensitySpec, TrackerSpec};
use polysim::hot::book::{Book, SnapshotOutcome};
use polysim::hot::quant::intensity::{IntensityFit, PacedIntensity};
use polysim::hot::tracker::MicroTracker;
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::{BOOK_CHUNK_LEVELS, BookChunk, BookChunkKind, Level, TradeEvent};

use crate::engine_support::ts;

const TICK: Price = Price(10);
const ASK: Price = Price(100_000);
const BID: Price = Price(ASK.0 - TICK.0);

fn snapshot_chunk(side: Side, price: Price, is_last_chunk: bool) -> BookChunk {
    let mut levels = [Level {
        price: Price(0),
        qty: Qty(0),
    }; BOOK_CHUNK_LEVELS];
    levels[0] = Level { price, qty: Qty(1) };
    BookChunk {
        instrument: InstrumentId(0),
        kind: BookChunkKind::Snapshot,
        side,
        levels,
        len: 1,
        is_last_chunk,
        update_id: 1,
        exchange_ts_us: None,
        received_ts_us: ts(0),
        queued_ts_us: ts(0),
    }
}

/// Tracker configured with intensity alone, its top of book already at BID/ASK.
fn tracker(max_depth_ticks: usize, half_life_secs: f64) -> MicroTracker {
    let spec = TrackerSpec {
        intensity: Some(IntensitySpec {
            max_depth_ticks,
            half_life_secs,
            min_events: 5.0,
        }),
        ..TrackerSpec::default()
    };
    let mut book = Book::new(4);
    for chunk in [
        snapshot_chunk(Side::Buy, BID, false),
        snapshot_chunk(Side::Sell, ASK, true),
    ] {
        assert_eq!(book.apply_snapshot_chunk(&chunk), SnapshotOutcome::Clean);
    }
    let mut tracker = MicroTracker::new(&spec, &[], Some(TICK));
    tracker.on_book(&book);
    tracker
}

fn ev(side: Side, price: Price, when: i64) -> TradeEvent {
    TradeEvent {
        instrument: InstrumentId(0),
        price,
        qty: Qty(1),
        side,
        exchange_ts_us: ts(when),
        exchange_sent_ts_us: None,
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    }
}

fn buy(tracker: &mut MicroTracker, ticks: i64, when: i64) {
    tracker.on_trade(&ev(Side::Buy, Price(ASK.0 + ticks * TICK.0), when));
}

fn geometric(tracker: &mut MicroTracker, seed: u64, k: f64, count: i64, dt_us: i64) {
    let (q, mut lcg) = ((-k).exp(), Lcg(seed));
    for i in 0..count {
        let depth = (lcg.unit().max(1e-12).ln() / q.ln()).floor() as i64;
        buy(tracker, depth, i * dt_us);
    }
}

#[test]
fn prints_inside_the_spread_or_without_a_book_are_counted_as_anomalies() {
    let mut tracker = tracker(8, 600.0);

    // Buy below the ask = stale top of book, clamped to bucket 0 and counted.
    buy(&mut tracker, -1, 0);
    assert_eq!(
        tracker
            .intensity()
            .expect("intensity configured")
            .inside_spread_count(),
        1
    );

    // A print with no book to anchor against cannot be bucketed at all.
    tracker.on_book_reset();
    tracker.on_trade(&ev(Side::Sell, BID, 1_000_000));
    assert_eq!(
        tracker
            .intensity()
            .expect("intensity configured")
            .without_book_count(),
        1
    );
}

#[test]
fn fit_recovers_a_and_k_and_warm_starts() {
    let (a_true, k_true) = (10.0, 0.5);
    let dt_us = (1e6 / a_true) as i64;
    let mut tracker = tracker(32, 1e9);
    geometric(&mut tracker, 0xC0FF_EE00, k_true, 4000, dt_us);

    let now = ts(3999 * dt_us);
    let counts = tracker.intensity().expect("intensity configured");
    let mut fit = IntensityFit::default();
    let estimate = fit.fit(counts, now);
    // Buy prints route to the ask side and nowhere else.
    assert!(estimate.bid.is_none(), "a buy print reached the bid side");
    let cold = estimate.ask.expect("cold");
    assert!(
        (cold.k_per_tick - k_true).abs() < 0.08,
        "k {}",
        cold.k_per_tick
    );
    assert!(
        (cold.a_per_sec / a_true - 1.0).abs() < 0.2,
        "a {}",
        cold.a_per_sec
    );

    // A warm refit seeds from the cache, so it takes no more iterations than the cold fit.
    let warm = fit.fit(counts, now).ask.expect("warm");
    assert!(warm.iterations <= cold.iterations, "warm not warmer");
    assert!(!warm.is_stale);
}

/// A source that configures no reach histogram gets no cadence either, and the two halves must
/// agree: the tracker hands out no counts, so a cadence-less pacer is never in a position to fit.
/// `None` is therefore NEVER — a zero interval would instead be due on every single call, and the
/// second half of this pin is the one that would catch that reading.
#[test]
fn a_pacer_with_no_cadence_never_fits() {
    let mut unconfigured = MicroTracker::new(&TrackerSpec::default(), &[], Some(TICK));
    unconfigured.on_trade(&ev(Side::Buy, ASK, 0));
    assert!(
        unconfigured.intensity().is_none(),
        "no reach histogram configured, so there are no counts to pace against"
    );

    let mut configured = tracker(8, 600.0);
    for event in 0..10 {
        buy(&mut configured, 0, event * 1_000_000);
    }
    let mut pacer = PacedIntensity::new(None);
    assert!(
        pacer
            .refit(configured.intensity(), ts(10_000_000))
            .is_none(),
        "a pacer with no cadence fitted counts it was never asked to pace against"
    );
}
