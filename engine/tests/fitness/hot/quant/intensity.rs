//! The fill-intensity estimator, driven through the tracker, because it must read the top of book
//! from before the trade: the book after the trade gives an error that no test would see.

use super::Lcg;
use engine::hot::book::{Book, SnapshotOutcome};
use engine::hot::quant::intensity::{IntensityFit, PacedIntensity, SideEstimate};
use engine::hot::quant::optimise::NelderMead;
use engine::hot::tracker::MicroTracker;
use engine::msg::ids::{InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{BookChunk, BookChunkKind, Level, TradeEvent};
use engine::runtime::config::{IntensitySpec, TrackerSpec};

use crate::engine_support::{ts, venue_ts};

const TICK: Price = Price::from_mantissa(10);
const ASK: Price = Price::from_mantissa(100_000);
const BID: Price = Price::from_mantissa(ASK.mantissa() - TICK.mantissa());

fn snapshot_chunk(side: Side, price: Price, is_last_chunk: bool) -> BookChunk {
    let mut chunk = BookChunk::new(InstrumentId(0), BookChunkKind::Snapshot, side);
    chunk.push_levels(&[Level {
        price,
        qty: Qty::from_mantissa(1),
    }]);
    chunk.is_last_chunk = is_last_chunk;
    chunk.update_id = 1;
    chunk
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
        qty: Qty::from_mantissa(1),
        side,
        exchange_ts_us: venue_ts(when),
        exchange_sent_ts_us: None,
    }
}

fn buy(tracker: &mut MicroTracker, ticks: i64, when: i64) {
    tracker.on_trade(
        &ev(
            Side::Buy,
            Price::from_mantissa(ASK.mantissa() + ticks * TICK.mantissa()),
            when,
        ),
        ts(when),
    );
}

fn geometric(tracker: &mut MicroTracker, seed: u64, k: f64, count: i64, dt_us: i64) {
    let (q, mut lcg) = ((-k).exp(), Lcg(seed));
    for i in 0..count {
        let depth = (lcg.unit().max(1e-12).ln() / q.ln()).floor() as i64;
        buy(tracker, depth, i * dt_us);
    }
}

/// A print `ticks` behind the touch of the side that it takes. Prints with one exchange stamp and
/// one side are one sweep.
fn print_at(
    tracker: &mut MicroTracker,
    side: Side,
    ticks: i64,
    exchange_us: i64,
    received_us: i64,
) {
    let behind = ticks * TICK.mantissa();
    let price = match side {
        Side::Buy => ASK.mantissa() + behind,
        Side::Sell => BID.mantissa() - behind,
    };
    tracker.on_trade(
        &ev(side, Price::from_mantissa(price), exchange_us),
        ts(received_us),
    );
}

fn relative_error(value: f64, expected: f64) -> f64 {
    ((value - expected) / expected).abs()
}

/// The factor by which counts decay over `secs`, as the counts compute it: e^(−λ·t) with
/// λ = ln 2 / half-life.
fn decay_factor(half_life_secs: f64, secs: f64) -> f64 {
    (-(std::f64::consts::LN_2 / half_life_secs) * secs).exp()
}

/// The exposure, in seconds, that the counts gather over `secs` from a start with none, each
/// second weighted by its decay to the end: (1 − e^(−λ·t)) / λ.
fn exposure_from_zero(half_life_secs: f64, secs: f64) -> f64 {
    (1.0 - decay_factor(half_life_secs, secs)) / (std::f64::consts::LN_2 / half_life_secs)
}

/// The optimum of the likelihood of the fit, in closed form. Each print adds one to each bucket from
/// the touch to the bucket that it reached, and the last bucket also holds each print beyond it.
/// With T the exposure, N the decayed count at the touch, S the sum of the counts behind the touch,
/// and n the count in the last bucket, the negative log likelihood of the fit is
/// T·A − N·ln(T·A) + S·k − (N − n)·ln(1 − e^(−k)), so its minimum is at A = N / T and
/// e^k = 1 + (N − n) / S. Each print is given as the bucket that it reached and its decay weight.
struct ClosedForm {
    a_per_sec: f64,
    k_per_tick: f64,
    events: f64,
}

fn closed_form(reached: &[(usize, f64)], max_depth: usize, exposure: f64) -> ClosedForm {
    let last = max_depth - 1;
    let touch: f64 = reached.iter().map(|&(_, weight)| weight).sum();
    let behind: f64 = reached
        .iter()
        .map(|&(bucket, weight)| weight * bucket.min(last) as f64)
        .sum();
    let in_last: f64 = reached
        .iter()
        .filter(|&&(bucket, _)| bucket >= last)
        .map(|&(_, weight)| weight)
        .sum();
    ClosedForm {
        a_per_sec: touch / exposure,
        k_per_tick: (1.0 + (touch - in_last) / behind).ln(),
        events: touch,
    }
}

/// One print more or less in any bucket of the tapes below moves A or k by more than 5%, and the
/// search stops once its simplex spans less than 1e-8 in each log parameter, so this tolerance tells
/// a wrong count from the stop of the search.
const FIT_TOLERANCE: f64 = 1e-4;

fn assert_fits(case: &str, estimate: Option<SideEstimate>, expected: &ClosedForm) {
    let estimate = estimate.unwrap_or_else(|| panic!("{case}: no fit"));
    assert!(!estimate.is_stale, "{case}: the fit is stale");
    assert!(
        relative_error(estimate.events, expected.events) < 1e-12,
        "{case}: {} prints at the touch, not {}",
        estimate.events,
        expected.events
    );
    assert!(
        relative_error(estimate.a_per_sec, expected.a_per_sec) < FIT_TOLERANCE,
        "{case}: A {} per second, not {}",
        estimate.a_per_sec,
        expected.a_per_sec
    );
    assert!(
        relative_error(estimate.k_per_tick, expected.k_per_tick) < FIT_TOLERANCE,
        "{case}: k {} per tick, not {}",
        estimate.k_per_tick,
        expected.k_per_tick
    );
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

    // The clamped print still counts at the touch: beside five prints there, the fit reads six.
    for exchange_us in 1..=5 {
        print_at(&mut tracker, Side::Buy, 0, exchange_us, 0);
    }
    let counts = tracker.intensity().expect("intensity configured");
    let ask = IntensityFit::default()
        .fit(counts, ts(1_000_000))
        .ask
        .expect("six prints at the touch reach the floor of five");
    let expected = 6.0 * decay_factor(600.0, 1.0);
    assert!(
        relative_error(ask.events, expected) < 1e-12,
        "{} prints at the touch, not {expected}: the print inside the spread was not counted there",
        ask.events
    );

    // A print with no book to anchor against cannot be bucketed at all.
    tracker.on_book_reset();
    tracker.on_trade(&ev(Side::Sell, BID, 1_000_000), ts(1_000_000));
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

    // A warm refit starts from the cached optimum. It must converge before the iteration cap and
    // meet the bounds of the cold fit.
    let warm = fit.fit(counts, now).ask.expect("warm");
    assert!(
        warm.converged && warm.iterations < NelderMead::<2>::MAX_ITERATIONS,
        "warm refit ran to its cap: {warm:?}"
    );
    assert!(!warm.is_stale);
    assert!(
        (warm.k_per_tick - k_true).abs() < 0.08,
        "warm k {}",
        warm.k_per_tick
    );
    assert!(
        (warm.a_per_sec / a_true - 1.0).abs() < 0.2,
        "warm a {}",
        warm.a_per_sec
    );
}

/// The counts are private, so these pins read them through the fit, whose optimum on each designed
/// tape has a closed form. A print counted in the wrong bucket, a sweep counted more than once, a
/// decay that does not run, or a pooled fit against the exposure of one side would each move A, k,
/// or the count at the touch, and the quotes would follow with no error. Each tape is received at
/// one stamp, so one decay factor scales each count, except where a case says otherwise.
#[test]
fn designed_tapes_fit_their_closed_form() {
    let one_second = ts(1_000_000);
    let reached = |buckets: &[usize], weight: f64| -> Vec<(usize, f64)> {
        buckets.iter().map(|&bucket| (bucket, weight)).collect()
    };

    // Reach buckets: each print fills each bucket from the touch to its own depth.
    let depths = [0, 1, 1, 2, 3, 0, 0, 2];
    let mut tape = tracker(8, 60.0);
    for (exchange_us, &depth) in (1..).zip(&depths) {
        print_at(&mut tape, Side::Buy, depth as i64, exchange_us, 0);
    }
    let estimate = IntensityFit::default().fit(tape.intensity().expect("configured"), one_second);
    let expected = closed_form(
        &reached(&depths, decay_factor(60.0, 1.0)),
        8,
        exposure_from_zero(60.0, 1.0),
    );
    assert_fits("reach buckets", estimate.ask, &expected);
    assert!(estimate.bid.is_none(), "a buy print reached the bid side");

    // The last bucket holds each print that reached it or went beyond it.
    let depths = [0, 1, 5, 0, 2, 9];
    let mut tape = tracker(4, 60.0);
    for (exchange_us, &depth) in (1..).zip(&depths) {
        print_at(&mut tape, Side::Buy, depth as i64, exchange_us, 0);
    }
    let estimate = IntensityFit::default().fit(tape.intensity().expect("configured"), one_second);
    let expected = closed_form(
        &reached(&depths, decay_factor(60.0, 1.0)),
        4,
        exposure_from_zero(60.0, 1.0),
    );
    assert_fits("last bucket", estimate.ask, &expected);

    // A sweep is one print that reached its deepest level: the prints at depths 0, 2, 1 and 3 with
    // one exchange stamp count once, to bucket 3. A new stamp starts a new print.
    let mut tape = tracker(8, 60.0);
    for exchange_us in 1..=5 {
        print_at(&mut tape, Side::Buy, 0, exchange_us, 0);
    }
    for depth in [0, 2, 1, 3] {
        print_at(&mut tape, Side::Buy, depth, 6, 0);
    }
    print_at(&mut tape, Side::Buy, 1, 7, 0);
    let estimate = IntensityFit::default().fit(tape.intensity().expect("configured"), one_second);
    let expected = closed_form(
        &reached(&[0, 0, 0, 0, 0, 3, 1], decay_factor(60.0, 1.0)),
        8,
        exposure_from_zero(60.0, 1.0),
    );
    assert_fits("sweep", estimate.ask, &expected);

    // Decay between prints: the prints received one half-life before the second batch weigh
    // e^(−ln 2), and the fit reads at the stamp of the second batch, where nothing has decayed yet.
    let depths = [0, 1, 0, 1, 0, 1];
    let mut tape = tracker(8, 2.0);
    for (exchange_us, &depth) in (1..).zip(&depths) {
        print_at(&mut tape, Side::Buy, depth as i64, exchange_us, 0);
    }
    for (exchange_us, &depth) in (7..).zip(&depths) {
        print_at(&mut tape, Side::Buy, depth as i64, exchange_us, 2_000_000);
    }
    let estimate =
        IntensityFit::default().fit(tape.intensity().expect("configured"), ts(2_000_000));
    let mut weighted = reached(&depths, decay_factor(2.0, 2.0));
    weighted.extend(reached(&depths, 1.0));
    let expected = closed_form(&weighted, 8, exposure_from_zero(2.0, 2.0));
    assert_fits("decay between prints", estimate.ask, &expected);

    // The pooled slot counts both sides against twice the exposure, so its A stays the rate of one
    // side.
    let buys = [0, 1, 2, 0, 1, 0];
    let sells = [0, 0, 1, 3, 0, 2];
    let mut tape = tracker(8, 60.0);
    for (exchange_us, &depth) in (1..).zip(&buys) {
        print_at(&mut tape, Side::Buy, depth as i64, exchange_us, 0);
    }
    for (exchange_us, &depth) in (7..).zip(&sells) {
        print_at(&mut tape, Side::Sell, depth as i64, exchange_us, 0);
    }
    let counts = tape.intensity().expect("configured");
    let mut fit = IntensityFit::default();
    let fresh = fit.fit(counts, one_second);
    let (weight, exposure) = (decay_factor(60.0, 1.0), exposure_from_zero(60.0, 1.0));
    assert_fits(
        "pooled, ask",
        fresh.ask,
        &closed_form(&reached(&buys, weight), 8, exposure),
    );
    assert_fits(
        "pooled, bid",
        fresh.bid,
        &closed_form(&reached(&sells, weight), 8, exposure),
    );
    let mut both = reached(&buys, weight);
    both.extend(reached(&sells, weight));
    assert_fits("pooled", fresh.all, &closed_form(&both, 8, 2.0 * exposure));

    // The stale path: ten half-lives later each slot holds far fewer than five prints at the touch,
    // so each gives its last fit, marked stale, which a reader drops.
    let stale = fit.fit(counts, ts(600_000_000));
    for (slot, fresh, stale) in [
        ("bid", fresh.bid, stale.bid),
        ("ask", fresh.ask, stale.ask),
        ("pooled", fresh.all, stale.all),
    ] {
        let fresh = fresh.expect("a fresh fit");
        assert_eq!(
            stale,
            Some(SideEstimate {
                is_stale: true,
                ..fresh
            }),
            "{slot}: the stale fit is not the last fit, re-dated"
        );
        assert!(stale.and_then(SideEstimate::fresh).is_none(), "{slot}");
    }
}

/// A source that configures no reach histogram gets no cadence either, and the two halves must
/// agree: the tracker hands out no counts, so a cadence-less pacer is never in a position to fit.
/// `None` is therefore NEVER — a zero interval would instead be due on every single call, and the
/// second half of this pin is the one that would catch that reading.
#[test]
fn a_pacer_with_no_cadence_never_fits() {
    let mut unconfigured = MicroTracker::new(&TrackerSpec::default(), &[], Some(TICK));
    unconfigured.on_trade(&ev(Side::Buy, ASK, 0), ts(0));
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

/// A paced fit waits a tenth of the half-life of its counts, to the nearest microsecond. A cast
/// alone rounds toward zero, so it loses one microsecond where the product falls just below a whole
/// one, and each refit then comes that much early.
#[test]
fn refit_interval_rounds_to_the_nearest_microsecond() {
    let interval = |half_life_secs: f64| {
        PacedIntensity::refit_interval(&IntensitySpec {
            half_life_secs,
            ..IntensitySpec::default()
        })
        .micros()
    };
    // A tenth of each half-life, by hand: 120 s gives 12 s, 0.29 s gives 0.029 s, and 0.071 s
    // gives 0.0071 s. In binary the products of the last two fall just below 29,000 and 7,100.
    assert_eq!(interval(120.0), 12_000_000);
    assert_eq!(interval(0.29), 29_000);
    assert_eq!(interval(0.071), 7_100);

    // Half-lives from 1 ms to 1 day, drawn evenly in their logarithm.
    let (shortest, longest) = (1e-3f64.ln(), 86_400f64.ln());
    let mut lcg = Lcg(0x5EED_0071);
    for _ in 0..10_000 {
        let half_life_secs = (shortest + (longest - shortest) * lcg.unit()).exp();
        let product = half_life_secs * 0.1 * 1e6;
        let micros = interval(half_life_secs);
        assert!(
            (micros as f64 - product).abs() <= 0.5,
            "half-life {half_life_secs} s: {micros} us is not the microsecond nearest {product}"
        );
    }
}
