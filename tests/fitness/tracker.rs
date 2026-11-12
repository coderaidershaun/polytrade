//! MicroTracker fitness: the volume clock samples its configured field once per closed bar; a fixed
//! event replay produces byte-identical tracker state across runs against hand-computed
//! microprice/imbalance/candle/sample values; and `on_rotation` wipes the tracker back to fresh.
//! Also pins `realised_vol_per_sec`, which reads those candle closes — its 1/sqrt(Δt) scaling is
//! what makes venues sampled on different bucket sizes comparable.

use polysim::config::{
    CandlesSpec, ImbalanceSpec, KlineInterval, SpinField, SpinSampledSpec, TrackerSpec,
    VolumeBarsSpec, VolumeThreshold, WindowsSpec,
};
use polysim::hot::book::{Book, SnapshotOutcome};
use polysim::hot::quant::volatility::{Returns, realised_vol_per_sec};
use polysim::hot::tracker::{MicroTracker, SideFilter};
use polysim::ids::{Price, Side};
use polysim::time::DurationUs;
use proptest::prelude::*;

use crate::engine_support::{ONE, kline, snapshot_pair, trade};

fn book_with(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Book {
    let (bid_chunk, ask_chunk) = snapshot_pair(0, bids, asks, 0);
    let mut book = Book::new(16);
    let first = book.apply_snapshot_chunk(&bid_chunk);
    let second = book.apply_snapshot_chunk(&ask_chunk);
    assert_eq!(
        (first, second),
        (SnapshotOutcome::Clean, SnapshotOutcome::Clean)
    );
    book
}

fn close_to(actual: f64, expected: f64, tol: f64) -> bool {
    (actual - expected).abs() <= tol
}

proptest! {
    /// The sampled series has to stay in step with the bars cutting it, so the count is taken
    /// against the traded-notional oracle rather than against the bars themselves: a clock closing
    /// the wrong number of bars would otherwise carry the sample count along with it and still
    /// agree. The bar arithmetic is `volume_bars::volume_bars_split_exactly_and_conserve`'s; what
    /// this adds is the book, so there is a microprice to sample at all. `keep`/`window` sit above
    /// the 2400-bar worst case here, so nothing rolls out of either.
    #[test]
    fn every_closed_bar_samples_its_field_once(
        threshold_usd in 1u64..=4,
        trades in prop::collection::vec(
            (
                1_000_000i64..=ONE,
                1_000_000i64..=20 * ONE,
                prop_oneof![Just(Side::Buy), Just(Side::Sell)],
            ),
            0..120,
        ),
    ) {
        let spec = TrackerSpec {
            volume_bars: Some(VolumeBarsSpec {
                threshold: VolumeThreshold::Fixed(threshold_usd),
                keep: 4096,
                sampled: Some(SpinSampledSpec {
                    fields: vec![SpinField::Microprice],
                    window: 4096,
                }),
            }),
            ..TrackerSpec::default()
        };
        let mut tracker = MicroTracker::new(&spec, &[], None);
        tracker.on_book(&book_with(&[(100 * ONE, ONE)], &[(101 * ONE, ONE)]));

        let target_mantissa = i128::from(threshold_usd) * i128::from(ONE);
        let mut cumulative: i128 = 0;
        for (i, &(price, qty, side)) in trades.iter().enumerate() {
            cumulative += i128::from(price) * i128::from(qty) / i128::from(ONE);
            tracker.on_trade(&trade(0, price, qty, side, i as i64));
        }

        let expected = (cumulative / target_mantissa) as usize;
        let samples = tracker
            .volume_sampled(SpinField::Microprice)
            .map_or(0, |queue| queue.len());
        prop_assert_eq!(samples, expected, "sample count != floor(cum_notional / target)");
    }
}

fn hand_spec() -> TrackerSpec {
    TrackerSpec {
        trades_all: Some(WindowsSpec { windows: vec![4] }),
        microprice: Some(WindowsSpec { windows: vec![4] }),
        imbalance: Some(ImbalanceSpec {
            top_n: 3,
            windows: vec![4],
        }),
        candles: Some(CandlesSpec { keep: 8 }),
        spin_sampled: Some(SpinSampledSpec {
            fields: vec![
                SpinField::Microprice,
                SpinField::Imbalance,
                SpinField::BestBid,
            ],
            window: 4,
        }),
        volume_bars: Some(VolumeBarsSpec {
            threshold: VolumeThreshold::Fixed(1),
            keep: 4,
            sampled: Some(SpinSampledSpec {
                fields: vec![SpinField::Microprice],
                window: 4,
            }),
        }),
        ..TrackerSpec::default()
    }
}

fn replay(spec: &TrackerSpec) -> MicroTracker {
    let mut tracker = MicroTracker::new(spec, &[KlineInterval::OneMinute], None);
    let book = book_with(
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE)],
    );
    tracker.on_trade(&trade(0, 100 * ONE, ONE, Side::Buy, 1));
    tracker.on_trade(&trade(0, 100 * ONE + ONE / 2, 3 * ONE, Side::Sell, 2));
    tracker.on_book(&book);
    // A bar-closing trade after the book samples the current microprice onto the volume clock.
    tracker.on_trade(&trade(0, 100 * ONE, 1_000_000, Side::Buy, 3));
    tracker.on_kline(&kline(
        0,
        KlineInterval::OneMinute,
        (100 * ONE, 102 * ONE, 99 * ONE, 101 * ONE),
        false,
        60_000_000,
    ));
    tracker.on_kline(&kline(
        0,
        KlineInterval::OneMinute,
        (100 * ONE, 103 * ONE, 98 * ONE, 100 * ONE + ONE / 2),
        true,
        60_000_000,
    ));
    tracker.on_spin();
    tracker
}

/// A fixed replay is deterministic and lands on the hand-computed values. The imbalance case
/// uses `top_n = 3` against a one-level ask side, exercising the clamp.
#[test]
fn replay_is_deterministic_with_hand_values() {
    let spec = hand_spec();
    let first = replay(&spec);
    let second = replay(&spec);
    assert_eq!(first, second, "identical replay diverged");

    // best bid (100.0, 2.0), best ask (101.0, 1.0): microprice weights each price by the
    // opposite size, so it is (100*1 + 101*2) / 3.
    let microprice = first.last_microprice().expect("microprice computed");
    assert!(
        close_to(microprice, (100.0 * 1.0 + 101.0 * 2.0) / 3.0, 1e-9),
        "microprice {microprice}"
    );
    assert_eq!(
        first
            .microprice_series(4)
            .expect("microprice series")
            .last(),
        Some(microprice)
    );

    // top_n=3 over bids [2.0, 1.0] and asks [1.0]: (3 - 1) / (3 + 1) = 0.5.
    let imbalance = first.last_imbalance().expect("imbalance computed");
    assert!(close_to(imbalance, 0.5, 1e-12), "imbalance {imbalance}");
    assert_eq!(
        first.imbalance_series(4).expect("imbalance series").last(),
        Some(imbalance)
    );

    let candles = first
        .candles(KlineInterval::OneMinute)
        .expect("candles configured");
    assert_eq!(candles.open, None, "open slot cleared on close");
    let last = candles.closed.last().expect("one closed candle");
    assert_eq!(last.close, Price(100 * ONE + ONE / 2));
    assert_eq!(last.high, Price(103 * ONE));

    let best_bid_sample = first
        .spin_sampled(SpinField::BestBid)
        .expect("spin best_bid series")
        .last()
        .expect("one spin sample");
    assert!(
        close_to(best_bid_sample, 100.0, 1e-9),
        "best bid sample {best_bid_sample}"
    );

    // The bar-closing trade after the book sampled the current microprice onto the volume clock.
    let sampled = first
        .volume_sampled(SpinField::Microprice)
        .expect("volume-clock series")
        .last()
        .expect("one volume-clock sample");
    assert!(
        close_to(sampled, microprice, 1e-9),
        "volume-clock sample {sampled}"
    );

    assert_eq!(
        first
            .trades_price(SideFilter::All, 4)
            .expect("trades_all price")
            .len(),
        3
    );
    assert!(
        first.trades_price(SideFilter::Buy, 4).is_none(),
        "trades_buy not configured"
    );
}

fn full_spec() -> TrackerSpec {
    TrackerSpec {
        trades_all: Some(WindowsSpec {
            windows: vec![4, 16],
        }),
        trades_buy: Some(WindowsSpec { windows: vec![4] }),
        trades_sell: Some(WindowsSpec { windows: vec![4] }),
        microprice: Some(WindowsSpec { windows: vec![4] }),
        imbalance: Some(ImbalanceSpec {
            top_n: 3,
            windows: vec![4],
        }),
        candles: Some(CandlesSpec { keep: 8 }),
        spin_sampled: Some(SpinSampledSpec {
            fields: vec![SpinField::Microprice, SpinField::BestBid],
            window: 4,
        }),
        volume_bars: Some(VolumeBarsSpec {
            threshold: VolumeThreshold::Fixed(1),
            keep: 4,
            sampled: Some(SpinSampledSpec {
                fields: vec![SpinField::Microprice],
                window: 4,
            }),
        }),
        ..TrackerSpec::default()
    }
}

/// `on_rotation` returns every configured series, the latest values and the trade-side state to the
/// physical state right after `new`, by clearing the queues and resetting the clocks. Replay
/// determinism leans on that equality.
/// The `unconfigured_klines` diagnostic is a lifetime counter, not window state, so it survives.
#[test]
fn on_rotation_clears_tracker_to_fresh() {
    let spec = full_spec();
    let intervals = [KlineInterval::OneMinute];
    let mut tracker = MicroTracker::new(&spec, &intervals, None);
    let book = book_with(
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE)],
    );

    // Populate every series, both clocks, latest, and trade-side state.
    tracker.on_book(&book);
    for i in 0..6i64 {
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
        tracker.on_trade(&trade(0, 100 * ONE, 2_000_000, side, i));
    }
    tracker.on_kline(&kline(
        0,
        KlineInterval::OneMinute,
        (100 * ONE, 102 * ONE, 99 * ONE, 101 * ONE),
        true,
        60_000_000,
    ));
    tracker.on_spin();
    assert_ne!(
        tracker,
        MicroTracker::new(&spec, &intervals, None),
        "setup must populate state — else cleared==fresh passes vacuously"
    );

    tracker.on_rotation();
    assert_eq!(
        tracker,
        MicroTracker::new(&spec, &intervals, None),
        "rotation wipes the tracker back to its post-new physical state"
    );

    // The unconfigured-interval diagnostic is a lifetime counter, not window state: it survives.
    tracker.on_kline(&kline(
        0,
        KlineInterval::FiveMinutes,
        (100 * ONE, 100 * ONE, 100 * ONE, 100 * ONE),
        true,
        0,
    ));
    assert_eq!(
        tracker.unconfigured_kline_count(),
        1,
        "unconfigured kline counted"
    );
    tracker.on_rotation();
    assert_eq!(
        tracker.unconfigured_kline_count(),
        1,
        "diagnostic counter survives rotation"
    );
}

fn secs(count: i64) -> DurationUs {
    DurationUs::from_micros(count * 1_000_000)
}

/// Hand-computed against the population stdev of consecutive returns, then divided by
/// `sqrt(interval_secs)`. Absolute returns of `[100, 110, 99]` are `+10, -11` — mean `-0.5`,
/// both deviations `10.5`, so stdev `10.5`. Log returns of `[1, e, 1]` are `+1, -1` — stdev `1`.
#[test]
fn realised_vol_is_per_second_and_skips_unusable_closes() {
    let absolute = realised_vol_per_sec(
        [100.0, 110.0, 99.0].into_iter(),
        Returns::Absolute,
        secs(10),
    );
    assert!(
        close_to(absolute.expect("two returns"), 10.5 / 10.0f64.sqrt(), 1e-12),
        "absolute-return stdev divided by sqrt(10s)"
    );

    let log = realised_vol_per_sec(
        [1.0, std::f64::consts::E, 1.0].into_iter(),
        Returns::Log,
        secs(60),
    );
    assert!(
        close_to(log.expect("two returns"), 1.0 / 60.0f64.sqrt(), 1e-12),
        "log-return stdev divided by sqrt(60s)"
    );

    // Poly closes every 10s and binance every 60s must land on one comparable unit: the SAME
    // closes at the two spacings give equal raw stdevs; WITH the sqrt(interval) scaling the 10s
    // figure reads sqrt(6) hotter, the honest per-second reading of the same moves packed tighter.
    let closes = [100.0, 110.0, 99.0, 101.0];
    let fast = realised_vol_per_sec(closes.into_iter(), Returns::Absolute, secs(10))
        .expect("three returns");
    let slow = realised_vol_per_sec(closes.into_iter(), Returns::Absolute, secs(60))
        .expect("three returns");
    assert!(
        close_to(fast / slow, 6.0f64.sqrt(), 1e-12),
        "the same closes at 10s vs 60s spacing differ by sqrt(6), got {fast} vs {slow}"
    );

    // Non-finite and non-positive closes are dropped before returns are taken, so the surviving
    // sequence is the clean one — a NaN would otherwise poison every later feature value.
    let dirty = realised_vol_per_sec(
        [100.0, 0.0, 110.0, f64::NAN, 99.0].into_iter(),
        Returns::Absolute,
        secs(10),
    );
    assert_eq!(
        dirty, absolute,
        "unusable closes drop out of the return chain"
    );

    assert_eq!(
        realised_vol_per_sec([100.0].into_iter(), Returns::Absolute, secs(10)),
        None,
        "one close yields no return"
    );
    assert_eq!(
        realised_vol_per_sec([100.0, f64::NAN].into_iter(), Returns::Log, secs(10)),
        None,
        "one usable close yields no return"
    );
}
