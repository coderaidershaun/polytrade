//! Volume-bar fitness: the traded-notional clock cuts every closed bar to exactly its target and
//! loses neither notional nor trades doing it, exactly rather than approximately; a klines target
//! arms only on a trailing average it can stand behind and never moves under an open bar; and the
//! dispatch fan-out hands the strategy exactly the bars that closed, oldest first.

use polysim::config::{CandlesSpec, KlineInterval, TrackerSpec, VolumeBarsSpec, VolumeThreshold};
use polysim::hot::strategy::{Registration, Strategy, StrategyCtx};
use polysim::hot::tracker::{MicroTracker, VolumeBar, VolumeBarSeries};
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::{InboundMessage, KlineEvent};
use polysim::msg::persist::{FeatureId, PersistRecord};
use polysim::time::TsUs;
use proptest::prelude::*;
use rtrb::Consumer;

use crate::engine_support::{
    ONE, engine_without_warmup, instrument_row, metrics_ring, persist_ring, pop, strategy_log_ring,
    tracker_spec_all, trade, ts,
};

/// Closed-bar retention above the worst case every generator below can produce, so nothing is
/// evicted and the conservation equalities stay exact.
const AMPLE_KEEP: usize = 4096;

fn fixed_spec(threshold_usd: u64) -> TrackerSpec {
    // No `sampled` block: field sample parity is the sibling `tracker::every_closed_bar_samples_its_field_once`
    // test's job (it seeds a book so the microprice is present). This one pins the bar arithmetic.
    TrackerSpec {
        volume_bars: Some(VolumeBarsSpec {
            threshold: VolumeThreshold::Fixed(threshold_usd),
            keep: AMPLE_KEEP,
            sampled: None,
        }),
        ..TrackerSpec::default()
    }
}

/// Filled notional and booked trade arrivals across the whole clock — closed bars plus the one
/// still filling. What conservation is measured over.
fn totals(series: &VolumeBarSeries) -> (i128, u64) {
    let mut notional = 0i128;
    let mut count = 0u64;
    for bar in series.closed.iter().chain(series.open) {
        notional += i128::from(bar.buy_notional) + i128::from(bar.sell_notional);
        count += u64::from(bar.trade_arrivals);
    }
    (notional, count)
}

proptest! {
    /// The whole exactness contract in one stream. Every closed bar holds precisely its target and
    /// the open one strictly less; no notional and no trade is lost or double-counted; and with a
    /// fixed target the bar count IS `floor(cumulative / target)`, which only exact splitting gives
    /// — first-crossing-takes-all would drop the excess and undercount.
    ///
    /// Worst case, stated so the equalities are provably eviction-free: 120 trades of at most
    /// 1.0 × 20.0 = 20 USD is 2400 USD of notional, which the smallest target (1 USD) cuts into at
    /// most 2400 bars — well under [`AMPLE_KEEP`].
    #[test]
    fn volume_bars_split_exactly_and_conserve(
        threshold_usd in 1u64..=4,
        trades in prop::collection::vec(
            (
                1_000_000i64..=ONE,
                // from zero: a dust trade with no notional at all still books its count
                0i64..=20 * ONE,
                prop_oneof![Just(Side::Buy), Just(Side::Sell)],
            ),
            0..120,
        ),
    ) {
        let mut tracker = MicroTracker::new(&fixed_spec(threshold_usd), &[], None);
        let target = i128::from(threshold_usd) * i128::from(ONE);

        let mut fed_notional: i128 = 0;
        let mut reported = 0usize;
        for (index, &(price, qty, side)) in trades.iter().enumerate() {
            fed_notional += i128::from(price) * i128::from(qty) / i128::from(ONE);
            reported += tracker.on_trade(&trade(0, price, qty, side, index as i64));
        }

        let series = tracker.volume_bars().expect("volume clock configured");
        for bar in series.closed.iter() {
            prop_assert_eq!(
                i128::from(bar.buy_notional) + i128::from(bar.sell_notional),
                i128::from(bar.target),
                "a closed bar holds exactly its target"
            );
        }
        if let Some(open) = series.open {
            prop_assert!(
                open.buy_notional + open.sell_notional < open.target,
                "the open bar is strictly below its target, else it would have closed"
            );
        }

        let (filled, counted) = totals(series);
        prop_assert_eq!(filled, fed_notional, "notional conserved across the clock");
        prop_assert_eq!(counted, trades.len() as u64, "every trade booked to exactly one bar");

        let expected_bars = (fed_notional / target) as usize;
        prop_assert_eq!(series.closed.len(), expected_bars, "closed bars != floor(cumulative / target)");
        prop_assert_eq!(reported, expected_bars, "reported closures != bars actually closed");
    }
}

fn klines_spec(candles_keep: usize) -> TrackerSpec {
    TrackerSpec {
        candles: Some(CandlesSpec { keep: candles_keep }),
        volume_bars: Some(VolumeBarsSpec {
            threshold: VolumeThreshold::Klines,
            keep: AMPLE_KEEP,
            sampled: None,
        }),
        ..TrackerSpec::default()
    }
}

fn closed_1m_kline(open_ts_us: i64, quote_volume: i64) -> KlineEvent {
    KlineEvent {
        instrument: InstrumentId(0),
        interval: KlineInterval::OneMinute,
        open_ts_us: ts(open_ts_us),
        open: Price(100 * ONE),
        high: Price(100 * ONE),
        low: Price(100 * ONE),
        close: Price(100 * ONE),
        base_volume: Qty(ONE),
        quote_volume,
        trade_count: 1,
        is_closed: true,
        exchange_ts_us: ts(open_ts_us),
        exchange_sent_ts_us: None,
        received_ts_us: ts(open_ts_us),
        queued_ts_us: ts(open_ts_us),
    }
}

fn klines_tracker(count: usize, quote_volume: i64) -> MicroTracker {
    let mut tracker = MicroTracker::new(&klines_spec(200), &[KlineInterval::OneMinute], None);
    for index in 0..count {
        tracker.on_kline(&closed_1m_kline(index as i64 * 60_000_000, quote_volume));
    }
    tracker
}

/// A trade worth `usd` at unit price, so the notional reads straight off the argument.
fn worth(usd: i64, when: i64) -> polysim::msg::inbound::TradeEvent {
    trade(0, ONE, usd * ONE, Side::Buy, when)
}

/// The target the clock would cut a new bar against — observed the only way state exposes it, by
/// opening one. The probe trade is a single mantissa unit, far below any legal target, so it can
/// never close the bar it opens; `None` means the clock is dormant and opened nothing.
fn probe_target(tracker: &mut MicroTracker, when: i64) -> Option<i64> {
    tracker.on_trade(&trade(0, ONE, 1, Side::Buy, when));
    tracker
        .volume_bars()
        .and_then(|series| series.open)
        .map(|bar| bar.target)
}

/// A dormant klines clock accumulates NOTHING — bars cut against a target the history cannot yet
/// justify would be incomparable with every bar after it, so trades pour through untouched.
fn assert_dormant(tracker: &mut MicroTracker, when: i64, reason: &str) {
    let closed = tracker.on_trade(&worth(1_000, when));
    let series = tracker.volume_bars().expect("volume clock configured");
    assert_eq!(closed, 0, "{reason}: a dormant clock closes no bars");
    assert_eq!(series.closed.len(), 0, "{reason}: no bars cemented");
    assert_eq!(series.open, None, "{reason}: nothing accumulated");
}

/// The klines target is the mean quote volume of the trailing closed 1m candles, and the clock stays
/// dormant until that mean is worth standing behind: below 60 candles it is a guess, and below one
/// whole quote unit it is dust — a target that small would have one ordinary trade close thousands
/// of bars inside a single dispatch, each firing a strategy callback.
#[test]
fn klines_clock_arms_only_on_a_mean_it_can_stand_behind() {
    let mut short_history = klines_tracker(59, 10 * ONE);
    assert_dormant(&mut short_history, 1, "59 closed candles");

    let mut armed = klines_tracker(60, 10 * ONE);
    assert_eq!(
        probe_target(&mut armed, 1),
        Some(10 * ONE),
        "target is the mean of the trailing closed candles"
    );

    // Mixed history: the mean is taken over min(1440, available), so all 90 count here.
    let mut mixed = klines_tracker(60, 10 * ONE);
    for index in 60..90 {
        mixed.on_kline(&closed_1m_kline(index * 60_000_000, 40 * ONE));
    }
    assert_eq!(
        probe_target(&mut mixed, 1),
        Some((60 * 10 * ONE + 30 * 40 * ONE) / 90),
        "target is the mean over every available candle"
    );

    let mut silent = klines_tracker(120, 0);
    assert_dormant(&mut silent, 1, "an all-zero quote-volume history");

    let mut dust = klines_tracker(120, ONE - 1);
    assert_dormant(&mut dust, 1, "a sub-unit mean");

    let mut floor = klines_tracker(120, ONE);
    assert_eq!(
        probe_target(&mut floor, 1),
        Some(ONE),
        "one whole quote unit is exactly at the floor, so it arms"
    );
}

/// A bar is cut against the target it opened with. The rolling mean moves on every 1m close, but
/// applying the new one mid-bar would cement a bar holding neither target — the exactness invariant
/// the whole feature rests on. The new mean lands on the next bar instead.
#[test]
fn a_kline_closing_mid_bar_leaves_the_open_bars_target_frozen() {
    let mut tracker = klines_tracker(60, 10 * ONE);
    assert_eq!(tracker.on_trade(&worth(4, 1)), 0, "4 of a 10 USD target");

    // Ten candles of 80 USD lift the trailing mean well clear of the 10 the open bar was cut against.
    for index in 60..70 {
        tracker.on_kline(&closed_1m_kline(index * 60_000_000, 80 * ONE));
    }
    let open = tracker
        .volume_bars()
        .and_then(|series| series.open)
        .expect("a bar is filling");
    assert_eq!(
        open.target,
        10 * ONE,
        "the open bar keeps the target it was cut against"
    );

    assert_eq!(
        tracker.on_trade(&worth(6, 2)),
        1,
        "6 more fills the frozen 10 USD target"
    );
    let series = tracker.volume_bars().expect("volume clock configured");
    let closed = series.closed.last().expect("one closed bar");
    assert_eq!(closed.target, 10 * ONE);
    assert_eq!(
        closed.buy_notional,
        10 * ONE,
        "closed at the frozen target, not the new mean"
    );
    assert_eq!(series.open, None);

    let mean = (60 * 10 * ONE + 10 * 80 * ONE) / 70;
    assert_eq!(
        probe_target(&mut tracker, 3),
        Some(mean),
        "the refreshed mean stamps the next bar"
    );
}

/// Rotation puts an ARMED klines clock back to dormant: the new window's trade sizes are a new
/// distribution, and the candles that justified the old target are wiped with it.
/// `tracker::on_rotation_clears_tracker_to_fresh` covers the fixed-target clock and the rest of the
/// tracker; only the klines arm can show `next_target` itself being surrendered.
#[test]
fn rotation_disarms_a_klines_clock() {
    let intervals = [KlineInterval::OneMinute];
    let spec = klines_spec(200);
    let mut tracker = klines_tracker(60, 10 * ONE);
    assert_eq!(
        tracker.on_trade(&worth(25, 1)),
        2,
        "the clock is armed and cutting"
    );
    assert_ne!(
        tracker,
        MicroTracker::new(&spec, &intervals, None),
        "setup must populate state — else cleared==fresh passes vacuously"
    );

    tracker.on_rotation();
    assert_eq!(
        tracker,
        MicroTracker::new(&spec, &intervals, None),
        "rotation wipes the armed clock back to its post-new dormant state"
    );
    assert_dormant(&mut tracker, 2, "a rotated klines clock");
}

/// Records what dispatch handed it as one feature row per bar, valued with the bar's trade
/// arrivals — the only field distinguishing bars a single trade closed, since arrivals book to the
/// bar the trade landed in and the bars it pours through carry zero. A reversed fan-out is visible
/// in those values.
struct BarProbe {
    trade_arrivals: Option<FeatureId>,
}

impl Strategy for BarProbe {
    fn features(&self) -> &'static [&'static str] {
        &["bar_trade_arrivals"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.trade_arrivals = registration.features.first().copied();
    }

    fn on_volume(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId, bar: &VolumeBar) {
        ctx.emit(
            self.trade_arrivals.expect("registered"),
            instrument,
            f64::from(bar.trade_arrivals),
        );
    }
}

fn delivered_bars(consumer: &mut Consumer<PersistRecord>) -> Vec<(f64, TsUs)> {
    let mut rows = Vec::new();
    while let Ok(record) = consumer.pop() {
        if let PersistRecord::Feature(row) = record {
            rows.push((row.value, row.event_ts_us));
        }
    }
    rows
}

/// The fan-out delivers one callback per closed bar and nothing else: a trade that closes k bars
/// fires k times, oldest first, and every one of them is stamped with that closing trade's RECEIPT
/// — not its exchange stamp, and not a clock read, so a replay stamps them identically — which the
/// deliberately distinct stamps below prove.
#[test]
fn on_volume_fires_once_per_closed_bar_oldest_first() {
    let instruments = [instrument_row(0, tracker_spec_all(100), 128)];
    let (sink, mut consumer) = persist_ring(4096);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_producer, _metrics_consumer) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(BarProbe {
            trade_arrivals: None,
        }),
        sink,
        log_sink,
        metrics_producer,
    );

    // Half of the 1 USD target: opens a bar, closes none.
    let mut half = trade(0, ONE, ONE / 2, Side::Buy, 1_000);
    half.received_ts_us = ts(1_500);
    engine.dispatch(pop(0, 0), &InboundMessage::Trade(half));
    assert_eq!(
        delivered_bars(&mut consumer),
        Vec::new(),
        "a bar that never filled is never handed over"
    );

    // 4.5 targets on top of the half already banked: fills the open bar and cuts four more.
    let mut sweep = trade(0, ONE, 9 * ONE / 2, Side::Buy, 2_000);
    sweep.received_ts_us = ts(2_500);
    engine.dispatch(pop(0, 0), &InboundMessage::Trade(sweep));

    let delivered = delivered_bars(&mut consumer);
    assert_eq!(
        delivered,
        vec![
            (2.0, ts(2_500)),
            (0.0, ts(2_500)),
            (0.0, ts(2_500)),
            (0.0, ts(2_500)),
            (0.0, ts(2_500)),
        ],
        "five bars, oldest (the one both trades arrived in) first, all stamped with the receipt"
    );
    assert_eq!(
        engine.unretained_volume_bars(),
        0,
        "every closed bar was still retained when the fan-out ran"
    );
}
