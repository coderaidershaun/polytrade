//! EGARCH seeding across warmup: the REST kline backfill lands inside the warmup span, where the
//! engine feeds the tracker but suppresses the strategy callback (state warms with the strategy
//! unaware). The strategy must still seed its fit history from the tracker on the first live spin —
//! otherwise `egarch_vol_lt` would take hours of live 1m candles to reach its 300 floor. Regression
//! for that ordering bug.

use polysim::config::{KlineInterval, TableKind};
use polysim::hot::strategy::StrategyConfig;
use polysim::msg::inbound::InboundMessage;
use polysim::msg::persist::{FeatureId, PersistRecord};
use polysim::time::DurationUs;

use crate::engine_support::{
    NOMINAL_SPIN, ONE, engine_view, engine_with_ui, instrument_row, kline, metrics_ring,
    persist_ring, pop, recorder_feature_id, recorder_spec, snapshot_pair, spin, strategy_log_ring,
    tracker_spec_all,
};
use crate::micro_strategy::MicroRecorder;

/// The fitted EGARCH parameters. They share `egarch_vol_lt`'s gate (a cached fit), so a live spin
/// that emits one emits all.
const EGARCH_PARAM_NAMES: [&str; 5] = [
    "egarch_omega",
    "egarch_gamma",
    "egarch_theta",
    "egarch_beta",
    "egarch_uncond_vol_lt",
];

#[test]
fn egarch_seeds_from_backfill_absorbed_during_warmup() {
    let egarch_vol_lt = recorder_feature_id("egarch_vol_lt");
    let instruments = [instrument_row(0, tracker_spec_all(100), 128)];

    let (sink, mut consumer) = persist_ring(4096);
    let (log_sink, _log_consumer) = strategy_log_ring(64);
    let (metrics_sink, _metrics_consumer) = metrics_ring(64);
    let strategy = Box::new(MicroRecorder::from_spec(
        &recorder_spec(vec![TableKind::Features]),
        engine_view(NOMINAL_SPIN),
    ));
    let (mut engine, _ui_books, _ui_events) = engine_with_ui(
        &instruments,
        strategy,
        sink,
        log_sink,
        metrics_sink,
        DurationUs::from_secs(10),
    );

    // The first message anchors warmup at ts 0; a valid book so the spin has a mid.
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, ONE)], &[(101 * ONE, ONE)], 0);
    engine.dispatch(pop(0, 0), &InboundMessage::Book(bids));
    engine.dispatch(pop(0, 0), &InboundMessage::Book(asks));

    // 310 closed 1m candles, all stamped inside the 10s warmup span: the tracker absorbs them while
    // the strategy's on_kline stays suppressed, exactly like a boot backfill burst.
    for candle in 0..310i64 {
        let when = candle * 30_000; // the last lands at 9.27s, still inside warmup
        engine.dispatch(
            pop(0, 0),
            &InboundMessage::Kline(kline(
                0,
                KlineInterval::OneMinute,
                (100 * ONE, 101 * ONE, 99 * ONE, (100 + candle % 5) * ONE),
                true,
                when,
            )),
        );
    }
    while consumer.pop().is_ok() {}

    // A spin past the warmup span completes warmup and delivers itself — the strategy's first live
    // callback. It must seed closes_1m from the tracker and fit, emitting egarch_vol_lt this tick.
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 10_000_000)));

    let emitted: std::collections::HashSet<FeatureId> = std::iter::from_fn(|| consumer.pop().ok())
        .filter_map(|record| match record {
            PersistRecord::Feature(row) => Some(row.feature),
            _ => None,
        })
        .collect();
    assert!(
        emitted.contains(&egarch_vol_lt),
        "egarch_vol_lt must emit on the first live spin, seeded from the warmup-absorbed backfill"
    );
    for name in EGARCH_PARAM_NAMES {
        assert!(
            emitted.contains(&recorder_feature_id(name)),
            "{name} must emit alongside egarch_vol_lt — the fitted params share its gate"
        );
    }
}
