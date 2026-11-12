//! The monitor scenes: fixed event streams, the same on every run.

use polysim::config::ExecutionMode;
use polysim::hot::exec::{
    CloseReason, ExecHalt, HaltReason, OrderState, RejectOrigin, RejectReason,
};
use polysim::ids::{AssetId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::RejectClass;
use polysim::msg::ui::{UiBookState, UiCatalog, UiInstrument};
use polysim::time::TsUs;
use polysim::workstation::monitor::model::SystemNote;
use polysim::workstation::monitor::view::{Channel, MonitorDetail, MonitorUiState};

use crate::fixture::BASE_TS;
use crate::monitor_feed::{Feed, MonitorScene, QUOTE_QTY_MILLI, SPIN};

pub fn monitor_scenes() -> Vec<MonitorScene> {
    vec![
        full(),
        warmup(),
        long_values(),
        unseen(),
        btc_scale(),
        halted(),
        pinned_order(),
    ]
}

/// A live monitor: a valid book and quote (bid delta 2, mid 65992, ask delta 3), features with a
/// recently changed row, a stale row, a real zero and normal rows, and content in all four
/// channels.
fn full() -> MonitorScene {
    let mut feed = Feed::new(1, FULL_FEATURES.len());
    feed.lifecycle(SystemNote::Starting);
    feed.lifecycle(SystemNote::Ready);

    // Start valid, so the awaiting-then-valid pair below shows as one resync row.
    restate_touch(&mut feed, UiBookState::Valid, 1_000_000);

    feed.feature(2, 1_000_000, 2.0000);
    feed.feature(3, 1_000_000, 0.7431);
    feed.feature(4, 1_000_000, 0.6120);
    feed.feature(5, 1_000_000, -0.0004);
    feed.feature(6, 1_000_000, 0.3300);
    feed.feature(7, 1_000_000, 0.4100);
    feed.feature(8, 1_000_000, 12.5000);
    feed.feature(9, 1_000_000, 1.8000);
    feed.feature(10, 1_000_000, 0.9200);
    feed.feature(11, 1_050_000, 0.0000);
    // Stale: seven spins older than the freshest event.
    feed.feature(1, 600_000, 42.0000);

    feed.trade(1_100_000, Side::Buy, 65994, 512);
    restate_touch(&mut feed, UiBookState::AwaitingSnapshot, 1_100_000);
    // A confirmed bid and an in-flight ask, two states that must look different side by side.
    feed.order(1_200_000, 1, Side::Buy, 65990, OrderState::Live);
    feed.order(1_200_000, 2, Side::Sell, 65995, OrderState::PendingNew);
    feed.trade(1_150_000, Side::Sell, 65990, 1_240);

    // Re-emitted with a different value at the freshest time, so the row shows as changed.
    feed.feature(0, 1_290_000, -0.5000);
    feed.feature(0, 1_300_000, -0.8000);
    restate_touch(&mut feed, UiBookState::Valid, 1_300_000);
    feed.quote(1_300_000, Some(65990), Some(65995));

    feed.fill(1_260_000, Side::Buy, 65990, QUOTE_QTY_MILLI);
    feed.fill(1_280_000, Side::Sell, 65995, QUOTE_QTY_MILLI);
    feed.order(
        1_260_000,
        1,
        Side::Buy,
        65990,
        OrderState::Closed(CloseReason::Filled),
    );
    // A routine refusal, which must read as normal rather than as an alarm.
    feed.refusal(
        1_265_000,
        Side::Sell,
        RejectOrigin::Venue {
            class: RejectClass::Gone,
            code: -2010,
        },
    );
    feed.rotation(1_290_000);
    feed.trade(1_250_000, Side::Buy, 65995, 88);
    // Skip three sequence numbers, so the System channel shows an events-lost row.
    feed.skip(3);
    feed.trade(1_300_000, Side::Sell, 65989, 2_100);

    feed.finish("full live monitor", FULL_FEATURES, MonitorUiState::new())
}

fn restate_touch(feed: &mut Feed, state: UiBookState, ts: i64) {
    feed.book(state, ts, &[(65990, 640)], &[(65994, 512)]);
}

/// Before any reading arrives, so every cell shows as missing.
fn warmup() -> MonitorScene {
    let mut feed = Feed::new(1, FULL_FEATURES.len());
    feed.lifecycle(SystemNote::Starting);
    restate_touch(&mut feed, UiBookState::AwaitingSnapshot, 1_000_000);
    feed.finish(
        "warmup — all cells --",
        FULL_FEATURES,
        MonitorUiState::new(),
    )
}

/// Long feature names and extreme values, which must not break the layout.
fn long_values() -> MonitorScene {
    let mut feed = Feed::new(1, LONG_FEATURES.len());
    feed.lifecycle(SystemNote::Ready);
    restate_touch(&mut feed, UiBookState::Valid, 1_000_000);
    feed.feature(0, 1_000_000, 99999999.9999);
    feed.feature(1, 1_000_000, -12345678.5000);
    feed.feature(2, 1_000_000, f64::INFINITY);
    feed.feature(3, 1_000_000, f64::NEG_INFINITY);
    feed.feature(4, 1_000_000, f64::NAN);
    feed.feature(5, 1_000_000, 0.0001);
    feed.quote(1_000_000, Some(65990), Some(65995));
    feed.trade(1_000_000, Side::Buy, 65994, 99_999_999);
    feed.finish("long names / values", LONG_FEATURES, MonitorUiState::new())
}

/// The System channel scrolled away, so the badge counts the rows not yet seen.
fn unseen() -> MonitorScene {
    let mut feed = Feed::new(1, FULL_FEATURES.len());
    feed.lifecycle(SystemNote::Starting);
    feed.lifecycle(SystemNote::Ready);
    for step in 0..40 {
        feed.rotation(1_000_000 + step * SPIN.micros());
    }
    let appended = feed.system_appended();
    let mut state = MonitorUiState::new();
    state.active_tab = Channel::System;
    state.set_scrolled_away(Channel::System, appended.saturating_sub(7), 220.0);
    feed.finish("unseen-count | scrolled away", FULL_FEATURES, state)
}

/// A BTC-scale book on a 0.01 tick, where the mid is a ten-glyph price and a quantity runs to six
/// digits. The catalog names both assets, so the account band fills in.
fn btc_scale() -> MonitorScene {
    btc_feed().feed.finish(
        "BTC scale | 0.01 tick",
        FULL_FEATURES,
        MonitorUiState::new(),
    )
}

/// The BTC scene with the kill switch thrown, so the status strip shows a halt.
fn halted() -> MonitorScene {
    let mut feed = btc_feed().feed;
    feed.execution(
        1_310_000,
        ExecHalt::Halted {
            reason: HaltReason::RealisedLoss,
            halted_ts_us: TsUs::from_micros(BASE_TS + 1_310_000),
        },
    );
    feed.finish("halted", FULL_FEATURES, MonitorUiState::new())
}

/// The BTC scene with a tap already on the filled order, as a finger on the Orders tab would leave
/// it.
fn pinned_order() -> MonitorScene {
    let scene = btc_feed();
    let mut state = MonitorUiState::new();
    state.active_tab = Channel::Orders;
    state.tap(Some(MonitorDetail::Row {
        channel: Channel::Orders,
        ordinal: scene.filled_ordinal,
    }));
    scene.feed.finish("pinned order row", FULL_FEATURES, state)
}

struct BtcFeed {
    feed: Feed,
    /// The filled order's place in the Orders channel's append order.
    filled_ordinal: u64,
}

const BTC: AssetId = AssetId(0);
const USDT: AssetId = AssetId(1);
const BID_TICK: i64 = 11_823_450;
const ASK_TICK: i64 = 11_823_451;

fn btc_feed() -> BtcFeed {
    let tick = Price(FIXED_SCALE / 100);
    let mut feed = Feed::with_tick(1, FULL_FEATURES.len(), tick);
    feed.set_catalog(btc_catalog(tick));
    feed.lifecycle(SystemNote::Starting);
    feed.lifecycle(SystemNote::Ready);

    btc_touch(&mut feed, UiBookState::Valid, 1_000_000);
    for (feature, value) in BTC_FEATURE_VALUES.iter().enumerate() {
        feed.feature(feature as u16, 1_000_000, *value);
    }

    feed.balance(1_000_000, BTC, 123_456_000, 1_000_000);
    feed.balance(1_000_000, USDT, 25_000_000_000_000, 118_234_500_000);
    // A balance in an asset the catalog does not name is counted, not shown.
    feed.balance(1_000_000, AssetId::UNKNOWN, 5_000_000, 0);
    feed.execution(1_000_000, ExecHalt::Armed);

    feed.trade(1_100_000, Side::Buy, ASK_TICK, 123_456_789);
    btc_touch(&mut feed, UiBookState::AwaitingSnapshot, 1_100_000);
    feed.partial_order(1_200_000, 0x2a7, Side::Buy, BID_TICK - 3, 3_500);
    feed.order(
        1_210_000,
        0x2a8,
        Side::Sell,
        ASK_TICK + 3,
        OrderState::PendingNew,
    );
    feed.trade(1_150_000, Side::Sell, BID_TICK, 99_999);

    btc_touch(&mut feed, UiBookState::Valid, 1_300_000);
    feed.quote(1_300_000, Some(BID_TICK - 3), Some(ASK_TICK + 3));

    let filled_ordinal = feed.orders_appended();
    feed.order(
        1_260_000,
        0x29f,
        Side::Buy,
        BID_TICK - 10,
        OrderState::Closed(CloseReason::Filled),
    );
    feed.fill(1_262_000, Side::Buy, BID_TICK - 10, QUOTE_QTY_MILLI);
    feed.fill(1_281_000, Side::Sell, ASK_TICK + 3, 1_234);
    // Long enough that the status strip has to cut it.
    feed.refusal(
        1_265_000,
        Side::Sell,
        RejectOrigin::Local(RejectReason::NotionalAboveMax),
    );
    feed.rotation(1_290_000);
    feed.trade(1_250_000, Side::Buy, ASK_TICK + 1, 2_100);
    feed.skip(3);
    feed.trade(1_300_000, Side::Sell, BID_TICK - 1, 1_500);

    BtcFeed {
        feed,
        filled_ordinal,
    }
}

fn btc_touch(feed: &mut Feed, state: UiBookState, ts: i64) {
    feed.book(
        state,
        ts,
        &[(BID_TICK, 640), (BID_TICK - 1, 1_200)],
        &[(ASK_TICK, 512), (ASK_TICK + 1, 900)],
    );
}

fn btc_catalog(tick: Price) -> UiCatalog {
    UiCatalog {
        strategy_id: "fixture".into(),
        window_title: "Polysim - fixture".into(),
        execution_mode: Some(ExecutionMode::Sim),
        spin_interval_us: SPIN.micros() as u64,
        instruments: vec![UiInstrument {
            instrument_id: InstrumentId(0),
            display: "BTCUSDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            base_asset: BTC,
            quote_asset: USDT,
            tick_size: Some(tick),
            lot_size: Some(Qty(FIXED_SCALE / 1000)),
            qty_scale: FIXED_SCALE,
        }],
        feature_names: FULL_FEATURES
            .iter()
            .map(|name| Box::<str>::from(*name))
            .collect(),
    }
}

/// One value per name in `FULL_FEATURES`, in the same order.
const BTC_FEATURE_VALUES: [f64; FULL_FEATURES.len()] = [
    -0.8000, 42.0000, 2.0000, 0.7431, 0.6120, -0.0004, 0.3300, 0.4100, 12.5000, 1.8000, 0.9200,
    0.0000,
];

const FULL_FEATURES: &[&str] = &[
    "microprice_offset",
    "queue_ahead_ratio",
    "spread_ticks",
    "hawkes_buy_intensity",
    "hawkes_sell_intensity",
    "kyle_lambda",
    "vpin_short",
    "vpin_long",
    "gueant_a",
    "gueant_k",
    "resilience_ou",
    "net_inventory",
];

const LONG_FEATURES: &[&str] = &[
    "gueant_optimal_half_spread_reservation_offset_ticks",
    "hawkes_multivariate_cross_excitation_decay_estimate",
    "kyle_lambda_signed_dollar_volume_price_impact_slope",
    "vpin_long_bucketed_order_flow_toxicity_running_mean",
    "resilience_ornstein_uhlenbeck_mean_reversion_lambda",
    "microprice",
];
