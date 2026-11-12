//! The engine, tape and readers the shipped recorder's seam tests share. Each test file owns its
//! price level and lot step, because those are what its claim is about.

use polysim::config::{IntensitySpec, KlineInterval, TableKind};
use polysim::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use polysim::hot::strategy::StrategyConfig;
use polysim::ids::{AssetId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{ExecCommand, ExecLaneItem};
use polysim::msg::inbound::InboundMessage;
use polysim::msg::persist::PersistRecord;
use polysim::registry::InstrumentRow;
use polysim::sink::ExecSink;
use polysim::time::DurationUs;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, account_chunk, book_reset, delta_chunk, detached_setup, engine_view, instrument_row,
    kline, persist_ring, pop, recorder_spec, snapshot_pair, spin, stream_ready, tracker_spec_all,
    trade, ui_event_ring,
};
use crate::exec_support::{drain_commands, exec_settings, open_orders_snapshot};
use crate::micro_strategy::features::FEATURE_NAMES;
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};

const INSTRUMENT: InstrumentId = InstrumentId(0);
const BASE_ASSET: AssetId = AssetId(0);
const QUOTE_ASSET: AssetId = AssetId(1);

pub(crate) const TICK: i64 = ONE / 100;

pub(crate) const SPIN_INTERVAL: DurationUs = DurationUs::from_secs(3);

/// One closed candle per step, so the EGARCH close floor is reached in hundreds of messages rather
/// than the hundreds of seconds a realistic 1m cadence would take. The fit reads a SERIES, not a
/// calendar, so compressing the arrivals changes when it warms and not what it warms into.
pub(crate) const STEPS: i64 = 900;
pub(crate) const STEP_US: i64 = 500_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlacedOrder {
    pub(crate) side: Side,
    pub(crate) price: Price,
    pub(crate) qty: Qty,
}

pub(crate) fn drain_places(commands: &mut Consumer<ExecLaneItem>) -> Vec<PlacedOrder> {
    drain_commands(commands)
        .into_iter()
        .filter_map(|command| match command {
            ExecCommand::Place {
                side, price, qty, ..
            } => Some(PlacedOrder { side, price, qty }),
            _ => None,
        })
        .collect()
}

pub(crate) fn spin_columns(persist: &mut Consumer<PersistRecord>) -> Vec<(&'static str, f64)> {
    let mut columns = Vec::new();
    while let Ok(record) = persist.pop() {
        if let PersistRecord::Feature(row) = record {
            columns.push((FEATURE_NAMES[usize::from(row.feature.0)], row.value));
        }
    }
    columns
}

pub(crate) fn column_value(columns: &[(&'static str, f64)], name: &str) -> Option<f64> {
    columns
        .iter()
        .find(|(column, _)| *column == name)
        .map(|(_, value)| *value)
}

pub(crate) fn recorder_engine(
    instruments: &[InstrumentRow],
) -> (HotEngine, Consumer<PersistRecord>, Consumer<ExecLaneItem>) {
    let (persistence, persist) = persist_ring(8_192);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(256);
    let spec = recorder_spec::<MicroRecorderParams>(vec![TableKind::Features]);
    let strategy = Box::new(MicroRecorder::from_spec(&spec, engine_view(SPIN_INTERVAL)));
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink: ui_event_ring(1_024).0,
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer),
            settings: exec_settings(),
            run_nonce: 0,
        }),
        ..detached_setup(instruments, strategy)
    });
    (engine, persist, commands)
}

/// The stream is up and the open orders and balances are known. Without all three nothing is ever
/// placed, and every assertion after this call would pass for the wrong reason.
pub(crate) fn make_ready(engine: &mut HotEngine, when: i64) {
    engine.dispatch(pop(0, 0), &stream_ready(INSTRUMENT, when));
    open_orders_snapshot(engine, when);
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Account(account_chunk(
            &[(BASE_ASSET, 1_000 * ONE), (QUOTE_ASSET, 1_000_000 * ONE)],
            when,
        )),
    );
}

pub(crate) fn seed_book(engine: &mut HotEngine, base: i64, when: i64) {
    // A two-tick touch. The Guéant re-anchoring is `A·e^(k·half_spread_ticks)`, so a wide synthetic
    // spread overflows to infinity on a steep fitted k and the whole quote family goes null.
    let (bids, asks) = snapshot_pair(
        0,
        &[(base, 2 * ONE), (base - 5 * TICK, ONE)],
        &[(base + 2 * TICK, ONE), (base + 7 * TICK, 2 * ONE)],
        when + 1,
    );
    for message in [
        InboundMessage::BookReset(book_reset(0, when)),
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
    ] {
        engine.dispatch(pop(0, 0), &message);
    }
}

/// One step: a print walking into its own side of the book, depth churn, a closed candle, and the
/// spin that emits. Both aggressor sides, because a side whose prints never reach a level never
/// identifies an intensity fit and never produces a Guéant quote at all.
pub(crate) fn step_messages(base: i64, index: i64, when: i64) -> [InboundMessage; 4] {
    let (side, price) = match index % 2 == 0 {
        true => (Side::Buy, base + 2 * TICK + (index % 4) * TICK),
        false => (Side::Sell, base - (index % 4) * TICK),
    };
    let (level_side, level) = match index % 2 == 0 {
        true => (Side::Sell, base + 7 * TICK),
        false => (Side::Buy, base - 5 * TICK),
    };
    [
        InboundMessage::Trade(trade(0, price, ONE / 100, side, when)),
        InboundMessage::Book(delta_chunk(
            0,
            level_side,
            &[(level, (1 + index % 5) * ONE)],
            when,
        )),
        InboundMessage::Kline(kline(
            0,
            KlineInterval::OneMinute,
            (
                base,
                base + 5 * TICK,
                base - 5 * TICK,
                base + (index % 5) * TICK,
            ),
            true,
            when,
        )),
        InboundMessage::SpinTick(spin(index as u64, when)),
    ]
}

pub(crate) fn recorded_row(step: i64) -> InstrumentRow {
    let mut row = instrument_row(0, tracker_spec_all(100), 128);
    row.tick_size = Some(Price(TICK));
    row.lot_size = Some(Qty(step));
    row.min_qty = Some(Qty(step));
    row.min_notional = Some(ONE);
    row.tracker.intensity = Some(IntensitySpec {
        max_depth_ticks: 16,
        half_life_secs: 600.0,
        min_events: 5.0,
    });
    row
}
