//! What each timed mode drives: one hot engine on one Binance spot instrument with a strategy that
//! does nothing, the recorded trades it folds, and a metrics core that a thread of its own drains.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use rtrb::RingBuffer;

use engine::hot::dispatch::{ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::sink::{ExposureSink, StrategyLogSink, UiBookSink, UiEventSink};
use engine::hot::strategy::{EngineView, Strategy, StrategyConfig};
use engine::msg::ids::{AssetId, FIXED_SCALE, InstrumentId, Price, Qty};
use engine::msg::inbound::{InboundMessage, SpinTick, TradeEvent};
use engine::msg::time::{ClockSource, DurationUs, EngineClock, InvariantCounter, Stamper};
use engine::runtime::config::{
    BinanceMarket, KlineInterval, LatencyConfig, NoParams, StrategySpec, Subscriptions,
    TrackerSpec, VenueMarket,
};
use engine::runtime::metrics::{
    LatencyRow, Metrics, MetricsParts, MetricsSetup, RollupRow, RollupSink, SampleProducers,
};
use engine::runtime::registry::InstrumentRow;
use engine::venues::binance::parse::{ParseContext, parse_agg_trade, parse_combined_frame};

use crate::clock;

const RECORDED_TRADES: &str = include_str!("../../engine/fixtures/binance/spot_aggTrade.jsonl");

/// A spin rides among the trades as often as the timer of a busy engine would send one.
const TRADES_PER_SPIN: usize = 1_000;

/// The engine's own output rings are never read here, so they fill and then drop and count, as
/// they do when a reader falls behind.
const OUTPUT_RING: usize = 1_024;

/// The wake of the harness's metrics thread, as in the engine.
const METRICS_POLL: Duration = Duration::from_millis(5);

/// A fresh anchor this often keeps the rate of the counter exact, as in the engine. Without it a
/// rate taken over two milliseconds drifts by milliseconds over a run.
const ANCHOR_INTERVAL: DurationUs = DurationUs::from_millis(100);

/// A strategy that reads nothing and decides nothing, so a run times the engine alone.
pub struct Silent;

impl Strategy for Silent {}

impl StrategyConfig for Silent {
    type Params = NoParams;

    fn from_spec(_spec: &StrategySpec<NoParams>, _engine: EngineView) -> Self {
        Silent
    }
}

pub fn instrument() -> InstrumentRow {
    InstrumentRow {
        instrument_id: InstrumentId(0),
        market: VenueMarket::Binance(BinanceMarket::Spot),
        venue_symbol: "btcusdt".into(),
        display: "BTC/USDT spot".into(),
        base: "BTC".into(),
        quote: "USDT".into(),
        base_asset: AssetId(0),
        quote_asset: AssetId(1),
        tick_size: Some(Price(FIXED_SCALE / 100)),
        lot_size: Some(Qty(1_000)),
        min_qty: Some(Qty(1_000)),
        min_notional: Some(5 * FIXED_SCALE),
        max_num_orders: None,
        max_num_order_amends: None,
        max_price: None,
        qty_scale: FIXED_SCALE,
        subscriptions: Subscriptions::default(),
        kline_intervals: vec![KlineInterval::OneMinute],
        book_capacity: 128,
        max_exposure_quote: 1_000_000 * FIXED_SCALE,
        tracker: TrackerSpec::default(),
    }
}

/// The hot engine over `instruments`, wired to output rings that nobody reads.
pub fn engine(instruments: &[InstrumentRow]) -> HotEngine {
    HotEngine::new(HotEngineSetup {
        instruments,
        strategy: Box::new(Silent),
        persistence: None,
        strategy_log_sink: StrategyLogSink::new(RingBuffer::new(OUTPUT_RING).0),
        ui_book_sink: UiBookSink::new(RingBuffer::new(OUTPUT_RING).0),
        ui_event_sink: UiEventSink::new(RingBuffer::new(OUTPUT_RING).0),
        link: None,
        warmup: DurationUs::ZERO,
        exec: None,
        exposure: ExposureWiring {
            restored: &[],
            sink: ExposureSink::new(RingBuffer::new(OUTPUT_RING).0),
        },
    })
}

/// The recorded spot trades of the fixtures, in their recorded order. The harness stamps each one
/// again as it sends it.
pub fn recorded_trades() -> Vec<TradeEvent> {
    let placeholder = ParseContext {
        instrument: InstrumentId(0),
        received_ts_us: engine::msg::time::TsUs::from_micros(0),
    };
    RECORDED_TRADES
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let frame = parse_combined_frame(line).expect("a recorded frame has its envelope");
            parse_agg_trade(&frame.data, placeholder)
                .expect("a recorded trade parses")
                .trade
        })
        .collect()
}

/// The message to send as number `index` of a run, stamped now: mostly trades, with a spin among
/// them.
pub fn message(trades: &[TradeEvent], index: usize, clock: &EngineClock) -> InboundMessage {
    let at = clock::now(clock);
    if index % TRADES_PER_SPIN == TRADES_PER_SPIN - 1 {
        return InboundMessage::SpinTick(SpinTick {
            seq: (index / TRADES_PER_SPIN) as u64,
            received_ts_us: at,
            queued_ts_us: at,
        });
    }
    let mut trade = trades[index % trades.len()];
    trade.received_ts_us = at;
    trade.queued_ts_us = at;
    InboundMessage::Trade(trade)
}

/// Pins the calling thread when the run names a core. On Linux a run that names a core and cannot
/// pin there stops, because its numbers would describe a thread that floats.
///
/// # Errors
/// On Linux, when the pin fails.
pub fn pin(core: Option<usize>) -> anyhow::Result<()> {
    let Some(id) = core else {
        return Ok(());
    };
    let is_pinned = core_affinity::set_for_current(core_affinity::CoreId { id });
    if cfg!(target_os = "linux") && !is_pinned {
        anyhow::bail!("cannot pin to core {id}: a timed run on linux needs its pin");
    }
    if !cfg!(target_os = "linux") {
        eprintln!(
            "core {id} is not pinned on {}: these numbers are for development only",
            std::env::consts::OS
        );
    }
    Ok(())
}

/// A metrics core with time passed in, run on a thread of its own as the engine runs it.
pub struct MetricsRun {
    stop: Arc<AtomicBool>,
    join: JoinHandle<Vec<LatencyRow>>,
}

#[derive(Default)]
struct Rows(Vec<LatencyRow>);

impl RollupSink for Rows {
    fn write(&mut self, row: RollupRow) {
        if let RollupRow::Latency(row) = row {
            self.0.push(row);
        }
    }
}

impl MetricsRun {
    /// The write ends of its rings, and the stamper that the samples must read.
    pub fn start(clock: &EngineClock) -> (MetricsRun, SampleProducers, Stamper) {
        let stamper = Stamper::Counter(quanta::Clock::new());
        let scale = clock::first_scale(&stamper, clock);
        let clock_source = ClockSource::check(InvariantCounter::detect())
            .unwrap_or_else(|refusal| refusal.fallback());
        let MetricsParts {
            mut metrics,
            samples,
            ..
        } = Metrics::with_rings(MetricsSetup {
            latency: LatencyConfig {
                interval_secs: 3_600,
                ..LatencyConfig::default()
            },
            clock_source,
            queue_count: 1,
            scale,
            start_ts_us: clock::now(clock),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_clock = clock.clone();
        let thread_stamper = stamper.clone();
        let join = std::thread::spawn(move || {
            let mut rows = Rows::default();
            let mut next_anchor_ts_us = clock::now(&thread_clock) + ANCHOR_INTERVAL;
            while !thread_stop.load(Ordering::Acquire) {
                let now = clock::now(&thread_clock);
                if now >= next_anchor_ts_us {
                    let (before, at, after) = clock::anchor_reads(&thread_stamper, &thread_clock);
                    if let Err(refusal) = metrics.take_anchor(before, at, after) {
                        eprintln!("{refusal}");
                    }
                    next_anchor_ts_us = at + ANCHOR_INTERVAL;
                }
                metrics.pass(now, &mut rows);
                std::thread::sleep(METRICS_POLL);
            }
            metrics.finish(clock::now(&thread_clock), &mut rows);
            rows.0
        });
        (MetricsRun { stop, join }, samples, stamper)
    }

    /// The rows of the one interval that the run filled.
    pub fn finish(self) -> Vec<LatencyRow> {
        self.stop.store(true, Ordering::Release);
        self.join
            .join()
            .expect("the metrics thread of the harness does not panic")
    }
}
