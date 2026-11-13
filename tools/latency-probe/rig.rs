//! What each timed mode drives: one hot engine on one Binance spot instrument with a strategy that
//! does nothing, the recorded trades it folds, and the engine's own metrics thread and log.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Context;

use rtrb::RingBuffer;

use engine::hot::dispatch::{ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::sink::{ExposureSink, StrategyLogSink, UiBookSink, UiEventSink};
use engine::hot::strategy::{EngineView, Strategy, StrategyConfig};
use engine::log::{LogConfig, LogHandle};
use engine::msg::ids::{AssetId, FIXED_SCALE, InstrumentId, Notional, Price, Qty};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::time::{ClockSource, DurationUs, EngineClock, InvariantCounter, Stamper};
use engine::runtime::config::{
    BinanceMarket, KlineInterval, LatencyConfig, NoParams, StrategySpec, Subscriptions,
    TrackerSpec, VenueMarket,
};
use engine::runtime::metrics::{
    Metrics, MetricsParts, MetricsSetup, MetricsThread, MetricsThreadSetup, SampleProducers,
};
use engine::runtime::pin_to_core;
use engine::runtime::registry::InstrumentRow;
use engine::venues::binance::parse::{ParseContext, parse_agg_trade, parse_combined_frame};

use crate::clock;

const RECORDED_TRADES: &str = include_str!("../../engine/fixtures/binance/spot_aggTrade.jsonl");

/// A spin rides among the trades as often as the timer of a busy engine would send one.
const TRADES_PER_SPIN: usize = 1_000;

/// The engine's own output rings are never read here, so they fill and then drop and count, as
/// they do when a reader falls behind.
const OUTPUT_RING: usize = 1_024;

/// The engine's log writes its file below the run folder, as a full start does.
const LOG_DIR: &str = "logs";

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
        tick_size: Some(Price::from_mantissa(FIXED_SCALE / 100)),
        lot_size: Some(Qty::from_mantissa(1_000)),
        min_qty: Some(Qty::from_mantissa(1_000)),
        min_notional: Some(Notional::from_mantissa(5 * FIXED_SCALE)),
        max_num_orders: None,
        max_num_order_amends: None,
        max_price: None,
        qty_scale: FIXED_SCALE,
        subscriptions: Subscriptions::default(),
        kline_intervals: vec![KlineInterval::OneMinute],
        book_capacity: 128,
        max_exposure_quote: Notional::from_mantissa(1_000_000 * FIXED_SCALE),
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
        received_ts_us: engine::msg::time::LocalTs::from_micros(0),
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
        let tick = SpinTick {
            seq: (index / TRADES_PER_SPIN) as u64,
        };
        return InboundMessage::new(at, MessageBody::SpinTick(tick));
    }
    let mut trade = trades[index % trades.len()];
    // No venue sits behind the harness, so the venue's stamps are the send itself and the venue
    // stages read zero rather than the age of the recording.
    trade.exchange_ts_us = at.as_venue_calendar();
    trade.exchange_sent_ts_us = Some(at.as_venue_calendar());
    InboundMessage::new(at, MessageBody::Trade(trade))
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

/// The engine's own metrics thread and log, as a full start runs them. The rows of each interval
/// reach the log as its lines, and the harness prints those lines when the run ends.
pub struct MetricsRun {
    thread: MetricsThread,
    log: LogHandle,
    stem: &'static str,
    /// How long each log file of the stem was before this run, so only the lines of this run print.
    logged_before: HashMap<PathBuf, u64>,
}

impl MetricsRun {
    /// Starts the log, then the metrics thread. Hands back the write ends of the thread's rings and
    /// the stamper that the samples must read.
    ///
    /// # Errors
    /// When the log or the metrics thread cannot start.
    pub fn start(
        clock: &EngineClock,
        stem: &'static str,
    ) -> anyhow::Result<(MetricsRun, SampleProducers, Stamper)> {
        let logged_before = log_lengths(stem);
        let log = engine::log::init(
            &LogConfig {
                dir: PathBuf::from(LOG_DIR),
                file_stem: stem.into(),
                ..LogConfig::default()
            },
            clock,
            pin_to_core,
        )
        .context("the harness cannot start the engine's log")?;
        let stamper = clock::counter_stamper();
        let scale = clock::first_scale(&stamper, clock);
        let clock_source = ClockSource::check(InvariantCounter::detect())
            .unwrap_or_else(|refusal| refusal.fallback());
        let MetricsParts {
            metrics, samples, ..
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
        let thread = MetricsThread::spawn(MetricsThreadSetup {
            metrics,
            clock: clock.clone(),
            stamper: stamper.clone(),
            core_id: None,
            pin: pin_to_core,
        })
        .context("the harness cannot start the metrics thread")?;
        let run = MetricsRun {
            thread,
            log,
            stem,
            logged_before,
        };
        Ok((run, samples, stamper))
    }

    /// Stops the metrics thread, which closes its interval into the log, drains the log, and prints
    /// the latency lines of the run.
    ///
    /// # Errors
    /// When the log folder cannot be read.
    pub fn finish(self, label: &str) -> anyhow::Result<()> {
        self.thread.stop();
        self.log.drain();
        print_latency_lines(label, self.stem, &self.logged_before)
    }
}

fn log_files(stem: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(LOG_DIR) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(stem))
        })
        .collect()
}

fn log_lengths(stem: &str) -> HashMap<PathBuf, u64> {
    log_files(stem)
        .into_iter()
        .filter_map(|path| Some((path.clone(), std::fs::metadata(&path).ok()?.len())))
        .collect()
}

/// A debug build logs to stderr, so only a release build leaves a file to print from.
fn print_latency_lines(
    label: &str,
    stem: &str,
    logged_before: &HashMap<PathBuf, u64>,
) -> anyhow::Result<()> {
    let files = log_files(stem);
    if files.is_empty() {
        println!("development {label}: no log file; a debug build writes its log to stderr");
    }
    for path in files {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        let start = logged_before.get(&path).copied().unwrap_or(0) as usize;
        let lines = text.get(start..).unwrap_or("").lines().filter(|line| {
            line.contains("] engine::runtime::assembly::metrics") || line.contains(" WARN ")
        });
        for line in lines {
            println!("development {label}: {line}");
        }
    }
    Ok(())
}
