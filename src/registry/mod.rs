//! Frozen identity: turns a `Config` into a `Registry` — dense instrument ids, producer
//! grouping, input queues. Submodules are stages of that one build (`rows`, `assets`,
//! `grouping`, `validate`), so the `Registry` and the vocabulary it answers in stay here
//! and callers never name a stage.

mod assets;
mod grouping;
mod rows;
mod validate;

use crate::config::{
    BinanceEnv, Config, ConfigError, KlineInterval, Subscriptions, TrackerSpec, VenueMarket,
};
use crate::ids::{AssetId, InstrumentId, Price, Qty, QueueId};

pub use assets::AssetDictionary;
pub use grouping::{ConnectionCategory, ProducerGroup};

use grouping::group_producers;
use rows::{binance_env, build_instrument_rows, check_strategy_instruments};

const MAX_INPUT_QUEUES: usize = 20;

#[derive(Debug, Clone)]
pub struct InstrumentRow {
    pub instrument_id: InstrumentId,
    pub market: VenueMarket,
    pub venue_symbol: Box<str>,
    pub display: Box<str>,
    pub base: Box<str>,
    pub quote: Box<str>,
    pub base_asset: AssetId,
    pub quote_asset: AssetId,
    pub tick_size: Option<Price>,
    pub lot_size: Option<Qty>,
    pub min_qty: Option<Qty>,
    /// The minimum order value, as a 1e-8 quote mantissa.
    pub min_notional: Option<i64>,
    pub max_num_orders: Option<u32>,
    /// `None` on futures, where no amend-count filter is published.
    pub max_num_order_amends: Option<u32>,
    /// Highest price the venue accepts, where it publishes one. Absent on Binance; present on
    /// Polymarket, whose prices are probabilities — an aggressive price walked past `1 − tick`
    /// there is rejected outright rather than merely optimistic.
    pub max_price: Option<Price>,
    pub qty_scale: i64,
    pub subscriptions: Subscriptions,
    pub kline_intervals: Vec<KlineInterval>,
    pub book_capacity: usize,
    /// The exposure ceiling, as a 1e-8 mantissa. A configured risk budget, validated
    /// positive and finite at build time.
    pub max_exposure_quote: i64,
    pub tracker: TrackerSpec,
}

/// Binance scale-preflight limits, as `exchangeInfo` publishes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BinanceLimits {
    pub min_qty: Qty,
    pub min_notional: i64,
    pub max_num_orders: u32,
    pub max_num_order_amends: Option<u32>,
}

/// Polymarket's order limits. A separate type from [`BinanceLimits`] because the venue publishes a
/// different SET, not different values of the same set: there is no minimum notional at all, the
/// order ceiling is our own deliberate constant rather than a venue filter, and the amend budget is
/// exactly zero — the venue has no amend endpoint, which is a stronger statement than Binance's
/// absent filter and is what degrades the reconciler's shrink to a cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolyLimits {
    pub min_qty: Qty,
    pub max_num_orders: u32,
    pub max_price: Price,
}

/// An order rate limit scoped to the whole account, not to one symbol. Lives on Registry,
/// not on InstrumentRow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderRateLimit {
    pub interval: RateInterval,
    pub interval_num: u32,
    pub limit: u64,
}

/// Unrecognized windows are startup refusals, not silently skipped (avoids overstating budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateInterval {
    Second,
    Minute,
    Hour,
    Day,
}

impl RateInterval {
    pub fn parse(wire: &str) -> Option<RateInterval> {
        match wire {
            "SECOND" => Some(RateInterval::Second),
            "MINUTE" => Some(RateInterval::Minute),
            "HOUR" => Some(RateInterval::Hour),
            "DAY" => Some(RateInterval::Day),
            _ => None,
        }
    }

    pub fn as_secs(self) -> u64 {
        match self {
            RateInterval::Second => 1,
            RateInterval::Minute => 60,
            RateInterval::Hour => 3_600,
            RateInterval::Day => 86_400,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Registry {
    instruments: Vec<InstrumentRow>,
    assets: AssetDictionary,
    binance_env: Option<BinanceEnv>,
    /// Empty until the Binance scale preflight runs.
    order_rate_limits: Vec<OrderRateLimit>,
    producer_groups: Vec<ProducerGroup>,
    timer_queue_id: QueueId,
    /// `None` when the config carries no `link:` block.
    link_queue_id: Option<QueueId>,
    /// Acks and stream reports share one queue: their relative order can race over the
    /// network, but sharing one queue keeps that order deterministic for replay.
    exec_queue_id: Option<QueueId>,
    input_queue_count: usize,
}

impl Registry {
    /// # Errors
    /// A strategy instrument that names no row, or a tracker block the source cannot serve.
    pub fn build<P>(config: &Config<P>) -> Result<Registry, ConfigError> {
        let (instruments, assets) = build_instrument_rows(&config.source)?;
        check_strategy_instruments(&config.strategy.instruments, &instruments)?;

        let producer_groups = group_producers(&instruments);
        let timer_queue_id = QueueId(producer_groups.len() as u8);
        let link_queue_id = config.link.is_some().then(|| QueueId(timer_queue_id.0 + 1));
        // Exec is appended last: queue ids are recorded on messages, and shifting one
        // would re-attribute an entire recorded tape.
        let exec_queue_id = execution_queue_id(config, link_queue_id.unwrap_or(timer_queue_id));
        let last_queue_id = exec_queue_id.or(link_queue_id).unwrap_or(timer_queue_id);
        let input_queue_count = usize::from(last_queue_id.0) + 1;
        // Config alone cannot reach this ceiling; only a bug in the grouping logic could.
        debug_assert!(
            input_queue_count <= MAX_INPUT_QUEUES,
            "{input_queue_count} input queues from one source, max {MAX_INPUT_QUEUES}"
        );

        Ok(Registry {
            instruments,
            assets,
            binance_env: binance_env(&config.source),
            order_rate_limits: Vec::new(),
            producer_groups,
            timer_queue_id,
            link_queue_id,
            exec_queue_id,
            input_queue_count,
        })
    }

    /// # Panics
    /// Out-of-range id. Ids are dense and registry-issued, so this can only mean a bug.
    pub fn instrument(&self, id: InstrumentId) -> &InstrumentRow {
        &self.instruments[usize::from(id.0)]
    }

    pub(crate) fn set_scales(&mut self, id: InstrumentId, tick_size: Price, lot_size: Qty) {
        let row = &mut self.instruments[usize::from(id.0)];
        row.tick_size = Some(tick_size);
        row.lot_size = Some(lot_size);
    }

    pub(crate) fn set_binance_limits(&mut self, id: InstrumentId, limits: BinanceLimits) {
        let row = &mut self.instruments[usize::from(id.0)];
        row.min_qty = Some(limits.min_qty);
        row.min_notional = Some(limits.min_notional);
        row.max_num_orders = Some(limits.max_num_orders);
        row.max_num_order_amends = limits.max_num_order_amends;
    }

    /// `min_notional` is deliberately left absent: Polymarket floors an order by SHARES and by
    /// nothing else, and a zero stamped here would read as a floor the venue never set.
    pub(crate) fn set_poly_limits(&mut self, id: InstrumentId, limits: PolyLimits) {
        let row = &mut self.instruments[usize::from(id.0)];
        row.min_qty = Some(limits.min_qty);
        row.max_num_orders = Some(limits.max_num_orders);
        row.max_num_order_amends = Some(0);
        row.max_price = Some(limits.max_price);
    }

    pub fn instruments(&self) -> &[InstrumentRow] {
        &self.instruments
    }

    pub(crate) fn set_order_rate_limits(&mut self, limits: Vec<OrderRateLimit>) {
        // One source means one stamped market; a second stamp would silently keep only the
        // last budget.
        debug_assert!(
            self.order_rate_limits.is_empty(),
            "order rate limits stamped twice — a second binance market would overwrite the first"
        );
        self.order_rate_limits = limits;
    }

    pub fn assets(&self) -> &AssetDictionary {
        &self.assets
    }

    /// The venue's ORDERS budget, validated and stamped at startup. Execution bring-up turns these
    /// into the engine's placement meter, so quoting stops before the venue's own limiter does.
    pub fn order_rate_limits(&self) -> &[OrderRateLimit] {
        &self.order_rate_limits
    }

    pub fn binance_env(&self) -> Option<BinanceEnv> {
        self.binance_env
    }

    pub fn producer_groups(&self) -> &[ProducerGroup] {
        &self.producer_groups
    }

    pub fn input_queue_count(&self) -> usize {
        self.input_queue_count
    }

    pub fn timer_queue_id(&self) -> QueueId {
        self.timer_queue_id
    }

    pub fn link_queue_id(&self) -> Option<QueueId> {
        self.link_queue_id
    }

    pub fn exec_queue_id(&self) -> Option<QueueId> {
        self.exec_queue_id
    }
}

// An armed mode owns an input queue exactly when its venue has an edge to feed one. Config
// validation refuses the other combinations first; this stays the structural answer, so a queue is
// never allocated for an edge that will not exist.
fn execution_queue_id<P>(config: &Config<P>, previous: QueueId) -> Option<QueueId> {
    let mode = config.execution.as_ref()?.mode;
    (mode.is_enabled() && config.source.supports_execution(mode)).then(|| QueueId(previous.0 + 1))
}
