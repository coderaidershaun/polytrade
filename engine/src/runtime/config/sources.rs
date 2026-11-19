//! One item of the `sources:` list: which venue an engine reads, which instruments, which streams,
//! and the orders it sends there. It also holds `VenueMarket`, the venue plus its sub-market, so one
//! registry can hold Binance and Polymarket rows without either venue's wire words leaking into the
//! other.

use serde::Deserialize;

use super::ConfigError;
use super::execution::{ExecutionConfig, ExecutionMode, check_money};
use super::tracker::TrackerSpec;
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::DurationUs;

pub use crate::msg::kinds::KlineInterval;

/// The feed sits in its own key: an execution block beside the tag of the feed would be read through
/// the tag's buffer, which reads the bare word `off` as a boolean.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub feed: SourceSpec,
    #[serde(default)]
    pub execution: Option<ExecutionConfig>,
}

impl SourceConfig {
    /// The execution block of this source when its mode sends orders.
    pub(crate) fn armed_execution(&self) -> Option<&ExecutionConfig> {
        self.execution
            .as_ref()
            .filter(|execution| execution.mode.is_enabled())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "exchange", rename_all = "lowercase", deny_unknown_fields)]
pub enum SourceSpec {
    Binance {
        market: BinanceMarket,
        /// Single deployment field (avoid dual-env desync).
        #[serde(default)]
        env: BinanceEnv,
        base: String,
        quote: String,
        #[serde(default)]
        subscriptions: Subscriptions,
        #[serde(default = "default_kline_intervals")]
        kline_intervals: Vec<KlineInterval>,
        #[serde(default = "default_book_capacity")]
        book_capacity: usize,
        max_exposure_quote: f64,
        tracker: TrackerSpec,
    },
    Polymarket {
        series: PolySeries,
        #[serde(default)]
        subscriptions: PolySubscriptions,
        #[serde(default = "default_poly_book_capacity")]
        book_capacity: usize,
        max_exposure_quote: f64,
        tracker: TrackerSpec,
    },
}

impl SourceSpec {
    /// The venue as an operator spells it in `exchange:`.
    pub fn venue(&self) -> &'static str {
        match self {
            SourceSpec::Binance { .. } => "binance",
            SourceSpec::Polymarket { .. } => "polymarket",
        }
    }

    /// Which execution edges this venue actually has. Polymarket carries no simulated venue: the
    /// fill model is built on binance spot's depth granularity and aggregate trades.
    pub fn execution_modes(&self) -> &'static [ExecutionMode] {
        match self {
            SourceSpec::Binance { .. } => {
                &[ExecutionMode::Off, ExecutionMode::Sim, ExecutionMode::Live]
            }
            SourceSpec::Polymarket { .. } => &[ExecutionMode::Off, ExecutionMode::Live],
        }
    }

    pub fn supports_execution(&self, mode: ExecutionMode) -> bool {
        self.execution_modes().contains(&mode)
    }

    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        let max_exposure_quote = match self {
            SourceSpec::Binance {
                max_exposure_quote, ..
            } => *max_exposure_quote,
            SourceSpec::Polymarket {
                subscriptions,
                max_exposure_quote,
                ..
            } => {
                subscriptions.validate()?;
                *max_exposure_quote
            }
        };
        check_money("source.max_exposure_quote", max_exposure_quote)
    }
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum BinanceMarket {
        Spot = "spot",
        Perpetual = "perpetual",
    }
    pub fn as_str;
}

labelled_enum! {
    /// Config is the lower layer (adapter re-exports); names one type for both halves.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum BinanceEnv {
        #[default]
        Production = "production",
        Testnet = "testnet",
    }
    pub fn as_str;
}

labelled_enum! {
    /// Seam for additional series.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    pub enum PolySeries {
        #[serde(rename = "btc-updown-5m")]
        BtcUpDown5m = "btc-updown-5m",
    }
    pub fn as_str;
}

impl PolySeries {
    pub const fn window_len(self) -> DurationUs {
        match self {
            PolySeries::BtcUpDown5m => DurationUs::from_secs(300),
        }
    }

    /// The venue symbols this series registers, as `[up, down]` per rotation slot and in slot order.
    /// The rotation driver pairs its legs from here rather than re-typing the suffixes, so a symbol
    /// only ever has one spelling.
    pub fn slot_leg_symbols(self) -> [[Box<str>; 2]; 2] {
        let base = self.as_str();
        [["a-up", "a-down"], ["b-up", "b-down"]]
            .map(|slot| slot.map(|leg| format!("{base}-{leg}").into_boxed_str()))
    }

    pub fn slot_symbols(self) -> [Box<str>; 4] {
        let [[a_up, a_down], [b_up, b_down]] = self.slot_leg_symbols();
        [a_up, a_down, b_up, b_down]
    }

    /// The Gamma series this engine resolves windows from.
    pub const fn gamma_series_id(self) -> &'static str {
        match self {
            PolySeries::BtcUpDown5m => "10684",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueMarket {
    Binance(BinanceMarket),
    Polymarket(PolySeries),
}

impl VenueMarket {
    pub fn as_str(self) -> &'static str {
        match self {
            VenueMarket::Binance(market) => market.as_str(),
            VenueMarket::Polymarket(series) => series.as_str(),
        }
    }
}

/// No klines here (single market-data channel; unexpressable > default-true trap).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolySubscriptions {
    pub trades: bool,
    pub book_updates: bool,
    pub book_snapshots: bool,
}

impl Default for PolySubscriptions {
    fn default() -> Self {
        Self {
            trades: true,
            book_updates: true,
            book_snapshots: true,
        }
    }
}

impl PolySubscriptions {
    /// Polymarket delivers trades and the book on one combined channel, so a partial subscription
    /// is a promise this engine cannot keep: `trades: false` would still record trades.
    fn validate(self) -> Result<(), ConfigError> {
        if self.trades == self.book_updates && self.trades == self.book_snapshots {
            return Ok(());
        }
        let disabled: Vec<String> = Subscriptions::from(self)
            .disabled_streams()
            .iter()
            .map(|name| format!("{name}: false"))
            .collect();
        Err(ConfigError::Invalid {
            field: "source.subscriptions",
            value: disabled.join(", ").into(),
            expected: "all flags equal — polymarket's single combined channel delivers trades and books together, so v1 cannot honour a partial subscription",
        })
    }
}

impl From<PolySubscriptions> for Subscriptions {
    fn from(poly: PolySubscriptions) -> Self {
        Subscriptions {
            trades: poly.trades,
            book_updates: poly.book_updates,
            book_snapshots: poly.book_snapshots,
            klines: false,
        }
    }
}

/// All fields default true; omitted blocks enable the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Subscriptions {
    pub trades: bool,
    pub book_updates: bool,
    pub book_snapshots: bool,
    pub klines: bool,
}

impl Subscriptions {
    /// The market-data streams that are switched off, named as the config spells them. Klines are
    /// left out because no check that reads this depends on them.
    pub(super) fn disabled_streams(&self) -> Vec<&'static str> {
        [
            ("trades", self.trades),
            ("book_updates", self.book_updates),
            ("book_snapshots", self.book_snapshots),
        ]
        .into_iter()
        .filter(|(_, enabled)| !enabled)
        .map(|(name, _)| name)
        .collect()
    }
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self {
            trades: true,
            book_updates: true,
            book_snapshots: true,
            klines: true,
        }
    }
}

fn default_kline_intervals() -> Vec<KlineInterval> {
    vec![KlineInterval::OneMinute]
}

fn default_book_capacity() -> usize {
    10_000
}

fn default_poly_book_capacity() -> usize {
    128
}
