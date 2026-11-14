//! Turns YAML into a validated config, so a misspelt key is a named start error, not a lost
//! setting. Defaults live in code, and a caller names each block from this module.

mod engine;
mod error;
mod execution;
mod identity;
mod latency;
mod link;
mod logging;
mod senders;
mod simulated;
mod source_list;
mod sources;
mod tables;
mod tracker;

use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use source_list::check_execution_source;

pub use engine::{EngineConfig, IdleMode};
pub use error::ConfigError;
pub use execution::{ExecutionConfig, ExecutionMode, MAX_QUOTE_LEVELS};
pub use identity::{RunIdentity, StrategyId, TradingEngineId};
pub use latency::{LatencyBudgets, LatencyConfig, StageBudget};
pub use link::{ControllerLoss, LinkConfig, PeerSubscription};
pub use logging::LoggingConfig;
pub use senders::{SenderPlacement, SendersConfig};
pub use simulated::SimConfig;
pub use sources::{
    BinanceEnv, BinanceMarket, KlineInterval, PolySeries, PolySubscriptions, SourceConfig,
    SourceSpec, Subscriptions, VenueMarket,
};
pub use tables::{RecordedTables, TableKind};
pub use tracker::{
    CandlesSpec, EwmaVolSpec, ImbalanceSpec, IntensitySpec, SpinField, SpinSampledSpec,
    TrackerSpec, VolumeBarsSpec, VolumeThreshold, WindowsSpec,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use execution::DEFAULT_QUOTE_STOP_MARGIN_MS;
pub(crate) use execution::validated_mantissa;
pub(crate) use tracker::TARGET_WINDOW_CANDLES;

const MAX_OPERATIONAL_DURATION_MS: u64 = 24 * 60 * 60 * 1_000;

/// `P` is the strategy's own `params:` type, deserialized in the same single pass so
/// `deny_unknown_fields` holds inside it too; it defaults to [`NoParams`] for a knob-free strategy.
#[derive(Debug, Clone, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "P: serde::Deserialize<'de> + Default")
)]
pub struct Config<P = NoParams> {
    pub engine: EngineConfig,
    pub queues: QueuesConfig,
    /// Each venue that this engine reads, in the order that gives each its venue id.
    pub sources: Vec<SourceConfig>,
    pub strategy: StrategySpec<P>,
    pub persistence: Option<PersistenceConfig>,
    pub link: Option<LinkConfig>,
    #[serde(default)]
    pub exposure: ExposureConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub latency: LatencyConfig,
    #[serde(default)]
    pub senders: SendersConfig,
}

impl<P: serde::de::DeserializeOwned + Default> Config<P> {
    /// # Errors
    /// [`ConfigError::Read`] or errors from [`from_yaml`].
    ///
    /// [`from_yaml`]: Config::from_yaml
    pub fn load(path: &Path) -> Result<Config<P>, ConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Config::from_yaml(&contents)
    }

    /// # Errors
    /// [`ConfigError::Parse`] for malformed YAML or unknown/misspelt fields.
    pub fn from_yaml(contents: &str) -> Result<Config<P>, ConfigError> {
        let config: Config<P> =
            serde_saphyr::from_str(contents).map_err(|error| ConfigError::Parse {
                detail: error.to_string().into_boxed_str(),
            })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        self.engine.validate()?;
        if self.persistence.is_none() && !self.strategy.tables.is_empty() {
            let named: Vec<&str> = self
                .strategy
                .tables
                .iter()
                .copied()
                .map(TableKind::as_str)
                .collect();
            return Err(ConfigError::TablesWithoutPersistence {
                tables: named.join(", ").into_boxed_str(),
            });
        }
        check_range(
            "engine.spin_interval_us",
            self.engine.spin_interval_us,
            1_000..=60_000_000,
            "1000..=60000000 (1ms to 60s)",
        )?;
        check_range(
            "engine.warmup_secs",
            self.engine.warmup_secs,
            0..=3_600,
            "0..=3600 (0 disables warmup; beyond 1h a run is mostly suppressed)",
        )?;
        check_range(
            "engine.drain_deadline_ms",
            self.engine.drain_deadline_ms,
            1..=MAX_OPERATIONAL_DURATION_MS,
            "1..=86400000 (24h operational ceiling)",
        )?;
        self.queues.validate()?;
        if let Some(persistence) = &self.persistence {
            check_range(
                "persistence.retention_hours",
                persistence.retention_hours,
                1..=87_600,
                "1..=87600 (1 hour to 10 years)",
            )?;
            if let Some(limit) = persistence.seal_try_limit {
                check_range(
                    "persistence.seal_try_limit",
                    u64::from(limit),
                    1..=u64::from(u32::MAX),
                    "1 or more",
                )?;
            }
        }
        check_range(
            "exposure.max_failed_writes",
            u64::from(self.exposure.max_failed_writes),
            1..=u64::from(u32::MAX),
            "1 or more",
        )?;
        self.logging.validate(self.engine.drain_deadline_ms)?;
        self.latency.validate()?;
        self.senders.validate()?;
        if let Some(link) = &self.link {
            link.validate()?;
        }
        self.check_sources()?;
        for source in &self.sources {
            source.feed.validate()?;
            if let Some(execution) = &source.execution {
                execution.validate(self.engine.spin_interval_us)?;
                check_execution_source(&source.feed, execution.mode)?;
            }
        }
        Ok(())
    }
}

/// The slots of each ring. Each kind of input queue has its own capacity, because a full timer or
/// execution ring halts the engine while a full link ring drops a frame.
impl<P> Config<P> {
    pub(crate) fn seal_try_limit(&self) -> NonZeroU32 {
        let configured = self
            .persistence
            .as_ref()
            .and_then(|persistence| persistence.seal_try_limit);
        let limit = configured.unwrap_or_else(|| {
            let drain_deadline_us = self.engine.drain_deadline_ms.saturating_mul(1_000);
            let spins = drain_deadline_us.div_ceil(self.engine.spin_interval_us);
            u32::try_from(spins).unwrap_or(u32::MAX)
        });
        NonZeroU32::new(limit).unwrap_or(NonZeroU32::MIN)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuesConfig {
    /// The input ring of each market-data connection.
    #[serde(default = "default_input_capacity")]
    pub input_capacity: usize,
    /// `None` takes the spins of one `engine.drain_deadline_ms`, and 16 slots at least.
    #[serde(default)]
    pub timer_capacity: Option<usize>,
    #[serde(default = "default_link_capacity")]
    pub link_capacity: usize,
    /// `None` takes the capacity of the command lane, so the edge can answer each command that the
    /// lane holds.
    #[serde(default)]
    pub execution_capacity: Option<usize>,
    #[serde(default = "default_persistence_capacity")]
    pub persistence_capacity: usize,
}

impl QueuesConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let capacities = [
            ("queues.input_capacity", Some(self.input_capacity)),
            ("queues.timer_capacity", self.timer_capacity),
            ("queues.link_capacity", Some(self.link_capacity)),
            ("queues.execution_capacity", self.execution_capacity),
            (
                "queues.persistence_capacity",
                Some(self.persistence_capacity),
            ),
        ];
        for (field, capacity) in capacities {
            if let Some(capacity) = capacity {
                check_range(field, capacity as u64, 1..=u64::MAX, "greater than 0")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "P: serde::Deserialize<'de> + Default")
)]
pub struct StrategySpec<P = NoParams> {
    #[serde(default)]
    pub instruments: Instruments,
    #[serde(default)]
    pub tables: Vec<TableKind>,
    #[serde(default)]
    pub params: P,
}

/// The default type for a strategy with no knobs, which allows an empty `params: {}` block.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoParams {}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    #[serde(default = "default_data_dir")]
    pub dir: PathBuf,
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
    /// The tries of a waiting seal before quoting halts. `None` takes the spins of one
    /// `engine.drain_deadline_ms`: a writer that takes no seal for that long has stalled.
    #[serde(default)]
    pub seal_try_limit: Option<u32>,
}

/// Separate from `persistence:` and always in force, because position outlives any single run.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExposureConfig {
    #[serde(default = "default_exposure_dir")]
    pub dir: PathBuf,
    /// Failed writes of the file in a row, after which the writer reports once and quoting halts. A
    /// write that succeeds starts the count again.
    #[serde(default = "default_max_failed_writes")]
    pub max_failed_writes: u32,
}

impl Default for ExposureConfig {
    fn default() -> Self {
        ExposureConfig {
            dir: default_exposure_dir(),
            max_failed_writes: default_max_failed_writes(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Instruments {
    #[default]
    All,
    Explicit(Vec<Box<str>>),
}

impl<'de> Deserialize<'de> for Instruments {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct InstrumentsVisitor;

        impl<'de> serde::de::Visitor<'de> for InstrumentsVisitor {
            type Value = Instruments;

            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("\"all\" or a list of instrument symbols")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Instruments, E> {
                if value == "all" {
                    Ok(Instruments::All)
                } else {
                    Err(E::custom(format!(
                        "expected \"all\" or a list, got {value:?}"
                    )))
                }
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Instruments, A::Error> {
                let mut symbols = Vec::new();
                while let Some(symbol) = seq.next_element::<String>()? {
                    symbols.push(symbol.into_boxed_str());
                }
                Ok(Instruments::Explicit(symbols))
            }
        }

        deserializer.deserialize_any(InstrumentsVisitor)
    }
}

fn check_range(
    field: &'static str,
    value: u64,
    range: RangeInclusive<u64>,
    expected: &'static str,
) -> Result<(), ConfigError> {
    if range.contains(&value) {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        field,
        value: value.to_string().into(),
        expected,
    })
}

fn default_input_capacity() -> usize {
    65_536
}

fn default_link_capacity() -> usize {
    1_024
}

fn default_persistence_capacity() -> usize {
    65_536
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_retention_hours() -> u64 {
    6
}

fn default_exposure_dir() -> PathBuf {
    PathBuf::from("./exposure")
}

/// Three tries of a local file write take about 300 ms at the writer's poll.
fn default_max_failed_writes() -> u32 {
    3
}
