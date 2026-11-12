//! Turns YAML into a validated config. An unknown-field typo becomes a named startup
//! error rather than silent data loss. Defaults live in code, not in the file.
//! Each larger block a config file can carry has its own submodule, beside `error` and `identity`.
//! The root schema and the small blocks that only name a directory live here, so a caller writes
//! `config::LinkConfig` and never names a submodule.

mod error;
mod execution;
mod identity;
mod link;
mod simulated;
mod sources;
mod tables;
mod tracker;

use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::time::DurationUs;

pub use error::ConfigError;
pub use execution::{ExecutionConfig, ExecutionMode, MAX_QUOTE_LEVELS};
pub use identity::{RunIdentity, StrategyId, TradingEngineId};
pub use link::{ControllerLoss, LinkConfig, PeerSubscription};
pub use simulated::SimConfig;
pub use sources::{
    BinanceEnv, BinanceMarket, KlineInterval, PolySeries, PolySubscriptions, SourceSpec,
    Subscriptions, VenueMarket,
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
    pub source: SourceSpec,
    pub strategy: StrategySpec<P>,
    pub persistence: Option<PersistenceConfig>,
    pub link: Option<LinkConfig>,
    pub execution: Option<ExecutionConfig>,
    #[serde(default)]
    pub exposure: ExposureConfig,
    pub logging: LoggingConfig,
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
        if cfg!(target_os = "linux") && self.engine.hot_core_id.is_none() {
            return Err(ConfigError::MissingHotCoreId);
        }
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
        check_range(
            "queues.input_capacity",
            self.queues.input_capacity as u64,
            1..=u64::MAX,
            "greater than 0",
        )?;
        check_range(
            "queues.persistence_capacity",
            self.queues.persistence_capacity as u64,
            1..=u64::MAX,
            "greater than 0",
        )?;
        if let Some(persistence) = &self.persistence {
            check_range(
                "persistence.retention_hours",
                persistence.retention_hours,
                1..=87_600,
                "1..=87600 (1 hour to 10 years)",
            )?;
        }
        self.source.validate()?;
        if let Some(link) = &self.link {
            link.validate()?;
        }
        if let Some(execution) = &self.execution {
            execution.validate(self.engine.spin_interval_us)?;
            self.check_execution_source(execution.mode)?;
        }
        Ok(())
    }

    fn check_execution_source(&self, mode: ExecutionMode) -> Result<(), ConfigError> {
        if !mode.is_enabled() {
            return Ok(());
        }
        if !self.source.supports_execution(mode) {
            let supported: Vec<&str> = self
                .source
                .execution_modes()
                .iter()
                .copied()
                .map(ExecutionMode::as_str)
                .collect();
            return Err(ConfigError::ExecutionModeUnsupported {
                venue: self.source.venue(),
                mode: mode.as_str(),
                supported: supported.join(", ").into_boxed_str(),
            });
        }
        let SourceSpec::Binance {
            market,
            subscriptions,
            ..
        } = &self.source
        else {
            return Ok(());
        };
        if !mode.is_simulated() {
            return Ok(());
        }
        if *market != BinanceMarket::Spot {
            return Err(ConfigError::SimulatedExecutionMarket {
                market: market.as_str(),
            });
        }
        let missing = subscriptions.disabled_streams();
        if !missing.is_empty() {
            return Err(ConfigError::SimulatedExecutionSubscriptions {
                missing: missing.join(", ").into_boxed_str(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// This field is required on Linux; it is ignored on macOS.
    pub hot_core_id: Option<usize>,
    #[serde(default = "default_tokio_workers")]
    pub tokio_workers: usize,
    /// The engine's sampling cadence. Mandatory, with no default.
    pub spin_interval_us: u64,
    #[serde(default = "default_drain_deadline_ms")]
    pub drain_deadline_ms: u64,
    #[serde(default = "default_warmup_secs")]
    pub warmup_secs: u64,
}

impl EngineConfig {
    pub(crate) fn spin_interval(&self) -> DurationUs {
        DurationUs::from_micros(self.spin_interval_us as i64)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuesConfig {
    #[serde(default = "default_input_capacity")]
    pub input_capacity: usize,
    #[serde(default = "default_persistence_capacity")]
    pub persistence_capacity: usize,
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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_logs_dir")]
    pub dir: PathBuf,
}

/// Separate from `persistence:` and always in force, because position outlives any single run.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExposureConfig {
    #[serde(default = "default_exposure_dir")]
    pub dir: PathBuf,
}

impl Default for ExposureConfig {
    fn default() -> Self {
        ExposureConfig {
            dir: default_exposure_dir(),
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

fn default_tokio_workers() -> usize {
    2
}

fn default_drain_deadline_ms() -> u64 {
    5_000
}

fn default_warmup_secs() -> u64 {
    10
}

fn default_input_capacity() -> usize {
    65_536
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

fn default_logs_dir() -> PathBuf {
    PathBuf::from("./logs")
}

fn default_exposure_dir() -> PathBuf {
    PathBuf::from("./exposure")
}
