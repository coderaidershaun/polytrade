//! Who this run is. Never deserialized — the strategy binary declares both halves as literals, and
//! they key the log file, the parquet footer, the execution lease and the link's sender hash.

use super::ConfigError;

/// Lowercased, `^[a-z][a-z0-9-]*$`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrategyId(Box<str>);

impl StrategyId {
    /// # Errors
    /// [`ConfigError::Identifier`] if invalid.
    pub fn new(raw: &str) -> Result<Self, ConfigError> {
        validated_identifier("strategy id", raw).map(StrategyId)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Same rule as [`StrategyId`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradingEngineId(Box<str>);

impl TradingEngineId {
    /// # Errors
    /// [`ConfigError::Identifier`] if invalid.
    pub fn new(raw: &str) -> Result<Self, ConfigError> {
        validated_identifier("trading engine id", raw).map(TradingEngineId)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Two-part key: log files, parquet trees, footers (per-TE uniqueness).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunIdentity {
    pub strategy_id: StrategyId,
    pub te_id: TradingEngineId,
}

impl RunIdentity {
    /// # Errors
    /// [`ConfigError::Identifier`] if either id is invalid.
    pub fn new(strategy_id: &str, te_id: &str) -> Result<Self, ConfigError> {
        Ok(RunIdentity {
            strategy_id: StrategyId::new(strategy_id)?,
            te_id: TradingEngineId::new(te_id)?,
        })
    }
}

impl core::fmt::Display for RunIdentity {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "{}-{}", self.strategy_id.0, self.te_id.0)
    }
}

fn validated_identifier(kind: &'static str, raw: &str) -> Result<Box<str>, ConfigError> {
    let lowered = raw.to_lowercase();
    let mut chars = lowered.chars();
    let well_formed = match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {
            chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        }
        _ => false,
    };
    if !well_formed {
        return Err(ConfigError::Identifier {
            kind,
            raw: raw.into(),
            reason: "must match ^[a-z][a-z0-9-]*$ after lowercasing",
        });
    }
    Ok(lowered.into_boxed_str())
}
