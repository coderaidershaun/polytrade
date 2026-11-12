//! The `logging:` block: where the log file goes, how much each lane holds, and how long the drain
//! may take at shutdown.

use std::path::PathBuf;

use serde::Deserialize;

use super::{ConfigError, MAX_OPERATIONAL_DURATION_MS, check_range};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_logs_dir")]
    pub dir: PathBuf,
    /// Records each thread's lane and the strategy lane hold before they drop.
    #[serde(default = "default_ring_capacity")]
    pub ring_capacity: usize,
    #[serde(default = "default_drain_deadline_ms")]
    pub drain_deadline_ms: u64,
    /// Reopens of a failing log file in a row before the drain falls back to stderr.
    #[serde(default = "default_reopen_limit")]
    pub reopen_limit: u32,
}

impl LoggingConfig {
    /// The log drain runs inside the engine's own drain, so its deadline must end first: else the
    /// engine's watchdog cuts the last log lines off.
    pub(super) fn validate(&self, engine_deadline_ms: u64) -> Result<(), ConfigError> {
        check_range(
            "logging.ring_capacity",
            self.ring_capacity as u64,
            1..=u64::MAX,
            "greater than 0",
        )?;
        check_range(
            "logging.drain_deadline_ms",
            self.drain_deadline_ms,
            1..=MAX_OPERATIONAL_DURATION_MS,
            "1..=86400000 (24h operational ceiling)",
        )?;
        if self.drain_deadline_ms >= engine_deadline_ms {
            return Err(ConfigError::LogDeadline {
                log_deadline_ms: self.drain_deadline_ms,
                engine_deadline_ms,
            });
        }
        Ok(())
    }
}

fn default_logs_dir() -> PathBuf {
    PathBuf::from("./logs")
}

fn default_ring_capacity() -> usize {
    8_192
}

fn default_drain_deadline_ms() -> u64 {
    2_000
}

fn default_reopen_limit() -> u32 {
    3
}
