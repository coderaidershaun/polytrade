//! Latency config: how long one interval of the metrics thread lasts, how many intervals its window
//! holds, how often it reports to the workstation, and how many samples each ring buffers.

use serde::Deserialize;

use crate::msg::time::DurationUs;

use super::{ConfigError, check_range};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyConfig {
    /// One interval is the unit of a log line and of a table row.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_window_intervals")]
    pub window_intervals: u64,
    #[serde(default = "default_display_ms")]
    pub display_ms: u64,
    /// Must hold the samples of the longest stall of the metrics thread: 200,000 messages a second
    /// for 100 ms fill 20,000.
    #[serde(default = "default_sample_ring")]
    pub sample_ring: u64,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        LatencyConfig {
            interval_secs: default_interval_secs(),
            window_intervals: default_window_intervals(),
            display_ms: default_display_ms(),
            sample_ring: default_sample_ring(),
        }
    }
}

impl LatencyConfig {
    pub(crate) fn interval(&self) -> DurationUs {
        DurationUs::from_secs(self.interval_secs as i64)
    }

    pub(crate) fn display_period(&self) -> DurationUs {
        DurationUs::from_millis(self.display_ms as i64)
    }

    /// # Errors
    /// [`ConfigError::Invalid`] for a key outside its range.
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        check_range(
            "latency.interval_secs",
            self.interval_secs,
            1..=3_600,
            "1..=3600 (1s to 1h)",
        )?;
        check_range(
            "latency.window_intervals",
            self.window_intervals,
            1..=60,
            "1..=60",
        )?;
        check_range(
            "latency.display_ms",
            self.display_ms,
            100..=60_000,
            "100..=60000 (0.1s to 60s)",
        )?;
        check_range(
            "latency.sample_ring",
            self.sample_ring,
            1_024..=1_048_576,
            "1024..=1048576",
        )
    }
}

fn default_interval_secs() -> u64 {
    60
}

fn default_window_intervals() -> u64 {
    10
}

fn default_display_ms() -> u64 {
    1_000
}

fn default_sample_ring() -> u64 {
    32_768
}
