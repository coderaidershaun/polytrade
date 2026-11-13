//! The `engine:` block: the hot core and how the hot thread waits, the core of the housekeeping
//! threads, the runtime's workers, the spin cadence, and the deadlines of warmup and drain.

use std::time::Duration;

use serde::Deserialize;

use crate::msg::time::DurationUs;

use super::{ConfigError, check_range};

/// How the hot thread waits while every input queue is empty.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdleMode {
    /// Spins on its core, so the next message is popped the moment it lands. It takes the core
    /// whole.
    #[default]
    Spin,
    /// Sleeps `engine.park_us` and looks again, which leaves the core to other threads.
    Park,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Required on Linux while the hot thread spins; ignored where no thread can be pinned.
    pub hot_core_id: Option<usize>,
    #[serde(default)]
    pub idle: IdleMode,
    #[serde(default = "default_park_us")]
    pub park_us: u64,
    /// The core of the log drain and metrics threads. Absent, the OS places them.
    pub housekeeping_core_id: Option<usize>,
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

    pub(crate) fn park(&self) -> Duration {
        Duration::from_micros(self.park_us)
    }

    /// A hot thread that spins needs a core of its own, and only Linux can give it one.
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        let is_unpinned_spin = self.idle == IdleMode::Spin && self.hot_core_id.is_none();
        if cfg!(target_os = "linux") && is_unpinned_spin {
            return Err(ConfigError::SpinWithoutHotCore);
        }
        check_range(
            "engine.park_us",
            self.park_us,
            1..=10_000,
            "1..=10000 (1us to 10ms)",
        )
    }
}

fn default_park_us() -> u64 {
    50
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
