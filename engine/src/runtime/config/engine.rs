//! The `engine:` block: the hot core, the runtime's workers, the spin cadence, and the deadlines of
//! warmup and drain.

use serde::Deserialize;

use crate::msg::time::DurationUs;

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

fn default_tokio_workers() -> usize {
    2
}

fn default_drain_deadline_ms() -> u64 {
    5_000
}

fn default_warmup_secs() -> u64 {
    10
}
