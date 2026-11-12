//! Senders config: where the loops that take commands and link frames to the wire run, and how
//! often each one wakes when it shares its threads with the other edges.

use std::time::Duration;

use serde::Deserialize;

use super::{ConfigError, check_range};

/// A cpu set names 1,024 cores, and a pin to a core past it cannot even be asked for.
const MAX_CORE_ID: u64 = 1_023;

/// Where the senders run. `Shared` is the default: each sender wakes on a tick, on the runtime of
/// every other edge. `Pinned` gives them one thread on a core of their own, where a loop that never
/// sleeps sends each command on its next pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SenderPlacement {
    #[default]
    Shared,
    Pinned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendersConfig {
    #[serde(default)]
    pub placement: SenderPlacement,
    /// The core of the pinned senders' thread. Linux refuses `pinned` without one.
    #[serde(default)]
    pub core_id: Option<usize>,
    /// How often a shared sender wakes. Below 1 ms the runtime's timer cannot wake it.
    #[serde(default = "default_tick_us")]
    pub tick_us: u64,
    /// How often the link sends its frames when the senders are shared.
    #[serde(default = "default_link_flush_ms")]
    pub link_flush_ms: u64,
}

impl Default for SendersConfig {
    fn default() -> Self {
        SendersConfig {
            placement: SenderPlacement::default(),
            core_id: None,
            tick_us: default_tick_us(),
            link_flush_ms: default_link_flush_ms(),
        }
    }
}

impl SendersConfig {
    pub(crate) fn tick(&self) -> Duration {
        Duration::from_micros(self.tick_us)
    }

    pub(crate) fn link_flush(&self) -> Duration {
        Duration::from_millis(self.link_flush_ms)
    }

    /// # Errors
    /// [`ConfigError::Invalid`] for a key outside its range, or for `pinned` with no core on linux.
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        if cfg!(target_os = "linux")
            && self.placement == SenderPlacement::Pinned
            && self.core_id.is_none()
        {
            return Err(ConfigError::Invalid {
                field: "senders.core_id",
                value: "absent".into(),
                expected: "a core id when senders.placement is pinned on linux",
            });
        }
        if let Some(core_id) = self.core_id {
            check_range(
                "senders.core_id",
                core_id as u64,
                0..=MAX_CORE_ID,
                "0..=1023, the cores a cpu set can name",
            )?;
        }
        check_range(
            "senders.tick_us",
            self.tick_us,
            1_000..=1_000_000,
            "1000..=1000000 (1ms to 1s)",
        )?;
        check_range(
            "senders.link_flush_ms",
            self.link_flush_ms,
            1..=1_000,
            "1..=1000 (1ms to 1s)",
        )
    }
}

fn default_tick_us() -> u64 {
    1_000
}

fn default_link_flush_ms() -> u64 {
    10
}
