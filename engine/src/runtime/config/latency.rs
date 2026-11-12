//! Latency config: how long one interval of the metrics thread lasts, how many intervals its window
//! holds, how often it reports to the workstation, how many samples each ring buffers, and the
//! budget of each stage.

use serde::Deserialize;

use crate::msg::latency::Stage;
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
    /// Absent by default: a budget comes from runs on the host that trades, never from a guess.
    #[serde(default)]
    pub budget: LatencyBudgets,
}

/// The most that the 99th and the 99.9th percentile of one stage may read over the window, in
/// microseconds. A breach only reports: it writes a warning, flags the rows and counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageBudget {
    #[serde(default)]
    pub p99_us: Option<u64>,
    #[serde(default)]
    pub p999_us: Option<u64>,
}

/// One budget for each of the four stages from the venue to the wire. A stage with no budget is
/// not checked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyBudgets {
    #[serde(default)]
    pub venue_to_engine: Option<StageBudget>,
    #[serde(default)]
    pub wire_to_message: Option<StageBudget>,
    #[serde(default)]
    pub message_to_decision: Option<StageBudget>,
    #[serde(default)]
    pub decision_to_wire: Option<StageBudget>,
}

impl LatencyBudgets {
    /// `None` for a stage that has no budget, a part of a stage among them.
    pub fn of(&self, stage: Stage) -> Option<StageBudget> {
        match stage {
            Stage::VenueToEngine => self.venue_to_engine,
            Stage::WireToMessage => self.wire_to_message,
            Stage::MessageToDecision => self.message_to_decision,
            Stage::DecisionToWire => self.decision_to_wire,
            Stage::VenueInternal
            | Stage::QueueWait
            | Stage::Processing
            | Stage::BankAndLaneWait
            | Stage::Send
            | Stage::OrderRoundTrip => None,
        }
    }

    fn checked_keys(&self) -> [(&'static str, Option<u64>); 8] {
        let p99 = |budget: Option<StageBudget>| budget.and_then(|budget| budget.p99_us);
        let p999 = |budget: Option<StageBudget>| budget.and_then(|budget| budget.p999_us);
        [
            (
                "latency.budget.venue_to_engine.p99_us",
                p99(self.venue_to_engine),
            ),
            (
                "latency.budget.venue_to_engine.p999_us",
                p999(self.venue_to_engine),
            ),
            (
                "latency.budget.wire_to_message.p99_us",
                p99(self.wire_to_message),
            ),
            (
                "latency.budget.wire_to_message.p999_us",
                p999(self.wire_to_message),
            ),
            (
                "latency.budget.message_to_decision.p99_us",
                p99(self.message_to_decision),
            ),
            (
                "latency.budget.message_to_decision.p999_us",
                p999(self.message_to_decision),
            ),
            (
                "latency.budget.decision_to_wire.p99_us",
                p99(self.decision_to_wire),
            ),
            (
                "latency.budget.decision_to_wire.p999_us",
                p999(self.decision_to_wire),
            ),
        ]
    }
}

impl Default for LatencyConfig {
    fn default() -> Self {
        LatencyConfig {
            interval_secs: default_interval_secs(),
            window_intervals: default_window_intervals(),
            display_ms: default_display_ms(),
            sample_ring: default_sample_ring(),
            budget: LatencyBudgets::default(),
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
        )?;
        for (key, budget_us) in self.budget.checked_keys() {
            let Some(budget_us) = budget_us else {
                continue;
            };
            // The histograms stop at ten seconds, so a larger budget could never be broken.
            check_range(key, budget_us, 1..=10_000_000, "1..=10000000 (1us to 10s)")?;
        }
        Ok(())
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
