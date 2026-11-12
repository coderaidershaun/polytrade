//! Configuration for the simulated venue.

use serde::Deserialize;

use super::ConfigError;
use super::execution::{
    check_bounded_milliseconds, check_nonnegative_amount, check_positive_milliseconds,
};

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SimConfig {
    pub order_entry_latency_ms: u64,
    pub ack_latency_ms: u64,
    pub max_market_data_delay_ms: u64,
    pub heartbeat_ms: u64,
    pub opening_base_balance: f64,
    pub opening_quote_balance: f64,
    /// Negative is a rebate, paid in the quote asset.
    pub maker_fee_bps: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            order_entry_latency_ms: 15,
            ack_latency_ms: 15,
            max_market_data_delay_ms: 1_000,
            heartbeat_ms: 100,
            opening_base_balance: 0.01,
            opening_quote_balance: 1_000.0,
            maker_fee_bps: 10.0,
        }
    }
}

impl SimConfig {
    pub(super) fn validate(
        &self,
        spin_interval_us: u64,
        inflight_timeout_ms: u64,
    ) -> Result<(), ConfigError> {
        check_nonnegative_amount(
            "execution.sim.opening_base_balance",
            self.opening_base_balance,
        )?;
        check_nonnegative_amount(
            "execution.sim.opening_quote_balance",
            self.opening_quote_balance,
        )?;
        if !self.maker_fee_bps.is_finite() || self.maker_fee_bps.abs() > 10_000.0 {
            return Err(ConfigError::Invalid {
                field: "execution.sim.maker_fee_bps",
                value: self.maker_fee_bps.to_string().into(),
                expected: "basis points in -10000..=10000, negative for a rebate",
            });
        }

        check_positive_milliseconds(
            "execution.sim.max_market_data_delay_ms",
            self.max_market_data_delay_ms,
        )?;
        check_positive_milliseconds("execution.sim.heartbeat_ms", self.heartbeat_ms)?;
        check_bounded_milliseconds(
            "execution.sim.order_entry_latency_ms",
            self.order_entry_latency_ms,
        )?;
        check_bounded_milliseconds("execution.sim.ack_latency_ms", self.ack_latency_ms)?;

        let heartbeat_us = u128::from(self.heartbeat_ms) * 1_000;
        let slowest_heartbeat_us = heartbeat_us.max(u128::from(spin_interval_us));
        let worst_us = [
            u128::from(self.order_entry_latency_ms) * 1_000,
            u128::from(self.ack_latency_ms) * 1_000,
            u128::from(self.max_market_data_delay_ms) * 1_000,
            slowest_heartbeat_us,
            slowest_heartbeat_us,
        ]
        .into_iter()
        .sum::<u128>();
        let timeout_us = u128::from(inflight_timeout_ms) * 1_000;
        if worst_us >= timeout_us {
            return Err(ConfigError::Invalid {
                field: "execution.sim",
                value: worst_us.div_ceil(1_000).to_string().into(),
                expected: "entry + ack + market-data delay + two slowest producer heartbeats \
                           STRICTLY below execution.inflight_timeout_ms",
            });
        }
        Ok(())
    }
}
