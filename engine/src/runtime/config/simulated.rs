//! Configuration for the simulated venue, and what its validation proves: the source can be
//! simulated, a command is answered inside the in-flight timeout, and the fee is a rate it can charge.

use serde::Deserialize;

use super::ConfigError;
use super::execution::{
    check_bounded_milliseconds, check_nonnegative_amount, check_positive_milliseconds,
};
use crate::msg::ids::{FIXED_SCALE, fixed_mantissa};
use crate::msg::time::DurationUs;

const WHOLE_FILL_BPS: i64 = 10_000;

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
    /// The latency budget and the fee are built here, once, so the start reads them and never
    /// checks them again.
    pub(super) fn validate(
        &self,
        spin_interval_us: u64,
        inflight_timeout_ms: u64,
    ) -> Result<CheckedSimBlock, ConfigError> {
        check_nonnegative_amount(
            "execution.sim.opening_base_balance",
            self.opening_base_balance,
        )?;
        check_nonnegative_amount(
            "execution.sim.opening_quote_balance",
            self.opening_quote_balance,
        )?;
        let maker_fee = FeeBps::from_bps(self.maker_fee_bps)?;

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
        let too_slow = || ConfigError::Invalid {
            field: "execution.sim",
            value: worst_us.div_ceil(1_000).to_string().into(),
            expected: "entry + ack + market-data delay + two slowest producer heartbeats \
                       STRICTLY below execution.inflight_timeout_ms",
        };
        if worst_us >= timeout_us {
            return Err(too_slow());
        }
        let worst_round_trip = i64::try_from(worst_us).map_err(|_| too_slow())?;

        Ok(CheckedSimBlock {
            block: *self,
            budget: SimLatencyBudget {
                order_entry: millis(self.order_entry_latency_ms),
                ack: millis(self.ack_latency_ms),
                max_market_data_delay: millis(self.max_market_data_delay_ms),
                worst_round_trip: DurationUs::from_micros(worst_round_trip),
            },
            maker_fee,
        })
    }
}

/// Basis points as a 1e-8 mantissa, so a fractional rate stays exact. Negative is a rebate: the
/// fill pays the maker on top of what it traded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FeeBps(i64);

impl FeeBps {
    const WHOLE_FILL: i64 = WHOLE_FILL_BPS * FIXED_SCALE;

    /// The one way from a configured rate to the rate that a fill charges.
    ///
    /// # Errors
    /// [`ConfigError::Invalid`] for `execution.sim.maker_fee_bps` when the rate is not finite, or
    /// moves more than the whole fill as a fee or as a rebate.
    pub fn from_bps(bps: f64) -> Result<Self, ConfigError> {
        let is_in_range = bps.is_finite() && bps.abs() <= WHOLE_FILL_BPS as f64;
        match fixed_mantissa(bps) {
            Some(mantissa) if is_in_range => Ok(Self(mantissa)),
            _ => Err(ConfigError::Invalid {
                field: "execution.sim.maker_fee_bps",
                value: bps.to_string().into(),
                expected: "basis points in -10000..=10000, negative for a rebate",
            }),
        }
    }

    /// A rate is never more than the whole fill, so the fee is never larger than `amount`.
    pub(crate) fn charge(self, amount: i64) -> i64 {
        let fee = i128::from(amount) * i128::from(self.0) / i128::from(Self::WHOLE_FILL);
        i64::try_from(fee)
            .expect("a fee of at most the whole fill fits the amount it is charged on")
    }

    pub(crate) fn is_rebate(self) -> bool {
        self.0 < 0
    }
}

/// The latencies of a simulated venue, and its worst round trip, which validation found strictly
/// inside the in-flight timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SimLatencyBudget {
    order_entry: DurationUs,
    ack: DurationUs,
    max_market_data_delay: DurationUs,
    worst_round_trip: DurationUs,
}

impl SimLatencyBudget {
    pub(crate) fn order_entry(&self) -> DurationUs {
        self.order_entry
    }

    pub(crate) fn ack(&self) -> DurationUs {
        self.ack
    }

    pub(crate) fn max_market_data_delay(&self) -> DurationUs {
        self.max_market_data_delay
    }

    pub(crate) fn worst_round_trip(&self) -> DurationUs {
        self.worst_round_trip
    }
}

/// Validation found that the source can be simulated: Binance spot, with the trades and both depth
/// streams that the simulator taps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SimSupport(());

impl SimSupport {
    pub(super) fn of_checked_source() -> Self {
        Self(())
    }
}

/// A `sim:` block that passed validation. A block is validated in every mode, and it becomes the
/// proofs of a run only together with the support of its source.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct CheckedSimBlock {
    block: SimConfig,
    budget: SimLatencyBudget,
    maker_fee: FeeBps,
}

impl CheckedSimBlock {
    /// The support holds nothing: having one is the proof.
    pub(super) fn supported_by(self, _support: SimSupport) -> SimProofs {
        SimProofs {
            block: self.block,
            budget: self.budget,
            maker_fee: self.maker_fee,
        }
    }
}

/// What validation proved about a run in mode `sim`. Preflight builds the simulated venue only from
/// this value, so the start cannot skip a check of the config.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SimProofs {
    block: SimConfig,
    budget: SimLatencyBudget,
    maker_fee: FeeBps,
}

impl SimProofs {
    pub(crate) fn block(&self) -> &SimConfig {
        &self.block
    }

    pub(crate) fn budget(&self) -> SimLatencyBudget {
        self.budget
    }

    pub(crate) fn maker_fee(&self) -> FeeBps {
        self.maker_fee
    }
}

fn millis(ms: u64) -> DurationUs {
    DurationUs::from_millis(ms as i64)
}
