//! Order-placement limits that gate the execution edge between a strategy bug and an
//! actual loss.
//!
//! Mode `off` is armed and ready to enable; the absence of an execution block means the
//! run never trades at all. Every field is validated in both modes. Each fact about the
//! venue's own source lives in exactly one env field, never duplicated, because two fields
//! can silently drift apart.

use serde::Deserialize;

use crate::ids::fixed_mantissa;
use crate::labelled_enum::labelled_enum;

use super::simulated::SimConfig;
use super::{ConfigError, MAX_OPERATIONAL_DURATION_MS, RunIdentity};

/// The declarative ladder is fixed-size so quote publication, reconciliation and the UI all share
/// one compile-time bound. Configuration chooses how much of it is active, never its allocation.
pub const MAX_QUOTE_LEVELS: usize = 8;
// The refusal text spells this bound out, so changing it must fail here until the text changes too.
const _: () = assert!(MAX_QUOTE_LEVELS == 8);
/// Named because bring-up compares against it: a venue whose markets never close reads a margin
/// left at the default as "unset" and anything else as an operator expecting it to do something.
pub(crate) const DEFAULT_QUOTE_STOP_MARGIN_MS: u64 = 3_000;
const MAX_FLATTEN_SLACK_TICKS: u32 = 100;
const MAX_EXECUTION_DURATION_SECS: u64 = MAX_OPERATIONAL_DURATION_MS / 1_000;
const MAX_EXECUTION_DURATION_US: u128 = MAX_OPERATIONAL_DURATION_MS as u128 * 1_000;

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    #[cfg_attr(
        feature = "mcp",
        derive(rmcp::schemars::JsonSchema),
        schemars(crate = "rmcp::schemars")
    )]
    #[serde(rename_all = "snake_case")]
    pub enum ExecutionMode {
        Off = "off",
        Sim = "sim",
        Live = "live",
    }
    pub fn as_str;
}

impl ExecutionMode {
    #[inline]
    pub fn needs_credentials(self) -> bool {
        self == ExecutionMode::Live
    }

    #[inline]
    pub fn is_enabled(self) -> bool {
        matches!(self, ExecutionMode::Sim | ExecutionMode::Live)
    }

    #[inline]
    pub fn is_simulated(self) -> bool {
        self == ExecutionMode::Sim
    }

    /// Keeps an absent execution block distinct from `off` in recorded metadata.
    pub fn footer_value(mode: Option<Self>) -> &'static str {
        mode.map_or("absent", Self::as_str)
    }

    pub fn badge(mode: Option<Self>) -> &'static str {
        match mode {
            None | Some(ExecutionMode::Off) => "OFF",
            Some(ExecutionMode::Sim) => "SIM",
            Some(ExecutionMode::Live) => "LIVE",
        }
    }

    /// Separates simulated artifacts while preserving existing live paths.
    pub fn artifact_stem(mode: Option<Self>, identity: &RunIdentity) -> String {
        match mode {
            Some(ExecutionMode::Sim) => format!("{identity}-sim"),
            _ => identity.to_string(),
        }
    }

    pub fn artifact_segment(mode: Option<Self>) -> Option<&'static str> {
        matches!(mode, Some(ExecutionMode::Sim)).then_some("sim")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub mode: ExecutionMode,

    #[serde(default = "default_max_orders_per_side")]
    pub max_orders_per_side: u32,
    #[serde(default = "default_requote_threshold_ticks")]
    pub requote_threshold_ticks: u32,

    pub min_base_balance: f64,
    pub min_quote_balance: f64,

    pub max_order_notional_quote: f64,
    /// Compared as integer hundredths bps downstream.
    pub max_quote_distance_bps: f64,
    pub max_book_age_ms: u64,

    /// Post-only crosses have a separate counter.
    #[serde(default = "default_max_consecutive_rejects")]
    pub max_consecutive_rejects: u32,
    pub max_session_loss_quote: f64,

    #[serde(default = "default_inflight_timeout_ms")]
    pub inflight_timeout_ms: u64,
    /// Detects a silent listenKey expiry — Binance's user stream goes quiet with no error.
    /// Inert on Polymarket.
    #[serde(default = "default_exec_silence_spins")]
    pub exec_silence_spins: u32,
    /// Must exceed the venue's redelivery window, to avoid a collision between tenants.
    #[serde(default = "default_order_reap_secs")]
    pub order_reap_secs: u64,
    /// Binance's `recvWindow`. Inert on Polymarket, which authenticates by HMAC over its own stamp.
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u32,
    /// The clock skew past which Binance refuses every signed request. Inert on Polymarket.
    #[serde(default = "default_max_clock_skew_ms")]
    pub max_clock_skew_ms: u64,
    #[serde(default = "default_disconnect_sweep_secs")]
    pub disconnect_sweep_secs: u64,

    /// How long before a rotating market's close the engine stops quoting it and pulls what is
    /// resting. Inert on a venue whose instruments never rotate. Size it in several spins: the
    /// ladder comes down one order per side per spin.
    #[serde(default = "default_quote_stop_margin_ms")]
    pub quote_stop_margin_ms: u64,
    /// How far THROUGH the far touch a flatten prices, in ticks. Zero takes only what is at the
    /// touch, which on a venue that delays taker orders leaves the flatten repeatedly unfilled.
    #[serde(default = "default_flatten_slack_ticks")]
    pub flatten_slack_ticks: u32,
    /// Polymarket's binary-outcome taker fee: `shares × rate × p × (1 − p)`, charged on top of a
    /// marketable buy's notional, so the funds gate must reserve it. Zero on a venue that does not
    /// price fees this way — which is what leaves Binance behaviour untouched.
    #[serde(default = "default_taker_fee_rate")]
    pub taker_fee_rate: f64,

    /// Required when `mode` is `sim`. Under `off` or `live` it is still validated but unused, so
    /// switching modes is a one-line edit.
    pub sim: Option<SimConfig>,
}

impl ExecutionConfig {
    /// # Errors
    /// [`ConfigError::Invalid`] when limits or the simulated venue configuration are invalid.
    pub(super) fn validate(&self, spin_interval_us: u64) -> Result<(), ConfigError> {
        check_positive_milliseconds("execution.max_book_age_ms", self.max_book_age_ms)?;
        check_positive_milliseconds("execution.inflight_timeout_ms", self.inflight_timeout_ms)?;
        check_positive_seconds("execution.order_reap_secs", self.order_reap_secs)?;
        check_positive_seconds(
            "execution.disconnect_sweep_secs",
            self.disconnect_sweep_secs,
        )?;
        check_bounded_milliseconds("execution.max_clock_skew_ms", self.max_clock_skew_ms)?;
        self.validate_sim(spin_interval_us)?;
        // Zero, unlike the quote floor: this reserves the asset being SOLD, and where the base
        // asset IS the position (outcome shares) every positive value subtracts from each exit —
        // a full-position offer reads Underfunded, and a flatten rounds below the venue's minimum
        // size and strands the position.
        check_nonnegative_amount("execution.min_base_balance", self.min_base_balance)?;
        check_money("execution.min_quote_balance", self.min_quote_balance)?;
        check_money(
            "execution.max_order_notional_quote",
            self.max_order_notional_quote,
        )?;
        check_money(
            "execution.max_session_loss_quote",
            self.max_session_loss_quote,
        )?;
        check_positive(
            "execution.max_quote_distance_bps",
            self.max_quote_distance_bps,
        )?;
        if self.max_quote_distance_bps > 10_000.0 {
            return Err(ConfigError::Invalid {
                field: "execution.max_quote_distance_bps",
                value: self.max_quote_distance_bps.to_string().into(),
                expected: "a positive distance no greater than 10000 basis points (100%)",
            });
        }
        if !(1..=MAX_QUOTE_LEVELS).contains(&(self.max_orders_per_side as usize)) {
            return Err(ConfigError::Invalid {
                field: "execution.max_orders_per_side",
                value: self.max_orders_per_side.to_string().into(),
                expected: "1..=8, the fixed quote-ladder capacity",
            });
        }
        check_non_zero(
            "execution.exec_silence_spins",
            u64::from(self.exec_silence_spins),
        )?;
        // The counter records the reject before the comparison, so zero halts on the FIRST hard
        // one — the strictest setting there is, wearing the look of a disabled limit.
        check_non_zero(
            "execution.max_consecutive_rejects",
            u64::from(self.max_consecutive_rejects),
        )?;
        check_positive_milliseconds("execution.quote_stop_margin_ms", self.quote_stop_margin_ms)?;
        if self.flatten_slack_ticks > MAX_FLATTEN_SLACK_TICKS {
            return Err(ConfigError::Invalid {
                field: "execution.flatten_slack_ticks",
                value: self.flatten_slack_ticks.to_string().into(),
                expected: "0..=100 — a market order priced a hundred ticks through the touch is a \
                           fat finger, not a slack",
            });
        }
        // A rate at or above 1 charges more than the shares can ever be worth.
        if !self.taker_fee_rate.is_finite() || !(0.0..1.0).contains(&self.taker_fee_rate) {
            return Err(ConfigError::Invalid {
                field: "execution.taker_fee_rate",
                value: self.taker_fee_rate.to_string().into(),
                expected: "0.0..1.0, a fraction of the traded notional",
            });
        }
        Ok(())
    }

    fn validate_sim(&self, spin_interval_us: u64) -> Result<(), ConfigError> {
        let sim = match (self.mode, self.sim.as_ref()) {
            (_, Some(sim)) => sim,
            (ExecutionMode::Sim, None) => {
                return Err(ConfigError::Invalid {
                    field: "execution.sim",
                    value: "absent".into(),
                    expected: "a sim block, because mode is sim and its assumptions have no safe default",
                });
            }
            (_, None) => return Ok(()),
        };
        sim.validate(spin_interval_us, self.inflight_timeout_ms)?;
        let verdict_retention_us = u128::from(self.order_reap_secs) * 1_000_000
            + u128::from(self.inflight_timeout_ms) * 1_000;
        if verdict_retention_us > MAX_EXECUTION_DURATION_US {
            return Err(ConfigError::Invalid {
                field: "execution.sim",
                value: verdict_retention_us.to_string().into(),
                expected: "order reap plus in-flight timeout no greater than 24 hours",
            });
        }
        Ok(())
    }
}

/// Bounds the value to the mantissa range, so the conversion at bring-up cannot fail.
pub(super) fn check_money(field: &'static str, value: f64) -> Result<(), ConfigError> {
    check_positive(field, value)?;
    check_mantissa_range(field, value)
}

/// Named without a currency on purpose: quote money and base-asset balances both come through
/// here, and the only thing they share is that zero is legal and the value has to survive
/// conversion to a mantissa.
pub(super) fn check_nonnegative_amount(field: &'static str, value: f64) -> Result<(), ConfigError> {
    if !value.is_finite() || value < 0.0 {
        return Err(ConfigError::Invalid {
            field,
            value: value.to_string().into(),
            expected: "a non-negative, finite number",
        });
    }
    check_mantissa_range(field, value)
}

fn check_mantissa_range(field: &'static str, value: f64) -> Result<(), ConfigError> {
    if fixed_mantissa(value).is_some() {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        field,
        value: value.to_string().into(),
        expected: "within the i64 1e-8 mantissa range (below ~9.2e10 units)",
    })
}

/// Every caller has already bounded its value to the mantissa range while validating the config, so
/// a value that will not convert is a gap in validation rather than an operator mistake.
pub(crate) fn validated_mantissa(units: f64) -> i64 {
    fixed_mantissa(units)
        .expect("a config amount reached bring-up without being bounded to the mantissa range")
}

fn check_positive(field: &'static str, value: f64) -> Result<(), ConfigError> {
    if value.is_finite() && value > 0.0 {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        field,
        value: value.to_string().into(),
        expected: "a positive, finite number",
    })
}

pub(super) fn check_non_zero(field: &'static str, value: u64) -> Result<(), ConfigError> {
    if value > 0 {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        field,
        value: "0".into(),
        expected: "greater than 0 — zero disables the limit rather than setting it",
    })
}

pub(super) fn check_bounded_milliseconds(
    field: &'static str,
    value: u64,
) -> Result<(), ConfigError> {
    if value <= MAX_OPERATIONAL_DURATION_MS {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        field,
        value: value.to_string().into(),
        expected: "0..=86400000 milliseconds (24h operational ceiling)",
    })
}

pub(super) fn check_positive_milliseconds(
    field: &'static str,
    value: u64,
) -> Result<(), ConfigError> {
    check_non_zero(field, value)?;
    check_bounded_milliseconds(field, value)
}

fn check_positive_seconds(field: &'static str, value: u64) -> Result<(), ConfigError> {
    check_non_zero(field, value)?;
    if value <= MAX_EXECUTION_DURATION_SECS {
        return Ok(());
    }
    Err(ConfigError::Invalid {
        field,
        value: value.to_string().into(),
        expected: "1..=86400 seconds (24h operational ceiling)",
    })
}

fn default_max_orders_per_side() -> u32 {
    1
}

fn default_requote_threshold_ticks() -> u32 {
    1
}

fn default_max_consecutive_rejects() -> u32 {
    5
}

fn default_exec_silence_spins() -> u32 {
    30
}

fn default_disconnect_sweep_secs() -> u64 {
    30
}

fn default_order_reap_secs() -> u64 {
    60
}

fn default_max_clock_skew_ms() -> u64 {
    1_000
}

fn default_inflight_timeout_ms() -> u64 {
    5_000
}

fn default_recv_window_ms() -> u32 {
    5_000
}

fn default_quote_stop_margin_ms() -> u64 {
    DEFAULT_QUOTE_STOP_MARGIN_MS
}

fn default_flatten_slack_ticks() -> u32 {
    2
}

fn default_taker_fee_rate() -> f64 {
    0.0
}
