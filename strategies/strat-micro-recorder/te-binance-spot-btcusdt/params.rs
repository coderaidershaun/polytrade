//! Wire layer for YAML operator knobs (order size, exposure budgets). Model internals → strategy.rs constants, not config.

use serde::Deserialize;

const DEFAULT_ORDER_NOTIONAL: f64 = 10.0;

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MicroRecorderParams {
    /// Δ (inventory shock per fill, quote units).
    pub order_notional: f64,
}

impl Default for MicroRecorderParams {
    fn default() -> Self {
        Self {
            order_notional: DEFAULT_ORDER_NOTIONAL,
        }
    }
}
