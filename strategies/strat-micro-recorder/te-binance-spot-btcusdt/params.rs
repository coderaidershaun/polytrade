//! Wire layer for YAML operator knobs (order size, exposure budgets). Model internals → strategy.rs constants, not config.

use engine::hot::strategy::{ParamError, ParamValue, StrategyParams};
use serde::{Deserialize, Serialize};

const DEFAULT_ORDER_NOTIONAL: f64 = 10.0;

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(default)]
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

impl StrategyParams for MicroRecorderParams {
    fn check(&self) -> Result<(), ParamError> {
        if self.order_notional.is_finite() && self.order_notional > 0.0 {
            return Ok(());
        }
        Err(ParamError::Refused {
            field: "order_notional",
            seen: ParamValue::Float(self.order_notional),
            expected: "a positive number of quote units",
        })
    }
}
