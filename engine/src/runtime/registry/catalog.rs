//! The registry cut down to what a workstation shows. It is built here, where the registry is, so
//! the message vocabulary never names the registry.

use crate::msg::kinds::ExecutionMode;
use crate::msg::ui::{UiCatalog, UiInstrument};
use crate::runtime::config::StrategyId;

use super::Registry;

impl UiCatalog {
    /// Takes `feature_names` as given, because by the time the catalog is built the strategy has
    /// moved into the hot engine.
    pub(crate) fn from_registry(
        strategy_id: &StrategyId,
        execution_mode: Option<ExecutionMode>,
        spin_interval_us: u64,
        feature_names: Vec<Box<str>>,
        registry: &Registry,
    ) -> Self {
        let instruments = registry
            .instruments()
            .iter()
            .map(|row| UiInstrument {
                instrument_id: row.instrument_id,
                display: row.display.clone(),
                base: row.base.clone(),
                quote: row.quote.clone(),
                base_asset: row.base_asset,
                quote_asset: row.quote_asset,
                tick_size: row.tick_size,
                lot_size: row.lot_size,
                qty_scale: row.qty_scale,
            })
            .collect();
        Self {
            strategy_id: strategy_id.as_str().into(),
            window_title: format!("Polysim — {}", strategy_id.as_str()).into(),
            execution_mode,
            spin_interval_us,
            instruments,
            feature_names,
        }
    }
}
