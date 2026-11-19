//! The whole vocabulary of a strategy in one import: `use engine::hot::strategy::prelude::*;`.

pub use crate::hot::strategy::{
    Bps, Draws, EngineView, Fair, Fill, Market, MarketStates, NotReady, Notional, OrderReject,
    OrderUpdate, ParamError, ParamValue, PerMarket, Position, Price, Qty, Ready, Registration,
    Seed, SignalList, Strategy, StrategyConfig, StrategyCtx, StrategyParams, Ticks, Touch,
};
pub use crate::msg::ids::{InstrumentId, Side};
pub use crate::msg::inbound::{BookChunk, KlineEvent, MarketRotation, SpinTick, TradeEvent};
pub use crate::msg::time::DurationUs;
pub use crate::runtime::config::StrategySpec;
pub use crate::{alert, note, params, signals};
