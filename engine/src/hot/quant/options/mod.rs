//! Options maths in plain numbers for risk checks and hedges. Each function uses only arithmetic
//! that rounds the same way on every target, so a replay on any host gives the same bits.

mod arbitrage;
mod black76;
mod bump;
mod elementary;
mod error;
mod implied_volatility;

pub use arbitrage::{
    Pillar, forward_volatility, largest_rise, lowest_butterfly, volatility_between,
};
pub use black76::{Black76, CoinParity, Greeks, OptionRight};
pub use bump::{
    AccountDeltas, BumpGreeks, Bumps, SecondOrderGreeks, Shift, account_deltas, greeks_by_bump,
    second_order_by_bump,
};
pub(crate) use elementary::{exp, ln};
pub use error::OptionsError;
