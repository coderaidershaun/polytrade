//! Options maths in plain numbers for risk checks and hedges. Each function uses only arithmetic
//! that rounds the same way on every target, so a replay on any host gives the same bits.

mod black76;
mod elementary;
mod error;

pub use black76::{Black76, CoinParity, Greeks, OptionRight};
pub(crate) use elementary::{exp, ln};
pub use error::OptionsError;
