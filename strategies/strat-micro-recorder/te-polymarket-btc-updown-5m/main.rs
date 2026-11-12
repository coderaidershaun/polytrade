//! Polymarket engine: publishes both up legs' quotes, depth, intensity and volume for the Binance
//! peer, and makes a naive market on the up leg whose window is open.
use std::process::ExitCode;

mod strategy;

fn main() -> ExitCode {
    polysim::runtime::run_trading_engine::<strategy::PolyUpPublisher>(
        "strat-micro-recorder",
        "te-polymarket-btc-updown-5m",
    )
}
