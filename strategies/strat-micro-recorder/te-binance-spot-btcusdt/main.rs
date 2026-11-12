//! One trading engine of strat-micro-recorder, bound to Binance spot BTC/USDT.
//! (strategy-id, te-id) keys config path, data tree, log file.
use std::process::ExitCode;

mod strategy;

fn main() -> ExitCode {
    polysim::runtime::run_trading_engine::<strategy::MicroRecorder>(
        "strat-micro-recorder",
        "te-binance-spot-btcusdt",
    )
}
