//! Re-folds one recorded run of the Binance spot engine of strat-micro-recorder from its tape into
//! a run of kind `replay`: `--run <run folder>`, `--config <path>`, `--out <persistence root>`.
use std::process::ExitCode;

mod strategy;

fn main() -> ExitCode {
    lab::backtest::run_replay::<strategy::MicroRecorder>(
        "strat-micro-recorder",
        "te-binance-spot-btcusdt",
    )
}
