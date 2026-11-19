//! Re-folds one recorded run of the Polymarket engine of strat-micro-recorder from its tape into a
//! run of kind `replay`: `--run <run folder>`, `--config <path>`, `--out <persistence root>`.
use std::process::ExitCode;

mod strategy;

fn main() -> ExitCode {
    lab::backtest::run_replay::<strategy::PolyUpPublisher>(
        "strat-micro-recorder",
        "te-polymarket-btc-updown-5m",
    )
}
