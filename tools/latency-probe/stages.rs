//! Mode `stages`: one full start of an engine on public venue data, whose metrics thread writes
//! every stage to its log once an interval. The run ends on SIGTERM or SIGINT, like any engine.

use std::path::Path;
use std::process::ExitCode;

use crate::config::load_harness_config;
use crate::rig::Silent;

pub fn run(config: &Path) -> ExitCode {
    // The engine loads the same file again; this load exists to refuse a live engine before it.
    if let Err(refusal) = load_harness_config(config) {
        eprintln!("latency-probe stages refused: {refusal}");
        return ExitCode::FAILURE;
    }
    let exit = engine::runtime::run_trading_engine::<Silent>("latency-probe", "stages", |code| {
        std::process::exit(code)
    });
    ExitCode::from(exit.code())
}
