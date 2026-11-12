//! Entry point: `run_trading_engine::<S>(strategy_id, te_id)`. Headless, owns main thread, opens no window.
//! Workstation is separate process. Terminal: prints failures.

use std::path::PathBuf;
use std::process::ExitCode;

use crate::config::{Config, RunIdentity};
use crate::hot::strategy::{EngineView, Strategy, StrategyConfig};

use super::Engine;

/// Run strategy S as trading engine, drain on SIGINT/SIGTERM/fatal. Config from --config or default path.
/// Returns SUCCESS only on graceful drain; all failures printed first.
pub fn run_trading_engine<S>(strategy_id: &str, te_id: &str) -> ExitCode
where
    S: Strategy + StrategyConfig + 'static,
{
    let identity = match RunIdentity::new(strategy_id, te_id) {
        Ok(identity) => identity,
        Err(error) => {
            print_error(&format!("{strategy_id}-{te_id}"), &error);
            return ExitCode::FAILURE;
        }
    };
    let label = identity.to_string();
    let Some(config_path) = config_path_from_args(&identity) else {
        eprintln!("{label}: --config requires a path argument");
        return ExitCode::FAILURE;
    };
    let Some(engine) = start_engine::<S>(identity, config_path) else {
        return ExitCode::FAILURE;
    };

    let report = engine.run_until_shutdown();
    println!(
        "{label} exit ({}): {}",
        if report.graceful { "graceful" } else { "fatal" },
        report.reason
    );
    if report.graceful { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

/// Load config, build strategy, start engine. None on failure; already printed (or logged by Engine::start).
fn start_engine<S>(identity: RunIdentity, config_path: PathBuf) -> Option<Engine>
where
    S: Strategy + StrategyConfig + 'static,
{
    let label = identity.to_string();
    let config = match Config::<S::Params>::load(&config_path) {
        Ok(config) => config,
        Err(error) => {
            print_error(&label, &error);
            return None;
        }
    };
    let engine_view = EngineView {
        spin_interval: config.engine.spin_interval(),
    };
    let strategy = Box::new(S::from_spec(&config.strategy, engine_view));
    match Engine::start(identity, config, strategy) {
        Ok(engine) => Some(engine),
        Err(error) => {
            print_error(&label, &error);
            None
        }
    }
}

/// Parse --config <path> / --config=<path>. Unknown args ignored. Missing flag -> default. No value -> None (error).
fn config_path_from_args(identity: &RunIdentity) -> Option<PathBuf> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if let Some(path) = arg.strip_prefix("--config=") {
            return Some(PathBuf::from(path));
        }
        if arg == "--config" {
            return args.next().map(PathBuf::from);
        }
    }
    Some(default_config_path(identity))
}

fn default_config_path(identity: &RunIdentity) -> PathBuf {
    PathBuf::from(format!(
        "strategies/{}/{}/config.yaml",
        identity.strategy_id.as_str(),
        identity.te_id.as_str()
    ))
}

/// Print error + source chain. Lib code -> no anyhow report available.
fn print_error(label: &str, error: &dyn std::error::Error) {
    eprintln!("{label} failed to start: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
}
