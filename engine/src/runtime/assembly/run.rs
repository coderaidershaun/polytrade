//! Entry point: `run_trading_engine::<S>(strategy_id, te_id)`. Headless, owns main thread, opens no window.
//! Workstation is separate process. Terminal: prints failures.

use std::path::PathBuf;

use crate::hot::strategy::{EngineView, Strategy, StrategyConfig};
use crate::runtime::config::{Config, RunIdentity};
use crate::runtime::pin::pin_to_core;

use super::Engine;
use super::drain::ForcedExit;
use super::exit::Exit;
use super::preflight::Preflight;

/// Run strategy S as trading engine, drain on SIGINT/SIGTERM/fatal. Config from --config or default path.
/// Each failure is printed before it returns; the binary exits with [`Exit::code`]. `forced_exit`
/// ends the process when the drain passes `engine.drain_deadline_ms`.
pub fn run_trading_engine<S>(strategy_id: &str, te_id: &str, forced_exit: ForcedExit) -> Exit
where
    S: Strategy + StrategyConfig + 'static,
{
    let identity = match RunIdentity::new(strategy_id, te_id) {
        Ok(identity) => identity,
        Err(error) => {
            print_error(&format!("{strategy_id}-{te_id}"), &error);
            return Exit::StartRefused;
        }
    };
    let label = identity.to_string();
    let Some(config_path) = config_path_from_args(&identity) else {
        eprintln!("{label}: --config requires a path argument");
        return Exit::StartRefused;
    };
    let Some(engine) = start_engine::<S>(identity, config_path) else {
        return Exit::StartRefused;
    };

    let exit = engine.run_until_shutdown(forced_exit);
    println!(
        "{label} exit ({}): {exit}",
        if exit.code() == 0 { "graceful" } else { "fatal" }
    );
    exit
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
    let preflight = Preflight {
        identity: &identity,
        config: &config,
        strategy: strategy.as_ref(),
    };
    let plan = match preflight.run() {
        Ok(plan) => plan,
        Err(error) => {
            print_error(&label, &error);
            return None;
        }
    };
    match Engine::start(config, strategy, plan, pin_to_core) {
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
