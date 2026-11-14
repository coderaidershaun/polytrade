//! The one start of an engine binary, so each binary starts, drains and exits the same way. The
//! engine is headless and owns the main thread, and the workstation runs as its own process.

use std::path::PathBuf;

use crate::hot::strategy::{EngineView, Strategy, StrategyConfig};
use crate::msg::failure::FailureClass;
use crate::runtime::config::{Config, RunIdentity};
use crate::runtime::pin::pin_to_core;

use super::Engine;
use super::drain::ForcedExit;
use super::exit::Exit;
use super::preflight::Preflight;

/// Runs strategy `S` as a trading engine and drains it on SIGINT, SIGTERM or a fatal trip. The
/// config comes from `--config`, or from the default path. Each failure is printed before it
/// returns, and the binary exits with [`Exit::code`]. `forced_exit` ends the process when the drain
/// passes `engine.drain_deadline_ms`.
pub fn run_trading_engine<S>(strategy_id: &str, te_id: &str, forced_exit: ForcedExit) -> Exit
where
    S: Strategy + StrategyConfig + 'static,
{
    let identity = match RunIdentity::new(strategy_id, te_id) {
        Ok(identity) => identity,
        Err(error) => {
            let label = format!("{strategy_id}-{te_id}");
            return refuse(&label, &error, error.failure_class());
        }
    };
    let label = identity.to_string();
    let Some(config_path) = config_path_from_args(&identity) else {
        eprintln!("{label}: --config requires a path argument");
        return Exit::StartRefused;
    };
    let engine = match start_engine::<S>(identity, config_path) {
        Ok(engine) => engine,
        Err(refused) => return refused,
    };

    let exit = engine.run_until_shutdown(forced_exit);
    println!(
        "{label} exit ({}): {exit}",
        if exit.code() == 0 { "graceful" } else { "fatal" }
    );
    exit
}

fn start_engine<S>(identity: RunIdentity, config_path: PathBuf) -> Result<Engine, Exit>
where
    S: Strategy + StrategyConfig + 'static,
{
    let label = identity.to_string();
    let config = Config::<S::Params>::load(&config_path)
        .map_err(|error| refuse(&label, &error, error.failure_class()))?;
    let engine_view = EngineView {
        spin_interval: config.engine.spin_interval(),
    };
    let strategy = Box::new(S::from_spec(&config.strategy, engine_view));
    let preflight = Preflight {
        identity: &identity,
        config: &config,
        strategy: strategy.as_ref(),
    };
    let plan = preflight
        .run()
        .map_err(|error| refuse(&label, &error, error.failure_class()))?;
    Engine::start(config, strategy, plan, pin_to_core)
        .map_err(|error| refuse(&label, &error, error.failure_class()))
}

/// Reads `--config <path>` or `--config=<path>` and ignores each other argument. With no flag the
/// config has its default path, and a flag with no value gives `None`, which refuses the start.
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

/// Prints the error and each cause under it, since a library hands its binary no anyhow report, and
/// gives the exit by the class of the error.
fn refuse(label: &str, error: &dyn std::error::Error, class: FailureClass) -> Exit {
    eprintln!("{label} failed to start: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        eprintln!("  caused by: {cause}");
        source = cause.source();
    }
    Exit::refused(class)
}
