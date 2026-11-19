//! The one start of each `replay` binary: it reads its arguments and the config, re-folds one run,
//! and exits 0 on a whole re-fold and 1 on a refusal, so each engine replays the same way.

use std::path::PathBuf;
use std::process::ExitCode;

use engine::hot::strategy::{Strategy, StrategyConfig};
use engine::runtime::config::Config;

use super::refold::{RefoldPlan, refold};

/// Re-folds the run that `--run <folder>` names, with the config of `--config <path>` (by default
/// `strategies/<strategy-id>/<te-id>/config.yaml`), below `--out <root>` (by default the
/// `persistence.dir` of that config). Each failure is printed with each cause under it.
pub fn run_replay<S>(strategy_id: &str, te_id: &str) -> ExitCode
where
    S: Strategy + StrategyConfig + 'static,
{
    let label = format!("{strategy_id}-{te_id}-replay");
    let arguments = match Arguments::read(std::env::args().skip(1), strategy_id, te_id) {
        Ok(arguments) => arguments,
        Err(problem) => return refused(&label, &problem),
    };
    let config_text = match std::fs::read_to_string(&arguments.config) {
        Ok(text) => text,
        Err(error) => {
            return refused(
                &label,
                &format!(
                    "cannot read the config {}: {error}",
                    arguments.config.display()
                ),
            );
        }
    };
    let config = match Config::<S::Params>::from_yaml(&config_text) {
        Ok(config) => config,
        Err(error) => return refused(&label, &chain(&error)),
    };
    let out_root = match arguments.out.or_else(|| {
        config
            .persistence
            .as_ref()
            .map(|persistence| persistence.dir.clone())
    }) {
        Some(root) => root,
        None => {
            return refused(
                &label,
                "the config has no persistence block and no --out was given",
            );
        }
    };
    let plan = RefoldPlan {
        source_run: arguments.run,
        config,
        config_text,
        out_root,
    };
    match refold::<S>(plan) {
        Ok(outcome) => {
            println!(
                "{label}: folded {} messages into {}, state digest {:?}",
                outcome.folded,
                outcome.run_dir.display(),
                outcome.state_digest
            );
            ExitCode::SUCCESS
        }
        Err(error) => refused(&label, &chain(&error)),
    }
}

struct Arguments {
    run: PathBuf,
    config: PathBuf,
    out: Option<PathBuf>,
}

impl Arguments {
    fn read(
        mut arguments: impl Iterator<Item = String>,
        strategy_id: &str,
        te_id: &str,
    ) -> Result<Self, String> {
        let mut run = None;
        let mut config = None;
        let mut out = None;
        while let Some(flag) = arguments.next() {
            let slot = match flag.as_str() {
                "--run" => &mut run,
                "--config" => &mut config,
                "--out" => &mut out,
                other => {
                    return Err(format!(
                        "unknown argument {other:?}; use --run, --config, --out"
                    ));
                }
            };
            let value = arguments
                .next()
                .ok_or_else(|| format!("{flag} needs a path after it"))?;
            *slot = Some(PathBuf::from(value));
        }
        Ok(Self {
            run: run.ok_or("--run <source run folder> is required")?,
            config: config.unwrap_or_else(|| {
                PathBuf::from(format!("strategies/{strategy_id}/{te_id}/config.yaml"))
            }),
            out,
        })
    }
}

fn chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str("\n  caused by: ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

fn refused(label: &str, problem: &str) -> ExitCode {
    eprintln!("{label} refused: {problem}");
    ExitCode::FAILURE
}
