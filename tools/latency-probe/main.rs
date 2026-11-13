//! The timing harness times the step, the sender, the idle wait and a full start, each in its own
//! mode and unchanged. A number from a host that does not trade is for development only.

mod clock;
mod config;
mod idle;
mod rig;
mod sender;
mod stages;
mod step;
mod tally;

use std::collections::HashMap;
use std::process::ExitCode;

use anyhow::{Context, bail};

use engine::hot::spawn::IdleWait;
use engine::runtime::config::SenderPlacement;

use idle::IdleRun;
use sender::SenderRun;
use step::StepRun;

const USAGE: &str = "usage:
  latency-probe step --stamps on|off [--messages N] [--rate HZ] [--core ID]
  latency-probe sender --placement shared|pinned [--commands N] [--rate HZ] [--tick-us US] [--core ID]
  latency-probe idle --wait spin|park [--park-us US] [--messages N] [--gap-us US] [--core ID]
  latency-probe stages --config PATH";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(mode) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    };
    if mode == "stages" {
        let arguments: Vec<String> = args.collect();
        return match config::stages_config_path(&arguments) {
            Ok(config) => stages::run(&config),
            Err(refusal) => {
                eprintln!("latency-probe stages refused: {refusal}\n{USAGE}");
                ExitCode::FAILURE
            }
        };
    }
    let options = match options(args) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("latency-probe: {error:#}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    match run_timed(&mode, &options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("latency-probe {mode}: {error:#}\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}

fn run_timed(mode: &str, options: &HashMap<String, String>) -> anyhow::Result<()> {
    let core = optional_number(options, "core")?.map(|core| core as usize);
    match mode {
        "step" => StepRun {
            are_stamps_on: switch(options, "stamps").context("step needs --stamps on|off")?,
            messages: number(options, "messages", 200_000)? as usize,
            rate_hz: number(options, "rate", 20_000)?,
            core,
        }
        .run()?,
        "sender" => SenderRun {
            placement: placement(options).context("sender needs --placement shared|pinned")?,
            commands: number(options, "commands", 20_000)? as usize,
            rate_hz: number(options, "rate", 2_000)?,
            tick_us: number(options, "tick-us", 1_000)?,
            core,
        }
        .run()?,
        "idle" => IdleRun {
            wait: wait(options).context("idle needs --wait spin|park")?,
            messages: number(options, "messages", 20_000)? as usize,
            gap_us: number(options, "gap-us", 500)?,
            core,
        }
        .run()?,
        other => bail!("unknown mode {other:?}"),
    }
    Ok(())
}

/// Each `--name value` pair after the mode.
fn options(mut args: impl Iterator<Item = String>) -> anyhow::Result<HashMap<String, String>> {
    let mut options = HashMap::new();
    while let Some(flag) = args.next() {
        let Some(name) = flag.strip_prefix("--") else {
            bail!("expected a --flag, got {flag:?}");
        };
        if name.contains('=') {
            bail!("write {flag:?} as `--name value`");
        }
        let value = args
            .next()
            .with_context(|| format!("--{name} needs a value"))?;
        if options.insert(name.to_owned(), value).is_some() {
            bail!("--{name} is given twice");
        }
    }
    Ok(options)
}

fn optional_number(options: &HashMap<String, String>, name: &str) -> anyhow::Result<Option<u64>> {
    options
        .get(name)
        .map(|raw| {
            raw.parse::<u64>()
                .with_context(|| format!("--{name} {raw:?} is not a whole number"))
        })
        .transpose()
}

fn number(options: &HashMap<String, String>, name: &str, default: u64) -> anyhow::Result<u64> {
    Ok(optional_number(options, name)?.unwrap_or(default))
}

fn switch(options: &HashMap<String, String>, name: &str) -> anyhow::Result<bool> {
    match options.get(name).map(String::as_str) {
        Some("on") => Ok(true),
        Some("off") => Ok(false),
        other => bail!("--{name} is {other:?}, not on or off"),
    }
}

fn placement(options: &HashMap<String, String>) -> anyhow::Result<SenderPlacement> {
    match options.get("placement").map(String::as_str) {
        Some("shared") => Ok(SenderPlacement::Shared),
        Some("pinned") => Ok(SenderPlacement::Pinned),
        other => bail!("--placement is {other:?}, not shared or pinned"),
    }
}

fn wait(options: &HashMap<String, String>) -> anyhow::Result<IdleWait> {
    match options.get("wait").map(String::as_str) {
        Some("spin") => Ok(IdleWait::Spin),
        Some("park") => Ok(idle::park(number(options, "park-us", 50)?)),
        other => bail!("--wait is {other:?}, not spin or park"),
    }
}
