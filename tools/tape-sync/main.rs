//! Ships each sealed tape file of each closed run to a destination folder, and deletes the local
//! file only once the checksum of the shipped copy equals it.

mod runs;
mod ship;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};

use runs::{RunState, find_runs};
use ship::{ShipError, ship_run};

const USAGE: &str = "usage: tape-sync --source <persistence dir> --destination <folder>";

/// What one sync did. Each refused file or run is a REJECT: the tool says why, keeps the local
/// file, and goes on with the rest, and the exit code tells a scheduler that a person must look.
#[derive(Debug, Default)]
struct Tally {
    shipped: usize,
    open_runs: usize,
    refused: usize,
}

fn main() -> ExitCode {
    match sync() {
        Ok(tally) => {
            println!(
                "tape-sync: shipped {} files, left {} open runs, refused {}",
                tally.shipped, tally.open_runs, tally.refused
            );
            if tally.refused == 0 { ExitCode::SUCCESS } else { ExitCode::from(1) }
        }
        Err(error) => {
            eprintln!("tape-sync: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn sync() -> Result<Tally> {
    let (source, destination) = arguments()?;
    let runs = find_runs(&source)
        .with_context(|| format!("failed to list the runs below {}", source.display()))?;
    let mut tally = Tally::default();
    for run in runs {
        match run.state {
            RunState::Open => tally.open_runs += 1,
            RunState::Unknown(why) => {
                eprintln!("tape-sync: left {}: {why}", run.dir.display());
                tally.refused += 1;
            }
            RunState::Closed => {
                let outcome = ship_run(&run, &destination);
                tally.shipped += outcome.shipped;
                for error in &outcome.refused {
                    report(error);
                }
                tally.refused += outcome.refused.len();
            }
        }
    }
    Ok(tally)
}

/// One line for each refused file, with each cause under it.
fn report(error: &ShipError) {
    eprintln!("tape-sync: kept the local file: {error}");
    let mut cause = std::error::Error::source(error);
    while let Some(inner) = cause {
        eprintln!("  caused by: {inner}");
        cause = inner.source();
    }
}

fn arguments() -> Result<(PathBuf, PathBuf)> {
    let mut source = None;
    let mut destination = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" => source = args.next().map(PathBuf::from),
            "--destination" => destination = args.next().map(PathBuf::from),
            other => bail!("unknown argument {other:?}; {USAGE}"),
        }
    }
    match (source, destination) {
        (Some(source), Some(destination)) => Ok((source, destination)),
        _ => bail!("{USAGE}"),
    }
}
