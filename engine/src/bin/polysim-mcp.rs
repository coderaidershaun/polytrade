//! Composition only: the agent's MCP server as its own process, speaking to its clients over stdio
//! or loopback HTTP. Its stdout carries the protocol under stdio, so nothing here prints to stdout
//! under either transport.
//!
//! ```text
//! cargo run --release --features mcp --bin polysim-mcp -- \
//!     [--root <path>] [--engines <dir>] [--http <port> [--watch-stdin]]
//! ```
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use polysim::mcp::{Fleet, FleetTiming, StdinWatch, StopOutcome, serve_http, serve_stdio};

const USAGE: &str =
    "usage: polysim-mcp [--root <path>] [--engines <dir>] [--http <port> [--watch-stdin]]

  --root         the repo checkout whose strategies/ folder lists the engines. Defaults to the
                 current directory
  --engines      the folder of prebuilt engine binaries. Defaults to the folder of this
                 executable
  --http         serve MCP at http://127.0.0.1:<port>/mcp instead of stdio, so many clients share
                 one server and its engines. 0 lets the OS pick the port
  --watch-stdin  with --http only: when stdin closes, stop every engine this server started and
                 exit, so a parent holding the stdin pipe open takes the server down with it";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Arguments {
    root: PathBuf,
    engines: Option<PathBuf>,
    transport: Transport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Transport {
    Stdio,
    Http { port: u16, stdin_watch: StdinWatch },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("polysim-mcp: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let Some(arguments) = parse_arguments(std::env::args().skip(1))? else {
        eprintln!("{USAGE}");
        return Ok(());
    };
    let root = std::fs::canonicalize(&arguments.root)
        .with_context(|| format!("cannot resolve the root {}", arguments.root.display()))?;
    let bin_dir = match arguments.engines {
        Some(engines) => std::fs::canonicalize(&engines).with_context(|| {
            format!("cannot resolve the engine directory {}", engines.display())
        })?,
        None => executable_dir()?,
    };
    let fleet = Fleet::new(root, bin_dir, FleetTiming::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("cannot build the tokio runtime")?;
    let report = runtime.block_on(async {
        match arguments.transport {
            Transport::Stdio => serve_stdio(fleet).await,
            Transport::Http { port, stdin_watch } => serve_http(fleet, port, stdin_watch).await,
        }
    });
    // Reading stdin blocks a thread that cannot be cancelled, so a plain drop of the runtime would
    // wait until the host closes stdin.
    runtime.shutdown_background();
    for outcome in report.stopped {
        eprintln!("polysim-mcp: {}", describe(outcome));
    }
    report.session.context("mcp session failed")
}

fn parse_arguments(mut arguments: impl Iterator<Item = String>) -> Result<Option<Arguments>> {
    let mut root = PathBuf::from(".");
    let mut engines = None;
    let mut port = None;
    let mut stdin_watch = StdinWatch::Ignore;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--watch-stdin" => {
                stdin_watch = StdinWatch::EndAtEof;
                continue;
            }
            _ => {}
        }
        let (flag, mut inline_value) = match argument.split_once('=') {
            Some((flag, value)) => (flag, Some(value.to_owned())),
            None => (argument.as_str(), None),
        };
        let mut value_of = |what: &str| {
            inline_value
                .take()
                .or_else(|| arguments.next())
                .with_context(|| format!("{flag} needs {what}"))
        };
        match flag {
            "--root" => root = PathBuf::from(value_of("a path")?),
            "--engines" => engines = Some(PathBuf::from(value_of("a directory")?)),
            "--http" => {
                let value = value_of("a port")?;
                port = Some(value.parse().with_context(|| {
                    format!("--http needs a port from 0 to 65535, got {value:?}")
                })?);
            }
            _ => bail!("unknown argument {argument:?}\n\n{USAGE}"),
        }
    }
    let transport = match (port, stdin_watch) {
        (Some(port), stdin_watch) => Transport::Http { port, stdin_watch },
        (None, StdinWatch::Ignore) => Transport::Stdio,
        (None, StdinWatch::EndAtEof) => bail!(
            "--watch-stdin needs --http: a stdio server already ends when stdin closes\n\n{USAGE}"
        ),
    };
    Ok(Some(Arguments {
        root,
        engines,
        transport,
    }))
}

fn executable_dir() -> Result<PathBuf> {
    let executable = std::env::current_exe().context("cannot locate this executable")?;
    let dir = executable
        .parent()
        .context("this executable has no parent directory")?;
    Ok(dir.to_path_buf())
}

fn describe(outcome: StopOutcome) -> String {
    match outcome.result {
        Ok(stopped) => format!(
            "stopped {} (exit code {}, forced {})",
            outcome.identity,
            stopped
                .exit_code
                .map_or_else(|| "none".to_owned(), |code| code.to_string()),
            stopped.forced
        ),
        Err(error) => format!(
            "could not stop {}: {:#}",
            outcome.identity,
            anyhow::Error::from(error)
        ),
    }
}
