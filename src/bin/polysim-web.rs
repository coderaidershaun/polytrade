//! Composition only: the browser workstation's server as its own process. It serves the bundle
//! `trunk build` wrote and carries each open tab's link datagrams to one running trading engine.
//! Stopping it stops nothing on the engines.
//!
//! ```text
//! cargo run --release --features web --bin polysim-web -- \
//!     --strategy strat-micro-recorder --link 127.0.0.1:9310
//! ```
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use polysim::web::{WebConfig, serve};

const USAGE: &str = "usage: polysim-web --strategy <strategy-id> --link <addr> [--link <addr>]... \
                     [--token <token>] [--port <port>] [--bundle <dir>] [--mcp <url>]

  --strategy  the trading engine's strategy id. Not carried on the wire: a receiver must already
              know it to compute the strategy hash its own frame guard checks
  --link      a trading engine's link bind address. Repeatable, and a PICKER - each tab follows one
              engine at a time, chosen from the link bar
  --token     the engine's link token, if its config sets one
  --port      the loopback port to serve on. 0 lets the OS pick (default 7879)
  --bundle    the folder `trunk build` wrote (default browser/dist)
  --mcp       the polysim-mcp address the app descriptor advertises to Jet";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("polysim-web: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let config = match WebConfig::parse(std::env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{USAGE}");
            return Ok(());
        }
        Err(error) => bail!("{:#}\n\n{USAGE}", anyhow::Error::new(error)),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("cannot build the tokio runtime")?;
    runtime
        .block_on(serve(config))
        .context("web server exited with an error")
}
