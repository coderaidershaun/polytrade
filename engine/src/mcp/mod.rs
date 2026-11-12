//! The MCP server an agent uses to operate the engines. It is an operator process: it reaches an
//! engine only through the operating system, never through its state.

mod catalog;
mod child;
mod error;
mod fleet;
mod http;
mod reply;
mod tools;

use std::net::SocketAddr;

use rmcp::ServiceExt;
use rmcp::service::{QuitReason, ServerInitializeError};
use tokio::signal::unix::{Signal, SignalKind, signal};

pub use error::FleetError;
pub use fleet::{Fleet, FleetTiming, StopOutcome};
pub use reply::{EngineListing, EngineSummary, ModeChange, Started, Stopped};
pub use tools::PolysimMcp;

#[derive(Debug)]
pub struct ServeReport {
    pub session: Result<(), ServeError>,
    pub stopped: Vec<StopOutcome>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("the mcp handshake failed")]
    Initialize(#[source] Box<ServerInitializeError>),
    #[error("the mcp session ended abnormally")]
    Session(#[source] tokio::task::JoinError),
    #[error("cannot install the {signal} handler")]
    SignalHandler {
        signal: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot bind {address}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot read stdin, so the server can no longer tell whether its parent is alive")]
    StdinRead(#[source] std::io::Error),
}

/// Under `EndAtEof` the session also ends when stdin closes, exactly as it does on SIGTERM, so a
/// parent that holds the other end of a pipe takes the server and its engines down when it dies.
/// Under `Ignore` stdin is never read, so a null stdin cannot end the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StdinWatch {
    Ignore,
    EndAtEof,
}

/// Serves on stdin and stdout until the client hangs up or the process gets SIGTERM or SIGINT.
/// Every engine this server started is stopped before it returns, whichever way the session ended.
pub async fn serve_stdio(fleet: Fleet) -> ServeReport {
    serve_until_exit(fleet, |server| async move {
        let running = server
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|error| ServeError::Initialize(Box::new(error)))?;
        match running.waiting().await {
            Ok(QuitReason::JoinError(error)) | Err(error) => Err(ServeError::Session(error)),
            Ok(_) => Ok(()),
        }
    })
    .await
}

/// Serves streamable HTTP at `http://127.0.0.1:<port>/mcp`, on loopback only, until the process
/// gets SIGTERM or SIGINT, or until stdin closes when `stdin_watch` asks for that. Port 0 lets the
/// operating system pick a free port, and the line the server prints on stderr once it listens
/// names that port. Every session drives the same fleet, so an engine outlives the client that
/// started it. Every engine this server started is stopped before it returns.
pub async fn serve_http(fleet: Fleet, port: u16, stdin_watch: StdinWatch) -> ServeReport {
    serve_until_exit(fleet, |server| async move {
        match stdin_watch {
            StdinWatch::Ignore => http::serve(server, port).await,
            StdinWatch::EndAtEof => tokio::select! {
                result = http::serve(server, port) => result,
                result = http::wait_until_stdin_closes() => result,
            },
        }
    })
    .await
}

/// Every transport must serve through here, so none can skip stopping the engines this server
/// started. A signal drops the session future at any point, even mid-handshake, so a transport
/// must do all of its setup inside `run_session` and clean up when that future is dropped.
async fn serve_until_exit<S>(fleet: Fleet, run_session: impl FnOnce(PolysimMcp) -> S) -> ServeReport
where
    S: Future<Output = Result<(), ServeError>>,
{
    let (mut terminate, mut interrupt) = match exit_signals() {
        Ok(signals) => signals,
        Err(error) => {
            return ServeReport {
                session: Err(error),
                stopped: Vec::new(),
            };
        }
    };
    let server = PolysimMcp::new(fleet);
    let session = tokio::select! {
        result = run_session(server.clone()) => result,
        Some(()) = terminate.recv() => Ok(()),
        Some(()) = interrupt.recv() => Ok(()),
    };
    let stopped = server.stop_all().await;
    ServeReport { session, stopped }
}

fn exit_signals() -> Result<(Signal, Signal), ServeError> {
    let install = |kind, name| {
        signal(kind).map_err(|source| ServeError::SignalHandler {
            signal: name,
            source,
        })
    };
    Ok((
        install(SignalKind::terminate(), "SIGTERM")?,
        install(SignalKind::interrupt(), "SIGINT")?,
    ))
}
