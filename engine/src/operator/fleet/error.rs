//! Every way an operator request can be refused. Each message is written for the agent that made
//! the request: it names the engine or the file, and says what to do next when that is known.

use std::io;
use std::path::PathBuf;
use std::process::ExitStatus;

use crate::runtime::config::{ConfigError, RunIdentity};

#[derive(Debug, thiserror::Error)]
pub enum FleetError {
    #[error(transparent)]
    InvalidIdentity { source: ConfigError },
    #[error("the server is shutting down and starts no more engines")]
    ShuttingDown,
    #[error(
        "no trading engine {identity} under {}; list_strategies shows the ones that exist",
        .strategies.display()
    )]
    UnknownEngine {
        identity: RunIdentity,
        strategies: PathBuf,
    },
    #[error("{identity} is already running as pid {pid}")]
    AlreadyRunning { identity: RunIdentity, pid: u32 },
    #[error("{identity} is not running")]
    NotRunning { identity: RunIdentity },
    #[error("{identity} is stopping as pid {pid}; wait for its stop to end")]
    Stopping { identity: RunIdentity, pid: u32 },
    #[error(
        "{identity} is running in a process this server did not start, which holds its identity lock {}; stop that process first",
        .lock.display()
    )]
    RunningElsewhere {
        identity: RunIdentity,
        lock: PathBuf,
    },
    #[error("cannot read the identity lock {}", .path.display())]
    LockProbe {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("no binary at {}; build it with `cargo build --release --bin {bin}`", .path.display())]
    BinaryMissing { path: PathBuf, bin: String },
    #[error(
        "{identity} exited during start-up ({status}); the last lines of its console log:\n{tail}"
    )]
    ExitedEarly {
        identity: RunIdentity,
        status: ExitStatus,
        tail: String,
    },
    #[error(
        "{identity} is running as pid {pid}; stop it first, because it reads its mode only at boot"
    )]
    ModeLockedWhileRunning { identity: RunIdentity, pid: u32 },
    #[error(
        "{} has no mode: line with a value inside the execution: block of a source; put the value on \
         the mode: line, or add the block by hand",
        .path.display()
    )]
    NoModeLine { path: PathBuf },
    #[error("{} cannot be read as a config: {detail}", .path.display())]
    ConfigUnreadable { path: PathBuf, detail: Box<str> },
    #[error(
        "{} was not changed: after the edit it would read as mode {read_back}, so edit the mode \
         line by hand",
        .path.display()
    )]
    RewriteNotVerified {
        path: PathBuf,
        read_back: &'static str,
    },
    #[error("cannot read the directory {}", .path.display())]
    ReadDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot open the console log {}", .path.display())]
    ConsoleLog {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot read {}", .path.display())]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot write {}; the config is as it was", .path.display())]
    ConfigWrite {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "cannot write {}, and cannot remove its temporary file {} ({cleanup}); the config is as \
         it was, and the temporary file can be deleted by hand",
        .path.display(),
        .temporary.display()
    )]
    ConfigWriteLeftBehind {
        path: PathBuf,
        #[source]
        source: io::Error,
        temporary: PathBuf,
        cleanup: io::Error,
    },
    #[error(
        "{} now holds the new mode, but its folder {} could not be synced, so the edit is not \
         durable: a power cut can bring back the old mode; read the file before a start",
        .path.display(),
        .directory.display()
    )]
    ConfigNotSynced {
        path: PathBuf,
        directory: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot start {}", .path.display())]
    Spawn {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot send SIGTERM to {identity} (pid {pid})")]
    Signal {
        identity: RunIdentity,
        pid: u32,
        #[source]
        source: io::Error,
    },
    #[error("cannot SIGKILL {identity} (pid {pid})")]
    Kill {
        identity: RunIdentity,
        pid: u32,
        #[source]
        source: io::Error,
    },
    #[error("cannot read the exit status of {identity}")]
    Wait {
        identity: RunIdentity,
        #[source]
        source: io::Error,
    },
}
