//! Each way a re-fold refuses its tape or fails, and the class of each: a tape that cannot give back
//! what its run folded is refused before the first fold, so no half run lands on disk.

use std::path::PathBuf;

use engine::msg::failure::{FailureClass, Halt};
use engine::runtime::RunFoldError;
use engine::runtime::config::ConfigError;
use engine::runtime::manifest::ManifestError;
use engine::runtime::registry::TermsMismatch;
use engine::tape::{PersistError, TapeDecodeError};

#[derive(thiserror::Error, Debug)]
pub enum ReplayError {
    #[error(
        "the tape lost seq {first_lost_seq} to {last_lost_seq}, and a replay never crosses a gap"
    )]
    Gap {
        first_lost_seq: u64,
        last_lost_seq: u64,
    },
    #[error("the tape jumps from seq {after_seq} to seq {next_seq} with no gap row")]
    Hole { after_seq: u64, next_seq: u64 },
    #[error("a fault stopped the tape of the source run, so its end is missing")]
    TapeStopped,
    #[error(
        "the source run {} is still open: its engine may still write its tape, so a re-fold waits \
         for its end",
        .run_dir.display()
    )]
    SourceOpen { run_dir: PathBuf },
    #[error("{} does not open with the boot row", .file.display())]
    BootMissing { file: PathBuf },
    #[error("the source run holds no tape file below {}", .dir.display())]
    NoTape { dir: PathBuf },
    #[error(
        "{} records tape kind {kind} `{name}` with digest {found:#018x}, and this build reads it as \
         {expected:#018x}",
        .file.display()
    )]
    LayoutMismatch {
        kind: u16,
        name: Box<str>,
        found: u64,
        expected: u64,
        file: PathBuf,
    },
    #[error("{} records tape kind {kind}, which this build does not read", .file.display())]
    UnknownKind { kind: u16, file: PathBuf },
    #[error("the tape is of contract version {found}, and this build reads up to {supported}")]
    ContractNewer { found: u32, supported: u32 },
    #[error(
        "the source run was folded with config {found:#018x}, and the config given is {expected:#018x}"
    )]
    ConfigMismatch { found: u64, expected: u64 },
    #[error("{}: {detail}", .file.display())]
    Footer { file: PathBuf, detail: Box<str> },
    #[error("cannot read the manifest {}", .file.display())]
    Manifest {
        file: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse the manifest {}", .file.display())]
    ManifestParse {
        file: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("cannot read {}", .file.display())]
    Read {
        file: PathBuf,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("{}: {detail}", .file.display())]
    Column { file: PathBuf, detail: Box<str> },
    #[error("tape row seq {seq} of kind {kind} does not decode")]
    Decode {
        seq: u64,
        kind: u16,
        #[source]
        source: TapeDecodeError,
    },
    #[error("the config of the replay does not build")]
    Config(#[source] ConfigError),
    #[error("the terms of the source run do not fit the instruments of the config")]
    Terms(#[source] TermsMismatch),
    #[error("the fold of the replay does not build")]
    Fold(#[source] RunFoldError),
    #[error("cannot write the manifest of the replay")]
    ReplayManifest(#[source] ManifestError),
    #[error("the replay lost {count} {what}, which only a bug of the replay can cause")]
    Lost { what: &'static str, count: u64 },
    #[error("the writer of the replay failed")]
    Writer(#[source] PersistError),
}

impl ReplayError {
    /// A tape that cannot give back its run refuses the replay before it starts. A tape of another
    /// layout or config is a tape this build must not read, so it is rejected whole.
    pub fn failure_class(&self) -> FailureClass {
        match self {
            ReplayError::LayoutMismatch { .. }
            | ReplayError::UnknownKind { .. }
            | ReplayError::ContractNewer { .. }
            | ReplayError::ConfigMismatch { .. } => FailureClass::Reject,
            ReplayError::Writer(error) => error.failure_class(),
            ReplayError::Gap { .. }
            | ReplayError::Hole { .. }
            | ReplayError::TapeStopped
            | ReplayError::SourceOpen { .. }
            | ReplayError::BootMissing { .. }
            | ReplayError::NoTape { .. }
            | ReplayError::Footer { .. }
            | ReplayError::Manifest { .. }
            | ReplayError::ManifestParse { .. }
            | ReplayError::Read { .. }
            | ReplayError::Column { .. }
            | ReplayError::Decode { .. }
            | ReplayError::Config(_)
            | ReplayError::Terms(_)
            | ReplayError::Fold(_)
            | ReplayError::ReplayManifest(_)
            | ReplayError::Lost { .. } => FailureClass::Halt(Halt::Start),
        }
    }
}
