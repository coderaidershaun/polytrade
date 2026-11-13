//! How a run ended, decided once from what its drain saw, and the code that the binary exits with,
//! so a fatal, a passed deadline, or a lost order can never read as a clean stop.

use std::fmt;

use crate::runtime::shutdown::FatalReason;
use crate::venues::exec::SweepFailure;

/// What started the drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// A request to stop, by its name: a signal, or a request of an operator.
    Request(&'static str),
    Fatal(FatalReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainDeadline {
    Kept,
    Passed,
}

/// A step of the drain that did not finish clean. The drain writes its cause to the log as well.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum DrainFailure {
    #[error(transparent)]
    Sweep(SweepFailure),
    /// The position file is what the next start restores, so a failed write loses position.
    #[error("exposure drain failed: {cause}")]
    Exposure { cause: Box<str> },
    #[error("persistence drain failed: {cause}")]
    Persistence { cause: Box<str> },
}

/// What the drain saw, which is all that the exit is decided from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainOutcome {
    pub trigger: Trigger,
    /// The first reason of the fatal latch, read after the drain, so a panic while draining counts.
    pub fatal: Option<FatalReason>,
    /// Each step that failed, in the order the drain ran them: the sweep of the execution edge,
    /// then exposure, then persistence. The first one names the exit.
    pub failures: Vec<DrainFailure>,
    pub deadline: DrainDeadline,
}

/// How a run ended. Only `Graceful` exits with code 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    Graceful { trigger: &'static str },
    DrainFailed(DrainFailure),
    DeadlinePassed,
    Halted(FatalReason),
    StartRefused,
}

impl Exit {
    /// A fatal reason names the exit before a passed deadline, and a passed deadline before a
    /// failed step. Only a request to stop with none of the three is graceful.
    pub fn decide(outcome: DrainOutcome) -> Self {
        let DrainOutcome {
            trigger,
            fatal,
            failures,
            deadline,
        } = outcome;
        let name = match trigger {
            Trigger::Fatal(reason) => return Exit::Halted(reason),
            Trigger::Request(name) => name,
        };
        if let Some(reason) = fatal {
            return Exit::Halted(reason);
        }
        if deadline == DrainDeadline::Passed {
            return Exit::DeadlinePassed;
        }
        match failures.into_iter().next() {
            Some(failure) => Exit::DrainFailed(failure),
            None => Exit::Graceful { trigger: name },
        }
    }

    pub fn code(&self) -> u8 {
        match self {
            Exit::Graceful { .. } => 0,
            Exit::DrainFailed(_) | Exit::DeadlinePassed | Exit::Halted(_) | Exit::StartRefused => 1,
        }
    }
}

impl fmt::Display for Exit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exit::Graceful { trigger } => write!(formatter, "received {trigger}"),
            Exit::DrainFailed(failure) => write!(formatter, "{failure}"),
            Exit::DeadlinePassed => write!(formatter, "the drain passed its deadline"),
            Exit::Halted(reason) => write!(formatter, "{reason}"),
            Exit::StartRefused => write!(formatter, "the start was refused"),
        }
    }
}
