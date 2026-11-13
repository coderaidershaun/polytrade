//! How a run ended, decided once from what its drain saw, and the code that the binary exits with,
//! so a fatal, a passed deadline, or a lost order can never read as a clean stop.

use std::fmt;

use crate::msg::failure::{FailureClass, Halt};
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

impl DrainFailure {
    /// A running engine tries a failed write of the position file again, but the drain has no try
    /// left, and the next start would restore a stale position, so here it halts the engine.
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            DrainFailure::Sweep(failure) => failure.failure_class(),
            DrainFailure::Exposure { .. } | DrainFailure::Persistence { .. } => {
                FailureClass::Halt(Halt::Engine)
            }
        }
    }
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
        // Each failure is read by its class, even when a fatal or the deadline names the exit.
        let failed_steps: Vec<Exit> = failures.into_iter().map(Exit::of_failed_step).collect();
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
        let first_failed_step = failed_steps.into_iter().next();
        first_failed_step.unwrap_or(Exit::Graceful { trigger: name })
    }

    /// A failed step of the drain halts the engine. The drain has no reaction to another class, so
    /// one here is a bug of the drain.
    fn of_failed_step(failure: DrainFailure) -> Self {
        match failure.failure_class() {
            FailureClass::Halt(Halt::Engine) => Exit::DrainFailed(failure),
            other => unreachable!("the drain failure {failure} has class {other:?}, and no exit"),
        }
    }

    /// The exit of a start that its own check refused, from the class of the error.
    ///
    /// # Panics
    /// If the class is not a refused start: a start error of another class is a bug of the start
    /// code.
    pub fn refused(class: FailureClass) -> Self {
        match class {
            FailureClass::Halt(Halt::Start) => Exit::StartRefused,
            other => panic!("a start error of class {other:?} has no exit"),
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
