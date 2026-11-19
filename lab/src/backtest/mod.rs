//! Replays recorded history through the fold of the engine: on one thread, with no clock and no
//! async runtime, so a replay folds exactly what the hot thread folded.

mod check;
mod command;
mod compare;
mod driver;
mod error;
mod reader;
mod refold;

pub use command::run_replay;
pub use compare::{RefoldDiff, check_refold};
pub use driver::ReplayDriver;
pub use error::ReplayError;
pub use reader::{TapeReader, TapeRow};
pub use refold::{RefoldOutcome, RefoldPlan, refold};
