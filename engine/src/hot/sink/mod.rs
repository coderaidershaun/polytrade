//! Every ring producer the hot thread writes to. One type per full-ring policy, and the
//! policy is the file: `drop_counting` loses the message and counts — a lagging reader costs a
//! counted gap, never a stall; `latched` holds the one record that must never drop (the
//! persistence seal) until the ring accepts it; `replacing` keeps only the newest snapshot and
//! blocks bounded on `Drop`, because the run's final position must reach disk; `exec` banks each
//! command until the ring to the edge takes it: `ExecSink` offers no `push` at all — only the
//! command bank in the same file drains the ring, FIFO — and that guarantee is the privacy of
//! that file.
//!
//! "Sink" means exactly this family. Off-thread file writers are writers or outputs, never sinks.

mod drop_counting;
mod exec;
mod latched;
mod replacing;

pub use drop_counting::{
    DropCountingSink, LinkSink, SampleSink, StrategyLogSink, UiBookSink, UiEventSink,
};
pub use exec::{ExecLaneBudget, ExecSink, exec_lane_capacity};
pub use latched::PersistSink;
pub use replacing::ExposureSink;

pub(crate) use exec::PendingCommands;
