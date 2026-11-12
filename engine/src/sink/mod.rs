//! Every ring producer the hot thread writes to. One type per full-ring policy, and the
//! policy is the file: `drop_counting` loses the message and counts — those consumers want
//! only the freshest state, so a lagging reader costs a gap, never a stall; `latched` holds
//! the one record that must never drop (the persistence seal) until the ring accepts it;
//! `replacing` keeps only the newest snapshot and blocks bounded on `Drop`, because the run's
//! final position must reach disk. The banked policy is re-exported from `hot::exec::command`:
//! `ExecSink` offers no `push` at all — only its command bank drains the ring, FIFO — and that
//! guarantee is module privacy there, which moving the type here would dissolve.
//!
//! "Sink" means exactly this family. Off-thread file writers are writers or outputs, never sinks.

mod drop_counting;
mod latched;
mod replacing;

/// Defined beside the `PendingCommands` that drains it; re-exported here as its only public path.
pub use crate::hot::exec::command::ExecSink;
pub use drop_counting::{
    DropCountingSink, LinkSink, MetricsSink, StrategyLogSink, UiBookSink, UiEventSink,
};
pub use latched::PersistSink;
pub use replacing::ExposureSink;
