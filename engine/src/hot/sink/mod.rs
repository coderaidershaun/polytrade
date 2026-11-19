//! Every ring producer the hot thread writes to. One type per full-ring policy, and the
//! policy is the file: `drop_counting` loses the message and counts — a lagging reader costs a
//! counted gap, never a stall; `research` drops a research row and counts it by table, and holds
//! its seal until the ring accepts it; `audit` banks each audit row behind a full ring and halts
//! quoting by a message at the high mark of the bank, so no audit row is lost while quoting runs;
//! `tape` latches one gap on a full ring and loses the records until whole books fit again;
//! `replacing` keeps only the newest snapshot and blocks bounded on `Drop`, because the run's final
//! position must reach disk; `exec` banks each command until the ring to the edge takes it:
//! `ExecSink` offers no `push` at all — only the command bank in the same file drains the ring,
//! FIFO — and that guarantee is the privacy of that file.
//!
//! "Sink" means exactly this family. Off-thread file writers are writers or outputs, never sinks.

mod audit;
mod drop_counting;
mod exec;
mod replacing;
mod research;
mod tape;

pub use audit::{AUDIT_BANK_HIGH_MARK, AUDIT_BANK_ROWS, AUDIT_RING_CAPACITY, AuditBank, AuditSink};
pub use drop_counting::{
    DropCountingSink, LinkSink, SampleSink, StrategyLogSink, UiBookSink, UiEventSink,
};
pub use exec::{ExecLaneBudget, ExecSink, exec_lane_capacity};
pub use replacing::{ExposureReceiver, ExposureSink};
pub use research::ResearchSink;
pub use tape::{TAPE_RING_CAPACITY, TapeSink, touched_tape_ring};

pub(crate) use exec::PendingCommands;
