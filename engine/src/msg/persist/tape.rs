//! What the hot thread hands the tape: each message that it pops, with its queue and its place in
//! the sequence, the books at each file border, and the one record of a run of lost records. Each
//! is a fixed-size copy, so the tee never allocates.

use crate::msg::ids::{InstrumentId, Qty, QueueId, VenueId};
use crate::msg::inbound::{BookChunk, InboundMessage};
use crate::msg::time::LocalTs;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TapeRecord {
    /// A message as the hot thread popped it, before the fold handled it.
    Message {
        seq: u64,
        queue: QueueId,
        message: InboundMessage,
    },
    /// One chunk of one book as the fold held it at a file border, so each file can be read
    /// without the files before it.
    BookSnapshot {
        seq: u64,
        received_ts_us: LocalTs,
        chunk: BookChunk,
    },
    /// The records that a full ring refused. A replay stops here rather than fold past it.
    Gap(TapeGap),
    /// Close the open tape file; the next record opens a new one.
    Seal,
}

/// A run of records that the tape lost, by their sequence numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TapeGap {
    pub first_lost_seq: u64,
    pub last_lost_seq: u64,
    /// The received stamp of the first lost record.
    pub received_ts_us: LocalTs,
}

/// The start state of the fold, which no message carries: the run nonce of each venue that trades
/// and each position restored from the run before. It is the first row of each tape, stamped with
/// the boot stamp of the run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootRecord {
    pub venues: Vec<BootVenue>,
    pub exposures: Vec<BootExposure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BootVenue {
    pub venue: VenueId,
    pub run_nonce: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BootExposure {
    pub instrument: InstrumentId,
    pub position_base: Qty,
    pub cash_quote: i64,
    pub basis_quote: i64,
}
