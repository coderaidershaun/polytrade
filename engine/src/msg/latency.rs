//! The samples that measure latency, and the names of what they measure. The hot thread and the
//! senders push them raw and fixed in size, and only the metrics thread turns them into numbers.

use core::mem::offset_of;

use crate::msg::ids::{ClientOrderId, MAX_INPUT_QUEUES, QueueId};
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::{Tick, TsUs};

labelled_enum! {
    /// What a sample times.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(u8)]
    pub enum Category {
        Trade = "trade",
        BookDelta = "book_delta",
        BookSnapshot = "book_snapshot",
        Kline = "kline",
        /// A spin carries the cost of the decision: the quote pass, the reconcile pass, the emits.
        Spin = "spin",
        Exec = "exec",
        /// A command on its way from the decision to the wire.
        Command = "command",
    }
    pub fn label;
    pub const ALL;
}

labelled_enum! {
    /// Each span that latency is reported for: the four stages from the venue to the wire, their
    /// parts, and the round trip of an order.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Stage {
        /// From the venue's send stamp, or its event stamp when it sends none, to our receive.
        VenueToEngine = "venue_to_engine",
        /// From the venue's match to its send, only where the venue publishes both.
        VenueInternal = "venue_internal",
        /// From the socket read to the push into an input queue.
        WireToMessage = "wire_to_message",
        /// From the push into an input queue to the end of the fold of that message.
        MessageToDecision = "message_to_decision",
        /// From the push into an input queue to the pop.
        QueueWait = "queue_wait",
        /// The whole fold of one message, the execution pass and the emits included.
        Processing = "processing",
        /// From the moment a command enters the bank to the moment the transport takes its bytes.
        DecisionToWire = "decision_to_wire",
        /// From the moment a command enters the bank to the pop by its sender.
        BankAndLaneWait = "bank_and_lane_wait",
        /// From the pop by the sender to the moment the transport takes the bytes.
        Send = "send",
        /// From our send of a request to our receive of its answer.
        OrderRoundTrip = "order_round_trip",
    }
    pub fn label;
    pub const ALL;
}

labelled_enum! {
    /// What a command asks of the venue.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(u8)]
    pub enum CommandKind {
        Place = "place",
        Amend = "amend",
        Cancel = "cancel",
        /// A read of order state, which changes nothing at the venue.
        Reconcile = "reconcile",
    }
    pub fn label;
}

/// One message through the hot thread: the driver's two ticks and the stamps that the message
/// carried. It fills one cache line, so its push is one copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C, align(64))]
pub struct StageSample {
    pub pop_tick: Tick,
    /// Taken after the whole fold of the message, the execution pass and the emits included.
    pub done_tick: Tick,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
    /// [`StageSample::ABSENT`] when the venue sends no send stamp.
    pub exchange_sent_ts_us: TsUs,
    /// [`StageSample::ABSENT`] when the venue gives no event stamp, or the stamp is our own.
    pub exchange_ts_us: TsUs,
    /// [`StageSample::ABSENT`] unless the message answers a request that we timed.
    pub request_sent_ts_us: TsUs,
    pub category: Category,
    pub queue: QueueId,
    /// The driver's count of pops. It wraps.
    pub pop_seq: u32,
}

impl StageSample {
    /// Stands in for a stamp that the message does not carry. An `Option` would double the size of
    /// the sample.
    pub const ABSENT: TsUs = TsUs::from_micros(i64::MIN);
}

const _: () = {
    assert!(size_of::<StageSample>() == 64 && align_of::<StageSample>() == 64);
    assert!(offset_of!(StageSample, pop_tick) == 0);
    assert!(offset_of!(StageSample, done_tick) == 8);
    assert!(offset_of!(StageSample, received_ts_us) == 16);
    assert!(offset_of!(StageSample, queued_ts_us) == 24);
    assert!(offset_of!(StageSample, exchange_sent_ts_us) == 32);
    assert!(offset_of!(StageSample, exchange_ts_us) == 40);
    assert!(offset_of!(StageSample, request_sent_ts_us) == 48);
    assert!(offset_of!(StageSample, category) == 56);
    assert!(offset_of!(StageSample, queue) == 57);
    assert!(offset_of!(StageSample, pop_seq) == 60);
};

/// One command through a sender: when it entered the bank, when the sender popped it, and when the
/// transport took its bytes. It fills one cache line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C, align(64))]
pub struct WireSample {
    /// Read when the command entered the bank. A command that waits there keeps it.
    pub issue_tick: Tick,
    pub pop_tick: Tick,
    /// Read when the transport took the bytes of the command.
    pub wire_tick: Tick,
    /// The message time of the spin that decided the command.
    pub issued_ts_us: TsUs,
    pub client_id: ClientOrderId,
    pub kind: CommandKind,
    /// The input queue of the execution edge that sent the command, which names its venue.
    pub queue: QueueId,
}

const _: () = {
    assert!(size_of::<WireSample>() == 64 && align_of::<WireSample>() == 64);
    assert!(offset_of!(WireSample, issue_tick) == 0);
    assert!(offset_of!(WireSample, pop_tick) == 8);
    assert!(offset_of!(WireSample, wire_tick) == 16);
    assert!(offset_of!(WireSample, issued_ts_us) == 24);
    assert!(offset_of!(WireSample, client_id) == 32);
    assert!(offset_of!(WireSample, kind) == 40);
    assert!(offset_of!(WireSample, queue) == 41);
};

/// What the driver counts once for each spin: the backlog of each input queue, and the drop and
/// event counters of the hot thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, align(64))]
pub struct CounterSample {
    /// The message time of the spin.
    pub spin_ts_us: TsUs,
    pub spin_seq: u64,
    /// Messages that wait in each input queue, by queue id, once the spin itself is popped.
    pub backlog: [u32; MAX_INPUT_QUEUES],
    pub counters: EngineCounters,
}

const _: () = {
    assert!(size_of::<CounterSample>() == 256 && align_of::<CounterSample>() == 64);
    assert!(offset_of!(CounterSample, spin_ts_us) == 0);
    assert!(offset_of!(CounterSample, spin_seq) == 8);
    assert!(offset_of!(CounterSample, backlog) == 16);
    assert!(offset_of!(CounterSample, counters) == 96);
};

/// The drop and event counts of the fold since the start of the run, carried in each counter
/// sample.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineCounters {
    pub persist_dropped: u64,
    pub book_trimmed: u64,
    pub book_remove_missing: u64,
    pub klines_unconfigured: u64,
    pub intensity_inside_spread: u64,
    pub intensity_without_book: u64,
    pub snapshots_dropped: u64,
    pub orders_submitted: u64,
    pub bank_drops: LaneDrops,
    pub ui_books_dropped: u64,
    pub ui_events_dropped: u64,
    pub link_dropped: u64,
    /// Position snapshots replaced before the exposure writer took them. Nonzero means the file on
    /// disk trails the hot side, so a hard kill would restore a stale position.
    pub exposure_superseded: u64,
}

/// Dropped records per lane: lane full on push (output drop+count, never fatal).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct LaneDrops {
    pub features: u64,
    pub persist: u64,
    pub logs: u64,
    pub link: u64,
}
