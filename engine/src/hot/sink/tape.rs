//! The tape ring: the tee of each popped message. A full ring never stalls the fold: it latches one
//! gap, loses each record until the ring has room for the gap, a seal and a snapshot of each book,
//! and then pushes them together, so the file after a gap opens with whole books.

use rtrb::{Consumer, Producer, RingBuffer};

use crate::hot::book::Book;
use crate::msg::failure::FailureClass;
use crate::msg::ids::{InstrumentId, QueueId, Side};
use crate::msg::inbound::{BOOK_CHUNK_LEVELS, BookChunk, BookChunkKind, InboundMessage, Level};
use crate::msg::persist::{TapeGap, TapeRecord};
use crate::msg::time::LocalTs;

/// The slots of the tape ring. A recording of full Binance depth peaked at 918 slots at the start,
/// 1.4 % of this.
pub const TAPE_RING_CAPACITY: usize = 65_536;

pub struct TapeSink {
    producer: Producer<TapeRecord>,
    /// The sequence number of the next record that the tee offers. The boot row holds 0.
    next_seq: u64,
    gap: Option<TapeGap>,
    lost: u64,
    gaps: u64,
}

impl TapeSink {
    /// A full ring loses the record, and the tape says where.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    pub fn new(producer: Producer<TapeRecord>) -> Self {
        Self {
            producer,
            next_seq: 1,
            gap: None,
            lost: 0,
            gaps: 0,
        }
    }

    /// Records lost to a full ring.
    pub fn lost(&self) -> u64 {
        self.lost
    }

    /// Runs of lost records, each one gap row on the tape.
    pub fn gaps(&self) -> u64 {
        self.gaps
    }

    /// At a spin that crosses a seal border, before the record of that spin: the open file closes,
    /// and the next one opens with each book as the fold holds it.
    pub fn seal_with_snapshots(&mut self, books: &[Book], received_ts_us: LocalTs) {
        if self.gap.is_some() {
            return;
        }
        let snapshot_chunks = snapshot_chunks(books);
        if self.producer.slots() < 1 + snapshot_chunks {
            self.lose(snapshot_chunks as u64, received_ts_us);
            return;
        }
        self.push(TapeRecord::Seal);
        self.push_snapshots(books, received_ts_us);
    }

    /// The record of one popped message, before the fold handles it. While a gap is open, the
    /// record goes only when the gap, a seal and the snapshots fit before it.
    #[inline]
    pub fn record(&mut self, queue: QueueId, message: &InboundMessage, books: &[Book]) {
        let received_ts_us = message.header.received_ts_us;
        if let Some(gap) = self.gap {
            let snapshot_chunks = snapshot_chunks(books);
            if self.producer.slots() < 3 + snapshot_chunks {
                self.lose(1, received_ts_us);
                return;
            }
            self.gap = None;
            self.push(TapeRecord::Gap(gap));
            self.push(TapeRecord::Seal);
            self.push_snapshots(books, received_ts_us);
        }
        // A plain push, not a count of the free slots first: the count reads the reader's index
        // on each call, and the push reads it only when its own copy says the ring is full.
        let record = TapeRecord::Message {
            seq: self.next_seq,
            queue,
            message: *message,
        };
        if self.producer.push(record).is_err() {
            self.lose(1, received_ts_us);
            return;
        }
        self.next_seq += 1;
    }

    #[cold]
    fn lose(&mut self, count: u64, received_ts_us: LocalTs) {
        let first = self.next_seq;
        self.next_seq += count;
        self.lost += count;
        let last = self.next_seq - 1;
        match &mut self.gap {
            Some(gap) => gap.last_lost_seq = last,
            None => {
                self.gaps += 1;
                self.gap = Some(TapeGap {
                    first_lost_seq: first,
                    last_lost_seq: last,
                    received_ts_us,
                });
            }
        }
    }

    fn push_snapshots(&mut self, books: &[Book], received_ts_us: LocalTs) {
        for (index, book) in books.iter().enumerate() {
            let instrument = InstrumentId(index as u16);
            self.push_side(instrument, Side::Buy, book.bids(), false, received_ts_us);
            self.push_side(instrument, Side::Sell, book.asks(), true, received_ts_us);
        }
    }

    /// One chunk at least for each side, so an empty side reads as empty and not as missing.
    fn push_side(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        levels: &[Level],
        is_last_side: bool,
        received_ts_us: LocalTs,
    ) {
        let mut rest = levels;
        loop {
            let mut chunk = BookChunk::new(instrument, BookChunkKind::Snapshot, side);
            let taken = chunk.push_levels(rest);
            rest = &rest[taken..];
            chunk.is_last_chunk = is_last_side && rest.is_empty();
            let seq = self.take_seq();
            self.push(TapeRecord::BookSnapshot {
                seq,
                received_ts_us,
                chunk,
            });
            if rest.is_empty() {
                return;
            }
        }
    }

    fn take_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Each caller has counted the free slots first.
    fn push(&mut self, record: TapeRecord) {
        assert!(
            self.producer.push(record).is_ok(),
            "the tape ring filled between its count of free slots and its push — single producer violated"
        );
    }
}

/// A tape ring with each slot written and read once, so no record of the run is the first touch of
/// a page: a first touch costs the hot thread a page fault, about a microsecond.
///
/// # Panics
/// If `capacity` is 0.
pub fn touched_tape_ring(capacity: usize) -> (Producer<TapeRecord>, Consumer<TapeRecord>) {
    assert!(capacity > 0, "a tape ring holds at least one record");
    let (mut producer, mut consumer) = RingBuffer::new(capacity);
    for _ in 0..capacity {
        assert!(
            producer.push(TapeRecord::Seal).is_ok(),
            "an empty ring of {capacity} slots takes {capacity} records"
        );
    }
    for _ in 0..capacity {
        assert!(
            consumer.pop().is_ok(),
            "a ring that took {capacity} records gives {capacity} back"
        );
    }
    (producer, consumer)
}

/// The chunks of one snapshot of each book.
fn snapshot_chunks(books: &[Book]) -> usize {
    let side = |levels: usize| levels.div_ceil(BOOK_CHUNK_LEVELS).max(1);
    books
        .iter()
        .map(|book| side(book.bids().len()) + side(book.asks().len()))
        .sum()
}
