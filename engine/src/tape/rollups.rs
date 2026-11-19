//! The ring from the metrics thread to the tape writer. It has its own producer, because a ring
//! has one writer and the research ring belongs to the hot thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rtrb::{Consumer, Producer, RingBuffer};

use crate::msg::failure::FailureClass;
use crate::runtime::metrics::{RollupRow, RollupSink};

/// About 120 rows close in each interval of the metrics thread, so the ring holds several.
pub const ROLLUP_RING_CAPACITY: usize = 1_024;

/// The write end, held by the metrics thread.
pub struct RollupRows {
    producer: Producer<RollupRow>,
    dropped: Arc<AtomicU64>,
}

impl RollupRows {
    /// A full ring drops the row and counts it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    /// The count of dropped rows, shared, so the metrics thread can report it.
    pub fn drops_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.dropped)
    }
}

impl RollupSink for RollupRows {
    fn write(&mut self, row: RollupRow) {
        if self.producer.push(row).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The write end for the metrics thread and the read end for the writer.
pub fn rollup_ring(capacity: usize) -> (RollupRows, Consumer<RollupRow>) {
    let (producer, consumer) = RingBuffer::new(capacity);
    let rows = RollupRows {
        producer,
        dropped: Arc::new(AtomicU64::new(0)),
    };
    (rows, consumer)
}
