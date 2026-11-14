//! The two lanes into the exporter: a copy of each line that the log drain writes, and each row
//! of the metrics thread. A full lane drops and counts, so no producer ever waits for a collector.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rtrb::{Producer, RingBuffer};

use crate::log::{CopiedLine, LaneReader};
use crate::msg::failure::FailureClass;
use crate::runtime::metrics::{RollupRow, RollupSink};

use super::OtlpPlan;

/// The write end of the rows lane: the metrics thread writes each row of a closed interval here.
pub struct OtlpRows {
    producer: Producer<RollupRow>,
    drops: Arc<AtomicU64>,
}

impl OtlpRows {
    /// A full lane drops the row and counts it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;
}

impl RollupSink for OtlpRows {
    fn write(&mut self, row: RollupRow) {
        if self.producer.push(row).is_err() {
            // The metrics thread is the one writer, so a load and a store count the drop.
            let dropped = self.drops.load(Ordering::Relaxed) + 1;
            self.drops.store(dropped, Ordering::Relaxed);
        }
    }
}

/// The read ends of the lanes, and where the exporter sends what it reads there.
pub struct OtlpLanes {
    pub(super) plan: OtlpPlan,
    pub(super) lines: LaneReader<CopiedLine>,
    pub(super) rows: LaneReader<RollupRow>,
}

pub(super) fn rows_lane(capacity: usize) -> (OtlpRows, LaneReader<RollupRow>) {
    let (producer, consumer) = RingBuffer::new(capacity);
    let drops = Arc::new(AtomicU64::new(0));
    let rows = OtlpRows {
        producer,
        drops: Arc::clone(&drops),
    };
    (rows, LaneReader::new(consumer, drops))
}
