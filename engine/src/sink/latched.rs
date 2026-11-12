//! The one record that must never drop: the seal latches until the persistence ring accepts it.

use rtrb::Producer;

use crate::msg::persist::PersistRecord;

/// A dropped seal leaves Parquet unsealed and the run's research data unreadable, so SealAll
/// latches until the ring accepts it. Ordinary rows are still dropped and counted on a full ring.
pub struct PersistSink {
    producer: Producer<PersistRecord>,
    dropped: u64,
    is_seal_pending: bool,
}

impl PersistSink {
    pub fn new(producer: Producer<PersistRecord>) -> Self {
        Self {
            producer,
            dropped: 0,
            is_seal_pending: false,
        }
    }

    pub(crate) fn push(&mut self, record: PersistRecord) {
        if self.is_seal_pending {
            self.flush_seal();
        }
        // A row pushed ahead of a pending seal lands in the wrong file, so the seal goes first.
        if self.is_seal_pending || self.producer.push(record).is_err() {
            self.dropped += 1;
        }
    }

    /// Closes and rolls every open file, ordered after any records already banked this spin.
    #[cold]
    pub(crate) fn request_seal(&mut self) {
        self.is_seal_pending = true;
        self.flush_seal();
    }

    #[inline]
    pub(crate) fn retry_pending_seal(&mut self) {
        if self.is_seal_pending {
            self.flush_seal();
        }
    }

    #[cold]
    fn flush_seal(&mut self) {
        if self.producer.push(PersistRecord::SealAll).is_ok() {
            self.is_seal_pending = false;
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}
