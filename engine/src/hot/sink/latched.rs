//! The one record that must never drop: the seal waits until the persistence ring accepts it, and
//! the config caps the tries before the fold halts quoting.

use std::num::NonZeroU32;

use rtrb::Producer;

use crate::msg::failure::{FailureClass, Halt};
use crate::msg::persist::PersistRecord;

/// A dropped seal leaves Parquet unsealed and the run's research data unreadable, so SealAll
/// waits until the ring accepts it. Ordinary rows are still dropped and counted on a full ring.
pub struct PersistSink {
    producer: Producer<PersistRecord>,
    dropped: u64,
    seal: Seal,
    seal_try_limit: NonZeroU32,
}

/// Where the latest seal stands. Only the request and the retry of each spin count a try.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seal {
    Sent,
    /// The ring refused the seal this many times, fewer than the cap.
    Waiting {
        tries: u32,
    },
    /// The ring refused each try that the cap allows. The seal still goes when room returns, and
    /// the fold hears of the cap one time.
    PastCap {
        tries: u32,
        is_reported: bool,
    },
}

impl PersistSink {
    /// A full ring drops a row and counts it.
    pub const ROW_WHEN_FULL: FailureClass = FailureClass::Reject;
    /// A full ring keeps the seal and tries it again, up to the cap.
    pub const SEAL_WHEN_FULL: FailureClass = FailureClass::Retry;
    /// A seal past its cap halts quoting.
    pub const SEAL_PAST_CAP: FailureClass = FailureClass::Halt(Halt::Quoting);

    pub fn new(producer: Producer<PersistRecord>, seal_try_limit: NonZeroU32) -> Self {
        Self {
            producer,
            dropped: 0,
            seal: Seal::Sent,
            seal_try_limit,
        }
    }

    pub(crate) fn push(&mut self, record: PersistRecord) {
        if self.is_seal_pending() {
            self.send_seal();
        }
        // A row pushed ahead of a pending seal lands in the wrong file, so the seal goes first.
        if self.is_seal_pending() || self.producer.push(record).is_err() {
            self.dropped += 1;
        }
    }

    /// Closes and rolls every open file, ordered after any records already banked this spin.
    #[cold]
    pub(crate) fn request_seal(&mut self) {
        self.seal = Seal::Waiting { tries: 0 };
        self.try_seal();
    }

    /// Tries a waiting seal again. Gives the count of tries one time: at the first retry that finds
    /// the seal past its cap.
    #[inline]
    pub(crate) fn retry_pending_seal(&mut self) -> Option<u32> {
        match self.seal {
            Seal::Sent => return None,
            Seal::Waiting { .. } => self.try_seal(),
            Seal::PastCap { .. } => self.send_seal(),
        }
        self.take_past_cap()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    fn is_seal_pending(&self) -> bool {
        self.seal != Seal::Sent
    }

    #[cold]
    fn try_seal(&mut self) {
        let Seal::Waiting { tries } = self.seal else {
            return;
        };
        let tries = tries + 1;
        self.send_seal();
        if self.seal == Seal::Sent {
            return;
        }
        self.seal = if tries < self.seal_try_limit.get() {
            Seal::Waiting { tries }
        } else {
            Seal::PastCap {
                tries,
                is_reported: false,
            }
        };
    }

    #[cold]
    fn send_seal(&mut self) {
        if self.producer.push(PersistRecord::SealAll).is_ok() {
            self.seal = Seal::Sent;
        }
    }

    fn take_past_cap(&mut self) -> Option<u32> {
        let Seal::PastCap {
            tries,
            is_reported: false,
        } = self.seal
        else {
            return None;
        };
        self.seal = Seal::PastCap {
            tries,
            is_reported: true,
        };
        Some(tries)
    }
}
