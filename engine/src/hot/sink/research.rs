//! The research ring: a full ring drops a row and counts it by table, so research pressure never
//! stalls the fold, while the seal waits until the ring accepts it, up to a cap of tries.

use std::num::NonZeroU32;

use rtrb::Producer;

use crate::msg::failure::{FailureClass, Halt};
use crate::msg::latency::ResearchDrops;
use crate::msg::persist::ResearchRecord;

/// A dropped seal leaves Parquet unsealed and the run's research data unreadable, so SealAll
/// waits until the ring accepts it. Ordinary rows are still dropped and counted on a full ring.
pub struct ResearchSink {
    producer: Producer<ResearchRecord>,
    dropped: ResearchDrops,
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
    /// The ring refused each try that the cap allows. The seal gets no further push until the fold
    /// hears of the cap, one time. After that it still goes when room returns.
    PastCap {
        tries: u32,
        is_reported: bool,
    },
}

impl ResearchSink {
    /// A full ring drops a row and counts it.
    pub const ROW_WHEN_FULL: FailureClass = FailureClass::Reject;
    /// A full ring keeps the seal and tries it again, up to the cap.
    pub const SEAL_WHEN_FULL: FailureClass = FailureClass::Retry;
    /// A seal past its cap halts quoting.
    pub const SEAL_PAST_CAP: FailureClass = FailureClass::Halt(Halt::Quoting);

    pub fn new(producer: Producer<ResearchRecord>, seal_try_limit: NonZeroU32) -> Self {
        Self {
            producer,
            dropped: ResearchDrops::default(),
            seal: Seal::Sent,
            seal_try_limit,
        }
    }

    pub(crate) fn push(&mut self, record: ResearchRecord) {
        if self.can_push_seal() {
            self.send_seal();
        }
        // A row pushed ahead of a pending seal lands in the wrong file, so the seal goes first.
        if self.is_seal_pending() || self.producer.push(record).is_err() {
            self.count_drop(&record);
        }
    }

    #[cold]
    fn count_drop(&mut self, record: &ResearchRecord) {
        let drops = &mut self.dropped;
        let count = match record {
            ResearchRecord::Feature(_) => &mut drops.features,
            ResearchRecord::Trade(_) => &mut drops.trades,
            ResearchRecord::BookEvent(_) => &mut drops.book_events,
            ResearchRecord::Kline(_) => &mut drops.klines,
            ResearchRecord::LinkFrame(_) => &mut drops.link_frames,
            ResearchRecord::Mid(_) => &mut drops.mids,
            ResearchRecord::NoQuote(_) => &mut drops.no_quotes,
            // A seal never comes through here: it waits in the latch.
            ResearchRecord::SealAll => return,
        };
        *count += 1;
    }

    /// Closes and rolls every open file, ordered after any records already banked this spin. A
    /// seal that waits already closes every file once it goes, so a second request keeps its count
    /// of tries, and a writer that takes no seal still reaches the cap.
    #[cold]
    pub(crate) fn request_seal(&mut self) {
        if self.is_seal_pending() {
            return;
        }
        self.seal = Seal::Waiting { tries: 0 };
        self.try_seal();
    }

    /// Tries a waiting seal again. A seal at its cap gets no try here: the first retry that finds it
    /// there gives the count of tries, one time.
    #[inline]
    pub(crate) fn retry_pending_seal(&mut self) -> Option<u32> {
        match self.seal {
            Seal::Sent => return None,
            Seal::Waiting { .. } => self.try_seal(),
            Seal::PastCap {
                is_reported: true, ..
            } => self.send_seal(),
            Seal::PastCap {
                is_reported: false, ..
            } => {}
        }
        self.take_past_cap()
    }

    pub fn dropped(&self) -> ResearchDrops {
        self.dropped
    }

    fn is_seal_pending(&self) -> bool {
        self.seal != Seal::Sent
    }

    /// No push between the cap and the halt: one there would make the cap one more than the config
    /// says.
    fn can_push_seal(&self) -> bool {
        matches!(
            self.seal,
            Seal::Waiting { .. }
                | Seal::PastCap {
                    is_reported: true,
                    ..
                }
        )
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
        if self.producer.push(ResearchRecord::SealAll).is_ok() {
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
