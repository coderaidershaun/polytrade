//! The audit ring: no order, fill, incident or ledger row is lost while quoting runs. A full ring
//! banks the row, and at the high mark of the bank the sink posts one message that halts quoting.

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::time::Duration;

use rtrb::Producer;

use crate::hot::ingress::QueueProducer;
use crate::log::hot_error;
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::inbound::{AuditBankFull, InboundMessage, MessageBody};
use crate::msg::persist::AuditRecord;

/// Slots of the ring from the hot thread to the writer.
pub const AUDIT_RING_CAPACITY: usize = 4_096;

/// Rows the bank holds behind a full ring, reserved at the start so a push never allocates.
pub const AUDIT_BANK_ROWS: usize = 65_536;

/// Three quarters of the bank: the rows above it keep room for the sweep that the halt sends.
pub const AUDIT_BANK_HIGH_MARK: usize = 49_152;

/// Seals that can wait in the bank at once, each behind rows that came after the one before it.
const SEAL_SLOTS: usize = 64;

const FLUSH_ATTEMPT_INTERVAL: Duration = Duration::from_millis(2);
const FLUSH_ATTEMPTS: u32 = 250;

/// The size of the bank and the count of banked rows at which quoting halts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditBank {
    pub rows: usize,
    pub high_mark: usize,
}

impl AuditBank {
    pub const SHIPPED: AuditBank = AuditBank {
        rows: AUDIT_BANK_ROWS,
        high_mark: AUDIT_BANK_HIGH_MARK,
    };
}

/// The oldest seal that the ring has not taken yet. It waits in the bank behind each row banked
/// before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SealWait {
    tries: u32,
    is_reported: bool,
}

pub struct AuditSink {
    producer: Producer<AuditRecord>,
    /// First in, first out, with room for each row and each seal slot, reserved at the start.
    bank: VecDeque<AuditRecord>,
    limits: AuditBank,
    banked_rows: usize,
    banked_seals: usize,
    seal: Option<SealWait>,
    seal_try_limit: NonZeroU32,
    /// The audit status queue of the hot thread. `None` in a test that reads no halt.
    status: Option<QueueProducer>,
    is_full_posted: bool,
    banked_peak: u64,
    lost: u64,
}

impl AuditSink {
    /// A full ring keeps the row in the bank.
    pub const WHEN_FULL: FailureClass = FailureClass::Retry;
    /// The bank at its high mark halts quoting, by a message on the audit status queue.
    pub const BANK_AT_HIGH_MARK: FailureClass = FailureClass::Halt(Halt::Quoting);
    /// A bank at capacity drops the row and counts it. Quoting halted before that.
    pub const BANK_FULL: FailureClass = FailureClass::Reject;
    /// A seal that the ring has not taken at the cap of its tries halts quoting.
    pub const SEAL_PAST_CAP: FailureClass = FailureClass::Halt(Halt::Quoting);

    /// # Panics
    /// When the high mark is 0 or above the rows of the bank: a halt that can never come, or one
    /// that comes before the first row.
    pub fn new(
        producer: Producer<AuditRecord>,
        limits: AuditBank,
        seal_try_limit: NonZeroU32,
    ) -> Self {
        assert!(
            limits.high_mark > 0 && limits.high_mark <= limits.rows,
            "the high mark of the audit bank must lie in 1..={}, got {}",
            limits.rows,
            limits.high_mark
        );
        Self {
            producer,
            bank: VecDeque::with_capacity(limits.rows + SEAL_SLOTS),
            limits,
            banked_rows: 0,
            banked_seals: 0,
            seal: None,
            seal_try_limit,
            status: None,
            is_full_posted: false,
            banked_peak: 0,
            lost: 0,
        }
    }

    /// The queue that takes the one message of a bank at its high mark.
    pub fn reporting_to(mut self, status: QueueProducer) -> Self {
        self.status = Some(status);
        self
    }

    /// The most rows the bank held at once.
    pub fn banked_peak(&self) -> u64 {
        self.banked_peak
    }

    /// Rows dropped because the bank was at capacity.
    pub fn lost(&self) -> u64 {
        self.lost
    }

    /// Records that wait in the bank for room in the ring, a waiting seal included.
    pub fn banked(&self) -> usize {
        self.bank.len()
    }

    /// The bank goes first, so the rows reach the ring in the order they were pushed.
    #[inline]
    pub(crate) fn push(&mut self, record: AuditRecord) {
        self.flush();
        if self.bank.is_empty() && self.producer.push(record).is_ok() {
            return;
        }
        self.bank_row(record);
    }

    /// Closes the audit files after each row pushed before it. A seal behind rows banked after the
    /// last seal waits in its own slot, so each file ends where the fold said. A seal right behind
    /// another, or one with no slot left, merges into the one before it.
    #[cold]
    pub(crate) fn request_seal(&mut self) {
        self.flush();
        if self.bank.is_empty() && self.producer.push(AuditRecord::SealAll).is_ok() {
            return;
        }
        let is_behind_a_seal = matches!(self.bank.back(), Some(AuditRecord::SealAll));
        if is_behind_a_seal || self.banked_seals == SEAL_SLOTS {
            return;
        }
        self.bank.push_back(AuditRecord::SealAll);
        self.banked_seals += 1;
        self.seal.get_or_insert(SealWait {
            tries: 1,
            is_reported: false,
        });
    }

    /// Drains the bank, and counts one more try of a seal that still waits. The first retry that
    /// finds the seal at its cap gives the count of tries, one time.
    #[inline]
    pub(crate) fn retry_pending_seal(&mut self) -> Option<u32> {
        self.flush();
        let limit = self.seal_try_limit.get();
        let wait = self.seal.as_mut()?;
        if wait.is_reported {
            return None;
        }
        wait.tries = wait.tries.saturating_add(1);
        if wait.tries < limit {
            return None;
        }
        wait.is_reported = true;
        Some(wait.tries)
    }

    #[inline]
    fn flush(&mut self) {
        while let Some(&front) = self.bank.front() {
            if self.producer.push(front).is_err() {
                return;
            }
            self.bank.pop_front();
            match front {
                AuditRecord::SealAll => self.seal_sent(),
                _ => self.banked_rows -= 1,
            }
        }
    }

    /// The next seal in the bank counts its tries from 1 again.
    #[cold]
    fn seal_sent(&mut self) {
        self.banked_seals -= 1;
        self.seal = (self.banked_seals > 0).then_some(SealWait {
            tries: 1,
            is_reported: false,
        });
    }

    #[cold]
    fn bank_row(&mut self, record: AuditRecord) {
        if self.banked_rows == self.limits.rows {
            self.lost += 1;
            return;
        }
        self.bank.push_back(record);
        self.banked_rows += 1;
        self.banked_peak = self.banked_peak.max(self.banked_rows as u64);
        if self.banked_rows >= self.limits.high_mark && !self.is_full_posted {
            self.post_bank_full(record);
        }
    }

    /// Stamped with the row that crossed the mark, so a re-fold of the tape halts at the same
    /// message. Posted once for each run.
    #[cold]
    fn post_bank_full(&mut self, record: AuditRecord) {
        self.is_full_posted = true;
        let (Some(status), Some(received_ts_us)) = (self.status.as_mut(), record.received_ts_us())
        else {
            return;
        };
        let full = AuditBankFull {
            banked: self.banked_rows as u32,
            capacity: self.limits.rows as u32,
        };
        status.push(InboundMessage::new(
            received_ts_us,
            MessageBody::AuditBankFull(full),
        ));
    }
}

/// The hot thread ends after the last message, so the rows still in the bank go to the writer here,
/// with a bounded wait: each is a record of money.
impl Drop for AuditSink {
    fn drop(&mut self) {
        for _ in 0..FLUSH_ATTEMPTS {
            self.flush();
            if self.bank.is_empty() || self.producer.is_abandoned() {
                return;
            }
            std::thread::sleep(FLUSH_ATTEMPT_INTERVAL);
        }
        hot_error!(
            "the audit bank still held {} rows when the hot thread ended — the tape lacks them",
            self.banked_rows as u64
        );
    }
}
