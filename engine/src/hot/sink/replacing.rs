//! Newest position snapshot only, held pending until the writer takes it — including at run end.
//! The snapshots go to the reader and come back, so the hot thread never allocates or frees one.

use std::time::Duration;

use rtrb::{Consumer, Producer, PushError, RingBuffer};

use crate::log::hot_warn;
use crate::msg::failure::FailureClass;
use crate::runtime::exposure::ExposureSnapshot;

/// Output ring: replace pending, retry cadence, destructor drains final.
pub struct ExposureSink {
    producer: Producer<ExposureSnapshot>,
    returned: Consumer<ExposureSnapshot>,
    /// The snapshots that no ring holds. The pool holds one for each slot of the ring and two
    /// more: the pending one, and the one that the reader copies.
    spare: Vec<ExposureSnapshot>,
    pending: Option<ExposureSnapshot>,
    superseded: u64,
}

/// The read end of the exposure lane. It copies each snapshot out and hands it back to the sink at
/// once, so a reader never holds one.
pub struct ExposureReceiver {
    snapshots: Consumer<ExposureSnapshot>,
    returned: Producer<ExposureSnapshot>,
}

/// The writer drains its ring on a 100ms poll; 2ms x 250 attempts spans five polls, long enough
/// to survive a brief writer hiccup without hanging shutdown.
const FLUSH_ATTEMPT_INTERVAL: Duration = Duration::from_millis(2);
const FLUSH_ATTEMPTS: u32 = 250;
const FLUSH_DEADLINE_MS: u64 = FLUSH_ATTEMPT_INTERVAL.as_millis() as u64 * FLUSH_ATTEMPTS as u64;

/// Destructor flushes final snapshot (covers drain + fatal + panic unwind). Blocking safe at thread end, bounded.
impl Drop for ExposureSink {
    fn drop(&mut self) {
        for _ in 0..FLUSH_ATTEMPTS {
            self.retry_pending();
            // Abandoned ring -> writer gone, nothing to do.
            if !self.is_pending() || self.producer.is_abandoned() {
                return;
            }
            std::thread::sleep(FLUSH_ATTEMPT_INTERVAL);
        }
        hot_warn!(
            "exposure: the run's final position never reached the writer after {} ms — the file on disk is one change stale",
            FLUSH_DEADLINE_MS
        );
    }
}

impl ExposureSink {
    /// A full ring keeps the newest snapshot and drops the older one that waited.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    /// A lane whose ring holds `capacity` snapshots, each with room for `instruments` instruments.
    /// Its pool of snapshots is made here, at start.
    pub fn lane(capacity: usize, instruments: usize) -> (ExposureSink, ExposureReceiver) {
        let pool = capacity + 2;
        let (producer, snapshots) = RingBuffer::new(capacity);
        let (returning, returned) = RingBuffer::new(pool);
        let spare = (0..pool)
            .map(|_| ExposureSnapshot::for_instruments(instruments))
            .collect();
        let sink = ExposureSink {
            producer,
            returned,
            spare,
            pending: None,
            superseded: 0,
        };
        let receiver = ExposureReceiver {
            snapshots,
            returned: returning,
        };
        (sink, receiver)
    }

    /// Keeps `snapshot` as the newest state: it replaces a pending one in place, and goes to the
    /// ring when there is room.
    pub fn push(&mut self, snapshot: &ExposureSnapshot) {
        self.take_back_returned();
        if let Some(pending) = self.pending.as_mut() {
            pending.copy_from(snapshot);
            self.superseded += 1;
        } else {
            let Some(mut spare) = self.spare.pop() else {
                pool_spent();
            };
            spare.copy_from(snapshot);
            self.pending = Some(spare);
        }
        self.retry_pending();
    }

    #[inline]
    pub fn retry_pending(&mut self) {
        let Some(snapshot) = self.pending.take() else {
            return;
        };
        if let Err(PushError::Full(snapshot)) = self.producer.push(snapshot) {
            self.pending = Some(snapshot);
        }
    }

    #[inline]
    pub fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Nonzero means the writer is falling behind the hot side.
    pub fn superseded(&self) -> u64 {
        self.superseded
    }

    /// The pool has a fixed count, so the spare list never grows past the room it was made with.
    fn take_back_returned(&mut self) {
        while let Ok(snapshot) = self.returned.pop() {
            debug_assert!(self.spare.len() < self.spare.capacity());
            self.spare.push(snapshot);
        }
    }
}

#[cold]
#[inline(never)]
fn pool_spent() -> ! {
    panic!(
        "the exposure lane has no spare snapshot: its pool holds one for each slot of the ring, one \
         pending and one that the reader copies, and the reader hands each one back as it copies it"
    )
}

impl ExposureReceiver {
    /// Copies the next snapshot into `into` and hands it back to the sink. `false` when none waits.
    ///
    /// # Panics
    /// `into` has room for fewer instruments than the snapshot holds. Both ends of a lane are sized
    /// by one registry, so that is a bug.
    pub fn pop_into(&mut self, into: &mut ExposureSnapshot) -> bool {
        let Ok(snapshot) = self.snapshots.pop() else {
            return false;
        };
        into.copy_from(&snapshot);
        // The return ring has room for each snapshot of the pool, so it always takes one back.
        assert!(
            self.returned.push(snapshot).is_ok(),
            "the return ring of the exposure lane is full, so a snapshot came back that the pool never made"
        );
        true
    }
}
