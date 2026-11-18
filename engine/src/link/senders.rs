//! The counts of each address that sends peer frames into the link queue: frames that a full queue
//! dropped, and frames above the sender's rate in one heartbeat period.

use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU32;

use super::subscribers::LINK_MAX_GATE_KEYS;

/// One row for each sender the sequence gate can tell apart.
pub const LINK_MAX_SENDERS: usize = LINK_MAX_GATE_KEYS;

/// What one sender lost since the start of the run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SenderCount {
    /// Frames dropped because the link queue was full.
    pub queue_full: u64,
    /// Frames refused because the sender was above its rate.
    pub over_rate: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SenderRow {
    sender: SocketAddr,
    count: SenderCount,
    frames_in_period: u32,
}

/// Where the frame of one sender is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SenderSlot {
    Row(usize),
    Others,
}

/// A fixed table, so counting never allocates.
///
/// WARNING: a row is never given back. A peer that comes back on a new port takes a new row, and
/// once the table is full each new sender shares the one row of `others` and its one rate.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SenderCounts {
    rows: [Option<SenderRow>; LINK_MAX_SENDERS],
    others: SenderCount,
    others_in_period: u32,
}

/// Each sender with a row and the others, without the empty rows.
impl fmt::Debug for SenderCounts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.rows()).finish()
    }
}

impl Default for SenderCounts {
    fn default() -> Self {
        Self {
            rows: [None; LINK_MAX_SENDERS],
            others: SenderCount::default(),
            others_in_period: 0,
        }
    }
}

impl SenderCounts {
    /// The counts of `sender`, zero for a sender that has no row.
    pub fn of(&self, sender: SocketAddr) -> SenderCount {
        self.rows
            .iter()
            .map_while(|row| *row)
            .find(|row| row.sender == sender)
            .map_or_else(SenderCount::default, |row| row.count)
    }

    /// The counts of each sender that came after the table was full, together.
    pub fn others(&self) -> SenderCount {
        self.others
    }

    /// Each sender with a row, then `None` for the others once they lost a frame.
    pub fn rows(&self) -> impl Iterator<Item = (Option<SocketAddr>, SenderCount)> + '_ {
        let senders = self
            .rows
            .iter()
            .map_while(|row| *row)
            .map(|row| (Some(row.sender), row.count));
        let others = (self.others != SenderCount::default()).then_some((None, self.others));
        senders.chain(others)
    }

    /// Counts one frame of `sender` against `rate` for this period.
    ///
    /// # Errors
    /// A frame above the rate is counted as refused, and the error holds the refusals of its row.
    pub(crate) fn admit(
        &mut self,
        sender: SocketAddr,
        rate: NonZeroU32,
    ) -> Result<SenderSlot, u64> {
        let slot = self.slot_of(sender);
        let (count, frames_in_period) = self.entry(slot);
        if *frames_in_period >= rate.get() {
            count.over_rate += 1;
            return Err(count.over_rate);
        }
        *frames_in_period += 1;
        Ok(slot)
    }

    pub(crate) fn count_queue_full(&mut self, slot: SenderSlot) {
        self.entry(slot).0.queue_full += 1;
    }

    /// Each sender gets its whole rate again.
    pub(crate) fn start_period(&mut self) {
        for row in self.rows.iter_mut().map_while(Option::as_mut) {
            row.frames_in_period = 0;
        }
        self.others_in_period = 0;
    }

    fn slot_of(&mut self, sender: SocketAddr) -> SenderSlot {
        let free_or_own = self
            .rows
            .iter()
            .position(|row| row.is_none_or(|row| row.sender == sender));
        let Some(index) = free_or_own else {
            return SenderSlot::Others;
        };
        self.rows[index].get_or_insert(SenderRow {
            sender,
            count: SenderCount::default(),
            frames_in_period: 0,
        });
        SenderSlot::Row(index)
    }

    fn entry(&mut self, slot: SenderSlot) -> (&mut SenderCount, &mut u32) {
        match slot {
            SenderSlot::Row(index) => {
                let row = self.rows[index]
                    .as_mut()
                    .expect("a slot names a row that holds its sender");
                (&mut row.count, &mut row.frames_in_period)
            }
            SenderSlot::Others => (&mut self.others, &mut self.others_in_period),
        }
    }
}
