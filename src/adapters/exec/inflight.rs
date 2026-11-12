//! The requests that have left and are still owed an answer. One that passes its deadline is
//! reconciled and never re-sent: the venue may have executed it and only the answer was lost, so a
//! second copy could double the order.

use crate::time::{DurationUs, TsUs};
use crate::warn;

use super::{ExecRequest, RequestId};

const CROWDED: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InFlightRequest {
    pub request_id: RequestId,
    /// Kept because a bare venue error names no order, so the request is the only record of what
    /// the answer is about.
    pub request: ExecRequest,
    pub sent_at: TsUs,
    /// The hot reconcile sequence the request answers, re-attached to whatever comes back.
    pub recon_seq: u64,
}

#[derive(Debug)]
pub struct InFlightTable {
    entries: Vec<InFlightRequest>,
    timeout: DurationUs,
    timed_out: u64,
}

impl InFlightTable {
    pub fn new(timeout: DurationUs) -> Self {
        Self {
            entries: Vec::with_capacity(CROWDED),
            timeout,
            timed_out: 0,
        }
    }

    pub fn record(&mut self, entry: InFlightRequest) {
        self.entries.push(entry);
        // On the crossing only: the table stays over the mark for as long as the crowd lasts, and
        // a warning per request is a flood at exactly the moment one is least wanted.
        if self.entries.len() == CROWDED + 1 {
            self.report_crowding();
        }
    }

    pub fn take(&mut self, request_id: RequestId) -> Option<InFlightRequest> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.request_id == request_id)?;
        Some(self.entries.swap_remove(position))
    }

    /// Takes the oldest entry that matches, keeping the rest in the order they were recorded: when
    /// several entries could match one answer, the oldest is the one it answers. [`Self::take`]
    /// reorders the table, so a caller that relies on this order must not mix the two.
    pub fn take_where(
        &mut self,
        matches: impl FnMut(&InFlightRequest) -> bool,
    ) -> Option<InFlightRequest> {
        let position = self.entries.iter().position(matches)?;
        Some(self.entries.remove(position))
    }

    pub fn take_expired(&mut self, now: TsUs) -> Vec<InFlightRequest> {
        let timeout = self.timeout;
        let mut expired = Vec::new();
        self.entries.retain(|entry| {
            let is_live = now.diff(entry.sent_at) < timeout;
            if !is_live {
                expired.push(*entry);
            }
            is_live
        });
        self.timed_out += expired.len() as u64;
        expired
    }

    /// A dead connection loses every answer still owed, so each of these is as unresolved as a
    /// timed-out request.
    pub fn take_all(&mut self) -> Vec<InFlightRequest> {
        std::mem::take(&mut self.entries)
    }

    pub fn timed_out(&self) -> u64 {
        self.timed_out
    }

    #[cold]
    fn report_crowding(&self) {
        warn!(
            "an execution edge has {} requests outstanding — above {CROWDED}, which the send policy should make unreachable",
            self.entries.len()
        );
    }
}
