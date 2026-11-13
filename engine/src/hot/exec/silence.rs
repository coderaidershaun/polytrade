//! The order stream can go quiet with no error. A span of message time with no stream event while an
//! order works gets one read of open orders, and reads that keep showing what the stream missed halt.

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::log::hot_warn;
use crate::msg::exec::{ExecCommand, ExecKind};
use crate::msg::time::{DurationUs, LocalTs};

use super::engine::ExecEngine;
use super::gates::HaltReason;

/// The silence of one venue's order stream: when the quiet began, the read that it sent, and how
/// many reads in a row showed what the stream missed.
#[derive(Debug, Default)]
pub(super) struct StreamSilence {
    /// The fold time that the quiet is measured from: the last stream event, the last read, or the
    /// last spin with no working order. `None` before the first spin.
    quiet_since: Option<LocalTs>,
    /// The read of the last silence, until each of its instruments has ended.
    read: Option<SilenceRead>,
    /// Reads that showed a change the stream never reported, with no stream event between.
    missed_events: u32,
}

/// One read of open orders: one reconcile sequence for each instrument, in a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SilenceRead {
    first_seq: u64,
    last_seq: u64,
    ends_left: u64,
    has_changed_a_slot: bool,
}

digest_by_hash!(SilenceRead);
digest_fields! { StreamSilence { quiet_since, read, missed_events } }

impl SilenceRead {
    fn holds(&self, recon_seq: u64) -> bool {
        (self.first_seq..=self.last_seq).contains(&recon_seq)
    }
}

impl StreamSilence {
    /// Only an event of the stream ends a quiet and clears the missed events. An answer to an own
    /// read never does.
    pub(super) fn observe_stream_event(&mut self, at: LocalTs) {
        self.quiet_since = Some(at);
        self.missed_events = 0;
    }

    /// The quiet counts only while an order works, so a spin with none starts it again.
    fn observe_spin(&mut self, now: LocalTs, has_working: bool) {
        if !has_working || self.quiet_since.is_none() {
            self.quiet_since = Some(now);
        }
    }

    fn is_due(&self, now: LocalTs, span: DurationUs) -> bool {
        self.quiet_since
            .is_some_and(|since| now.diff(since) >= span)
    }

    /// A read that a later silence replaces is never judged: its answer may never come.
    fn open_read(&mut self, at: LocalTs, first_seq: u64, instruments: u64) {
        self.quiet_since = Some(at);
        self.read = (instruments > 0).then_some(SilenceRead {
            first_seq,
            last_seq: first_seq + instruments - 1,
            ends_left: instruments,
            has_changed_a_slot: false,
        });
    }

    pub(super) fn observe_read_change(&mut self, recon_seq: u64) {
        if let Some(read) = self.read.as_mut().filter(|read| read.holds(recon_seq)) {
            read.has_changed_a_slot = true;
        }
    }

    /// The count of missed events once the last instrument of a read that changed a slot has
    /// ended, and `None` while the read waits for an end or found nothing the stream missed.
    fn end_read_instrument(&mut self, recon_seq: u64) -> Option<u32> {
        let read = self.read.as_mut().filter(|read| read.holds(recon_seq))?;
        read.ends_left -= 1;
        if read.ends_left > 0 {
            return None;
        }
        let has_changed_a_slot = read.has_changed_a_slot;
        self.read = None;
        if !has_changed_a_slot {
            return None;
        }
        self.missed_events += 1;
        Some(self.missed_events)
    }
}

/// A report on an order comes from the stream. An acknowledgment answers an own request, and a
/// snapshot answers an own read, so neither shows that the stream lives.
pub(super) fn is_stream_report(kind: ExecKind) -> bool {
    match kind {
        ExecKind::ReportNew
        | ExecKind::ReportTrade
        | ExecKind::ReportCanceled
        | ExecKind::ReportExpired
        | ExecKind::ReportRejected
        | ExecKind::ReportAmended => true,
        ExecKind::AckPlaced
        | ExecKind::PlaceNotSent
        | ExecKind::AmendNotSent
        | ExecKind::AckCanceled
        | ExecKind::AckAmended
        | ExecKind::AckFailed
        | ExecKind::SnapshotOrder
        | ExecKind::SnapshotEnd
        | ExecKind::StreamReset
        | ExecKind::StreamReady => false,
    }
}

impl ExecEngine {
    /// The silence deadline is a difference of message stamps, never a count of spins, so a timer
    /// that fires late or fast reads at the same moment of message time.
    pub(super) fn detect_silence(&mut self, venue: usize, now: LocalTs) {
        let slots = self.venues[venue].slots.clone();
        let has_working = self
            .orders
            .slots_in_use()
            .skip_while(|index| *index < slots.start)
            .take_while(|index| *index < slots.end)
            .any(|index| self.orders.slot(index).state().is_working());
        let state = &mut self.venues[venue];
        state.silence.observe_spin(now, has_working);
        if has_working && state.silence.is_due(now, state.settings.stream_silence) {
            self.request_open_orders(venue, now);
        }
    }

    /// One read of the open orders of each instrument of the venue, numbered in a row.
    #[cold]
    fn request_open_orders(&mut self, venue: usize, now: LocalTs) {
        let first_seq = self.venues[venue].recon_seq + 1;
        for instrument in self.venues[venue].instrument_ids() {
            self.venues[venue].recon_seq += 1;
            let command = ExecCommand::ReconcileOpenOrders {
                instrument,
                recon_seq: self.venues[venue].recon_seq,
            };
            self.bank_or_halt(venue, command, now);
        }
        let state = &mut self.venues[venue];
        let instruments = state.recon_seq + 1 - first_seq;
        state.silence.open_read(now, first_seq, instruments);
    }

    /// Judges a silence read once its last instrument has ended. A read that changed a slot showed
    /// what the stream missed, so the edge subscribes again, and past the cap quoting halts.
    pub(super) fn end_silence_read(&mut self, venue: usize, recon_seq: u64, now: LocalTs) {
        let Some(missed_events) = self.venues[venue].silence.end_read_instrument(recon_seq) else {
            return;
        };
        self.on_missed_event(venue, missed_events, now);
    }

    #[cold]
    fn on_missed_event(&mut self, venue: usize, missed_events: u32, now: LocalTs) {
        let cap = self.venues[venue].settings.max_stream_resets;
        hot_warn!(
            "a read of open orders showed a change that the order stream never reported: missed event {} of a cap of {}",
            missed_events,
            cap
        );
        if missed_events > cap {
            return self.halt(HaltReason::StreamSilent, now);
        }
        self.bank_or_halt(venue, ExecCommand::ResubscribeStream, now);
    }
}
