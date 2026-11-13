//! The log drain: it reads each lane in batches into one output, reports drops, and drains each
//! lane under a deadline at shutdown. Only here does a hot record become text.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use super::lane::{AnyLane, DrainChannels, LaneReader, close_thread_lane};
use super::output::{DropNotice, LogOutput, Output};
use super::record::{HotRecord, TextRecord};
use crate::msg::time::{DurationUs, EngineClock, LocalTs};

const IDLE_POLL: Duration = Duration::from_millis(1);

/// What one drain did over its life, returned when it finishes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Record lines that reached a writer.
    pub written: u64,
    /// Records that a full lane refused.
    pub dropped: u64,
    /// Records that were read and never reached a writer, or that still waited at the deadline.
    pub lost: u64,
    pub io_errors: u64,
    pub reopens: u64,
    /// Tags of the lanes that had not ended, or not emptied, when the deadline passed.
    pub unfinished: Vec<&'static str>,
}

impl fmt::Display for DrainReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "log drain wrote {} line(s), dropped {}, lost {}, with {} I/O error(s) and {} reopen(s)",
            self.written, self.dropped, self.lost, self.io_errors, self.reopens
        )?;
        if !self.unfinished.is_empty() {
            write!(
                formatter,
                "; lanes unfinished at the deadline: {}",
                self.unfinished.join(", ")
            )?;
        }
        Ok(())
    }
}

struct DrainRing {
    tag: &'static str,
    reader: AnyLane,
    reported_drops: u64,
}

impl DrainRing {
    fn len(&self) -> usize {
        match &self.reader {
            AnyLane::Text(reader) => reader.len(),
            AnyLane::Hot(reader) => reader.len(),
        }
    }

    fn dropped(&self) -> u64 {
        match &self.reader {
            AnyLane::Text(reader) => reader.dropped(),
            AnyLane::Hot(reader) => reader.dropped(),
        }
    }

    /// The producing unit is gone and its ring is empty, so nothing can arrive here again — polling
    /// it for the rest of the run would cost every later iteration.
    fn is_spent(&self) -> bool {
        match &self.reader {
            AnyLane::Text(reader) => reader.is_abandoned() && reader.is_empty(),
            AnyLane::Hot(reader) => reader.is_abandoned() && reader.is_empty(),
        }
    }
}

/// The drain as a value: lanes in, lines out to one output. The process drain thread runs one, and
/// a caller can run its own over lanes it built, with no process-wide state.
pub struct LogDrain {
    rings: Vec<DrainRing>,
    output: Output,
    clock: EngineClock,
    /// Lanes and backtraces that the process log hands over while it runs.
    channels: Option<DrainChannels>,
    /// Reused for the text of each hot record, so rendering allocates only while it grows.
    text: String,
    dropped: u64,
}

impl LogDrain {
    /// Time on the drain's own lines and the shutdown deadline come from `clock`.
    pub fn new(output: LogOutput, clock: EngineClock) -> Self {
        Self {
            rings: Vec::new(),
            output: Output::new(output),
            clock,
            channels: None,
            text: String::new(),
            dropped: 0,
        }
    }

    pub(super) fn with_channels(
        output: LogOutput,
        clock: EngineClock,
        channels: DrainChannels,
    ) -> Self {
        let mut drain = Self::new(output, clock);
        drain.channels = Some(channels);
        drain
    }

    /// Lines of this lane carry `tag` in their tag column.
    pub fn add_hot_lane(&mut self, tag: &'static str, reader: LaneReader<HotRecord>) {
        self.add_ring(tag, AnyLane::Hot(reader));
    }

    /// Lines of this lane carry `tag` in their tag column.
    pub fn add_text_lane(&mut self, tag: &'static str, reader: LaneReader<TextRecord>) {
        self.add_ring(tag, AnyLane::Text(reader));
    }

    fn add_ring(&mut self, tag: &'static str, reader: AnyLane) {
        self.rings.push(DrainRing {
            tag,
            reader,
            reported_drops: 0,
        });
    }

    /// Writes every record that waits in each lane, then each new drop count. Returns whether any
    /// line was written.
    #[expect(
        clippy::disallowed_methods,
        reason = "a log line and its file date take wall time, which is never engine state"
    )]
    pub fn drain_once(&mut self) -> bool {
        let absorbed = self.absorb_registrations();
        let wrote_records = self.write_records(None);
        let wrote_backtraces = self.write_backtraces();
        let wrote_drops = self.write_drops();
        let now = self.clock.now();
        self.output.flush_if_due(now);
        absorbed || wrote_records || wrote_backtraces || wrote_drops
    }

    /// Drains until each lane has ended and is empty, or until `deadline` passes, then flushes and
    /// syncs the output. The calling thread's own text lane is closed first, so the drain does not
    /// wait for the thread that waits on it. A record still waiting at the deadline is lost and
    /// counted, and its lane is named in the report.
    #[expect(
        clippy::disallowed_methods,
        reason = "a log line and its file date take wall time, which is never engine state"
    )]
    pub fn finish(mut self, deadline: Duration) -> DrainReport {
        close_thread_lane();
        let deadline_at = self.clock.now() + DurationUs::from_micros(deadline_micros(deadline));
        let mut unfinished = Vec::new();
        loop {
            self.absorb_registrations();
            let is_in_time = self.write_records(Some(deadline_at));
            self.write_backtraces();
            self.write_drops();
            if self.rings.is_empty() {
                break;
            }
            if !is_in_time || self.clock.now() >= deadline_at {
                unfinished = self.give_up();
                break;
            }
            thread::sleep(IDLE_POLL);
        }
        let now = self.clock.now();
        self.output.sync(now);
        let counts = self.output.counts();
        let lost_at_deadline: u64 = self.rings.iter().map(|ring| ring.len() as u64).sum();
        DrainReport {
            written: counts.written,
            dropped: self.dropped,
            lost: counts.lost + lost_at_deadline,
            io_errors: counts.io_errors,
            reopens: counts.reopens,
            unfinished,
        }
    }

    /// The deadline passed: count the drops still unreported, and name each lane left.
    fn give_up(&mut self) -> Vec<&'static str> {
        for ring in &mut self.rings {
            let total = ring.dropped();
            self.dropped += total - ring.reported_drops;
            ring.reported_drops = total;
        }
        self.rings.iter().map(|ring| ring.tag).collect()
    }

    fn absorb_registrations(&mut self) -> bool {
        let Some(channels) = self.channels.as_ref() else {
            return false;
        };
        let mut absorbed = false;
        while let Ok(registration) = channels.registrations.try_recv() {
            self.rings.push(DrainRing {
                tag: registration.tag,
                reader: registration.reader,
                reported_drops: 0,
            });
            absorbed = true;
        }
        // Taken after the lane registrations, so that a copy lane opened before a lane registered
        // is in place before the first line of that lane.
        #[cfg(feature = "otlp")]
        while let Ok(copies) = channels.copies.try_recv() {
            self.output.copy_into(copies);
        }
        absorbed
    }

    /// Writes each lane's waiting records as one batch. With a deadline, stops at the first record
    /// past it and returns false.
    #[expect(
        clippy::disallowed_methods,
        reason = "a log line and its file date take wall time, which is never engine state"
    )]
    fn write_records(&mut self, deadline_at: Option<LocalTs>) -> bool {
        for ring in &mut self.rings {
            let is_in_time = drain_ring(ring, &mut self.output, &mut self.text, || {
                deadline_at.is_none_or(|at| self.clock.now() < at)
            });
            if !is_in_time {
                return false;
            }
        }
        true
    }

    fn write_backtraces(&mut self) -> bool {
        let Some(channels) = self.channels.as_ref() else {
            return false;
        };
        let mut wrote = false;
        while let Ok(backtrace) = channels.backtraces.try_recv() {
            self.output.write_backtrace(&backtrace);
            wrote = true;
        }
        wrote
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "a log line and its file date take wall time, which is never engine state"
    )]
    fn write_drops(&mut self) -> bool {
        let now = self.clock.now();
        let mut wrote = false;
        for ring in &mut self.rings {
            let total = ring.dropped();
            if total > ring.reported_drops {
                let since_last = total - ring.reported_drops;
                let notice = DropNotice { since_last, total };
                self.output.write_drop_notice(ring.tag, notice, now);
                self.dropped += since_last;
                ring.reported_drops = total;
                wrote = true;
            }
        }
        self.rings.retain(|ring| !ring.is_spent());
        wrote
    }
}

fn deadline_micros(deadline: Duration) -> i64 {
    i64::try_from(deadline.as_micros()).unwrap_or(i64::MAX)
}

/// Reads one lane as a batch. Returns false if `is_in_time` turned false before the batch ended;
/// the records not read stay in the lane, since a batch takes from the lane only what it yields.
fn drain_ring(
    ring: &mut DrainRing,
    output: &mut Output,
    text: &mut String,
    is_in_time: impl Fn() -> bool,
) -> bool {
    match &mut ring.reader {
        AnyLane::Text(reader) => {
            let consumer = reader.consumer_mut();
            let Ok(batch) = consumer.read_chunk(consumer.slots()) else {
                return true;
            };
            let mut records = batch.into_iter();
            while is_in_time() {
                let Some(record) = records.next() else {
                    return true;
                };
                output.write_line(ring.tag, record.callsite(), record.at(), record.text());
            }
            false
        }
        AnyLane::Hot(reader) => {
            let consumer = reader.consumer_mut();
            let Ok(batch) = consumer.read_chunk(consumer.slots()) else {
                return true;
            };
            let mut records = batch.into_iter();
            while is_in_time() {
                let Some(record) = records.next() else {
                    return true;
                };
                text.clear();
                // A String accepts every write, so rendering into it cannot fail.
                record.render(text).ok();
                output.write_line(ring.tag, record.callsite(), record.at(), text);
            }
            false
        }
    }
}

/// The body of the process drain thread: drain until asked to stop, then finish.
pub(super) fn run(
    mut log_drain: LogDrain,
    stop: Arc<AtomicBool>,
    deadline: Duration,
) -> DrainReport {
    loop {
        if stop.load(Ordering::Acquire) {
            return log_drain.finish(deadline);
        }
        if !log_drain.drain_once() {
            thread::sleep(IDLE_POLL);
        }
    }
}
