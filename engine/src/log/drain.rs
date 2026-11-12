//! Drain thread: round-robins producer rings + backtrace lane into one output, reports drops. The
//! text of each hot record is made here, never on the thread that logged it.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use rtrb::Consumer;

use super::lane::{DrainChannels, LaneReader};
use super::output::{DropNotice, Output};
use super::record::HotRecord;

const IDLE_POLL: Duration = Duration::from_millis(1);

struct DrainRing {
    tag: &'static str,
    reader: LaneReader,
    drops: Arc<AtomicU64>,
    reported_drops: u64,
}

impl DrainRing {
    /// The producing unit is gone and its ring is empty, so nothing can arrive here again — polling
    /// it for the rest of the run would cost every later iteration.
    fn is_spent(&self) -> bool {
        match &self.reader {
            LaneReader::Text(consumer) => consumer.is_abandoned() && consumer.is_empty(),
            LaneReader::Hot(consumer) => consumer.is_abandoned() && consumer.is_empty(),
        }
    }
}

/// The drain as a value: lanes in, lines out to one writer. The process drain thread runs one, and
/// a caller can run its own over lanes it built, with no process-wide state.
pub struct LogDrain {
    rings: Vec<DrainRing>,
    output: Output,
    /// Reused for the text of each hot record, so rendering allocates only while it grows.
    text: String,
}

impl LogDrain {
    /// Lines go to `writer`, flushed at once for WARN and ERROR.
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self::with_output(Output::new(writer))
    }

    fn with_output(output: Output) -> Self {
        Self {
            rings: Vec::new(),
            output,
            text: String::new(),
        }
    }

    /// Lines of this lane carry `tag` in their tag column.
    pub fn add_hot_lane(&mut self, tag: &'static str, reader: Consumer<HotRecord>) {
        self.add_ring(tag, LaneReader::Hot(reader), Arc::new(AtomicU64::new(0)));
    }

    fn add_ring(&mut self, tag: &'static str, reader: LaneReader, drops: Arc<AtomicU64>) {
        self.rings.push(DrainRing {
            tag,
            reader,
            drops,
            reported_drops: 0,
        });
    }

    /// Writes every record that waits in each lane, then each new drop count. Returns whether any
    /// line was written.
    pub fn drain_once(&mut self) -> bool {
        let wrote_records = self.write_records();
        let wrote_drops = self.write_drops();
        wrote_records || wrote_drops
    }

    fn write_records(&mut self) -> bool {
        let mut progressed = false;
        for ring in &mut self.rings {
            progressed |= drain_ring(ring, &mut self.output, &mut self.text);
        }
        progressed
    }

    fn write_drops(&mut self) -> bool {
        let mut progressed = false;
        for ring in &mut self.rings {
            let total = ring.drops.load(Ordering::Relaxed);
            if total > ring.reported_drops {
                let notice = DropNotice {
                    since_last: total - ring.reported_drops,
                    total,
                };
                self.output.write_drop_notice(ring.tag, notice);
                ring.reported_drops = total;
                progressed = true;
            }
        }
        self.rings.retain(|ring| !ring.is_spent());
        progressed
    }
}

fn drain_ring(ring: &mut DrainRing, output: &mut Output, text: &mut String) -> bool {
    let mut progressed = false;
    match &mut ring.reader {
        LaneReader::Text(consumer) => {
            while let Ok(record) = consumer.pop() {
                output.write_line(ring.tag, record.callsite(), record.at(), record.text());
                progressed = true;
            }
        }
        LaneReader::Hot(consumer) => {
            while let Ok(record) = consumer.pop() {
                text.clear();
                // A String accepts every write, so rendering into it cannot fail.
                record.render(text).ok();
                output.write_line(ring.tag, record.callsite(), record.at(), text);
                progressed = true;
            }
        }
    }
    progressed
}

pub(super) fn run(channels: DrainChannels, output: Output, drain: Arc<AtomicBool>) {
    let mut log_drain = LogDrain::with_output(output);
    loop {
        let progressed = drain_once(&mut log_drain, &channels);
        log_drain.output.flush_if_due();
        if drain.load(Ordering::Acquire) {
            drain_once(&mut log_drain, &channels);
            log_drain.output.flush();
            return;
        }
        if !progressed {
            thread::sleep(IDLE_POLL);
        }
    }
}

fn drain_once(log_drain: &mut LogDrain, channels: &DrainChannels) -> bool {
    let mut progressed = absorb_registrations(log_drain, channels);
    progressed |= log_drain.write_records();
    while let Ok(backtrace) = channels.backtraces.try_recv() {
        log_drain.output.write_backtrace(&backtrace);
        progressed = true;
    }
    progressed |= log_drain.write_drops();
    progressed
}

fn absorb_registrations(log_drain: &mut LogDrain, channels: &DrainChannels) -> bool {
    let mut absorbed = false;
    while let Ok(registration) = channels.registrations.try_recv() {
        log_drain.add_ring(registration.tag, registration.reader, registration.drops);
        absorbed = true;
    }
    absorbed
}
