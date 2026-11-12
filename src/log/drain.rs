//! Drain thread: round-robins producer rings + backtrace lane into one output, reports drops.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use rtrb::Consumer;

use super::output::{DropNotice, Output};
use super::producer::DrainChannels;
use super::record::LogRecord;

const IDLE_POLL: Duration = Duration::from_millis(1);

struct DrainRing {
    tag: &'static str,
    consumer: Consumer<LogRecord>,
    drops: Arc<AtomicU64>,
    reported_drops: u64,
}

impl DrainRing {
    /// The producing unit is gone and its ring is empty, so nothing can arrive here again — polling
    /// it for the rest of the run would cost every later iteration.
    fn is_spent(&self) -> bool {
        self.consumer.is_abandoned() && self.consumer.is_empty()
    }
}

pub(super) fn run(channels: DrainChannels, mut output: Output, drain: Arc<AtomicBool>) {
    let mut rings: Vec<DrainRing> = Vec::new();
    loop {
        let progressed = drain_once(&mut rings, &channels, &mut output);
        output.flush_if_due();
        if drain.load(Ordering::Acquire) {
            drain_once(&mut rings, &channels, &mut output);
            output.flush();
            return;
        }
        if !progressed {
            thread::sleep(IDLE_POLL);
        }
    }
}

fn drain_once(rings: &mut Vec<DrainRing>, channels: &DrainChannels, output: &mut Output) -> bool {
    let mut progressed = absorb_registrations(rings, channels);
    for ring in rings.iter_mut() {
        while let Ok(record) = ring.consumer.pop() {
            output.write_record(ring.tag, &record);
            progressed = true;
        }
    }
    while let Ok(backtrace) = channels.backtraces.try_recv() {
        output.write_backtrace(&backtrace);
        progressed = true;
    }
    for ring in rings.iter_mut() {
        let total = ring.drops.load(Ordering::Relaxed);
        if total > ring.reported_drops {
            let notice = DropNotice {
                since_last: total - ring.reported_drops,
                total,
            };
            output.write_drop_notice(ring.tag, notice);
            ring.reported_drops = total;
            progressed = true;
        }
    }
    rings.retain(|ring| !ring.is_spent());
    progressed
}

fn absorb_registrations(rings: &mut Vec<DrainRing>, channels: &DrainChannels) -> bool {
    let mut absorbed = false;
    while let Ok(registration) = channels.registrations.try_recv() {
        rings.push(DrainRing {
            tag: registration.tag,
            consumer: registration.consumer,
            drops: registration.drops,
            reported_drops: 0,
        });
        absorbed = true;
    }
    absorbed
}
