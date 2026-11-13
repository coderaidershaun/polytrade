//! The metrics thread: it turns the raw samples of the hot thread and the senders into percentiles,
//! so the hot thread pays only fixed-size pushes. It writes rows, log lines and the latency report.

mod error;
mod report;
mod ring;
mod rollup;
mod rows;
mod stage;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use rtrb::{Consumer, Producer, RingBuffer};

use crate::msg::time::{
    ClockSource, DurationUs, EngineClock, LocalTs, Stamper, Tick, TickAnchor, TickScale,
};
use crate::msg::ui::LatencyReport;
use crate::runtime::config::LatencyConfig;
use crate::runtime::pin::spawn_pinned;
use crate::{warn, warn_repeating};

use super::StartError;

pub use error::MetricsError;
pub use ring::{SampleProducers, SampleRing};
pub use rows::{BacklogRow, DropRow, LatencyRow, RollupRow, RollupSink, SinkKind};

use ring::{SampleConsumers, sample_rings};
use rollup::{LatencyRollup, RollupSettings};

/// The stage ring fills in about 160 ms at 200,000 messages a second, so a wake every few
/// milliseconds leaves wide room.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// A fresh anchor this often keeps the rate exact while the counter drifts against the clock.
const ANCHOR_INTERVAL: DurationUs = DurationUs::from_millis(100);

/// Two counter reads further apart than this were split by a preemption, so the clock reading
/// between them could sit anywhere in the gap.
const ANCHOR_SPREAD_LIMIT_NS: u64 = 20_000;

/// The first two anchors sit this far apart, to give a first rate before any sample is read.
const FIRST_ANCHOR_GAP: Duration = Duration::from_millis(2);

/// The workstation takes about one report a second, so a few slots cover a slow link actor.
const REPORT_RING_CAPACITY: usize = 8;

pub struct MetricsSetup {
    pub latency: LatencyConfig,
    /// Named in each row, so nobody takes numbers of the os clock for numbers of the cpu counter.
    pub clock_source: ClockSource,
    pub queue_count: usize,
    pub scale: TickScale,
    pub start_ts_us: LocalTs,
}

pub struct MetricsParts {
    pub metrics: Metrics,
    pub samples: SampleProducers,
    pub reports: Consumer<LatencyReport>,
}

/// What the metrics thread does on each wake, with time passed in, so a test can drive it.
pub struct Metrics {
    rollup: LatencyRollup,
    consumers: SampleConsumers,
    reports: Producer<LatencyReport>,
    reports_dropped: u64,
    display_period: DurationUs,
    next_report_ts_us: LocalTs,
}

impl Metrics {
    /// The metrics thread's state together with the write ends of its sample rings and the read end
    /// of its report ring.
    pub fn with_rings(setup: MetricsSetup) -> MetricsParts {
        let settings = RollupSettings {
            interval: setup.latency.interval(),
            window_intervals: setup.latency.window_intervals as usize,
            clock_source: setup.clock_source,
            queue_count: setup.queue_count,
            budget: setup.latency.budget,
        };
        let (samples, consumers) = sample_rings(setup.latency.sample_ring as usize);
        let (report_producer, reports) = RingBuffer::new(REPORT_RING_CAPACITY);
        let display_period = setup.latency.display_period();
        let metrics = Metrics {
            rollup: LatencyRollup::new(settings, setup.scale, setup.start_ts_us),
            consumers,
            reports: report_producer,
            reports_dropped: 0,
            display_period,
            next_report_ts_us: setup.start_ts_us + display_period,
        };
        MetricsParts {
            metrics,
            samples,
            reports,
        }
    }

    /// One wake at engine time `now`: drains each ring into the interval that is filling, closes
    /// each interval that has ended, and sends a report once the display period has passed.
    pub fn pass(&mut self, now: LocalTs, sink: &mut impl RollupSink) {
        self.rollup.note_time(now);
        self.consumers.drain_into(&mut self.rollup);
        self.rollup.advance(now, sink);
        if self.rollup.now() < self.next_report_ts_us {
            return;
        }
        self.next_report_ts_us = self.rollup.now() + self.display_period;
        let report = report::build_report(&mut self.rollup);
        if self.reports.push(report).is_err() {
            self.reports_dropped += 1;
            self.rollup.note_report_drops(self.reports_dropped);
        }
    }

    /// Takes an anchor whose engine time `at` was read between the counter reads `before` and
    /// `after`, and with it a more exact rate.
    ///
    /// # Errors
    /// [`MetricsError::AnchorSpread`] when the two counter reads sit too far apart. The last good
    /// anchor stays.
    #[expect(
        clippy::disallowed_methods,
        reason = "the metrics thread anchors ticks to engine time and turns them into time, off the fold"
    )]
    pub fn take_anchor(
        &mut self,
        before: Tick,
        at: LocalTs,
        after: Tick,
    ) -> Result<(), MetricsError> {
        let scale = self.rollup.scale();
        let spread_ns = scale.nanos(before, after).unwrap_or(u64::MAX);
        if spread_ns > ANCHOR_SPREAD_LIMIT_NS {
            return Err(MetricsError::AnchorSpread {
                spread_ns,
                limit_ns: ANCHOR_SPREAD_LIMIT_NS,
            });
        }
        if let Some(extended) = scale.extended_to(TickAnchor { tick: before, at }) {
            self.rollup.rescale(extended);
        }
        Ok(())
    }

    /// The last wake, at the stop of the run: what the rings still hold goes into the interval
    /// that is filling, and that interval closes early.
    pub fn finish(&mut self, now: LocalTs, sink: &mut impl RollupSink) {
        self.rollup.note_time(now);
        self.consumers.drain_into(&mut self.rollup);
        self.rollup.advance(now, sink);
        self.rollup.finish(sink);
    }
}

/// Until the tape gains a table for rollup rows, a run keeps them only as the log lines of each
/// interval.
struct NoTable;

impl RollupSink for NoTable {
    fn write(&mut self, _row: RollupRow) {}
}

pub(super) struct MetricsBringUp {
    pub thread: MetricsThread,
    pub samples: SampleProducers,
    pub reports: Consumer<LatencyReport>,
}

pub(super) struct MetricsBringUpSetup<'a> {
    pub latency: &'a LatencyConfig,
    pub clock: &'a EngineClock,
    /// The counter every latency stamp of the process reads, so the anchors place each tick.
    pub stamper: &'a Stamper,
    pub clock_source: ClockSource,
    pub queue_count: usize,
    /// The housekeeping core the thread pins itself to. Absent, the OS places it.
    pub core_id: Option<usize>,
}

/// # Errors
/// [`StartError::HousekeepingPin`] when the thread cannot pin itself to its core on Linux.
pub(super) fn metrics_bring_up(
    setup: MetricsBringUpSetup<'_>,
) -> Result<MetricsBringUp, StartError> {
    let stamper = setup.stamper.clone();
    let first = read_anchor(setup.clock, &stamper).0;
    std::thread::sleep(FIRST_ANCHOR_GAP);
    let second = read_anchor(setup.clock, &stamper).0;
    let scale = TickScale::between(first, second)
        .expect("the cpu counter and the engine clock both move in two milliseconds");
    let MetricsParts {
        metrics,
        samples,
        reports,
    } = Metrics::with_rings(MetricsSetup {
        latency: *setup.latency,
        clock_source: setup.clock_source,
        queue_count: setup.queue_count,
        scale,
        start_ts_us: second.at,
    });
    let thread = MetricsThread::spawn(MetricsThreadSetup {
        metrics,
        clock: setup.clock.clone(),
        stamper,
        core_id: setup.core_id,
    })?;
    Ok(MetricsBringUp {
        thread,
        samples,
        reports,
    })
}

/// The anchor, and the counter read after the clock, which bounds how exact the anchor is.
#[expect(
    clippy::disallowed_methods,
    reason = "the metrics thread anchors ticks to engine time and turns them into time, off the fold"
)]
fn read_anchor(clock: &EngineClock, stamper: &Stamper) -> (TickAnchor, Tick) {
    let tick = stamper.stamp();
    let at = clock.now();
    let after = stamper.stamp();
    (TickAnchor { tick, at }, after)
}

/// What the metrics thread runs, the clock and counter of its anchors, and its housekeeping core.
pub struct MetricsThreadSetup {
    pub metrics: Metrics,
    pub clock: EngineClock,
    pub stamper: Stamper,
    /// Absent, the OS places the thread.
    pub core_id: Option<usize>,
}

pub struct MetricsThread {
    join: JoinHandle<()>,
    stop: Arc<AtomicBool>,
}

impl MetricsThread {
    /// The thread pins itself to its core first, and runs nothing if the pin is refused.
    ///
    /// # Errors
    /// [`StartError::HousekeepingPin`] when the thread cannot pin itself to `core_id` on Linux.
    pub fn spawn(setup: MetricsThreadSetup) -> Result<MetricsThread, StartError> {
        let MetricsThreadSetup {
            metrics,
            clock,
            stamper,
            core_id,
        } = setup;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let join = spawn_pinned("polysim-metrics", core_id, move || {
            run(metrics, &clock, &stamper, &stop_flag);
        })
        .map_err(|refused| StartError::HousekeepingPin {
            core_id: refused.core_id,
            thread: "metrics",
        })?;
        Ok(MetricsThread { join, stop })
    }

    /// Closes the interval that is filling, so its log lines reach the log before the drain.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Release);
        if self.join.join().is_err() {
            warn!("metrics thread panicked — cause already logged");
        }
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "the metrics thread anchors ticks to engine time and turns them into time, off the fold"
)]
fn run(mut metrics: Metrics, clock: &EngineClock, stamper: &Stamper, stop: &AtomicBool) {
    crate::log::register_thread("metrics");
    let mut sink = NoTable;
    let mut refused_anchors = 0u64;
    let mut next_anchor_ts_us = clock.now() + ANCHOR_INTERVAL;
    while !stop.load(Ordering::Acquire) {
        let now = clock.now();
        if now >= next_anchor_ts_us {
            let (anchor, after) = read_anchor(clock, stamper);
            if let Err(refusal) = metrics.take_anchor(anchor.tick, anchor.at, after) {
                warn_repeating!(refused_anchors, "{refusal}");
            }
            next_anchor_ts_us = anchor.at + ANCHOR_INTERVAL;
        }
        metrics.pass(now, &mut sink);
        std::thread::sleep(POLL_INTERVAL);
    }
    metrics.finish(clock.now(), &mut sink);
}
