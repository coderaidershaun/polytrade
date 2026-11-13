//! The core of the metrics thread: samples in, histograms for each interval and the window, rows
//! out. Time moves only when the caller passes it in, so a test drives it with message time.

use hdrhistogram::Histogram;

use crate::msg::ids::{MAX_INPUT_QUEUES, QueueId};
use crate::msg::latency::{Category, CounterSample, Stage, StageSample, WireSample};
use crate::msg::time::{ClockSource, DurationUs, LocalTs, TickScale};
use crate::msg::ui::{LatencyCell, LatencyFlow, Percentile};
use crate::runtime::config::LatencyBudgets;

use super::report::{BudgetBreach, IntervalLog, RunCounts};
use super::ring::SampleRing;
use super::rows::{BacklogRow, DropRow, LatencyRow, RollupRow, RollupSink, SinkKind};
use super::stage::{Fault, PAIRS, stage_nanos, wire_nanos};

/// Below a nanosecond there is nothing to resolve. Above ten seconds a stage is broken, not slow,
/// and a value past the top is kept at the top.
const LOWEST_NS: u64 = 1;
const HIGHEST_NS: u64 = 10_000_000_000;
/// Two significant figures keep each percentile within one percent, in about 14 KiB a histogram.
const SIGNIFICANT_FIGURES: u8 = 2;

const BACKLOG_HALF_LIFE: DurationUs = DurationUs::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct RollupSettings {
    pub interval: DurationUs,
    pub window_intervals: usize,
    pub clock_source: ClockSource,
    pub queue_count: usize,
    pub budget: LatencyBudgets,
}

struct Interval {
    histogram: Histogram<u32>,
    max_ns: u64,
    backward: u64,
    negative: u64,
}

impl Interval {
    fn new() -> Interval {
        let histogram = Histogram::new_with_bounds(LOWEST_NS, HIGHEST_NS, SIGNIFICANT_FIGURES)
            .expect("the histogram bounds are constants that the crate accepts");
        Interval {
            histogram,
            max_ns: 0,
            backward: 0,
            negative: 0,
        }
    }

    fn clear(&mut self) {
        self.histogram.reset();
        self.max_ns = 0;
        self.backward = 0;
        self.negative = 0;
    }

    fn take(&mut self, outcome: Result<u64, Fault>) {
        match outcome {
            Ok(nanos) => {
                self.histogram.saturating_record(nanos);
                self.max_ns = self.max_ns.max(nanos);
            }
            Err(Fault::Backward) => self.backward += 1,
            Err(Fault::Negative) => self.negative += 1,
        }
    }
}

/// One interval slot that is filling and the closed ones of the window, which take turns: the
/// oldest closed slot is cleared and becomes the next one to fill.
struct PairWindow {
    category: Category,
    stage: Stage,
    slots: Vec<Interval>,
}

#[derive(Debug, Clone, Copy)]
struct Ema {
    value: f64,
    sampled_ts_us: LocalTs,
}

impl Ema {
    fn folded(previous: Option<Ema>, fresh: f64, at: LocalTs) -> Ema {
        let Some(previous) = previous else {
            return Ema {
                value: fresh,
                sampled_ts_us: at,
            };
        };
        let elapsed_us =
            (i128::from(at.micros()) - i128::from(previous.sampled_ts_us.micros())).max(0) as f64;
        let half_lives = elapsed_us / BACKLOG_HALF_LIFE.micros() as f64;
        let keep = (-std::f64::consts::LN_2 * half_lives).exp();
        Ema {
            value: previous.value + (fresh - previous.value) * (1.0 - keep),
            sampled_ts_us: at,
        }
    }
}

pub(super) struct LatencyRollup {
    settings: RollupSettings,
    scale: TickScale,
    pairs: Vec<PairWindow>,
    filling: usize,
    interval_index: i64,
    now: LocalTs,
    last_sample_ts_us: [Option<LocalTs>; Category::ALL.len()],
    backlog_total: Option<Ema>,
    backlog_queues: [Option<Ema>; MAX_INPUT_QUEUES],
    backlog_max: [u32; MAX_INPUT_QUEUES],
    drop_totals: [u64; SinkKind::ALL.len()],
    drops_at_interval_start: [u64; SinkKind::ALL.len()],
    counts: RunCounts,
    budget_breaches: u64,
    breaches: Vec<BudgetBreach>,
    scratch: Interval,
    rows: Vec<LatencyRow>,
}

impl LatencyRollup {
    pub(super) fn new(settings: RollupSettings, scale: TickScale, now: LocalTs) -> LatencyRollup {
        let slot_count = settings.window_intervals + 1;
        let pairs = PAIRS
            .iter()
            .map(|&(category, stage)| PairWindow {
                category,
                stage,
                slots: (0..slot_count).map(|_| Interval::new()).collect(),
            })
            .collect();
        LatencyRollup {
            interval_index: interval_index(now, settings.interval),
            settings,
            scale,
            pairs,
            filling: 0,
            now,
            last_sample_ts_us: [None; Category::ALL.len()],
            backlog_total: None,
            backlog_queues: [None; MAX_INPUT_QUEUES],
            backlog_max: [0; MAX_INPUT_QUEUES],
            drop_totals: [0; SinkKind::ALL.len()],
            drops_at_interval_start: [0; SinkKind::ALL.len()],
            counts: RunCounts::default(),
            budget_breaches: 0,
            breaches: Vec::with_capacity(PAIRS.len()),
            scratch: Interval::new(),
            rows: Vec::with_capacity(PAIRS.len()),
        }
    }

    pub(super) fn scale(&self) -> TickScale {
        self.scale
    }

    pub(super) fn rescale(&mut self, scale: TickScale) {
        self.scale = scale;
    }

    pub(super) fn now(&self) -> LocalTs {
        self.now
    }

    /// Samples recorded from now on are stamped `now` and still join the interval that is filling,
    /// even when `now` is past its end: they were produced before the wake that reads them.
    pub(super) fn note_time(&mut self, now: LocalTs) {
        self.now = self.now.max(now);
    }

    pub(super) fn record_stage(&mut self, sample: &StageSample) {
        let scale = self.scale;
        let slot = self.filling;
        for pair in &mut self.pairs {
            if pair.category != sample.category {
                continue;
            }
            if let Some(outcome) = stage_nanos(sample, pair.stage, &scale) {
                pair.slots[slot].take(outcome);
            }
        }
        self.last_sample_ts_us[sample.category as usize] = Some(self.now);
    }

    pub(super) fn record_wire(&mut self, sample: &WireSample) {
        let scale = self.scale;
        let slot = self.filling;
        for pair in &mut self.pairs {
            if pair.category != Category::Command {
                continue;
            }
            if let Some(outcome) = wire_nanos(sample, pair.stage, &scale) {
                pair.slots[slot].take(outcome);
            }
        }
        self.last_sample_ts_us[Category::Command as usize] = Some(self.now);
    }

    pub(super) fn record_counters(&mut self, sample: &CounterSample) {
        let at = sample.spin_ts_us;
        let mut total = 0u64;
        for (queue, &waiting) in sample.backlog.iter().enumerate() {
            total += u64::from(waiting);
            self.backlog_max[queue] = self.backlog_max[queue].max(waiting);
            self.backlog_queues[queue] = Some(Ema::folded(
                self.backlog_queues[queue],
                f64::from(waiting),
                at,
            ));
        }
        self.backlog_total = Some(Ema::folded(self.backlog_total, total as f64, at));
        self.counts = RunCounts {
            counters: sample.counters,
            idle_parks: sample.idle_parks,
        };
        let counters = &sample.counters;
        for (sink, total) in [
            (SinkKind::Persist, counters.persist_dropped),
            (SinkKind::UiBooks, counters.ui_books_dropped),
            (SinkKind::UiEvents, counters.ui_events_dropped),
            (SinkKind::Link, counters.link_dropped),
            (SinkKind::BankFeatures, counters.bank_drops.features),
            (SinkKind::BankPersist, counters.bank_drops.persist),
            (SinkKind::BankLogs, counters.bank_drops.logs),
            (SinkKind::BankLink, counters.bank_drops.link),
            (SinkKind::ExposureSuperseded, counters.exposure_superseded),
        ] {
            self.drop_totals[sink as usize] = total;
        }
    }

    pub(super) fn note_ring_drops(&mut self, ring: SampleRing, total: u64) {
        self.drop_totals[SinkKind::of_ring(ring) as usize] = total;
    }

    pub(super) fn note_report_drops(&mut self, total: u64) {
        self.drop_totals[SinkKind::LatencyReports as usize] = total;
    }

    /// Moves the clock to `now`, closing each interval that ends on the way. Only the first of
    /// them can hold samples: the rest of a long gap is empty intervals.
    pub(super) fn advance(&mut self, now: LocalTs, sink: &mut impl RollupSink) {
        self.note_time(now);
        let target = interval_index(self.now, self.settings.interval);
        let gap = target - self.interval_index;
        if gap <= 0 {
            return;
        }
        self.close_interval(sink);
        let slot_count = self.settings.window_intervals + 1;
        for _ in 0..gap.min(slot_count as i64) {
            self.rotate();
        }
        self.interval_index = target;
    }

    /// Closes the interval that is filling, at the stop of the run.
    pub(super) fn finish(&mut self, sink: &mut impl RollupSink) {
        self.close_interval(sink);
        self.rotate();
        self.interval_index += 1;
    }

    fn close_interval(&mut self, sink: &mut impl RollupSink) {
        let interval_start_ts_us = interval_start(self.interval_index, self.settings.interval);
        let interval_end_ts_us = interval_start_ts_us + self.settings.interval;
        self.rows.clear();
        for pair in &self.pairs {
            let interval = &pair.slots[self.filling];
            let count = interval.histogram.len();
            if count == 0 && interval.backward == 0 && interval.negative == 0 {
                continue;
            }
            let ring = if pair.category == Category::Command {
                SinkKind::WireSamples
            } else {
                SinkKind::StageSamples
            };
            self.rows.push(LatencyRow {
                interval_start_ts_us,
                interval_end_ts_us,
                category: pair.category,
                stage: pair.stage,
                count,
                dropped: self.drops_in_interval(ring),
                backward: interval.backward,
                negative: interval.negative,
                p50_ns: value_at(&interval.histogram, Percentile::P50),
                p99_ns: value_at(&interval.histogram, Percentile::P99),
                p999_ns: value_at(&interval.histogram, Percentile::P999),
                max_ns: interval.max_ns,
                budget_p99_ns: None,
                budget_p999_ns: None,
                is_over_budget: false,
                clock_source: self.settings.clock_source,
            });
        }
        self.check_budgets();
        for &row in &self.rows {
            sink.write(RollupRow::Latency(row));
        }
        for sink_kind in SinkKind::ALL {
            sink.write(RollupRow::Drop(DropRow {
                interval_end_ts_us,
                sink: sink_kind,
                dropped_total: self.drop_totals[sink_kind as usize],
            }));
        }
        let queue_ids = (0..self.settings.queue_count).map_while(|index| QueueId::new(index).ok());
        for queue_id in queue_ids {
            let Some(ema) = self.backlog_queues[queue_id.index()] else {
                continue;
            };
            sink.write(RollupRow::Backlog(BacklogRow {
                interval_end_ts_us,
                queue: queue_id,
                max: self.backlog_max[queue_id.index()],
                ema: ema.value,
            }));
        }
        self.budget_breaches += BudgetBreach::stage_count(&self.breaches);
        IntervalLog {
            rows: &self.rows,
            breaches: &self.breaches,
            drops: &self.interval_drops(),
            counts: &self.counts,
            budget_breaches: self.budget_breaches,
        }
        .write();
        self.backlog_max = [0; MAX_INPUT_QUEUES];
        self.drops_at_interval_start = self.drop_totals;
    }

    fn rotate(&mut self) {
        self.filling = (self.filling + 1) % (self.settings.window_intervals + 1);
        for pair in &mut self.pairs {
            pair.slots[self.filling].clear();
        }
    }

    fn drops_in_interval(&self, sink: SinkKind) -> u64 {
        let index = sink as usize;
        self.drop_totals[index].saturating_sub(self.drops_at_interval_start[index])
    }

    fn interval_drops(&self) -> [(SinkKind, u64); SinkKind::ALL.len()] {
        SinkKind::ALL.map(|sink| (sink, self.drops_in_interval(sink)))
    }

    /// Flags each row whose stage has a budget that the window broke, and notes the breach.
    fn check_budgets(&mut self) {
        self.breaches.clear();
        for index in 0..self.rows.len() {
            let row = self.rows[index];
            let Some(budget) = self.settings.budget.of(row.stage) else {
                continue;
            };
            let window =
                self.window_cell(|category, stage| category == row.category && stage == row.stage);
            let breach = BudgetBreach {
                category: row.category,
                stage: row.stage,
                window,
                budget,
            };
            let row = &mut self.rows[index];
            row.budget_p99_ns = budget.p99_us.map(micros_to_nanos);
            row.budget_p999_ns = budget.p999_us.map(micros_to_nanos);
            row.is_over_budget = breach.is_broken();
            if row.is_over_budget {
                self.breaches.push(breach);
            }
        }
    }

    /// The window of one flow for one stage: the closed intervals of the window and the one that
    /// is filling, over each category of the flow.
    pub(super) fn flow_cell(&mut self, flow: LatencyFlow, stage: Stage) -> LatencyCell {
        self.window_cell(|category, pair_stage| {
            pair_stage == stage && flow.categories().contains(&category)
        })
    }

    /// The closed intervals of the window and the one that is filling, over each pair that
    /// `is_included` takes.
    fn window_cell(&mut self, is_included: impl Fn(Category, Stage) -> bool) -> LatencyCell {
        self.scratch.clear();
        for pair in &self.pairs {
            if !is_included(pair.category, pair.stage) {
                continue;
            }
            for interval in &pair.slots {
                let merged = self.scratch.histogram.add(&interval.histogram);
                assert!(
                    merged.is_ok(),
                    "every histogram of the rollup shares one set of bounds"
                );
                self.scratch.max_ns = self.scratch.max_ns.max(interval.max_ns);
            }
        }
        let histogram = &self.scratch.histogram;
        LatencyCell {
            count: histogram.len(),
            p50_ns: value_at(histogram, Percentile::P50),
            p99_ns: value_at(histogram, Percentile::P99),
            p999_ns: value_at(histogram, Percentile::P999),
            max_ns: self.scratch.max_ns,
        }
    }

    pub(super) fn flow_age(&self, flow: LatencyFlow) -> Option<DurationUs> {
        let newest = flow
            .categories()
            .iter()
            .filter_map(|&category| self.last_sample_ts_us[category as usize])
            .max()?;
        Some(self.now.diff(newest))
    }

    pub(super) fn backlog_ema(&self) -> Option<f64> {
        self.backlog_total.map(|ema| ema.value)
    }
}

fn value_at(histogram: &Histogram<u32>, percentile: Percentile) -> u64 {
    if histogram.is_empty() {
        return 0;
    }
    histogram.value_at_quantile(percentile.quantile())
}

fn micros_to_nanos(micros: u64) -> u64 {
    micros.saturating_mul(1_000)
}

fn interval_index(at: LocalTs, interval: DurationUs) -> i64 {
    at.micros().div_euclid(interval.micros())
}

fn interval_start(index: i64, interval: DurationUs) -> LocalTs {
    LocalTs::from_micros(index.saturating_mul(interval.micros()))
}
