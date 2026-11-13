//! What the metrics thread tells people: the workstation report, the log lines of each closed
//! interval, and the budget check of each stage. No cell and no line holds a mean.

use crate::msg::latency::{Category, EngineCounters, Stage};
use crate::msg::ui::{
    LATENCY_REPORT_CELLS, LatencyCell, LatencyFlow, LatencyReading, LatencyReport, Percentile,
};
use crate::runtime::config::StageBudget;
use crate::{info, warn};

use super::error::MetricsError;
use super::ring::SampleRing;
use super::rollup::LatencyRollup;
use super::rows::{LatencyRow, SinkKind};

/// Past this a record would truncate and lose its last segments without a word.
const LINE_BUDGET: usize = crate::log::TEXT_CAPACITY;

pub(super) fn build_report(rollup: &mut LatencyRollup) -> LatencyReport {
    let mut report = LatencyReport {
        built_ts_us: rollup.now().to_peer(),
        ages: LatencyFlow::ALL.map(|flow| rollup.flow_age(flow)),
        cells: [LatencyCell::EMPTY; LATENCY_REPORT_CELLS],
        backlog_ema: rollup.backlog_ema(),
    };
    for flow in LatencyFlow::ALL {
        for &stage in flow.stages() {
            let cell = rollup.flow_cell(flow, stage);
            if let Some(slot) = report.cell_mut(flow, stage) {
                *slot = cell;
            }
        }
    }
    report
}

/// The run totals of the latest counter sample.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RunCounts {
    pub(super) counters: EngineCounters,
    pub(super) idle_parks: u64,
}

/// One stage of one category whose window broke its budget when an interval closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BudgetBreach {
    pub(super) category: Category,
    pub(super) stage: Stage,
    pub(super) window: LatencyCell,
    pub(super) budget: StageBudget,
}

impl BudgetBreach {
    pub(super) fn is_broken(&self) -> bool {
        let is_over = |value_ns: u64, budget_us: Option<u64>| {
            budget_us.is_some_and(|budget_us| value_ns > budget_us.saturating_mul(1_000))
        };
        self.window.count > 0
            && (is_over(self.window.p99_ns, self.budget.p99_us)
                || is_over(self.window.p999_ns, self.budget.p999_us))
    }

    /// Each stage counts once in an interval, however many of its categories broke the budget.
    pub(super) fn stage_count(breaches: &[BudgetBreach]) -> u64 {
        Stage::ALL
            .iter()
            .filter(|stage| breaches.iter().any(|breach| breach.stage == **stage))
            .count() as u64
    }
}

/// What one closed interval writes to the log.
pub(super) struct IntervalLog<'a> {
    pub(super) rows: &'a [LatencyRow],
    pub(super) breaches: &'a [BudgetBreach],
    pub(super) drops: &'a [(SinkKind, u64)],
    pub(super) counts: &'a RunCounts,
    /// Since the start of the run.
    pub(super) budget_breaches: u64,
}

impl IntervalLog<'_> {
    /// One line for each category that took a sample, one warning for each kind of fault and for
    /// each stage over its budget, and one line of the engine's own counters.
    pub(super) fn write(&self) {
        for category in Category::ALL {
            let mut line: Option<CappedLine> = None;
            for row in self.rows.iter().filter(|row| row.category == category) {
                line.get_or_insert_with(|| {
                    CappedLine::new(format!("latency {} us", category.label()))
                })
                .push(&stage_segment(row));
            }
            if let Some(line) = line {
                line.emit();
            }
        }
        warn_faults(self.rows);
        warn_ring_drops(self.drops);
        warn_budgets(self.breaches);
        emit_counters(self.drops, self.counts, self.budget_breaches);
    }
}

/// One warning for each stage, naming each category that broke the budget with its window.
fn warn_budgets(breaches: &[BudgetBreach]) {
    for stage in Stage::ALL {
        let mut line: Option<String> = None;
        for breach in breaches.iter().filter(|breach| breach.stage == stage) {
            let text = line.get_or_insert_with(|| {
                format!(
                    "latency budget broken: {} budget p99={} p999={} us, window",
                    stage.label(),
                    budget_text(breach.budget.p99_us),
                    budget_text(breach.budget.p999_us),
                )
            });
            text.push_str(&format!(
                " {}[n={} p99={} p999={}]",
                breach.category.label(),
                breach.window.count,
                micros_text(breach.window.p99_ns),
                micros_text(breach.window.p999_ns),
            ));
        }
        if let Some(line) = line {
            warn!("{line}");
        }
    }
}

fn budget_text(budget_us: Option<u64>) -> String {
    budget_us.map_or_else(|| "-".to_owned(), |budget_us| budget_us.to_string())
}

/// A breach is reported even on few samples, so the warning gives the raw value and its count.
fn micros_text(nanos: u64) -> String {
    format!("{:.1}", nanos as f64 / 1_000.0)
}

fn stage_segment(row: &LatencyRow) -> String {
    let cell = row.cell();
    let mut segment = format!(
        " {}[n={} p50={} p99={} p999={} max={}",
        row.stage.label(),
        row.count,
        reading_text(cell.reading(Percentile::P50)),
        reading_text(cell.reading(Percentile::P99)),
        reading_text(cell.reading(Percentile::P999)),
        reading_text(cell.max_reading()),
    );
    for (name, value) in [
        ("dropped", row.dropped),
        ("backward", row.backward),
        ("negative", row.negative),
    ] {
        if value > 0 {
            segment.push_str(&format!(" {name}={value}"));
        }
    }
    segment.push(']');
    segment
}

fn reading_text(reading: LatencyReading) -> String {
    match reading {
        LatencyReading::Micros(micros) => format!("{micros:.1}"),
        LatencyReading::Thin { .. } => "thin".to_owned(),
        LatencyReading::Absent => "-".to_owned(),
    }
}

fn warn_faults(rows: &[LatencyRow]) {
    let backward = rows.iter().find(|row| row.backward > 0);
    if let Some(row) = backward {
        warn!(
            "{}",
            MetricsError::Backward {
                category: row.category,
                stage: row.stage,
                count: row.backward,
            }
        );
    }
    let negative = rows.iter().find(|row| row.negative > 0);
    if let Some(row) = negative {
        warn!(
            "{}",
            MetricsError::NegativeVenueStage {
                category: row.category,
                stage: row.stage,
                count: row.negative,
            }
        );
    }
}

fn warn_ring_drops(drops: &[(SinkKind, u64)]) {
    for (sink, dropped) in drops {
        let ring = match sink {
            SinkKind::StageSamples => SampleRing::Stages,
            SinkKind::CounterSamples => SampleRing::Counters,
            SinkKind::WireSamples => SampleRing::Wires,
            _ => continue,
        };
        if *dropped > 0 {
            warn!(
                "{}",
                MetricsError::SampleRingFull {
                    ring,
                    dropped: *dropped,
                }
            );
        }
    }
}

fn emit_counters(drops: &[(SinkKind, u64)], counts: &RunCounts, budget_breaches: u64) {
    let counters = &counts.counters;
    let mut line = CappedLine::new("metrics counters".to_owned());
    // Always written, zero or not: these three are what "the engine lost nothing" is read off.
    line.push(&format!(
        " persist_dropped={} book_trimmed={} book_remove_missing={}",
        counters.persist_dropped, counters.book_trimmed, counters.book_remove_missing
    ));
    line.push_count("orders_submitted", counters.orders_submitted);
    line.push_group(
        "intensity_unanchored",
        &[
            ("inside_spread", counters.intensity_inside_spread),
            ("without_book", counters.intensity_without_book),
        ],
    );
    line.push_count("klines_unconfigured", counters.klines_unconfigured);
    line.push_group(
        "incidents",
        &[
            ("fee_gaps", counters.fee_gaps),
            ("stranded_positions", counters.stranded_positions),
            ("orphan_fills", counters.orphan_fills),
            ("unbooked_fees", counters.unbooked_fees),
            (
                "orphan_events_without_fill",
                counters.orphan_events_without_fill,
            ),
        ],
    );
    line.push_count("idle_parks", counts.idle_parks);
    line.push_count("budget_breaches", budget_breaches);
    let interval_drops: Vec<(&str, u64)> = drops
        .iter()
        .map(|(sink, dropped)| (sink.label(), *dropped))
        .collect();
    line.push_group("dropped_this_interval", &interval_drops);
    line.emit();
}

/// Emits as it fills: a segment that would cross [`LINE_BUDGET`] starts a fresh record that carries
/// the same prefix, so one summary spans as many records as it needs and no segment is ever cut.
struct CappedLine {
    prefix: String,
    line: String,
}

impl CappedLine {
    fn new(prefix: String) -> CappedLine {
        CappedLine {
            line: prefix.clone(),
            prefix,
        }
    }

    fn push(&mut self, segment: &str) {
        if self.line.len() > self.prefix.len() && self.line.len() + segment.len() > LINE_BUDGET {
            info!("{}", self.line);
            self.line.clone_from(&self.prefix);
        }
        self.line.push_str(segment);
    }

    /// A run total worth reading only once it has moved.
    fn push_count(&mut self, name: &str, value: u64) {
        if value > 0 {
            self.push(&format!(" {name}={value}"));
        }
    }

    /// Counters an operator reads together, so an all-zero group says nothing and is left out
    /// whole, and a zero inside a group that moved is left out too.
    fn push_group(&mut self, label: &str, counts: &[(&str, u64)]) {
        let moved: Vec<String> = counts
            .iter()
            .filter(|(_, value)| *value > 0)
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        if moved.is_empty() {
            return;
        }
        self.push(&format!(" {label}[{}]", moved.join(" ")));
    }

    fn emit(self) {
        if self.line.len() > self.prefix.len() {
            info!("{}", self.line);
        }
    }
}
