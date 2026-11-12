//! What the metrics thread tells people: the report that the workstation paints about once a
//! second, and the log lines of each closed interval. No cell and no line holds a mean.

use crate::msg::latency::{Category, EngineCounters};
use crate::msg::ui::{
    LATENCY_REPORT_CELLS, LatencyCell, LatencyFlow, LatencyReading, LatencyReport, Percentile,
};
use crate::{info, warn};

use super::error::MetricsError;
use super::ring::SampleRing;
use super::rollup::LatencyRollup;
use super::rows::{LatencyRow, SinkKind};

/// Past this a record would truncate and lose its last segments without a word.
const LINE_BUDGET: usize = crate::log::TEXT_CAPACITY;

pub(super) fn build_report(rollup: &mut LatencyRollup) -> LatencyReport {
    let mut report = LatencyReport {
        built_ts_us: rollup.now(),
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

/// One line for each category that took a sample, one warning for each kind of fault, and one line
/// of the engine's own counters.
pub(super) fn log_interval(
    rows: &[LatencyRow],
    drops: &[(SinkKind, u64)],
    counters: &EngineCounters,
) {
    for category in Category::ALL {
        let mut line: Option<CappedLine> = None;
        for row in rows.iter().filter(|row| row.category == category) {
            line.get_or_insert_with(|| CappedLine::new(format!("latency {} us", category.label())))
                .push(&stage_segment(row));
        }
        if let Some(line) = line {
            line.emit();
        }
    }
    warn_faults(rows);
    warn_ring_drops(drops);
    emit_counters(drops, counters);
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

fn emit_counters(drops: &[(SinkKind, u64)], counters: &EngineCounters) {
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
