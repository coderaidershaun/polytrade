//! Drains hot snapshot ring, emits an INFO summary per category + occupancy/drops on a fixed cadence — each figure is a running ten-minute window that OVERLAPS the last, and drop counters are run totals.

use std::time::Duration;

use rtrb::Consumer;
use tokio::task::JoinHandle;

use crate::hot::metrics::{Category, MetricsSnapshot, Stage};
use crate::{info, warn};

const DEFAULT_SUMMARY_SECS: u64 = 60;
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Test override (shorten window).
const SUMMARY_SECS_ENV: &str = "POLYSIM_METRICS_SUMMARY_SECS";
/// Past this a record truncates silently, dropping the LAST segments rather than reporting the loss.
const LINE_BUDGET: usize = crate::log::MSG_CAPACITY;

/// An output consumer with nothing to flush: [`MetricsHandle::abort`] alone tears it down, so
/// unlike its edge-producer peers it has no async `shutdown`.
pub(super) struct MetricsHandle {
    join: JoinHandle<()>,
}

impl MetricsHandle {
    /// Emits summary every window (60s or POLYSIM_METRICS_SUMMARY_SECS).
    pub(super) fn spawn(mut snapshots: Consumer<MetricsSnapshot>) -> MetricsHandle {
        let summary_window = summary_window();
        let body = async move {
            let mut ticker = tokio::time::interval(POLL_INTERVAL);
            let mut since_summary = Duration::ZERO;
            let mut latest: Option<MetricsSnapshot> = None;
            loop {
                ticker.tick().await;
                while let Ok(snapshot) = snapshots.pop() {
                    latest = Some(snapshot);
                }
                since_summary += POLL_INTERVAL;
                if since_summary < summary_window {
                    continue;
                }
                since_summary = Duration::ZERO;
                if let Some(snapshot) = latest.take() {
                    emit_summary(&snapshot);
                }
            }
        };
        let join = tokio::spawn(crate::log::tag_task("metrics", body));
        MetricsHandle { join }
    }

    pub(super) fn abort(self) {
        self.join.abort();
    }
}

fn summary_window() -> Duration {
    let default = Duration::from_secs(DEFAULT_SUMMARY_SECS);
    let raw = match std::env::var(SUMMARY_SECS_ENV) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            warn!(
                "{SUMMARY_SECS_ENV}={raw:?} is not valid unicode — using {DEFAULT_SUMMARY_SECS}s"
            );
            return default;
        }
    };
    match raw.parse::<u64>() {
        Ok(secs) if secs > 0 => Duration::from_secs(secs),
        _ => {
            warn!(
                "{SUMMARY_SECS_ENV}={raw:?} is not a positive integer — using {DEFAULT_SUMMARY_SECS}s"
            );
            default
        }
    }
}

fn emit_summary(snapshot: &MetricsSnapshot) {
    for category in Category::ALL {
        if snapshot.is_active(category) {
            emit_category(category, snapshot);
        }
    }
    emit_globals(snapshot);
}

/// Emits as it fills: a segment that would cross [`LINE_BUDGET`] starts a fresh record carrying the
/// same prefix, so one summary spans as many records as it needs and no segment is ever cut.
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

    /// Counters an operator reads together, so an all-zero group says nothing and is omitted whole.
    fn push_group(&mut self, label: &str, counts: &[(&str, u64)]) {
        if counts.iter().all(|&(_, value)| value == 0) {
            return;
        }
        let pairs: Vec<String> = counts
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        self.push(&format!(" {label}[{}]", pairs.join(" ")));
    }

    fn emit(self) {
        if self.line.len() > self.prefix.len() {
            info!("{}", self.line);
        }
    }
}

fn emit_category(category: Category, snapshot: &MetricsSnapshot) {
    let count = snapshot.stage(category, Stage::EndToEnd).count;
    let mut line = CappedLine::new(format!("metrics {} n={count}", category.label()));
    for stage in Stage::ALL {
        let stat = snapshot.stage(category, stage);
        if stat.count == 0 {
            continue;
        }
        line.push(&format!(
            " {}[mean={:.0} p50={} p99={} min={} max={}]",
            stage.label(),
            stat.mean_us(),
            stat.p50_us,
            stat.p99_us,
            stat.min_us,
            stat.max_us
        ));
    }
    line.emit();
}

fn emit_globals(snapshot: &MetricsSnapshot) {
    let mut line = CappedLine::new("metrics queues".to_string());
    for queue in 0..snapshot.queue_count as usize {
        let depth = snapshot.occupancy[queue];
        if depth.count == 0 {
            continue;
        }
        line.push(&format!(" q{queue}={:.1}/{}", depth.mean(), depth.max));
    }
    let counters = snapshot.counters;
    // Always reported, zero or not: these four are what "the engine lost nothing" is read off.
    line.push(&format!(
        " | persist_dropped={} book_trimmed={} book_remove_missing={} snapshots_dropped={}",
        counters.persist_dropped,
        counters.book_trimmed,
        counters.book_remove_missing,
        counters.snapshots_dropped
    ));
    line.push_count("orders_submitted", counters.orders_submitted);
    let bank = counters.bank_drops;
    line.push_group(
        "bank_dropped",
        &[
            ("features", bank.features),
            ("persist", bank.persist),
            ("logs", bank.logs),
            ("link", bank.link),
        ],
    );
    line.push_group(
        "intensity_unanchored",
        &[
            ("inside_spread", counters.intensity_inside_spread),
            ("without_book", counters.intensity_without_book),
        ],
    );
    line.push_count("klines_unconfigured", counters.klines_unconfigured);
    line.push_group(
        "ui_dropped",
        &[
            ("books", counters.ui_books_dropped),
            ("events", counters.ui_events_dropped),
        ],
    );
    line.push_count("link_dropped", counters.link_dropped);
    line.push_count("exposure_superseded", counters.exposure_superseded);
    line.emit();
}
