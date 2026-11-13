//! The OTLP/JSON bodies that the exporter posts, and how each line and each row becomes them.
//! As in the official examples, field names are lowerCamelCase and 64-bit integers are strings.

use serde::Serialize;

use crate::log::{CopiedLine, Level};
use crate::msg::time::LocalTs;
use crate::runtime::metrics::{LatencyRow, RollupRow};

const SCOPE: &str = "polysim";
const NANOS: &str = "ns";
const SAMPLES: &str = "{sample}";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct LogsRequest {
    resource_logs: [ResourceLogs; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceLogs {
    resource: Resource,
    scope_logs: [ScopeLogs; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScopeLogs {
    scope: Scope,
    log_records: Vec<LogRecord>,
}

/// No observed time: it would need a clock read, and a collector takes the time of the line.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LogRecord {
    time_unix_nano: String,
    severity_number: u8,
    severity_text: &'static str,
    body: AnyValue,
    attributes: [KeyValue; 4],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MetricsRequest {
    resource_metrics: [ResourceMetrics; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceMetrics {
    resource: Resource,
    scope_metrics: [ScopeMetrics; 1],
}

#[derive(Serialize)]
struct ScopeMetrics {
    scope: Scope,
    metrics: Vec<Metric>,
}

#[derive(Serialize)]
struct Resource {
    attributes: [KeyValue; 1],
}

#[derive(Serialize)]
struct Scope {
    name: &'static str,
}

/// Only gauges: a row holds the numbers of one closed interval, never a sum over the run.
#[derive(Serialize)]
struct Metric {
    name: &'static str,
    unit: &'static str,
    gauge: Gauge,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Gauge {
    data_points: Vec<DataPoint>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DataPoint {
    as_double: f64,
    time_unix_nano: String,
    attributes: Vec<KeyValue>,
}

#[derive(Serialize)]
struct KeyValue {
    key: &'static str,
    value: AnyValue,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum AnyValue {
    StringValue(String),
    IntValue(String),
}

impl LogsRequest {
    pub(super) fn of(service: &str, lines: &[CopiedLine]) -> LogsRequest {
        LogsRequest {
            resource_logs: [ResourceLogs {
                resource: Resource::of(service),
                scope_logs: [ScopeLogs {
                    scope: Scope { name: SCOPE },
                    log_records: lines.iter().map(LogRecord::of).collect(),
                }],
            }],
        }
    }
}

impl LogRecord {
    fn of(line: &CopiedLine) -> LogRecord {
        let callsite = line.record.callsite();
        let (severity_number, severity_text) = match callsite.level() {
            Level::Info => (9, "INFO"),
            Level::Warn => (13, "WARN"),
            Level::Error => (17, "ERROR"),
        };
        LogRecord {
            time_unix_nano: unix_nano(line.record.at()),
            severity_number,
            severity_text,
            body: AnyValue::StringValue(line.record.text().to_owned()),
            attributes: [
                text("polysim.tag", line.tag),
                text("polysim.module", callsite.module()),
                text("code.file.path", callsite.file()),
                KeyValue {
                    key: "code.line.number",
                    value: AnyValue::IntValue(callsite.line().to_string()),
                },
            ],
        }
    }
}

impl MetricsRequest {
    pub(super) fn of(service: &str, rows: &[RollupRow]) -> MetricsRequest {
        let mut gauges = Gauges::default();
        for row in rows {
            gauges.add_row(row);
        }
        MetricsRequest {
            resource_metrics: [ResourceMetrics {
                resource: Resource::of(service),
                scope_metrics: [ScopeMetrics {
                    scope: Scope { name: SCOPE },
                    metrics: gauges.0,
                }],
            }],
        }
    }
}

impl Resource {
    fn of(service: &str) -> Resource {
        Resource {
            attributes: [text("service.name", service)],
        }
    }
}

/// The metrics of one body: one for each name, with its points in row order.
#[derive(Default)]
struct Gauges(Vec<Metric>);

impl Gauges {
    fn add_row(&mut self, row: &RollupRow) {
        match row {
            RollupRow::Latency(row) => self.add_latency(row),
            RollupRow::Drop(row) => {
                let attributes = vec![text("polysim.sink", row.sink.label())];
                let point = DataPoint::of(row.dropped_total, row.interval_end_ts_us, attributes);
                self.add("polysim.drops", "{record}", point);
            }
            RollupRow::Backlog(row) => {
                let attributes = vec![text("polysim.queue", &row.queue.index().to_string())];
                let point = DataPoint::of(u64::from(row.max), row.interval_end_ts_us, attributes);
                self.add("polysim.backlog.max", "{message}", point);
            }
        }
    }

    fn add_latency(&mut self, row: &LatencyRow) {
        let fields = [
            Some(("polysim.latency.p50", NANOS, row.p50_ns)),
            Some(("polysim.latency.p99", NANOS, row.p99_ns)),
            Some(("polysim.latency.p999", NANOS, row.p999_ns)),
            Some(("polysim.latency.max", NANOS, row.max_ns)),
            Some(("polysim.latency.count", SAMPLES, row.count)),
            Some(("polysim.latency.dropped", SAMPLES, row.dropped)),
            Some(("polysim.latency.backward", SAMPLES, row.backward)),
            Some(("polysim.latency.negative", SAMPLES, row.negative)),
            row.budget_p99_ns
                .map(|nanos| ("polysim.latency.budget.p99", NANOS, nanos)),
            row.budget_p999_ns
                .map(|nanos| ("polysim.latency.budget.p999", NANOS, nanos)),
            Some((
                "polysim.latency.over_budget",
                "1",
                u64::from(row.is_over_budget),
            )),
        ];
        for (name, unit, value) in fields.into_iter().flatten() {
            let attributes = vec![
                text("polysim.category", row.category.label()),
                text("polysim.stage", row.stage.label()),
                text("polysim.clock_source", row.clock_source.label()),
            ];
            let point = DataPoint::of(value, row.interval_end_ts_us, attributes);
            self.add(name, unit, point);
        }
    }

    fn add(&mut self, name: &'static str, unit: &'static str, point: DataPoint) {
        match self.0.iter_mut().find(|metric| metric.name == name) {
            Some(metric) => metric.gauge.data_points.push(point),
            None => self.0.push(Metric {
                name,
                unit,
                gauge: Gauge {
                    data_points: vec![point],
                },
            }),
        }
    }
}

impl DataPoint {
    /// A double holds each count of a run and each latency below 104 days exactly.
    fn of(value: u64, at: LocalTs, attributes: Vec<KeyValue>) -> DataPoint {
        DataPoint {
            as_double: value as f64,
            time_unix_nano: unix_nano(at),
            attributes,
        }
    }
}

fn text(key: &'static str, value: &str) -> KeyValue {
    KeyValue {
        key,
        value: AnyValue::StringValue(value.to_owned()),
    }
}

/// A collector is another process, so it reads a local stamp as a stamp of a clock that is not its
/// own. The wire counts nanoseconds, so the last three digits are 0.
fn unix_nano(at: LocalTs) -> String {
    let micros = at.to_peer().micros();
    u64::try_from(micros)
        .unwrap_or(0)
        .saturating_mul(1_000)
        .to_string()
}
