//! What the OTLP export sends to a stand-in collector on loopback, checked against the official
//! examples, and when the exporter gives up on a request.
//!
//! The official examples are `examples/logs.json` and `examples/metrics.json` of
//! https://github.com/open-telemetry/opentelemetry-proto at commit
//! b3f75588eb23c5fca62264edd05d382de49beb1a, copied byte for byte with the license of that
//! repository.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use engine::msg::failure::FailureClass;
use engine::msg::time::LocalTs;
use engine::runtime::config::{Config, ExecutionMode, NoParams, RunIdentity};
use engine::runtime::metrics::{DropRow, RollupRow, RollupSink, SinkKind};
use engine::runtime::otlp::{OtlpPlan, OtlpReport, OtlpRows};
use serde_json::Value;

use super::config_guards::{
    BINANCE_SPOT_SOURCE, SIM_BLOCK, config_with_execution_on, execution_block_with,
};
use super::metrics::{Harness, spin_counters};

const STRATEGY: &str = "strat-otlp";
const ENGINE: &str = "te-contract";

/// How the stand-in answers one request.
#[derive(Debug, Clone)]
enum Answer {
    /// `200` with the body `{}`.
    Accepted,
    /// A status that is not 2xx, with a `Location` header when one is given.
    Status(u16, Option<String>),
    /// Reads the request and never answers. The connection stays open until the stand-in drops.
    Silent,
}

/// One request as the stand-in read it.
#[derive(Debug, Clone)]
struct Received {
    method: String,
    path: String,
    content_type: Option<String>,
    body: Value,
}

/// A collector on a loopback port of this process. The answer to each connection is the next one
/// of its list, and the last one repeats.
struct Collector {
    address: SocketAddr,
    received: Arc<Mutex<Vec<Received>>>,
    held: Arc<Mutex<Vec<TcpStream>>>,
    is_stopping: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl Collector {
    fn answering(answers: &[Answer]) -> Collector {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("the stand-in binds a loopback port");
        listener
            .set_nonblocking(true)
            .expect("the stand-in polls its listener");
        let address = listener.local_addr().expect("a bound address");
        let received = Arc::new(Mutex::new(Vec::new()));
        let held = Arc::new(Mutex::new(Vec::new()));
        let is_stopping = Arc::new(AtomicBool::new(false));
        let lanes = (
            Arc::clone(&received),
            Arc::clone(&held),
            Arc::clone(&is_stopping),
        );
        let answers = answers.to_vec();
        let accept = thread::spawn(move || accept_each(&listener, &answers, lanes));
        Collector {
            address,
            received,
            held,
            is_stopping,
            accept: Some(accept),
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn requests(&self) -> Vec<Received> {
        self.received
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The requests once `is_done` holds for them, or once `limit` has passed.
    fn requests_once(
        &self,
        limit: Duration,
        is_done: impl Fn(&[Received]) -> bool,
    ) -> Vec<Received> {
        let deadline = Instant::now() + limit;
        loop {
            let requests = self.requests();
            if is_done(&requests) || Instant::now() >= deadline {
                return requests;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.is_stopping.store(true, Ordering::Release);
        if let Some(accept) = self.accept.take() {
            accept.join().ok();
        }
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

type Shared = (
    Arc<Mutex<Vec<Received>>>,
    Arc<Mutex<Vec<TcpStream>>>,
    Arc<AtomicBool>,
);

fn accept_each(listener: &TcpListener, answers: &[Answer], shared: Shared) {
    let (received, held, is_stopping) = shared;
    let mut handlers = Vec::new();
    let mut next = 0;
    while !is_stopping.load(Ordering::Acquire) {
        let Ok((stream, _)) = listener.accept() else {
            thread::sleep(Duration::from_millis(2));
            continue;
        };
        let answer = answers[next.min(answers.len() - 1)].clone();
        next += 1;
        let received = Arc::clone(&received);
        let held = Arc::clone(&held);
        handlers.push(thread::spawn(move || {
            answer_one(stream, answer, &received, &held);
        }));
    }
    for handler in handlers {
        handler.join().ok();
    }
}

fn answer_one(
    stream: TcpStream,
    answer: Answer,
    received: &Mutex<Vec<Received>>,
    held: &Mutex<Vec<TcpStream>>,
) {
    stream
        .set_nonblocking(false)
        .expect("an accepted stream blocks");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("the stand-in bounds each read");
    let Some(request) = read_request(&stream) else {
        return;
    };
    received
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(request);
    match answer {
        Answer::Accepted => respond(stream, 200, None, "{}"),
        Answer::Status(status, location) => respond(stream, status, location, ""),
        Answer::Silent => held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(stream),
    }
}

/// One HTTP/1.1 request, read by its `Content-Length`.
fn read_request(stream: &TcpStream) -> Option<Received> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();
    let mut content_type = None;
    let mut length = 0;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':')?;
        match name.trim().to_ascii_lowercase().as_str() {
            "content-type" => content_type = Some(value.trim().to_owned()),
            "content-length" => length = value.trim().parse().ok()?,
            _ => {}
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    let body = serde_json::from_slice(&body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
    Some(Received {
        method,
        path,
        content_type,
        body,
    })
}

fn respond(mut stream: TcpStream, status: u16, location: Option<String>, body: &str) {
    let location = location
        .map(|url| format!("Location: {url}\r\n"))
        .unwrap_or_default();
    let response = format!(
        "HTTP/1.1 {status} Stand-In\r\n{location}Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).ok();
}

/// A loopback port where nothing listens: the listener that held it is gone.
fn down_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("the test takes a loopback port")
}

fn plan_for(collector: SocketAddr) -> OtlpPlan {
    let execution = execution_block_with("sim", SIM_BLOCK);
    let document = format!(
        "{}otlp:\n  collector: {collector}\n",
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution)
    );
    let config = Config::<NoParams>::from_yaml(&document).expect("the document is valid");
    let identity = RunIdentity::new(STRATEGY, ENGINE).expect("a valid identity");
    OtlpPlan::check(&config, &identity, Some(ExecutionMode::Sim))
        .expect("the otlp block passes its check")
        .expect("the document has an otlp block")
}

fn official(example: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/otlp")
        .join(example);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{} reads: {error}", path.display()));
    serde_json::from_str(&text).expect("an official example is JSON")
}

/// Where `payload` leaves the form of `example`: a key that the example lacks at that place, an
/// array element that fits no element of the example's array, or a value of another JSON kind.
fn misfit(payload: &Value, example: &Value, at: &str) -> Option<String> {
    match (payload, example) {
        (Value::Object(fields), Value::Object(official_fields)) => {
            fields
                .iter()
                .find_map(|(key, value)| match official_fields.get(key) {
                    None => Some(format!("{at}.{key} is not in the official form")),
                    Some(official) => misfit(value, official, &format!("{at}.{key}")),
                })
        }
        (Value::Array(items), Value::Array(official_items)) => {
            items.iter().enumerate().find_map(|(index, item)| {
                let fits = official_items
                    .iter()
                    .any(|official| misfit(item, official, at).is_none());
                (!fits).then(|| format!("{at}[{index}] fits no official element: {item}"))
            })
        }
        (Value::String(_), Value::String(_))
        | (Value::Number(_), Value::Number(_))
        | (Value::Bool(_), Value::Bool(_)) => None,
        _ => Some(format!(
            "{at} is {payload}, where the official form holds {example}"
        )),
    }
}

fn assert_official_form(request: &Received, path: &str, example: &str) {
    assert_eq!(request.method, "POST", "{path} is a POST");
    assert_eq!(request.path, path);
    assert_eq!(
        request.content_type.as_deref(),
        Some("application/json"),
        "{path} carries JSON"
    );
    if let Some(misfit) = misfit(&request.body, &official(example), "body") {
        panic!(
            "a {path} body leaves the official form of {example}: {misfit}\n{}",
            request.body
        );
    }
}

fn each(value: &Value) -> impl Iterator<Item = &Value> {
    value.as_array().into_iter().flatten()
}

fn text(value: &Value) -> String {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{value} is a string"))
        .to_owned()
}

/// One gauge point as a collector reads it.
#[derive(Debug, Clone, PartialEq)]
struct Point {
    name: String,
    unit: String,
    attributes: Vec<(String, String)>,
    value: f64,
    time_unix_nano: String,
}

fn points(body: &Value) -> Vec<Point> {
    let mut points = Vec::new();
    for resource in each(&body["resourceMetrics"]) {
        for scope in each(&resource["scopeMetrics"]) {
            for metric in each(&scope["metrics"]) {
                for kind in ["sum", "histogram", "exponentialHistogram", "summary"] {
                    assert!(
                        metric.get(kind).is_none(),
                        "the export sends gauges only, and {} holds a {kind}",
                        metric["name"]
                    );
                }
                points.extend(each(&metric["gauge"]["dataPoints"]).map(|point| {
                    Point {
                        name: text(&metric["name"]),
                        unit: text(&metric["unit"]),
                        attributes: each(&point["attributes"])
                            .map(|pair| (text(&pair["key"]), text(&pair["value"]["stringValue"])))
                            .collect(),
                        value: point["asDouble"].as_f64().expect("a point holds asDouble"),
                        time_unix_nano: text(&point["timeUnixNano"]),
                    }
                }));
            }
        }
    }
    points
}

fn point(name: &str, unit: &str, attributes: &[(&str, String)], value: u64, at: LocalTs) -> Point {
    Point {
        name: name.to_owned(),
        unit: unit.to_owned(),
        attributes: attributes
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect(),
        value: value as f64,
        time_unix_nano: (at.micros() * 1_000).to_string(),
    }
}

/// The gauge points that one row becomes, by the mapping of the export.
fn points_of_row(row: &RollupRow) -> Vec<Point> {
    match row {
        RollupRow::Latency(row) => {
            let attributes = [
                ("polysim.category", row.category.label().to_owned()),
                ("polysim.stage", row.stage.label().to_owned()),
                ("polysim.clock_source", row.clock_source.label().to_owned()),
            ];
            let mut fields = vec![
                ("polysim.latency.p50", "ns", row.p50_ns),
                ("polysim.latency.p99", "ns", row.p99_ns),
                ("polysim.latency.p999", "ns", row.p999_ns),
                ("polysim.latency.max", "ns", row.max_ns),
                ("polysim.latency.count", "{sample}", row.count),
                ("polysim.latency.dropped", "{sample}", row.dropped),
                ("polysim.latency.backward", "{sample}", row.backward),
                ("polysim.latency.negative", "{sample}", row.negative),
            ];
            fields.extend(
                row.budget_p99_ns
                    .map(|nanos| ("polysim.latency.budget.p99", "ns", nanos)),
            );
            fields.extend(
                row.budget_p999_ns
                    .map(|nanos| ("polysim.latency.budget.p999", "ns", nanos)),
            );
            fields.push((
                "polysim.latency.over_budget",
                "1",
                u64::from(row.is_over_budget),
            ));
            fields
                .into_iter()
                .map(|(name, unit, value)| {
                    point(name, unit, &attributes, value, row.interval_end_ts_us)
                })
                .collect()
        }
        RollupRow::Drop(row) => vec![point(
            "polysim.drops",
            "{record}",
            &[("polysim.sink", row.sink.label().to_owned())],
            row.dropped_total,
            row.interval_end_ts_us,
        )],
        RollupRow::Backlog(row) => vec![point(
            "polysim.backlog.max",
            "{message}",
            &[("polysim.queue", row.queue.index().to_string())],
            u64::from(row.max),
            row.interval_end_ts_us,
        )],
    }
}

/// The points of each metric name, in the order they were sent. A body groups its points by
/// name, so only the order within one name follows the rows.
fn by_name(points: &[Point]) -> BTreeMap<String, Vec<Point>> {
    let mut named: BTreeMap<String, Vec<Point>> = BTreeMap::new();
    for point in points {
        named
            .entry(point.name.clone())
            .or_default()
            .push(point.clone());
    }
    named
}

fn failed_requests(report: &OtlpReport) -> u64 {
    report.refused + report.unreached + report.unanswered + report.unwritten
}

/// Keeps each row that the metrics core writes, and passes it on to the rows lane.
struct Recorded {
    rows: OtlpRows,
    written: Vec<RollupRow>,
}

impl RollupSink for Recorded {
    fn write(&mut self, row: RollupRow) {
        self.written.push(row);
        self.rows.write(row);
    }
}

fn metrics_requests(requests: &[Received]) -> Vec<&Received> {
    requests
        .iter()
        .filter(|request| request.path == "/v1/metrics")
        .collect()
}

/// CONTRACT: each row of the metrics thread reaches the collector as gauge points in the official
/// form, one time each and with no mean, and the last pass of the stop sends the rows of the
/// interval that the stop closed.
#[test]
fn rollup_rows_reach_the_collector_as_otlp_metric_points() {
    let collector = Collector::answering(&[Answer::Accepted]);
    let (rows, lanes) = plan_for(collector.address()).open();
    let mut sink = Recorded {
        rows,
        written: Vec::new(),
    };
    let exporter = lanes.start().expect("the OS starts the exporter thread");
    let mut harness = Harness::new(10);
    let trade = harness.trade();
    harness.samples.stages.push(trade);
    harness
        .samples
        .counters
        .push(spin_counters(1_000_000, [5, 3]));
    harness
        .metrics
        .pass(LocalTs::from_micros(60_000_001), &mut sink);
    harness
        .metrics
        .finish(LocalTs::from_micros(60_500_000), &mut sink);
    let report = exporter.stop();

    let requests = collector.requests_once(Duration::from_secs(3), |requests| {
        !metrics_requests(requests).is_empty()
    });
    let metrics = metrics_requests(&requests);
    assert!(
        !metrics.is_empty(),
        "the collector got no /v1/metrics request within 3 s"
    );
    for request in &metrics {
        assert_official_form(request, "/v1/metrics", "metrics.json");
    }
    let sent: Vec<Point> = metrics
        .iter()
        .flat_map(|request| points(&request.body))
        .collect();
    let expected: Vec<Point> = sink.written.iter().flat_map(points_of_row).collect();
    assert_eq!(
        by_name(&sent),
        by_name(&expected),
        "each row reaches the collector as the points of its mapping, one time each"
    );
    let stamps: BTreeSet<&str> = sent
        .iter()
        .map(|point| point.time_unix_nano.as_str())
        .collect();
    assert_eq!(
        stamps,
        BTreeSet::from(["60000000000", "120000000000"]),
        "a point carries the end of its interval in nanoseconds, and the stop sends the rows of \
         the interval that it closed"
    );
    assert!(
        sent.iter()
            .all(|point| !point.name.contains("mean") && !point.name.contains("ema")),
        "the export sends no mean"
    );
    assert_eq!(report.rows_sent, sink.written.len() as u64, "{report}");
    assert_eq!(failed_requests(&report), 0, "{report}");
}

/// The drop count of one ring, as a row of an interval that ended in 2027.
fn drop_row(sink: SinkKind, dropped_total: u64) -> RollupRow {
    RollupRow::Drop(DropRow {
        interval_end_ts_us: LocalTs::from_micros(1_800_000_060_000_000),
        sink,
        dropped_total,
    })
}

fn write_rows(rows: &mut OtlpRows, batch: &[RollupRow]) {
    for row in batch {
        rows.write(*row);
    }
}

fn points_of_rows(batch: &[RollupRow]) -> Vec<Point> {
    batch.iter().flat_map(points_of_row).collect()
}

/// CONTRACT: a request that fails is dropped and counted, and never sent again, so the next
/// request carries only newer rows. No redirect is followed, and no failure reaches the engine:
/// each one is a reject.
#[test]
fn a_failed_send_is_dropped_counted_and_never_sent_again() {
    assert_eq!(OtlpRows::WHEN_FULL, FailureClass::Reject);
    let first = [
        drop_row(SinkKind::StageSamples, 1),
        drop_row(SinkKind::CounterSamples, 2),
        drop_row(SinkKind::WireSamples, 3),
    ];
    let newer = [
        drop_row(SinkKind::UiBooks, 4),
        drop_row(SinkKind::UiEvents, 5),
    ];

    let silent = Collector::answering(&[Answer::Silent]);
    let (mut rows, lanes) = plan_for(silent.address()).open();
    write_rows(&mut rows, &first);
    let exporter = lanes.start().expect("the OS starts the exporter thread");
    silent.requests_once(Duration::from_secs(3), |requests| !requests.is_empty());
    // The first request times out, and each pass after it finds no row to send.
    thread::sleep(Duration::from_millis(2_500));
    write_rows(&mut rows, &newer);
    let requests = silent.requests_once(Duration::from_secs(3), |requests| requests.len() >= 2);
    let counts: Vec<usize> = requests
        .iter()
        .map(|request| points(&request.body).len())
        .collect();
    assert_eq!(
        counts,
        [3, 2],
        "the stand-in got requests with points {counts:?}, expected [3, 2]"
    );
    assert_eq!(points(&requests[0].body), points_of_rows(&first));
    assert_eq!(
        points(&requests[1].body),
        points_of_rows(&newer),
        "no row of a request that timed out is sent again"
    );
    let report = exporter.stop();
    assert_eq!(
        (report.rows_sent, report.rows_failed),
        (0, 5),
        "the rows of each request that timed out are dropped and counted: {report}"
    );
    assert_eq!(
        (report.refused, report.unreached, report.unanswered),
        (0, 0, 2),
        "{report}"
    );

    let (mut rows, lanes) = plan_for(down_address()).open();
    write_rows(&mut rows, &first);
    let report = lanes
        .start()
        .expect("the OS starts the exporter thread")
        .stop();
    assert_eq!(
        (report.unreached, report.rows_failed, report.rows_sent),
        (1, 3, 0),
        "a collector that is down: {report}"
    );

    let refusing = Collector::answering(&[Answer::Status(503, None), Answer::Accepted]);
    let (mut rows, lanes) = plan_for(refusing.address()).open();
    write_rows(&mut rows, &first);
    let exporter = lanes.start().expect("the OS starts the exporter thread");
    refusing.requests_once(Duration::from_secs(3), |requests| !requests.is_empty());
    write_rows(&mut rows, &newer);
    let report = exporter.stop();
    let sent: Vec<Vec<Point>> = refusing
        .requests()
        .iter()
        .map(|request| points(&request.body))
        .collect();
    assert_eq!(
        sent,
        [points_of_rows(&first), points_of_rows(&newer)],
        "the request after a refusal carries only the newer rows"
    );
    assert_eq!(
        (report.refused, report.rows_failed, report.rows_sent),
        (1, 3, 2),
        "a collector that answers 503: {report}"
    );

    let location = Collector::answering(&[Answer::Accepted]);
    let elsewhere = format!("http://{}/v1/metrics", location.address());
    let redirecting = Collector::answering(&[Answer::Status(307, Some(elsewhere))]);
    let (mut rows, lanes) = plan_for(redirecting.address()).open();
    write_rows(&mut rows, &newer[..1]);
    let report = lanes
        .start()
        .expect("the OS starts the exporter thread")
        .stop();
    let followed = location.requests().len();
    assert_eq!(
        followed, 0,
        "the stand-in at the Location counted {followed} request, expected 0"
    );
    assert_eq!(redirecting.requests().len(), 1);
    assert_eq!(
        (report.refused, report.rows_failed, report.rows_sent),
        (1, 1, 0),
        "a redirect is a refusal: {report}"
    );
}
