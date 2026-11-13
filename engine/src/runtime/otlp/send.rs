//! The exporter thread: each second it takes what waits in the lanes and posts it, one time. A
//! request that fails is dropped and counted, never sent again.

use std::fmt;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;

use reqwest::Client;
use reqwest::header::CONTENT_TYPE;
use tokio::runtime::Runtime;

use crate::msg::failure::FailureClass;
use crate::runtime::metrics::RollupRow;
use crate::{error, info, warn_repeating};

use super::lanes::OtlpLanes;
use super::wire::MetricsRequest;
use super::{REQUEST_TIMEOUT, SEND_INTERVAL};

/// The running exporter. Its stop ends the thread and returns what it did.
pub struct OtlpExporter {
    pub(super) stop: Sender<()>,
    pub(super) join: JoinHandle<OtlpReport>,
}

/// What the exporter did over the run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtlpReport {
    /// Log lines and rollup rows of the requests that a collector took with a 2xx answer.
    pub lines_sent: u64,
    pub rows_sent: u64,
    /// Log lines and rollup rows of the requests that failed. Each was dropped, never sent again.
    pub lines_failed: u64,
    pub rows_failed: u64,
    /// Failed requests, by cause: a status that is not 2xx, a redirect included; no connection;
    /// no answer within the request limit; a body that could not be written.
    pub refused: u64,
    pub unreached: u64,
    pub unanswered: u64,
    pub unwritten: u64,
    /// Log lines and rollup rows that a full lane dropped before the exporter read them.
    pub lines_dropped: u64,
    pub rows_dropped: u64,
}

impl fmt::Display for OtlpReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "otlp export sent {} line(s) and {} row(s); failed requests dropped {} line(s) and {} \
             row(s) ({} refused, {} unreached, {} unanswered, {} unwritten); full lanes dropped {} \
             line(s) and {} row(s)",
            self.lines_sent,
            self.rows_sent,
            self.lines_failed,
            self.rows_failed,
            self.refused,
            self.unreached,
            self.unanswered,
            self.unwritten,
            self.lines_dropped,
            self.rows_dropped
        )
    }
}

/// The body of the exporter thread: a pass each second, and one last pass once the stop sender
/// is gone. It is the one place that acts on a failed send.
pub(super) fn run(mut lanes: OtlpLanes, stopped: &Receiver<()>) -> OtlpReport {
    crate::log::register_thread("otlp");
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(cause) => {
            error!("otlp export is off for this run: its runtime could not be built: {cause}");
            return OtlpReport::default();
        }
    };
    let client = match once_only_client() {
        Ok(client) => client,
        Err(cause) => {
            error!("otlp export is off for this run: its http client could not be built: {cause}");
            return OtlpReport::default();
        }
    };
    let collector = lanes.plan.collector;
    let mut poster = Poster {
        runtime,
        client,
        metrics_url: format!("http://{collector}/v1/metrics"),
        service: lanes.plan.service.clone(),
        report: OtlpReport::default(),
        failed_requests: 0,
    };
    info!(
        "otlp export to {collector} as {}, every 1 s",
        poster.service
    );
    loop {
        let wake = stopped.recv_timeout(SEND_INTERVAL);
        poster.pass(&mut lanes);
        if !matches!(wake, Err(RecvTimeoutError::Timeout)) {
            break;
        }
    }
    poster.report.rows_dropped = lanes.rows.dropped();
    poster.report
}

/// A client that sends each request at most one time, and only to the collector. Each setting
/// closes one way that a library sends a request again or sends it elsewhere: a proxy from the
/// environment, a request sent again on a reused connection, a retry, and a redirect followed.
fn once_only_client() -> reqwest::Result<Client> {
    Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .timeout(REQUEST_TIMEOUT)
        .build()
}

/// Why a request of the export failed. Each one is a reject: its records are dropped and counted,
/// and the request is never sent again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum SendFailure {
    #[error("the collector answered with status {status}")]
    Refused { status: u16 },
    #[error("no connection to the collector")]
    Unreached,
    #[error("no answer from the collector within 500 ms, or the connection broke")]
    Unanswered,
    #[error("the body could not be written as JSON")]
    Unwritten,
}

impl SendFailure {
    const fn failure_class(self) -> FailureClass {
        match self {
            SendFailure::Refused { .. }
            | SendFailure::Unreached
            | SendFailure::Unanswered
            | SendFailure::Unwritten => FailureClass::Reject,
        }
    }

    /// A connection that never opened sent no byte. Past it, the outcome is unknown.
    fn of(error: &reqwest::Error) -> SendFailure {
        if error.is_connect() { SendFailure::Unreached } else { SendFailure::Unanswered }
    }
}

struct Poster {
    runtime: Runtime,
    client: Client,
    metrics_url: String,
    service: Box<str>,
    report: OtlpReport,
    failed_requests: u64,
}

impl Poster {
    fn pass(&mut self, lanes: &mut OtlpLanes) {
        let rows: Vec<RollupRow> = std::iter::from_fn(|| lanes.rows.pop()).collect();
        if rows.is_empty() {
            return;
        }
        let count = rows.len() as u64;
        let outcome = match serde_json::to_vec(&MetricsRequest::of(&self.service, &rows)) {
            Ok(body) => self
                .runtime
                .block_on(post(&self.client, &self.metrics_url, body)),
            Err(_) => Err(SendFailure::Unwritten),
        };
        match outcome {
            Ok(()) => self.report.rows_sent += count,
            Err(failure) => self.reject(count, failure),
        }
    }

    fn reject(&mut self, rows: u64, failure: SendFailure) {
        match failure.failure_class() {
            FailureClass::Reject => {}
            FailureClass::Halt(_) | FailureClass::Reset | FailureClass::Retry => {
                unreachable!("each failure of the export is a reject, never {failure:?}")
            }
        }
        self.report.rows_failed += rows;
        match failure {
            SendFailure::Refused { .. } => self.report.refused += 1,
            SendFailure::Unreached => self.report.unreached += 1,
            SendFailure::Unanswered => self.report.unanswered += 1,
            SendFailure::Unwritten => self.report.unwritten += 1,
        }
        warn_repeating!(
            self.failed_requests,
            "otlp export dropped {rows} row(s) for {}: {failure}; the request is not sent again",
            self.metrics_url
        );
    }
}

/// One request, sent one time. Only the status of the answer is read.
async fn post(client: &Client, url: &str, body: Vec<u8>) -> Result<(), SendFailure> {
    let response = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| SendFailure::of(&error))?;
    let status = response.status();
    if !status.is_success() {
        return Err(SendFailure::Refused {
            status: status.as_u16(),
        });
    }
    Ok(())
}
