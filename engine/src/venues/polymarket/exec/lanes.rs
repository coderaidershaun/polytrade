//! The request half of the venue: placements, cancels, reads and the heartbeat all go by HTTP, on
//! three lanes, so that a slow request on one lane never delays the others.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::msg::time::{DurationUs, EngineClock};
use crate::venues::edge::http::TransportError;
use crate::warn;

use super::codec::EncodedRequest;
use super::rest::{ClobHttp, ClobResponse};
use super::sign::l2::RequestSigner;

/// Backpressure on a stalled venue. A full lane means something upstream is wrong.
pub(crate) const LANE_CAPACITY: usize = 64;

/// Three lanes, not one. A marketable order is held 250 ms at the venue and `POST /order` blocks
/// for that hold, so a cancel queued behind a placement would wait out a delay that the venue set
/// for another order; the venue also meters the two separately. The heartbeat gets the third lane
/// because ten seconds of silence cancel the whole book, so it must not wait behind a read that
/// runs into its timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Place,
    Control,
    Heartbeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Auth {
    Public,
    Signed,
}

/// A refused job is a DROPPED request, never a delayed one, so every caller has to say what that
/// costs it — hence the `must_use`. Silence here is how an unclaimed allowance refresh and an
/// unsent heartbeat both went unnoticed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Submitted {
    Queued,
    LaneFull,
}

impl Submitted {
    pub fn is_queued(self) -> bool {
        self == Submitted::Queued
    }
}

/// One request, and why it was asked for: the purpose comes back with the answer.
#[derive(Debug)]
pub struct RestJob<P> {
    pub purpose: P,
    pub request: EncodedRequest,
    auth: Auth,
}

impl<P> RestJob<P> {
    pub fn signed(purpose: P, request: EncodedRequest) -> Self {
        Self {
            purpose,
            request,
            auth: Auth::Signed,
        }
    }

    pub fn public(purpose: P, request: EncodedRequest) -> Self {
        Self {
            purpose,
            request,
            auth: Auth::Public,
        }
    }
}

#[derive(Debug)]
pub struct RestOutcome<P> {
    pub purpose: P,
    pub answer: Result<ClobResponse, TransportError>,
}

pub struct RestLanes<P> {
    place: mpsc::Sender<RestJob<P>>,
    control: mpsc::Sender<RestJob<P>>,
    heartbeat: mpsc::Sender<RestJob<P>>,
    joins: Vec<JoinHandle<()>>,
}

impl<P> RestLanes<P> {
    /// A full lane drops the job, which would skip a reconciliation with no sign, so the refusal is
    /// logged and returned.
    pub fn submit(&self, lane: Lane, job: RestJob<P>) -> Submitted {
        let sender = match lane {
            Lane::Place => &self.place,
            Lane::Control => &self.control,
            Lane::Heartbeat => &self.heartbeat,
        };
        match sender.try_send(job) {
            Ok(()) => Submitted::Queued,
            Err(error) => {
                report_full_lane(lane, &error.into_inner());
                Submitted::LaneFull
            }
        }
    }

    pub(crate) fn abort(&self) {
        for join in &self.joins {
            join.abort();
        }
    }
}

pub struct LaneSetup {
    pub http: Arc<ClobHttp>,
    pub signer: Arc<RequestSigner>,
    pub clock: EngineClock,
    pub venue_clock_offset: DurationUs,
}

/// One worker for each lane, on `runtime`. Each outcome reaches the one receiver when its worker
/// has the answer, so the outcomes of two lanes arrive in the order the venue answered them.
pub fn spawn_lanes<P: Send + 'static>(
    setup: LaneSetup,
    runtime: &tokio::runtime::Handle,
) -> (RestLanes<P>, mpsc::Receiver<RestOutcome<P>>) {
    let (outcomes_tx, outcomes_rx) = mpsc::channel(LANE_CAPACITY * 3);
    let mut joins = Vec::with_capacity(3);
    let mut lane = |tag: &'static str| {
        let (jobs_tx, jobs_rx) = mpsc::channel(LANE_CAPACITY);
        let worker = Worker {
            http: Arc::clone(&setup.http),
            signer: Arc::clone(&setup.signer),
            clock: setup.clock.clone(),
            venue_clock_offset: setup.venue_clock_offset,
        };
        let body = worker.run(jobs_rx, outcomes_tx.clone());
        joins.push(runtime.spawn(crate::log::tag_task(tag, body)));
        jobs_tx
    };
    let lanes = RestLanes {
        place: lane("polymarket-exec-place"),
        control: lane("polymarket-exec-control"),
        heartbeat: lane("polymarket-exec-heartbeat"),
        joins,
    };
    (lanes, outcomes_rx)
}

struct Worker {
    http: Arc<ClobHttp>,
    signer: Arc<RequestSigner>,
    clock: EngineClock,
    /// Added to the local clock so `POLY_TIMESTAMP` is stamped in the venue's own seconds.
    venue_clock_offset: DurationUs,
}

impl Worker {
    async fn run<P>(
        self,
        mut jobs: mpsc::Receiver<RestJob<P>>,
        outcomes: mpsc::Sender<RestOutcome<P>>,
    ) {
        while let Some(job) = jobs.recv().await {
            let RestJob {
                purpose,
                request,
                auth,
            } = job;
            let answer = self.execute(&request, auth).await;
            let outcome = RestOutcome { purpose, answer };
            if outcomes.send(outcome).await.is_err() {
                return;
            }
        }
    }

    /// The L2 headers are signed here rather than where the job is made: the timestamp that they
    /// carry is the send stamp, and this is the last moment before the bytes leave.
    async fn execute(
        &self,
        request: &EncodedRequest,
        auth: Auth,
    ) -> Result<ClobResponse, TransportError> {
        match auth {
            Auth::Public => self.http.send_public(request).await,
            Auth::Signed => {
                self.http
                    .send_signed(&self.signer, request, self.venue_seconds())
                    .await
            }
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    fn venue_seconds(&self) -> i64 {
        (self.clock.now() + self.venue_clock_offset).micros() / 1_000_000
    }
}

#[cold]
fn report_full_lane<P>(lane: Lane, job: &RestJob<P>) {
    warn!(
        "polymarket execution {lane:?} lane full at {LANE_CAPACITY} — dropped {} {}",
        job.request.method.as_str(),
        job.request.path
    );
}
