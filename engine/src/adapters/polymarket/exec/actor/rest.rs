//! The venue's request half. Polymarket has no request socket: placements, cancels and reads are
//! all HTTP, and the user stream only listens.
//!
//! Three lanes, not one. A marketable order is held 250 ms at the venue and `POST /order` BLOCKS for
//! that hold, so a cancel queued behind a place would inherit a delay the venue imposed on somebody
//! else's risk; the two buckets are metered separately at the venue too. The heartbeat gets the
//! third lane because going quiet for ten seconds cancels the whole book — it must not queue behind
//! a read that is waiting out a timeout.
//!
//! L2 headers are signed HERE rather than at the call site: the timestamp they carry is the send
//! stamp, and this is the last moment before the bytes leave.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::adapters::exec::{ExecRequest, RequestId};
use crate::adapters::http::TransportError;
use crate::ids::{AssetId, ClientOrderId, InstrumentId};
use crate::time::{DurationUs, EngineClock};
use crate::warn;

use super::super::codec::{EncodedRequest, PlaceRequestContext};
use super::super::rest::{ClobHttp, ClobResponse};
use super::super::sign::l2::RequestSigner;

/// Backpressure on a stalled venue. A full lane means something upstream is wrong.
pub(super) const LANE_CAPACITY: usize = 64;

/// Who asked for an open-orders page, and so whether its answer settles a resync read.
///
/// The hot side's reconcile counter and the resync pass counter both start at zero and step on
/// unrelated triggers. Compared as bare numbers they cross, and a crossing retires a pass read that
/// never landed — the pass then finishes on two of its three reads and opens quoting having never
/// seen the balance sweep. Naming the asker is what makes that impossible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenOrdersRead {
    /// Account-wide, one of the reads a resync pass is waiting on.
    Pass { resync_seq: u64 },
    /// One instrument's page, asked for by the hot side's silence detector. `recon_seq` belongs to
    /// the hot side and is the only sequence this variant may carry.
    Instrument {
        instrument: InstrumentId,
        recon_seq: u64,
    },
    /// The check a freshly bound token gets. Part of no pass.
    FreshBinding { instrument: InstrumentId },
}

impl OpenOrdersRead {
    /// The instrument a follow-on page must be scoped to. `None` is the account-wide read.
    pub(super) fn instrument(self) -> Option<InstrumentId> {
        match self {
            OpenOrdersRead::Pass { .. } => None,
            OpenOrdersRead::Instrument { instrument, .. }
            | OpenOrdersRead::FreshBinding { instrument } => Some(instrument),
        }
    }
}

/// Why the call was made, and everything needed to read its answer.
#[derive(Debug, Clone)]
pub(super) enum RestPurpose {
    /// A request the execution core minted. `place` carries what was asked, because this venue
    /// echoes neither the client id nor the price back on the placement path.
    Core {
        request_id: RequestId,
        request: ExecRequest,
        recon_seq: u64,
        place: Option<PlaceRequestContext>,
    },
    /// One page of open orders, and how far into the page walk it is.
    OpenOrders {
        read: OpenOrdersRead,
        page: u32,
        /// Orders the earlier pages of THIS walk already named. Absence from the whole read is what
        /// makes an order missing, and a resync walk can be in flight beside a hot-side one, so the
        /// accumulation rides the walk rather than sitting on the driver where they would mix.
        seen: Vec<ClientOrderId>,
    },
    /// One asset's balance. The venue has no multi-asset read, so a sweep is N of these.
    Balance {
        asset: AssetId,
        /// `None` for the post-fill restatement, which answers the account table and settles no
        /// admission gate.
        resync_seq: Option<u64>,
    },
    Trades {
        resync_seq: Option<u64>,
        page: u32,
    },
    /// A previous run's order, addressed by venue id because this run can mint no client id for it.
    PriorRunCancel {
        venue_order_id: Box<str>,
    },
    /// Rotation binding: tick and minimum size for the whole market.
    Market {
        condition_id: Arc<str>,
    },
    /// Rotation binding: which exchange contract this token's orders are signed against.
    NegRisk {
        condition_id: Arc<str>,
        instrument: InstrumentId,
    },
    /// The CLOB's allowance cache for one token, warmed before that token's first sell.
    Allowance {
        token_id: Box<str>,
    },
    Heartbeat,
    /// Every resting order on one token, in one call — the rotation sweep.
    MarketCancel {
        instrument: InstrumentId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Lane {
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
pub(super) enum Submitted {
    Queued,
    LaneFull,
}

impl Submitted {
    pub(super) fn is_queued(self) -> bool {
        self == Submitted::Queued
    }
}

#[derive(Debug)]
pub(super) struct RestJob {
    pub purpose: RestPurpose,
    pub request: EncodedRequest,
    auth: Auth,
}

impl RestJob {
    pub(super) fn signed(purpose: RestPurpose, request: EncodedRequest) -> Self {
        Self {
            purpose,
            request,
            auth: Auth::Signed,
        }
    }

    pub(super) fn public(purpose: RestPurpose, request: EncodedRequest) -> Self {
        Self {
            purpose,
            request,
            auth: Auth::Public,
        }
    }
}

#[derive(Debug)]
pub(super) struct RestOutcome {
    pub purpose: RestPurpose,
    pub answer: Result<ClobResponse, TransportError>,
}

pub(super) struct RestLanes {
    place: mpsc::Sender<RestJob>,
    control: mpsc::Sender<RestJob>,
    heartbeat: mpsc::Sender<RestJob>,
    joins: Vec<JoinHandle<()>>,
}

impl RestLanes {
    /// Dropped job = a silent reconciliation skip, so refusal is reported and returned.
    pub(super) fn submit(&self, lane: Lane, job: RestJob) -> Submitted {
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

    pub(super) fn abort(&self) {
        for join in &self.joins {
            join.abort();
        }
    }
}

pub(super) struct LaneSetup {
    pub http: Arc<ClobHttp>,
    pub signer: Arc<RequestSigner>,
    pub clock: EngineClock,
    pub venue_clock_offset: DurationUs,
}

pub(super) fn spawn_lanes(
    setup: LaneSetup,
    runtime: &tokio::runtime::Handle,
) -> (RestLanes, mpsc::Receiver<RestOutcome>) {
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
    async fn run(self, mut jobs: mpsc::Receiver<RestJob>, outcomes: mpsc::Sender<RestOutcome>) {
        while let Some(job) = jobs.recv().await {
            let answer = self.execute(&job).await;
            let outcome = RestOutcome {
                purpose: job.purpose,
                answer,
            };
            if outcomes.send(outcome).await.is_err() {
                return;
            }
        }
    }

    async fn execute(&self, job: &RestJob) -> Result<ClobResponse, TransportError> {
        match job.auth {
            Auth::Public => self.http.send_public(&job.request).await,
            Auth::Signed => {
                self.http
                    .send_signed(&self.signer, &job.request, self.venue_seconds())
                    .await
            }
        }
    }

    fn venue_seconds(&self) -> i64 {
        (self.clock.now() + self.venue_clock_offset).micros() / 1_000_000
    }
}

#[cold]
fn report_full_lane(lane: Lane, job: &RestJob) {
    warn!(
        "polymarket execution {lane:?} lane full at {LANE_CAPACITY} — dropped {} {}",
        job.request.method.as_str(),
        job.request.path
    );
}
