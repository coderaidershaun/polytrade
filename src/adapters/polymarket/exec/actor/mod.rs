//! What only this venue can answer: core decides, driver applies. The connect, serve and back-off
//! loop around it belongs to the shared edge chassis. The transport model is unique: no request
//! socket exists. All placements, cancels, and reads use HTTP across three lanes ([`rest`]). The
//! user stream receives events only and never replays after disconnect — reconnects trigger a full
//! state re-read. Polymarket mints the order id in the placement answer, making correlation
//! straightforward.

mod answer;
mod apply;
mod inbound;
mod poll;
mod reads;
mod rest;
mod resync;

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::adapters::edge::{
    COMMAND_POLL, EdgeControl, EdgeDriver, EventFunnel, HOUSEKEEPING, OfflineStep, RECENT_TRADES,
    RECONCILE_INTERVAL,
};
use crate::adapters::exec::{ExecCore, ExecSession, ExecStop, SessionOutcome, SessionSetup};
use crate::adapters::polymarket::rotation::WindowAssignment;
use crate::adapters::socket::{Reader, Writer};
use crate::ids::{AssetId, InstrumentId};
use crate::msg::exec::{AssetBalance, CancelReason, ExecLaneItem};
use crate::registry::InstrumentRow;
use crate::time::{DurationUs, TsUs};
use crate::{info, warn};

use super::codec::{OrderIndex, OrderSigner, SettlementWatermark, TokenTable, VenueAvailability};
use super::handle::PolymarketExecAdapterSetup;
use super::rest::ClobHttp;
use super::sign::l2::{ApiCredentials, RequestSigner};
use super::sign::order::SignatureType;

use super::binding::{Bindings, RETIRED_BINDINGS};
use super::correlate::{DelayedOrders, PendingFrames};
use rest::{Lane, LaneSetup, RestLanes, RestOutcome, Submitted, spawn_lanes};

pub(super) const USER_STREAM_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";

const PING_INTERVAL: Duration = Duration::from_secs(10);

// The venue cancels the book after 10s of silence, so a heartbeat goes out every 5s — this
// is the published cadence, not slack.
const HEARTBEAT_PERIOD: DurationUs = DurationUs::from_micros(5_000_000);

// A backstop that cancels any remaining orders as the assignment window closes. The hot
// engine already withdraws quotes on its own margin before that.
const CLOSE_MARGIN: DurationUs = DurationUs::from_micros(3_000_000);

// Sized to hold hot-engine orders plus any prior-run or adopted orders discovered during resync.
const ORDER_INDEX_CAPACITY: usize = 256;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

// Set below REQUEST_TIMEOUT so every placement gets an HTTP verdict before that expiry
// fires. This keeps the inflight timeout an exceptional path, never the normal case.
const REQUEST_HTTP_TIMEOUT: Duration = Duration::from_secs(4);

// The venue is temporarily unavailable; this is not a rejection of the order, which has no
// errors of its own.
#[derive(Debug, Clone, Copy)]
struct Parked {
    availability: VenueAvailability,
    until: TsUs,
}

#[derive(Default)]
struct DriverCounts {
    dropped_frames: u64,
    untracked_events: u64,
    unmatched_answers: u64,
    unmapped_left_alone: u64,
    adopted: u64,
    availability_refusals: u64,
    settlement_failures: u64,
    foreign_settlement_failures: u64,
}

// Once started, heartbeats commit forever: if they stop, the venue cancels this account's
// book as a crash-safety measure. The first one goes out as late as possible — just before
// the first order placement — and never stops after that.
#[derive(Debug)]
struct Heartbeat {
    id: Option<Box<str>>,
    is_started: bool,
    sent_at: TsUs,
}

impl Heartbeat {
    fn new() -> Self {
        Self {
            id: None,
            is_started: false,
            sent_at: TsUs::from_micros(0),
        }
    }
}

pub(super) struct Actor {
    session: ExecSession<EventFunnel>,
    tokens: TokenTable,
    orders: OrderIndex,
    bindings: Bindings,
    instruments: Vec<InstrumentRow>,
    signature_type: SignatureType,
    signer: OrderSigner,
    // The user stream uses this credential; no other channel does.
    credentials: ApiCredentials,
    control: EdgeControl,
    commands: rtrb::Consumer<ExecLaneItem>,
    lanes: RestLanes,
    outcomes: mpsc::Receiver<RestOutcome>,
    assignments: mpsc::Receiver<WindowAssignment>,
    pending: PendingFrames,
    delayed: DelayedOrders,
    heartbeat: Heartbeat,
    parked: Option<Parked>,
    /// Balances arrive one asset per call; the sweep is assembled here so only the last chunk arms
    /// the readiness gate.
    balances: Vec<AssetBalance>,
    balances_outstanding: usize,
    is_balance_sweep_readable: bool,
    /// A fill arrived mid-sweep; the account table needs a sweep issued after it.
    is_restatement_due: bool,
    /// Cancels sent for a previous run's orders. Readiness waits on them: quoting must not open
    /// over an order this run cannot name and did not choose.
    prior_run_cancels: usize,
    seen_trades: Vec<Box<str>>,
    /// How far this run has watched its own fills settle. Every balance chunk it forwards is
    /// stamped with this: the venue publishes no account clock, and its balance answers lag a fill,
    /// so settlement is the only evidence the money behind a reservation has actually moved.
    settled_through: SettlementWatermark,
    counts: DriverCounts,
    is_subscribed: bool,
}

impl Actor {
    pub(super) fn new(setup: PolymarketExecAdapterSetup, stop: ExecStop, runtime: &Handle) -> Self {
        let PolymarketExecAdapterSetup {
            instruments,
            credentials,
            key,
            wallet,
            commands,
            producer,
            assignments,
            venue_clock_offset,
            edge,
        } = setup;
        let request_signer = RequestSigner::new(&credentials, wallet.signer)
            .expect("the startup gate already built a request signer from these credentials");
        let http = ClobHttp::new(CONNECT_TIMEOUT, REQUEST_HTTP_TIMEOUT)
            .expect("the startup gate already built an http client with these settings");
        let (lanes, outcomes) = spawn_lanes(
            LaneSetup {
                http: Arc::new(http),
                signer: Arc::new(request_signer),
                clock: edge.clock.clone(),
                venue_clock_offset,
            },
            runtime,
        );
        let signer = OrderSigner::new(super::codec::OrderSignerSetup {
            key,
            maker: wallet.maker,
            signer: wallet.signer,
            signature_type: wallet.signature_type,
            api_key: credentials.api_key().to_owned(),
        });
        // A row whose venue order limit is unknown is skipped, not treated as zero. Folding the
        // whole sum to zero on one unknown makes the mirror reject the first venue order it sees,
        // which the resync escalates to a fatal.
        let mirror_capacity: usize = instruments
            .iter()
            .filter_map(|row| usize::try_from(row.max_num_orders?).ok())
            .sum();
        Self {
            session: ExecSession::new(SessionSetup {
                venue: "polymarket",
                core: ExecCore::with_limits(edge.max_orders_per_side, mirror_capacity),
                instruments: instruments.iter().map(|row| row.instrument_id).collect(),
                events: EventFunnel::new(producer, edge.clock.clone()),
                fatal: edge.fatal.clone(),
            }),
            tokens: TokenTable::with_retired_capacity(RETIRED_BINDINGS),
            orders: OrderIndex::with_capacity(ORDER_INDEX_CAPACITY),
            bindings: Bindings::default(),
            instruments,
            signature_type: wallet.signature_type,
            signer,
            credentials,
            control: EdgeControl::new(&edge, stop),
            commands,
            lanes,
            outcomes,
            assignments,
            pending: PendingFrames::new(),
            delayed: DelayedOrders::default(),
            heartbeat: Heartbeat::new(),
            parked: None,
            balances: Vec::new(),
            balances_outstanding: 0,
            is_balance_sweep_readable: true,
            is_restatement_due: false,
            prior_run_cancels: 0,
            seen_trades: Vec::with_capacity(RECENT_TRADES),
            settled_through: SettlementWatermark::NONE,
            counts: DriverCounts::default(),
            is_subscribed: false,
        }
    }

    fn base_asset(&self, instrument: InstrumentId) -> AssetId {
        self.instruments
            .iter()
            .find(|row| row.instrument_id == instrument)
            .map_or(AssetId::UNKNOWN, |row| row.base_asset)
    }

    /// Every polymarket row shares one collateral asset, so the first row's is the account's.
    fn quote_asset(&self) -> AssetId {
        self.instruments
            .first()
            .map_or(AssetId::UNKNOWN, |row| row.quote_asset)
    }

    fn submit(&self, lane: Lane, job: rest::RestJob) -> Submitted {
        self.lanes.submit(lane, job)
    }

    /// Sends the text "PING"; this vendor answers with text "PONG" rather than a
    /// WebSocket-level control pong, which is ignored.
    async fn send_ping(&mut self, writer: &mut Writer) -> Option<SessionOutcome> {
        match writer.send(Message::Text("PING".into())).await {
            Ok(()) => None,
            Err(error) => {
                warn!("polymarket execution could not send its keepalive: {error} — reconnecting");
                Some(SessionOutcome::Reconnect)
            }
        }
    }

    fn on_disconnected(&mut self) {
        self.is_subscribed = false;
        // Held frames would be explained against a stale index; resync rebuilds it.
        let held = self.pending.drain().len();
        if held > 0 {
            warn!(
                "polymarket execution dropped {held} stream frames held for correlation — the resync is the record now"
            );
        }
        self.session.lose_connection(self.control.clock.now());
    }
}

impl EdgeDriver for Actor {
    fn venue(&self) -> &'static str {
        "polymarket"
    }

    fn stream_url(&self) -> &str {
        USER_STREAM_URL
    }

    fn control(&mut self) -> &mut EdgeControl {
        &mut self.control
    }

    async fn serve(&mut self, mut writer: Writer, mut reader: Reader) -> SessionOutcome {
        let mut command_ticker = tokio::time::interval(COMMAND_POLL);
        let mut housekeeping = tokio::time::interval(HOUSEKEEPING);
        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        let mut ping = tokio::time::interval(PING_INTERVAL);

        self.is_subscribed = false;
        let outgoing = self.session.connect();
        if let Some(ended) = self.dispatch(outgoing, Some(&mut writer)).await {
            self.on_disconnected();
            return ended;
        }

        loop {
            let ended = tokio::select! {
                frame = reader.next() => self.on_frame(frame, &mut writer).await,
                _ = command_ticker.tick() => {
                    let outgoing = self.session.core.drain_commands(&mut self.commands);
                    self.dispatch(outgoing, Some(&mut writer)).await
                }
                _ = housekeeping.tick() => self.on_housekeeping(&mut writer).await,
                _ = reconcile.tick() => self.on_reconcile_tick(),
                _ = ping.tick() => self.send_ping(&mut writer).await,
                assignment = self.assignments.recv() => self.on_assignment(assignment),
                outcome = self.outcomes.recv() => self.on_rest_answer(outcome, &mut writer).await,
            };
            if let Some(ended) = ended {
                self.on_disconnected();
                return ended;
            }
        }
    }

    // While offline, HTTP answers are read as they arrive rather than just sleeping through them.
    async fn while_offline(&mut self, deadline: tokio::time::Instant) -> OfflineStep {
        match tokio::time::timeout_at(deadline, self.outcomes.recv()).await {
            Ok(Some(outcome)) => {
                self.on_rest_outcome(outcome, None).await;
                OfflineStep::Serviced
            }
            Ok(None) | Err(_) => OfflineStep::Ended,
        }
    }

    // Retries are paced to conserve the cancel budget, and the core tracks one outstanding
    // retry per order.
    async fn retry_sweep(&mut self, writer: Option<&mut Writer>) {
        let Some(reason) = self.control.claim_sweep_retry() else {
            return;
        };
        let outgoing = self.session.sweep(reason, None);
        self.dispatch(outgoing, writer).await;
    }

    async fn plan_exit(&mut self, reason: CancelReason, writer: Option<&mut Writer>) {
        if !self.control.open_exit(reason) {
            return;
        }
        warn!("polymarket execution pulling every order this run owns: {reason:?}");
        let outgoing = self.session.sweep(reason, None);
        self.dispatch(outgoing, writer).await;
    }

    fn report_stop(&self) {
        if let Some(exit) = &self.control.exit {
            info!(
                "polymarket execution stopping after a {:?} sweep — {} orders still mirrored, {} requests timed out, {} frames abandoned unmapped",
                exit.reason,
                self.session.core.mirror().len(),
                self.session.inflight.timed_out(),
                self.pending.abandoned()
            );
        }
        let counts = &self.counts;
        let is_notable = counts.adopted > 0
            || counts.unmapped_left_alone > 0
            || counts.dropped_frames > 0
            || counts.untracked_events > 0
            || counts.unmatched_answers > 0
            || counts.availability_refusals > 0
            || counts.settlement_failures > 0
            || counts.foreign_settlement_failures > 0
            || self.bindings.refused() > 0;
        if is_notable {
            info!(
                "polymarket execution correlation summary: {} orders adopted, {} unmapped left alone, {} cancels withheld for the taker hold, {} placements refused on venue state, {} bindings displaced before they completed, {} stream frames dropped, {} untracked-token events, {} answers matched no request, {} of this run's fills failed to settle, {} other accounts' did",
                counts.adopted,
                counts.unmapped_left_alone,
                self.delayed.withholds(),
                counts.availability_refusals,
                self.bindings.refused(),
                counts.dropped_frames,
                counts.untracked_events,
                counts.unmatched_answers,
                counts.settlement_failures,
                counts.foreign_settlement_failures,
            );
        }
    }

    fn abort_workers(&self) {
        self.lanes.abort();
    }
}
