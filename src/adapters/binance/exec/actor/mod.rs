//! WebSocket and REST together: effects become bytes, and bytes become messages. Correlation runs
//! on two paths, the request id and the stream event's CLIENT ORDER ID.

mod apply;
mod frame;
pub(super) mod handle;
mod inbound;
mod poll;
mod reconcile;
mod rest;
mod resync;

use std::time::Duration;

use futures_util::StreamExt;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::adapters::binance::rest::BinanceEnv;
use crate::adapters::binance::ws;
use crate::adapters::edge::{
    COMMAND_POLL, EdgeControl, EdgeDriver, EventFunnel, HOUSEKEEPING, OfflineStep, RECENT_TRADES,
    RECONCILE_INTERVAL,
};
use crate::adapters::exec::{
    EngineIdentity, ExecCore, ExecSession, ExecStop, SessionOutcome, SessionSetup,
};
use crate::adapters::socket::{Reader, Writer};
use crate::ids::{ClientOrderId, InstrumentId, TradeId, VenueOrderId};
use crate::msg::exec::{CancelReason, ExecLaneItem};
use crate::registry::AssetDictionary;
use crate::time::{DurationUs, TsUs};
use crate::{info, warn};

use super::{DecodeContext, RecvWindow, RequestSigner, SymbolTable};

use handle::BinanceExecAdapterSetup;
use rest::{RestChannels, RestJob, RestOutcome};

// Reconnects before the venue's 24-hour connection limit is reached.
const CONNECTION_LIFETIME: Duration = Duration::from_secs(23 * 60 * 60);
// Thirty minutes, expressed as a count of reconcile ticks so the resync check rides the
// existing tick stream rather than reading a clock of its own.
const CLOCK_RESYNC_TICKS: u32 = 360;
const RECENT_ORDERS: usize = 128;

#[derive(Default)]
struct DriverCounts {
    dropped_frames: u64,
    unroutable_frames: u64,
    unmatched_responses: u64,
    ignored_events: u64,
    untracked_events: u64,
    missed_fills: u64,
    unrecovered_fills: u64,
    cancels_skipped: u64,
}

/// The reconciler's route from a fill back to the order that placed it: `myTrades` names orders by
/// the venue's id and never carries a client id.
struct RecentOrder {
    venue_id: VenueOrderId,
    client_id: ClientOrderId,
}

struct TradeCursor {
    instrument: InstrumentId,
    from_id: Option<i64>,
    // The first answer adopts the cursor, so a page of history does not trigger a burst of
    // reconciliations.
    is_primed: bool,
}

struct Actor {
    session: ExecSession<EventFunnel>,
    symbols: SymbolTable,
    assets: AssetDictionary,
    identity: EngineIdentity,
    env: BinanceEnv,
    control: EdgeControl,
    recv_window: RecvWindow,
    // Clock skew beyond the receive window makes the venue refuse every signed request.
    loud_clock_skew: DurationUs,
    commands: rtrb::Consumer<ExecLaneItem>,
    signer: RequestSigner,
    // Travels as a WebSocket parameter, so it must never be logged or included in debug output.
    api_key: String,
    clock_offset: super::ClockOffset,
    rest_jobs: mpsc::Sender<RestJob>,
    rest_outcomes: mpsc::Receiver<RestOutcome>,
    rest_join: JoinHandle<()>,
    cursors: Vec<TradeCursor>,
    recent_orders: Vec<RecentOrder>,
    recent_trades: Vec<TradeId>,
    subscribe_failures: u32,
    is_rest_gone: bool,
    is_balance_snapshot_due: bool,
    reconcile_ticks: u32,
    counts: DriverCounts,
}

impl Actor {
    fn new(setup: BinanceExecAdapterSetup, stop: ExecStop, rt: &Handle) -> Self {
        let BinanceExecAdapterSetup {
            instruments,
            assets,
            credentials,
            rest,
            commands,
            producer,
            env,
            identity,
            recv_window,
            loud_clock_skew,
            edge,
        } = setup;
        let symbols = SymbolTable::new(
            instruments
                .iter()
                .map(|row| (row.instrument_id, row.venue_symbol.clone())),
        );
        let signer = RequestSigner::new(credentials.api_secret());
        let api_key = frame::api_key_text(credentials.api_key())
            .expect("the binance api key must be text — it rides the ws api as a json param");
        let clock_offset = rest.clock_offset();
        let channels = rest::spawn_rest_worker(rest, symbols.clone(), identity, rt);
        let RestChannels {
            jobs: rest_jobs,
            outcomes: rest_outcomes,
            join: rest_join,
        } = channels;
        // Sums each symbol's order limit; falls back to zero if any instrument lacks a
        // stamped limit.
        let mirror_capacity = instruments
            .iter()
            .try_fold(0usize, |total, row| {
                let limit = usize::try_from(row.max_num_orders?).ok()?;
                total.checked_add(limit)
            })
            .unwrap_or(0);
        Self {
            session: ExecSession::new(SessionSetup {
                venue: "binance",
                core: ExecCore::with_limits(edge.max_orders_per_side, mirror_capacity),
                instruments: instruments.iter().map(|row| row.instrument_id).collect(),
                events: EventFunnel::new(producer, edge.clock.clone()),
                fatal: edge.fatal.clone(),
            }),
            symbols,
            assets,
            identity,
            env,
            control: EdgeControl::new(&edge, stop),
            recv_window,
            loud_clock_skew,
            commands,
            signer,
            api_key,
            clock_offset,
            rest_jobs,
            rest_outcomes,
            rest_join,
            cursors: instruments
                .iter()
                .map(|row| TradeCursor {
                    instrument: row.instrument_id,
                    from_id: None,
                    is_primed: false,
                })
                .collect(),
            recent_orders: Vec::with_capacity(RECENT_ORDERS),
            recent_trades: Vec::with_capacity(RECENT_TRADES),
            subscribe_failures: 0,
            is_rest_gone: false,
            is_balance_snapshot_due: false,
            reconcile_ticks: 0,
            counts: DriverCounts::default(),
        }
    }

    fn decode_context(&self, received_ts_us: TsUs) -> DecodeContext<'_> {
        DecodeContext {
            symbols: &self.symbols,
            assets: &self.assets,
            identity: self.identity,
            received_ts_us,
        }
    }

    #[must_use]
    fn submit(&self, job: RestJob) -> bool {
        rest::submit(&self.rest_jobs, job)
    }
}

impl EdgeDriver for Actor {
    fn venue(&self) -> &'static str {
        "binance"
    }

    fn stream_url(&self) -> &str {
        ws::ws_api_url(self.env)
    }

    fn control(&mut self) -> &mut EdgeControl {
        &mut self.control
    }

    // The clock, the balances and the open-order reads are what seed the mirror.
    async fn start(&mut self) {
        let mut expected = self.pass_reads();
        if self.submit(RestJob::SyncClock) {
            expected += 1;
        }
        self.start_resync();
        let deadline = tokio::time::Instant::now() + self.control.sweep_deadline;
        while expected > 0 {
            match tokio::time::timeout_at(deadline, self.rest_outcomes.recv()).await {
                Ok(Some(outcome)) => {
                    self.on_rest_outcome(outcome, None).await;
                    expected -= 1;
                }
                Ok(None) => break,
                Err(_elapsed) => {
                    warn!(
                        "binance execution bootstrap incomplete after {}ms — starting with {expected} answers outstanding",
                        self.control.sweep_deadline.as_millis()
                    );
                    break;
                }
            }
        }
    }

    async fn serve(&mut self, mut writer: Writer, mut reader: Reader) -> SessionOutcome {
        let mut command_ticker = tokio::time::interval(COMMAND_POLL);
        let mut housekeeping = tokio::time::interval(HOUSEKEEPING);
        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        // The deadline is absolute so it cannot drift.
        let expiry = tokio::time::Instant::now() + CONNECTION_LIFETIME;

        let outgoing = self.session.connect();
        if let Some(ended) = self.dispatch(outgoing, Some(&mut writer)).await {
            self.session.lose_connection(self.control.clock.now());
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
                outcome = self.rest_outcomes.recv(), if !self.is_rest_gone => {
                    self.on_rest_answer(outcome, &mut writer).await
                }
                _ = tokio::time::sleep_until(expiry) => {
                    info!(
                        "binance execution reconnecting at {}h — the venue drops this connection at 24",
                        CONNECTION_LIFETIME.as_secs() / 3600
                    );
                    Some(SessionOutcome::Reconnect)
                }
            };
            if let Some(ended) = ended {
                self.session.lose_connection(self.control.clock.now());
                return ended;
            }
        }
    }

    // While offline, REST answers are still drained as they arrive rather than just sleeping
    // until the deadline.
    async fn while_offline(&mut self, deadline: tokio::time::Instant) -> OfflineStep {
        match tokio::time::timeout_at(deadline, self.rest_outcomes.recv()).await {
            // If the answer is a refused cancel, the retry rides on a later answer rather
            // than firing on its own timer.
            Ok(Some(outcome)) => {
                self.on_rest_outcome(outcome, None).await;
                OfflineStep::Serviced
            }
            Ok(None) => {
                self.is_rest_gone = true;
                OfflineStep::Ended
            }
            Err(_elapsed) => OfflineStep::Ended,
        }
    }

    // Retries are paced to avoid tripping the venue's IP ban, and the latch filters out
    // cancels that already have a sweep request in flight.
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
        warn!("binance execution pulling every order this run owns: {reason:?}");
        let outgoing = self.session.sweep(reason, None);
        self.dispatch(outgoing, writer).await;
    }

    fn report_stop(&self) {
        if let Some(exit) = &self.control.exit {
            info!(
                "binance execution stopping after a {:?} sweep — {} orders still mirrored, {} requests timed out",
                exit.reason,
                self.session.core.mirror().len(),
                self.session.inflight.timed_out()
            );
        }
        // Every count the driver keeps is reported here. A drop policy nobody can read the outcome
        // of is a variable, not a policy.
        let counts = &self.counts;
        info!(
            "binance execution counts: frames {} undecodable / {} unroutable, {} unmatched answers, {} events ignored ({} untracked), fills {} recovered / {} lost, {} cancels skipped",
            counts.dropped_frames,
            counts.unroutable_frames,
            counts.unmatched_responses,
            counts.ignored_events,
            counts.untracked_events,
            counts.missed_fills,
            counts.unrecovered_fills,
            counts.cancels_skipped
        );
    }

    fn abort_workers(&self) {
        self.rest_join.abort();
    }
}
