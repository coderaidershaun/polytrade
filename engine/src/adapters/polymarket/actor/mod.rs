//! Driver actor: socket/tick/ping/REST shell. Single-owner loop → monotone input. 5m rotation: T-60s subscribe, 404 probe teardown.

mod core;
mod effects;
mod leg;

pub use core::{DriverEffect, PolyDriverCore, SlotLegs};

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time::Interval;
use tokio_tungstenite::tungstenite::{Error as ProtocolError, Message};

use crate::adapters::backoff::BackoffCaps;
use crate::adapters::lane::{Flow, Lane, LaneSpec, SessionEnd, run_lane};
use crate::adapters::rest_quiet::RestQuiet;
use crate::config::PolySeries;
use crate::hot::spawn::QueueProducer;
use crate::link::RunState;
use crate::msg::persist::RotationRow;
use crate::registry::{InstrumentRow, ProducerGroup};
use crate::shutdown::{FatalSignal, RunStateCell, TaskHandle};
use crate::time::{EngineClock, TsUs};
use crate::{error, warn};

use super::discovery::{PolySchedule, Slot};
use super::parse::{is_frameless, parse_market_frame};
use super::rest::PolyRest;
use super::rotation::WindowAssignment;
use super::ws;
use crate::adapters::socket::{Socket, Writer};

use effects::{CoreEvent, Pending};

const TICK_INTERVAL: Duration = Duration::from_secs(1);
const RESULTS_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct PolymarketAdapterContext {
    pub clock: EngineClock,
    pub fatal: FatalSignal,
    pub run_state: RunStateCell,
    pub backoff: BackoffCaps,
    pub rotations_tx: mpsc::Sender<RotationRow>,
    /// Where the execution edge learns which token each leg is trading this window. `None` when no
    /// edge is armed.
    pub window_assignments: Option<mpsc::Sender<WindowAssignment>>,
}

pub struct PolymarketAdapter;

impl PolymarketAdapter {
    pub fn spawn(
        group: &ProducerGroup,
        series: PolySeries,
        instruments: &[InstrumentRow],
        producer: QueueProducer,
        context: PolymarketAdapterContext,
        rt: &Handle,
    ) -> TaskHandle {
        let (actor, results_rx) = Actor::new(group, series, instruments, producer, context);
        let join = rt.spawn(crate::log::tag_task("polymarket", actor.run(results_rx)));
        TaskHandle::new(join, "polymarket adapter")
    }
}

struct Actor {
    core: PolyDriverCore,
    schedule: PolySchedule,
    clock: EngineClock,
    fatal: FatalSignal,
    run_state: RunStateCell,
    backoff: BackoffCaps,
    producer: QueueProducer,
    rest: Arc<PolyRest>,
    rest_quiet: RestQuiet,
    results_tx: mpsc::Sender<CoreEvent>,
    rotations_tx: mpsc::Sender<RotationRow>,
    window_assignments: Option<mpsc::Sender<WindowAssignment>>,
    dropped_rotations: u64,
    dropped_bindings: u64,
    inflight: Vec<Pending>,
    dropped_frames: u64,
    suppressed_rest: u64,
    divergences: u64,
    label: String,
}

impl Actor {
    fn new(
        group: &ProducerGroup,
        series: PolySeries,
        instruments: &[InstrumentRow],
        producer: QueueProducer,
        context: PolymarketAdapterContext,
    ) -> (Self, mpsc::Receiver<CoreEvent>) {
        let members: Vec<&InstrumentRow> = instruments
            .iter()
            .filter(|row| group.instruments.contains(&row.instrument_id))
            .collect();
        let slot_legs = slot_legs_from_rows(series, &members);
        let schedule = PolySchedule::for_series(series);
        let (results_tx, results_rx) = mpsc::channel(RESULTS_CAPACITY);
        let rest = PolyRest::new(series).expect("build polymarket rest client at init");
        let actor = Self {
            core: PolyDriverCore::new(slot_legs, schedule),
            schedule,
            clock: context.clock,
            fatal: context.fatal,
            run_state: context.run_state,
            backoff: context.backoff,
            producer,
            rest: Arc::new(rest),
            rest_quiet: RestQuiet::new(),
            results_tx,
            rotations_tx: context.rotations_tx,
            window_assignments: context.window_assignments,
            dropped_rotations: 0,
            dropped_bindings: 0,
            inflight: Vec::new(),
            dropped_frames: 0,
            suppressed_rest: 0,
            divergences: 0,
            label: series.as_str().to_owned(),
        };
        (actor, results_rx)
    }

    async fn run(mut self, results_rx: mpsc::Receiver<CoreEvent>) {
        // The timers are made here, inside the spawned task, because a tokio timer panics when it
        // is made outside a runtime and `spawn` runs on the caller's thread.
        let ticks = tokio::time::interval(TICK_INTERVAL);
        let ping = tokio::time::interval(ws::PING_INTERVAL);
        self.bootstrap().await;
        let spec = LaneSpec {
            name: format!("polymarket adapter {}", self.label),
            url: ws::MARKET_URL.to_owned(),
            fatal: self.fatal.clone(),
            run_state: self.run_state.clone(),
            backoff: self.backoff,
        };
        let lane = MarketLane {
            actor: self,
            ticks,
            ping,
            results_rx,
        };
        run_lane(spec, lane).await;
    }

    /// Resolve current window for initial subscribe set. WS ops deferred to connect; Rotation/BookReset land now.
    async fn bootstrap(&mut self) {
        let now = self.clock.now();
        let current = self.schedule.current_window(now);
        match self.rest.resolve_slug(current.window_start_ts_us).await {
            Ok(market) => {
                let assignment = self
                    .core
                    .assignment_from_market(current.window_start_ts_us, &market);
                let effects =
                    collect_effects(|emit| self.core.on_window_resolved(now, assignment, emit));
                self.execute_offline(effects);
            }
            Err(error) => warn!(
                "polymarket adapter {} startup resolve of window {} failed: {error} — scheduler retries",
                self.label,
                current.window_start_ts_us.micros()
            ),
        }
        let now = self.clock.now();
        let effects = collect_effects(|emit| self.core.on_tick(now, emit));
        self.execute_offline(effects);
    }

    /// Re-baseline live legs, resend plain subscribe. False on socket write failure → backoff+reconnect.
    async fn prime_connection(&mut self, writer: &mut Writer) -> bool {
        let now = self.clock.now();
        let effects = collect_effects(|emit| self.core.on_reconnect(now, emit));
        if self.execute(effects, writer).await == Flow::Reconnect {
            return false;
        }
        let tokens = self.core.live_tokens();
        if tokens.is_empty() {
            return true;
        }
        let text = ws::subscribe_message(&tokens);
        if writer.send(Message::Text(text.into())).await.is_err() {
            warn!(
                "polymarket adapter {} initial subscribe failed — reconnecting",
                self.label
            );
            return false;
        }
        true
    }

    async fn on_socket_message(
        &mut self,
        frame: Option<Result<Message, ProtocolError>>,
        writer: &mut Writer,
    ) -> Flow {
        match frame {
            Some(Ok(Message::Text(text))) => {
                let now = self.clock.now();
                self.on_text(text.as_str(), now, writer).await
            }
            Some(Ok(Message::Close(_))) | None => Flow::Reconnect,
            Some(Ok(_)) => Flow::Continue,
            Some(Err(error)) => {
                warn!("polymarket adapter {} stream: {error}", self.label);
                Flow::Reconnect
            }
        }
    }

    async fn on_text(&mut self, text: &str, now: TsUs, writer: &mut Writer) -> Flow {
        if is_frameless(text) {
            return Flow::Continue;
        }
        match parse_market_frame(text, now) {
            Ok(frame) => {
                let effects = collect_effects(|emit| self.core.on_frame(now, &frame, emit));
                self.execute(effects, writer).await
            }
            Err(error) if error.is_fatal() => {
                error!("polymarket adapter {} fatal parse: {error}", self.label);
                self.fatal
                    .trip(format!("polymarket {}: {error}", self.label));
                Flow::Reconnect
            }
            Err(error) => {
                self.on_parse_error(error);
                Flow::Continue
            }
        }
    }

    async fn drive_tick(&mut self, now: TsUs, writer: &mut Writer) -> Flow {
        let effects = collect_effects(|emit| self.core.on_tick(now, emit));
        self.execute(effects, writer).await
    }

    async fn send_ping(&mut self, writer: &mut Writer) -> Flow {
        if writer.send(Message::Text(ws::PING.into())).await.is_err() {
            Flow::Reconnect
        } else {
            Flow::Continue
        }
    }

    async fn on_core_event(&mut self, event: Option<CoreEvent>, writer: &mut Writer) -> Flow {
        let Some(event) = event else {
            return Flow::Continue;
        };
        match event {
            CoreEvent::ResolveOk { start, market } => {
                self.clear_inflight(&Pending::Resolve(start));
                let now = self.clock.now();
                let assignment = self.core.assignment_from_market(start, &market);
                let effects =
                    collect_effects(|emit| self.core.on_window_resolved(now, assignment, emit));
                self.execute(effects, writer).await
            }
            CoreEvent::ResolveErr { start } => {
                self.clear_inflight(&Pending::Resolve(start));
                warn!(
                    "polymarket adapter {} resolve of window {} failed — scheduler retries",
                    self.label,
                    start.micros()
                );
                Flow::Continue
            }
            CoreEvent::Probed { tokens, outcome } => {
                self.clear_inflight(&Pending::Probe(tokens.clone()));
                let Some(outcome) = outcome else {
                    return Flow::Continue;
                };
                let now = self.clock.now();
                let effects =
                    collect_effects(|emit| self.core.on_probe_result(now, tokens, outcome, emit));
                self.execute(effects, writer).await
            }
            CoreEvent::RateLimited {
                pending,
                retry_after_secs,
            } => {
                self.clear_inflight(&pending);
                let wait = self.rest_quiet.open(retry_after_secs, Instant::now());
                warn!(
                    "polymarket adapter {} {} rate limited — rest quiet {}s",
                    self.label,
                    pending.api(),
                    wait.as_secs()
                );
                Flow::Continue
            }
        }
    }
}

/// The actor plus what outlives any one socket, so the tick and ping timers keep their schedule
/// across a reconnect.
struct MarketLane {
    actor: Actor,
    ticks: Interval,
    ping: Interval,
    results_rx: mpsc::Receiver<CoreEvent>,
}

impl Lane for MarketLane {
    async fn session(&mut self, socket: Socket) -> SessionEnd {
        let actor = &mut self.actor;
        let (mut writer, mut reader) = socket.split();
        if !actor.prime_connection(&mut writer).await {
            return SessionEnd::Reconnect {
                is_endpoint_proven: false,
            };
        }
        loop {
            if actor.fatal.is_tripped() {
                return SessionEnd::Stopped;
            }
            if actor.run_state.state() == RunState::Idle {
                return SessionEnd::Parked;
            }
            let flow = tokio::select! {
                frame = reader.next() => actor.on_socket_message(frame, &mut writer).await,
                _ = self.ticks.tick() => {
                    let now = actor.clock.now();
                    actor.drive_tick(now, &mut writer).await
                }
                _ = self.ping.tick() => actor.send_ping(&mut writer).await,
                event = self.results_rx.recv() => actor.on_core_event(event, &mut writer).await,
            };
            if flow == Flow::Reconnect {
                // The connection primed, and that proves the endpoint healthy even if no frame has
                // arrived yet.
                return SessionEnd::Reconnect {
                    is_endpoint_proven: true,
                };
            }
        }
    }
}

fn collect_effects(drive: impl FnOnce(&mut dyn FnMut(DriverEffect))) -> Vec<DriverEffect> {
    let mut effects = Vec::new();
    drive(&mut |effect| effects.push(effect));
    effects
}

/// Built through [`Slot::as_usize`], because that is how the core indexes the array it gets back: a
/// pair placed at the other position publishes each window's prices under the other slot's
/// instrument ids, and nothing downstream can tell.
fn slot_legs_from_rows(series: PolySeries, members: &[&InstrumentRow]) -> [SlotLegs; 2] {
    let leg = |symbol: &str| {
        members
            .iter()
            .find(|row| &*row.venue_symbol == symbol)
            .map(|row| row.instrument_id)
            .unwrap_or_else(|| panic!("polymarket group missing slot row {symbol}"))
    };
    let symbols = series.slot_leg_symbols();
    Slot::ALL.map(|slot| {
        let [up, down] = &symbols[slot.as_usize()];
        SlotLegs {
            up: leg(up),
            down: leg(down),
        }
    })
}
