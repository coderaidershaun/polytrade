//! When the strategy hears a market: each market warms for its own span of message time from its
//! own first message, never by the clock, so a replay calls back at the same points and a market
//! that connects late quotes from no cold estimator. Peer link frames are recorded here rather than
//! at the link actor, so a frame dropped on a full ring never reaches the tape.

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::hot::sink::LinkSink;
use crate::link::{LinkFrame, TopicId};
use crate::log::hot_info;
use crate::msg::ids::InstrumentId;
use crate::msg::inbound::{MessageBody, RunControl};
use crate::msg::persist::{LinkFrameRow, LinkRowKind, PersistRecord};
use crate::msg::run_state::RunAssertion;
use crate::msg::run_state::RunState;
use crate::msg::time::{DurationUs, FoldTime, LocalTs};
use crate::runtime::shutdown::RunStateCell;

use super::HotEngine;

/// Whether the engine calls the strategy back for one market and lets its quotes out. Only `Live`
/// does both. A market that is not `Live` has the quote permission `Neither`, and each quote that the
/// strategy declares for it is held and reported as a refusal of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyGate {
    /// `since` is the fold time of the first message of the market since the start or the last
    /// resume, and `None` until that message comes.
    Warming {
        since: Option<FoldTime>,
    },
    Live,
    /// The run is idle.
    Parked,
}

impl StrategyGate {
    #[inline]
    pub fn is_live(self) -> bool {
        self == StrategyGate::Live
    }
}

/// One gate for each market, indexed by instrument.
pub(super) struct MarketGates {
    span: DurationUs,
    gates: Vec<StrategyGate>,
}

digest_by_hash!(StrategyGate);
digest_fields! { MarketGates { span, gates } }

impl MarketGates {
    pub(super) fn new(span: DurationUs, markets: usize) -> Self {
        Self {
            span,
            gates: vec![armed(span); markets],
        }
    }

    /// The first message that names a market opens its gate. Then each gate whose span has passed
    /// by `now` turns `Live`, on any message, so a spin past the span is the first live callback.
    #[inline]
    pub(super) fn observe(&mut self, body: &MessageBody, now: FoldTime) {
        if let Some(market) = opened_market(body) {
            let gate = &mut self.gates[usize::from(market.0)];
            if *gate == (StrategyGate::Warming { since: None }) {
                *gate = StrategyGate::Warming { since: Some(now) };
            }
        }
        for index in 0..self.gates.len() {
            let StrategyGate::Warming { since: Some(since) } = self.gates[index] else {
                continue;
            };
            if now.as_local().diff(since.as_local()) >= self.span {
                self.turn_live(index);
            }
        }
    }

    #[cold]
    fn turn_live(&mut self, index: usize) {
        let is_first_live = !self.is_any_live();
        self.gates[index] = StrategyGate::Live;
        self.log_market_live(InstrumentId(index as u16));
        if is_first_live {
            self.log_callbacks_live();
        }
    }

    #[inline]
    pub(super) fn is_live(&self, market: InstrumentId) -> bool {
        self.gates[usize::from(market.0)].is_live()
    }

    /// The callbacks that belong to no market run while one market or more is live.
    #[inline]
    pub(super) fn is_any_live(&self) -> bool {
        self.gates.iter().any(|gate| gate.is_live())
    }

    pub(super) fn as_slice(&self) -> &[StrategyGate] {
        &self.gates
    }

    #[cold]
    fn park(&mut self) {
        self.gates.fill(StrategyGate::Parked);
    }

    /// Each market warms again from its own first message after the resume: the derived state
    /// was wiped, and a market that stays silent must not go live on cold estimators.
    #[cold]
    fn rearm(&mut self) {
        self.gates.fill(armed(self.span));
    }

    #[cold]
    fn log_market_live(&self, market: InstrumentId) {
        hot_info!(
            "warmup of instrument {} complete after {:.0}s of message time — its callbacks and quotes now live",
            market.0,
            self.span.to_secs()
        );
    }

    /// The operator's marker that the strategy hears the run: the first market is live.
    #[cold]
    fn log_callbacks_live(&self) {
        hot_info!(
            "warmup complete after {:.0}s of message time — strategy callbacks now live",
            self.span.to_secs()
        );
    }
}

/// A span of zero means no warmup, so each gate is live at once.
fn armed(span: DurationUs) -> StrategyGate {
    if span <= DurationUs::ZERO {
        StrategyGate::Live
    } else {
        StrategyGate::Warming { since: None }
    }
}

/// The market whose gate a message opens. Market data names its market. Execution and account
/// events open no gate: the venue answers for an account, not for the data of a market.
fn opened_market(body: &MessageBody) -> Option<InstrumentId> {
    match body {
        MessageBody::Trade(event) => Some(event.instrument),
        MessageBody::Book(chunk) => Some(chunk.instrument),
        MessageBody::BookReset(reset) => Some(reset.instrument),
        MessageBody::Kline(event) => Some(event.instrument),
        MessageBody::MarketRotation(rotation) => Some(rotation.instrument),
        MessageBody::SpinTick(_)
        | MessageBody::Link(_)
        | MessageBody::RunControl(_)
        | MessageBody::Exec(_)
        | MessageBody::Account(_) => None,
    }
}

/// Link wiring (paired facts: engine without link has no outbound ring or run-state reporter).
pub struct LinkWiring {
    pub sink: LinkSink,
    /// Dispatch publishes applied run state (link reads to check marker landed + report real state). Write-only.
    pub acknowledged: RunStateCell,
}

impl HotEngine {
    /// The frame is recorded before the strategy sees it, so on the tape it comes ahead of anything
    /// the strategy writes in reply.
    pub(super) fn on_link(&mut self, frame: &LinkFrame, received_ts_us: LocalTs) {
        self.record_link_payload(frame, received_ts_us);
        if !self.state.gates.is_any_live() {
            return;
        }
        let mut ctx = self.state.ctx(received_ts_us);
        self.strategy.on_link(&mut ctx, frame);
    }

    fn record_link_payload(&mut self, frame: &LinkFrame, received_ts_us: LocalTs) {
        let origin = frame.origin;
        let payload = &frame.payload;
        let values = payload.values();
        let count = values.len() as u16;
        for (slot, value) in values.iter().enumerate() {
            self.state
                .actions
                .push_persist(PersistRecord::LinkFrame(LinkFrameRow {
                    kind: LinkRowKind::Payload,
                    sender_te_hash: origin.sender_te_hash.0,
                    topic: origin.topic.0,
                    seq: origin.seq,
                    slot: slot as u16,
                    count,
                    value: *value,
                    event_ts_us: payload.event_ts_us,
                    received_ts_us,
                }));
        }
    }

    /// Only run-state input to dispatch (level-triggered markers; epoch dedups; republish ack cell).
    #[cold]
    pub(super) fn on_run_control(&mut self, control: &RunControl, received_ts_us: LocalTs) {
        self.record_run_control(control, received_ts_us);
        if control.desired.epoch > self.run.epoch {
            self.apply_run_state(control.desired, received_ts_us);
        }
        if let Some(report) = &self.run_report {
            report.store(self.run);
        }
    }

    #[cold]
    fn record_run_control(&mut self, control: &RunControl, received_ts_us: LocalTs) {
        let kind = match control.desired.state {
            RunState::Running => LinkRowKind::RunRunning,
            RunState::Idle => LinkRowKind::RunIdle,
        };
        self.state
            .actions
            .push_persist(PersistRecord::LinkFrame(LinkFrameRow {
                kind,
                sender_te_hash: 0,
                topic: TopicId::SUBSCRIBE.0,
                seq: control.desired.epoch,
                slot: 0,
                count: 0,
                value: 0.0,
                event_ts_us: received_ts_us.to_peer(),
                received_ts_us,
            }));
    }

    #[cold]
    fn apply_run_state(&mut self, desired: RunAssertion, received_ts: LocalTs) {
        let is_transition = desired.state != self.run.state;
        self.run = desired;
        if !is_transition {
            return;
        }
        match desired.state {
            RunState::Idle => self.park(),
            RunState::Running => self.resume(received_ts),
        }
    }

    /// Park: seal Parquet files (disk = complete). Drain bank first, seal after last emission.
    #[cold]
    fn park(&mut self) {
        hot_info!(
            "run state IDLE (epoch {}) — sealing parquet",
            self.run.epoch
        );
        self.drain_actions();
        self.state.gates.park();
        if let Some(sink) = self.state.sink.as_mut() {
            sink.request_seal();
        }
    }

    /// A value derived before the gap describes a market that the run did not see, so the fold
    /// throws its own away, then the strategy throws away its own, then each market warms again.
    /// The ledger stays: money is no estimate, and the profit of the session spans each park.
    #[cold]
    fn resume(&mut self, received_ts: LocalTs) {
        hot_info!(
            "run state RUNNING (epoch {}) — derived state reset, warmup re-armed",
            self.run.epoch
        );
        self.state.derived.reset_for_gap();
        let mut ctx = self.state.ctx(received_ts);
        self.strategy.on_resume(&mut ctx);
        self.state.gates.rearm();
    }
}
