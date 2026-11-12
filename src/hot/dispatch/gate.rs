//! When the strategy may be called back: warmup and run state follow the message sequence, never
//! the clock, so a replay calls back at the same points. Peer link frames are recorded here rather
//! than at the link actor, so a frame dropped on a full ring never reaches the tape.

use crate::info;
use crate::link::{InboundLink, RunState, TopicId};
use crate::msg::inbound::RunControl;
use crate::msg::persist::{LinkFrameRow, LinkRowKind, PersistRecord};
use crate::shutdown::{RunAssertion, RunStateCell};
use crate::sink::LinkSink;
use crate::time::{DurationUs, TsUs};

use super::HotEngine;

/// Warmup suppression (message-time only, not wall clock; replay needs same prefix).
pub(super) struct Warmup {
    span: DurationUs,
    first_message_ts: Option<TsUs>,
    is_complete: bool,
}

impl Warmup {
    pub(super) fn new(span: DurationUs) -> Self {
        Self {
            span,
            first_message_ts: None,
            is_complete: span <= DurationUs::ZERO,
        }
    }

    /// True on message ending warmup (same message delivered, logs once).
    #[inline]
    pub(super) fn observe(&mut self, received_ts: TsUs) -> bool {
        if self.is_complete {
            return false;
        }
        let first = *self.first_message_ts.get_or_insert(received_ts);
        self.is_complete = received_ts.diff(first) >= self.span;
        self.is_complete
    }

    /// Re-armed on resume (derived state wiped, estimators need unobserved run-up).
    #[cold]
    fn rearm(&mut self) {
        self.first_message_ts = None;
        self.is_complete = self.span <= DurationUs::ZERO;
    }
}

/// Link wiring (paired facts: engine without link has no outbound ring or run-state reporter).
pub struct LinkWiring {
    pub sink: LinkSink,
    /// Dispatch publishes applied run state (link reads to check marker landed + report real state). Write-only.
    pub acknowledged: RunStateCell,
}

impl HotEngine {
    /// Log warmup completion (once per run, operator's recording marker).
    #[cold]
    pub(super) fn log_warmup_complete(&self) {
        info!(
            "warmup complete after {:.0}s of message time — strategy callbacks now live",
            self.warmup.span.to_secs()
        );
    }

    /// Callback gate predicate (derived from message sequence, replay suppresses same callbacks).
    #[inline]
    pub(super) fn is_strategy_live(&self) -> bool {
        self.warmup.is_complete && self.run.state == RunState::Running
    }

    /// The frame is recorded before the strategy sees it, so on the tape it comes ahead of anything
    /// the strategy writes in reply.
    pub(super) fn on_link(&mut self, link: &InboundLink) {
        self.record_link_payload(link);
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(link.received_ts_us);
        self.strategy.on_link(&mut ctx, &link.frame);
    }

    fn record_link_payload(&mut self, link: &InboundLink) {
        let origin = link.frame.origin;
        let payload = &link.frame.payload;
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
                    received_ts_us: link.received_ts_us,
                }));
        }
    }

    /// Only run-state input to dispatch (level-triggered markers; epoch dedups; republish ack cell).
    #[cold]
    pub(super) fn on_run_control(&mut self, control: &RunControl) {
        self.record_run_control(control);
        if control.desired.epoch > self.run.epoch {
            self.apply_run_state(control.desired, control.received_ts_us);
        }
        if let Some(report) = &self.run_report {
            report.store(self.run);
        }
    }

    #[cold]
    fn record_run_control(&mut self, control: &RunControl) {
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
                event_ts_us: control.received_ts_us,
                received_ts_us: control.received_ts_us,
            }));
    }

    #[cold]
    fn apply_run_state(&mut self, desired: RunAssertion, received_ts: TsUs) {
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
        info!(
            "run state IDLE (epoch {}) — sealing parquet",
            self.run.epoch
        );
        self.drain_actions();
        if let Some(sink) = self.state.sink.as_mut() {
            sink.request_seal();
        }
    }

    /// Resume: wipe derived state (trackers, EwmaVol, estimators); poison from pre-pause gap. Ledger NOT wiped (money≠estimator, session PnL spans all parks).
    #[cold]
    fn resume(&mut self, received_ts: TsUs) {
        info!(
            "run state RUNNING (epoch {}) — derived state reset, warmup re-armed",
            self.run.epoch
        );
        for index in 0..self.state.trackers.len() {
            self.state.trackers[index].on_rotation();
            if let Some(ewma) = self.state.ewma[index].as_mut() {
                ewma.reset();
            }
        }
        let mut ctx = self.state.ctx(received_ts);
        self.strategy.on_resume(&mut ctx);
        self.warmup.rearm();
    }
}
