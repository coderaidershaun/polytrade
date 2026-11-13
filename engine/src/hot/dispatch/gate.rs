//! When the strategy may be called back: warmup and run state follow the message sequence, never
//! the clock, so a replay calls back at the same points. Peer link frames are recorded here rather
//! than at the link actor, so a frame dropped on a full ring never reaches the tape.

use crate::hot::digest::digest_fields;
use crate::hot::sink::LinkSink;
use crate::link::{LinkFrame, TopicId};
use crate::log::hot_info;
use crate::msg::inbound::RunControl;
use crate::msg::persist::{LinkFrameRow, LinkRowKind, PersistRecord};
use crate::msg::run_state::RunAssertion;
use crate::msg::run_state::RunState;
use crate::msg::time::{DurationUs, FoldTime, LocalTs};
use crate::runtime::shutdown::RunStateCell;

use super::HotEngine;

/// Warmup suppression (message-time only, not wall clock; replay needs same prefix).
pub(super) struct Warmup {
    span: DurationUs,
    first_message_ts: Option<LocalTs>,
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
    pub(super) fn observe(&mut self, now: FoldTime) -> bool {
        if self.is_complete {
            return false;
        }
        let now = now.as_local();
        let first = *self.first_message_ts.get_or_insert(now);
        self.is_complete = now.diff(first) >= self.span;
        self.is_complete
    }

    /// Re-armed on resume (derived state wiped, estimators need unobserved run-up).
    #[cold]
    fn rearm(&mut self) {
        self.first_message_ts = None;
        self.is_complete = self.span <= DurationUs::ZERO;
    }
}

digest_fields! { Warmup { span, first_message_ts, is_complete } }

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
        hot_info!(
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
    pub(super) fn on_link(&mut self, frame: &LinkFrame, received_ts_us: LocalTs) {
        self.record_link_payload(frame, received_ts_us);
        if !self.is_strategy_live() {
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
        if let Some(sink) = self.state.sink.as_mut() {
            sink.request_seal();
        }
    }

    /// Resume: wipe derived state (trackers, EwmaVol, estimators); poison from pre-pause gap. Ledger NOT wiped (money≠estimator, session PnL spans all parks).
    #[cold]
    fn resume(&mut self, received_ts: LocalTs) {
        hot_info!(
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
