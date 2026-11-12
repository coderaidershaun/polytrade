//! Async edge that advances the simulator from stamped inputs.

mod intake;

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::runtime::Handle;

use crate::adapters::IDLE_POLL;
use crate::adapters::binance::exec::{DecodeContext, SymbolTable};
use crate::adapters::exec::{EdgeHandle, EngineIdentity, ExecStop};
use crate::hot::spawn::QueueProducer;
use crate::link::RunState;
use crate::msg::exec::{CancelReason, ExecLaneItem, StampedExecCommand};
use crate::msg::inbound::InboundMessage;
use crate::registry::AssetDictionary;
use crate::shutdown::{FatalSignal, RunStateCell};
use crate::time::{EngineClock, TsUs};
use crate::{info, warn};

use super::core::latency::{LatencyBudget, rewound, shifted};
use super::core::market::ResetReason;
use super::driver::SimExecDriver;
use super::lanes::{LaneBuffer, SimHorizon};
use super::readiness::{ReadinessProof, SimReadiness};
use super::tap::MarketTapLane;

const LANE_POLL: Duration = Duration::from_millis(1);
const SWEEP_PASSES: u32 = 8;

pub(super) struct SimActor {
    pub(super) driver: SimExecDriver,
    pub(super) commands: rtrb::Consumer<ExecLaneItem>,
    pub(super) trades: MarketTapLane,
    pub(super) depth: MarketTapLane,
    pub(super) producer: QueueProducer,
    pub(super) engine_clock: EngineClock,
    pub(super) assets: AssetDictionary,
    pub(super) symbols: SymbolTable,
    pub(super) identity: EngineIdentity,
    pub(super) latency: LatencyBudget,
    pub(super) horizon: SimHorizon,
    pub(super) commands_due: LaneBuffer<StampedExecCommand>,
    pub(super) readiness: SimReadiness,
    pub(super) market_recovery_generation: u64,
    pub(super) applied_through_ts_us: Option<TsUs>,
    pub(super) has_opened: bool,
    pub(super) stop: ExecStop,
    pub(super) run_state: RunStateCell,
    pub(super) fatal: FatalSignal,
    pub(super) is_parked: bool,
    pub(super) sweep_deadline: Duration,
}

impl SimActor {
    /// Private on purpose: a venue started without its [`EdgeHandle`] has nothing that can ask it
    /// to sweep, so the only legitimate door is [`SimActor::spawn`].
    async fn run(mut self) {
        while !self.is_stopping() {
            self.follow_run_state();
            self.poll_lanes();
            self.open_venue();
            self.advance();
            self.arm_readiness();
            let period = match self.is_parked {
                true => IDLE_POLL,
                false => LANE_POLL,
            };
            tokio::time::sleep(period).await;
        }
        self.shut_down();
    }

    pub(super) fn spawn(self, rt: &Handle) -> EdgeHandle {
        let stop = self.stop.clone();
        let sweep_deadline = self.sweep_deadline;
        EdgeHandle {
            join: rt.spawn(crate::log::tag_task("exchange-sim", self.run())),
            stop,
            sweep_deadline,
            venue: "exchange simulator",
            missed_sweep_cost: "its unresolved orders end the run invalidated",
        }
    }

    fn open_venue(&mut self) {
        if self.has_opened || self.is_parked || !self.horizon.has_all_watermarks() {
            return;
        }
        let effective_ts_us = self.horizon.safe_venue_horizon();
        self.has_opened = true;
        self.driver.open(effective_ts_us);
    }

    fn advance(&mut self) {
        if !self.horizon.has_all_watermarks() {
            return;
        }
        loop {
            let horizon = self.horizon.safe_venue_horizon();
            if let Some(at_ts_us) = self.commands_due.peek_ts_us().filter(|at| *at <= horizon) {
                self.advance_venue(rewound(at_ts_us, crate::time::DurationUs::RESOLUTION));
                if let Some(command) = self.commands_due.next_due(horizon) {
                    self.driver.on_command(command, at_ts_us);
                }
                continue;
            }
            self.advance_venue(horizon);
            return;
        }
    }

    fn advance_venue(&mut self, to_ts_us: TsUs) {
        if self
            .applied_through_ts_us
            .is_some_and(|reached| to_ts_us < reached)
        {
            return;
        }
        self.applied_through_ts_us = Some(to_ts_us);
        let effect_ts_us = shifted(to_ts_us, [self.latency.ack, self.latency.order_entry]);
        self.publish(to_ts_us, |driver, context| {
            driver.advance_to(to_ts_us, effect_ts_us, context)
        });
        self.sync_market_recovery();
    }

    fn publish(
        &mut self,
        received_ts_us: TsUs,
        step: impl FnOnce(&mut SimExecDriver, DecodeContext<'_>) -> Vec<InboundMessage>,
    ) {
        let Self {
            driver,
            symbols,
            assets,
            identity,
            producer,
            engine_clock,
            ..
        } = self;
        let context = DecodeContext {
            symbols,
            assets,
            identity: *identity,
            received_ts_us,
        };
        for mut message in step(driver, context) {
            message.set_queued_ts_us(engine_clock.now());
            producer.push(message);
        }
    }

    fn sync_market_recovery(&mut self) {
        let recovery = self.driver.venue().market().recovery();
        if recovery.generation != self.market_recovery_generation {
            self.market_recovery_generation = recovery.generation;
            self.driver.close();
            self.has_opened = false;
            self.readiness.withdraw();
        }
        if recovery.snapshot_complete {
            self.readiness.prove(ReadinessProof::BookSnapshotComplete);
        }
        if recovery.bridging_delta {
            self.readiness.prove(ReadinessProof::DepthBridged);
        }
    }

    fn arm_readiness(&mut self) {
        if self.is_parked {
            return;
        }
        if !self.driver.can_arm() {
            return;
        }
        let Some(at_ts_us) = self.applied_through_ts_us else {
            return;
        };
        if !self.readiness.take_announcement() {
            return;
        }
        if !self.driver.venue().market().is_matching_live() {
            self.driver.venue_mut().restore_matching(at_ts_us);
        }
        info!("simulated venue ready — depth bridged, snapshot complete, three lanes advancing");
        self.publish(at_ts_us, |driver, context| {
            driver.announce_readiness(at_ts_us, context)
        });
    }

    fn follow_run_state(&mut self) {
        let is_idle = self.run_state.state() == RunState::Idle;
        if is_idle == self.is_parked {
            return;
        }
        self.is_parked = is_idle;
        if !is_idle {
            info!("simulated venue resumed — readiness is earned again from new progress");
            return;
        }
        info!("simulated venue parked — quotes pulled, market data still folded");
        self.sweep(CancelReason::Park);
        self.suspend(ResetReason::VenueParked);
    }

    fn is_stopping(&self) -> bool {
        self.stop.requested.load(Ordering::Acquire)
            || self.fatal.is_tripped()
            || self.trades.is_producer_gone()
            || self.depth.is_producer_gone()
    }

    fn shut_down(&mut self) {
        self.poll_lanes();
        self.advance();
        if let Some(final_ts_us) = self.drain_final_commands() {
            let through_ts_us = self
                .applied_through_ts_us
                .map_or(final_ts_us, |applied| applied.max(final_ts_us));
            self.advance_venue(through_ts_us);
        }
        let reason = match self.fatal.is_tripped() {
            true => CancelReason::Fatal,
            false => CancelReason::Shutdown,
        };
        let is_settled = match self.applied_through_ts_us {
            None => true,
            Some(_) => {
                self.sweep(reason);
                self.driver.is_swept()
            }
        };
        self.driver.log_summary();
        match is_settled {
            true => self.stop.settled.notify_one(),
            false => warn!(
                "simulated venue could not resolve every order in {SWEEP_PASSES} sweep passes — the handle's deadline ends the run with them invalidated"
            ),
        }
    }

    fn drain_final_commands(&mut self) -> Option<TsUs> {
        let mut drained = 0usize;
        let mut final_ts_us = None;
        while let Some(at_ts_us) = self.commands_due.peek_ts_us() {
            let command = self
                .commands_due
                .next_due(TsUs::from_micros(i64::MAX))
                .expect("the command just peeked remains in the lane");
            self.driver.on_command(command, at_ts_us);
            drained += 1;
            final_ts_us = Some(at_ts_us);
        }
        if drained > 0 {
            info!("simulated venue admitted {drained} final commands before forced sweep");
        }
        final_ts_us
    }

    fn sweep(&mut self, reason: CancelReason) {
        for _ in 0..SWEEP_PASSES {
            let Some(at_ts_us) = self.applied_through_ts_us else {
                return;
            };
            self.driver.begin_sweep(reason, at_ts_us);
            let released = shifted(at_ts_us, [self.latency.order_entry, self.latency.ack]);
            self.advance_venue(released);
            if self.driver.is_swept() && self.driver.owes_nothing() {
                return;
            }
        }
    }
}
