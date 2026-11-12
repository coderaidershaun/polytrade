//! Polling and buffering for simulator input lanes.

use super::super::core::market::ResetReason;
use super::super::lanes::SimLane;
use super::SimActor;
use crate::msg::exec::ExecLaneItem;

impl SimActor {
    pub(super) fn poll_lanes(&mut self) {
        self.intake_commands();
        self.intake_market();
    }

    fn intake_commands(&mut self) {
        while let Ok(item) = self.commands.pop() {
            match item {
                ExecLaneItem::Command(stamped) => {
                    self.horizon.observe(SimLane::Command, stamped.issued_ts_us);
                    if let Err(error) = self
                        .commands_due
                        .try_push(self.latency.arrival(stamped.issued_ts_us), stamped)
                    {
                        self.fatal.trip(format!("exchange simulator: {error}"));
                        return;
                    }
                }
                ExecLaneItem::Watermark(at_ts_us) => {
                    self.horizon.observe(SimLane::Command, at_ts_us)
                }
            }
        }
        let watermark = self.horizon.watermark(SimLane::Command);
        self.readiness.observe_lane(SimLane::Command, watermark);
    }

    fn intake_market(&mut self) {
        let Self {
            trades,
            depth,
            driver,
            latency,
            horizon,
            readiness,
            ..
        } = self;
        let latency = *latency;
        for (lane, tap) in [(SimLane::Trade, trades), (SimLane::Depth, depth)] {
            tap.drain(|batch| driver.venue_mut().on_market_batch(batch, latency));
            let Some(watermark) = tap.proven_watermark_ts_us() else {
                continue;
            };
            horizon.observe(lane, watermark);
            readiness.observe_lane(lane, watermark);
        }
    }

    #[cold]
    pub(super) fn suspend(&mut self, reason: ResetReason) {
        if !self.driver.venue().market().is_matching_live() {
            return;
        }
        if let Some(at_ts_us) = self.applied_through_ts_us {
            self.driver.venue_mut().suspend_matching(reason, at_ts_us);
            self.market_recovery_generation = self.driver.venue().market().recovery().generation;
        }
        self.driver.close();
        self.has_opened = false;
        self.readiness.withdraw();
    }
}
