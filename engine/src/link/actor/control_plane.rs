//! The control half of the link: it pushes the marker that the hot thread has not applied yet,
//! and parks the engine when the controller falls silent and the config asks for that.

use crate::msg::inbound::{InboundMessage, MessageBody, RunControl};
use crate::msg::run_state::{RunAssertion, RunState};
use crate::msg::time::LocalTs;
use crate::runtime::config::ControllerLoss;
use crate::warn;

use super::Actor;
use super::inbound::PEER_SILENCE_LIMIT;

impl Actor {
    /// Level-triggered: drop+count queue means edge-triggered push alone leaves adapters down on full slot. Dups harmless (dedup on epoch).
    pub(super) fn push_marker_if_needed(&mut self, now: LocalTs) {
        let Some(desired) = self.control.pending() else {
            return;
        };
        self.inbound.push(InboundMessage::new(
            now,
            MessageBody::RunControl(RunControl { desired }),
        ));
    }

    /// Dead controller must not decide fate; holding default, dead-man opt-in.
    pub(super) fn check_controller(&mut self, now: LocalTs) {
        let Some(last) = self.last_control_ts_us else {
            return;
        };
        if now.diff(last) < PEER_SILENCE_LIMIT || self.is_controller_lost {
            return;
        }
        self.is_controller_lost = true;
        match self.on_controller_loss {
            ControllerLoss::Hold => warn!(
                "no controller assertion for {}s — holding run state {:?}",
                PEER_SILENCE_LIMIT.to_secs(),
                self.control.desired().state()
            ),
            ControllerLoss::Idle => {
                let epoch = self.control.desired().load().epoch + 1;
                warn!(
                    "no controller assertion for {}s — dead-man parking the engine at epoch {epoch}",
                    PEER_SILENCE_LIMIT.to_secs()
                );
                self.control.desired().accept_if_newer(RunAssertion {
                    state: RunState::Idle,
                    epoch,
                });
                self.push_marker_if_needed(now);
            }
        }
    }
}
