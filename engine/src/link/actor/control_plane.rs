//! The control plane of the link: it judges each controller assertion, parks the engine when the
//! controller falls silent and the config asks for that, and pushes the pending marker. The time
//! comes in as a parameter.

use std::net::SocketAddr;

use crate::hot::ingress::ControlQueueProducer;
use crate::msg::run_state::{RunAssertion, RunState};
use crate::msg::time::{DurationUs, LocalTs};
use crate::runtime::config::ControllerLoss;
use crate::runtime::shutdown::DesiredWriter;
use crate::{info, warn};

use super::super::subscribers::LINK_SUBSCRIPTION_TTL;
use super::Actor;

/// What the control plane made of one assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssertionVerdict {
    /// Epoch 0: the sender subscribes and claims no control.
    NoClaim,
    /// The epoch is above the desired one, so the assertion is now the desired state.
    Accepted,
    /// The desired state holds an equal or a higher epoch, so the assertion changes nothing.
    Stale,
}

/// The desired run state of the link, and the watch on the controller that sets it. It holds the
/// one writer of the desired state. It reads no clock and owns no socket or queue, so the actor
/// passes the time and the control queue in.
pub struct ControlPlane {
    control: DesiredWriter,
    on_controller_loss: ControllerLoss,
    last_control_ts_us: Option<LocalTs>,
    is_controller_lost: bool,
}

impl ControlPlane {
    /// The silence of the controller after which `on_controller_loss` applies. It equals the
    /// lifetime of a subscription, so a controller that stopped subscribing is the one that is lost.
    pub const SILENCE_LIMIT: DurationUs = LINK_SUBSCRIPTION_TTL;

    pub fn new(control: DesiredWriter, on_controller_loss: ControllerLoss) -> Self {
        Self {
            control,
            on_controller_loss,
            last_control_ts_us: None,
            is_controller_lost: false,
        }
    }

    /// Each claim of control, a stale one included, proves that the controller is alive. Only a
    /// higher epoch changes the desired state.
    pub fn on_assertion(
        &mut self,
        from: SocketAddr,
        assertion: RunAssertion,
        now: LocalTs,
    ) -> AssertionVerdict {
        if assertion.epoch == 0 {
            return AssertionVerdict::NoClaim;
        }
        self.last_control_ts_us = Some(now);
        self.is_controller_lost = false;
        if !self.control.accept_if_newer(assertion) {
            return AssertionVerdict::Stale;
        }
        info!(
            "controller {from} asserts {:?} at epoch {}",
            assertion.state, assertion.epoch
        );
        AssertionVerdict::Accepted
    }

    /// Dead controller must not decide fate; holding default, dead-man opt-in. Gives the park
    /// that the dead-man rule asserted, one time for each loss of the controller.
    pub fn check_controller(&mut self, now: LocalTs) -> Option<RunAssertion> {
        let last = self.last_control_ts_us?;
        if now.diff(last) < Self::SILENCE_LIMIT || self.is_controller_lost {
            return None;
        }
        self.is_controller_lost = true;
        match self.on_controller_loss {
            ControllerLoss::Hold => {
                warn!(
                    "no controller assertion for {}s — holding run state {:?}",
                    Self::SILENCE_LIMIT.to_secs(),
                    self.control.desired().state
                );
                None
            }
            ControllerLoss::Idle => {
                let epoch = self.control.desired().epoch + 1;
                warn!(
                    "no controller assertion for {}s — dead-man parking the engine at epoch {epoch}",
                    Self::SILENCE_LIMIT.to_secs()
                );
                let park = RunAssertion {
                    state: RunState::Idle,
                    epoch,
                };
                self.control.accept_if_newer(park);
                Some(park)
            }
        }
    }

    /// The marker that the hot thread has not applied yet, or `None` once it caught up.
    pub fn pending(&self) -> Option<RunAssertion> {
        self.control.pending()
    }

    /// Level-triggered: the control queue drops and counts when it is full, so a push on each
    /// change alone would leave the edges parked after one drop. Pushing the pending marker at each
    /// heartbeat repairs it, and a duplicate is harmless because the fold applies each epoch once.
    pub fn push_pending(&self, markers: &mut ControlQueueProducer, now: LocalTs) {
        if let Some(desired) = self.pending() {
            markers.push(desired, now);
        }
    }

    pub fn desired(&self) -> RunAssertion {
        self.control.desired()
    }

    /// The run state that the hot thread reports it applied.
    pub fn acknowledged(&self) -> RunAssertion {
        self.control.acknowledged()
    }
}

impl Actor {
    pub(super) fn push_marker_if_needed(&mut self, now: LocalTs) {
        self.control.push_pending(&mut self.markers, now);
    }
}
