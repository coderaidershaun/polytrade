//! The control plane of the link: it judges each controller assertion, takes one only from a
//! listed controller, parks the engine when the controller falls silent and the config asks for
//! that, and pushes the pending marker. The time comes in as a parameter.

use std::net::{IpAddr, SocketAddr};

use crate::hot::ingress::ControlQueueProducer;
use crate::msg::run_state::{ControllerEpoch, RunAssertion, RunState};
use crate::msg::time::{DurationUs, LocalTs};
use crate::runtime::config::ControllerLoss;
use crate::runtime::shutdown::DesiredWriter;
use crate::{info, warn, warn_repeating};

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
    /// The IP address of the sender is not in `link.controllers`. The assertion is refused and
    /// counted for that address, and it does not refresh the timer of the controller loss.
    UnlistedSender,
    /// The top epoch, 2^63 - 1, is refused and counted: the dead-man park takes the desired epoch
    /// plus 1, so it must always have one.
    EpochTooHigh,
}

/// The rows of the table of refused assertions. A forger can send from many addresses, so the table
/// has a fixed size, and each address past it shares one count.
const REFUSED_ADDRESSES: usize = 16;

/// The assertions refused for each sender address that is not listed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RefusedAssertions {
    by_address: [Option<(IpAddr, u64)>; REFUSED_ADDRESSES],
    past_table: u64,
}

impl RefusedAssertions {
    /// The refusals of `address`, or 0 for an address that has no row of its own.
    pub fn of(&self, address: IpAddr) -> u64 {
        self.by_address
            .iter()
            .flatten()
            .find(|(seen, _)| *seen == address)
            .map_or(0, |(_, count)| *count)
    }

    pub fn total(&self) -> u64 {
        let in_table: u64 = self
            .by_address
            .iter()
            .flatten()
            .map(|(_, count)| count)
            .sum();
        in_table + self.past_table
    }

    fn count(&mut self, address: IpAddr) {
        for row in &mut self.by_address {
            match row {
                Some((seen, count)) if *seen == address => {
                    *count += 1;
                    return;
                }
                Some(_) => {}
                None => {
                    *row = Some((address, 1));
                    return;
                }
            }
        }
        self.past_table += 1;
    }
}

/// The desired run state of the link, and the watch on the controller that sets it. It holds the
/// one writer of the desired state. It reads no clock and owns no socket or queue, so the actor
/// passes the time and the control queue in.
pub struct ControlPlane {
    control: DesiredWriter,
    /// In the form of `IpAddr::to_canonical`, as the sender address is compared.
    controllers: Box<[IpAddr]>,
    on_controller_loss: ControllerLoss,
    last_control_ts_us: Option<LocalTs>,
    is_controller_lost: bool,
    refused_assertions: RefusedAssertions,
    refused_epochs: u64,
}

impl ControlPlane {
    /// The silence of the controller after which `on_controller_loss` applies. It equals the
    /// lifetime of a subscription, so a controller that stopped subscribing is the one that is lost.
    pub const SILENCE_LIMIT: DurationUs = LINK_SUBSCRIPTION_TTL;

    /// `controllers` holds IP addresses only: the relay sends each tab from a socket of its own,
    /// with a port that the OS selects, so no port names a controller.
    pub fn new(
        control: DesiredWriter,
        controllers: &[IpAddr],
        on_controller_loss: ControllerLoss,
    ) -> Self {
        if controllers.is_empty() {
            warn!("link.controllers is empty — this engine accepts no run assertion");
        }
        Self {
            control,
            controllers: controllers.iter().map(IpAddr::to_canonical).collect(),
            on_controller_loss,
            last_control_ts_us: None,
            is_controller_lost: false,
            refused_assertions: RefusedAssertions::default(),
            refused_epochs: 0,
        }
    }

    /// Each claim of control from a listed controller, a stale one included, proves that the
    /// controller is alive. Only a higher epoch changes the desired state. The address is checked
    /// first, so a sender that is not listed neither changes the state nor keeps the timer of the
    /// controller loss alive.
    pub fn on_assertion(
        &mut self,
        from: SocketAddr,
        assertion: RunAssertion,
        now: LocalTs,
    ) -> AssertionVerdict {
        if assertion.epoch == ControllerEpoch::ZERO {
            return AssertionVerdict::NoClaim;
        }
        let address = from.ip().to_canonical();
        if !self.controllers.contains(&address) {
            return self.refuse_unlisted(address, assertion);
        }
        self.last_control_ts_us = Some(now);
        self.is_controller_lost = false;
        if assertion.epoch == ControllerEpoch::MAX {
            warn_repeating!(
                self.refused_epochs,
                "controller {from} asserts the top epoch {}, which leaves no epoch for the dead-man park — refused {} assertions",
                assertion.epoch,
                self.refused_epochs
            );
            return AssertionVerdict::EpochTooHigh;
        }
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
                // Only a dead-man park can take the top epoch, so with none left the engine is
                // parked already.
                let epoch = self.control.desired().epoch.next()?;
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

    pub fn refused_assertions(&self) -> RefusedAssertions {
        self.refused_assertions
    }

    /// The assertions of a listed controller at the top epoch.
    pub fn refused_epochs(&self) -> u64 {
        self.refused_epochs
    }

    #[cold]
    fn refuse_unlisted(&mut self, address: IpAddr, assertion: RunAssertion) -> AssertionVerdict {
        self.refused_assertions.count(address);
        let refused = self.refused_assertions.of(address);
        if refused.is_power_of_two() {
            warn!(
                "{address} is not in link.controllers — refused its assertion of {:?} at epoch {} ({refused} from it so far)",
                assertion.state, assertion.epoch
            );
        }
        AssertionVerdict::UnlistedSender
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
