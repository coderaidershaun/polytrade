//! Link config: receivers name peers (senders declare in code).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroUsize;

use serde::Deserialize;

use super::{ConfigError, check_range};

const MAX_PEERS: usize = 16;

/// Five minutes of event frames at 1,000 a second, the rate the history spike measured with room
/// above the fastest shipped spin. Each frame takes `size_of::<UiEvent>()` bytes, reserved at start.
const DEFAULT_HISTORY_CAPACITY: usize = 300_000;

/// The spike sent 300,000 history frames at 128 a flush of 10 ms with no failed send and no live
/// frame late: five minutes of history reach a new tab in about 23 seconds.
const DEFAULT_HISTORY_PER_FLUSH: usize = 128;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkConfig {
    /// Security boundary = private network (WireGuard/Tailscale/VPC).
    pub bind: SocketAddr,
    #[serde(default)]
    pub allow_public_bind: bool,
    #[serde(default)]
    pub subscribe: Vec<PeerSubscription>,
    /// Cross-talk protection, not security (digest in clear).
    pub token: Option<Box<str>>,
    #[serde(default)]
    pub on_controller_loss: ControllerLoss,
    /// The IP addresses whose run assertions the engine takes, with no port. Empty by default: an
    /// engine with no list accepts no assertion.
    #[serde(default)]
    pub controllers: Vec<IpAddr>,
    /// The peer frames that one source address may send in each heartbeat of 1 s. `None` takes
    /// `queues.link_capacity`, so no one sender alone puts more than one full link queue into a
    /// second.
    #[serde(default)]
    pub sender_rate_per_sec: Option<u32>,
    /// The event frames the link actor keeps for a tab that opens late.
    #[serde(default = "default_history_capacity")]
    pub history_capacity: usize,
    /// The history frames that one flush sends, so a replay never delays the live frames.
    #[serde(default = "default_history_per_flush")]
    pub history_per_flush: usize,
}

fn default_history_capacity() -> usize {
    DEFAULT_HISTORY_CAPACITY
}

fn default_history_per_flush() -> usize {
    DEFAULT_HISTORY_PER_FLUSH
}

impl LinkConfig {
    /// Validated greater than 0.
    pub fn history_capacity(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.history_capacity).unwrap_or(NonZeroUsize::MIN)
    }

    /// Validated greater than 0.
    pub fn history_per_flush(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.history_per_flush).unwrap_or(NonZeroUsize::MIN)
    }

    /// # Errors
    /// [`ConfigError::PublicLinkBind`], [`ConfigError::TooManyLinkPeers`], or
    /// [`ConfigError::Invalid`] for a sender rate of 0 or a history key of 0.
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        if let Some(rate) = self.sender_rate_per_sec {
            check_range(
                "link.sender_rate_per_sec",
                u64::from(rate),
                1..=u64::from(u32::MAX),
                "1 or more",
            )?;
        }
        if !self.allow_public_bind && !is_private_bind(self.bind.ip()) {
            return Err(ConfigError::PublicLinkBind {
                bind: self.bind.to_string().into_boxed_str(),
            });
        }
        if self.subscribe.len() > MAX_PEERS {
            return Err(ConfigError::TooManyLinkPeers {
                count: self.subscribe.len(),
                max: MAX_PEERS,
            });
        }
        let history_keys = [
            ("link.history_capacity", self.history_capacity),
            ("link.history_per_flush", self.history_per_flush),
        ];
        for (field, value) in history_keys {
            check_range(field, value as u64, 1..=u64::MAX, "greater than 0")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSubscription {
    pub address: SocketAddr,
    #[serde(default)]
    pub topics: Vec<Box<str>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControllerLoss {
    #[default]
    Hold,
    Idle,
}

fn is_private_bind(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_private_v4(v4),
        IpAddr::V6(v6) => is_private_v6(v6),
    }
}

fn is_private_v4(ip: Ipv4Addr) -> bool {
    let [first, second, ..] = ip.octets();
    let is_carrier_grade_nat = first == 100 && (64..128).contains(&second);
    ip.is_loopback() || ip.is_private() || ip.is_link_local() || is_carrier_grade_nat
}

fn is_private_v6(ip: Ipv6Addr) -> bool {
    let leading = ip.segments()[0];
    let is_unique_local = leading & 0xfe00 == 0xfc00;
    let is_link_local = leading & 0xffc0 == 0xfe80;
    ip.is_loopback() || is_unique_local || is_link_local
}
