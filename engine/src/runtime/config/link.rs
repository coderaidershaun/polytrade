//! Link config: receivers name peers (senders declare in code).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use serde::Deserialize;

use super::ConfigError;

const MAX_PEERS: usize = 16;

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
}

impl LinkConfig {
    /// # Errors
    /// [`ConfigError::PublicLinkBind`] or [`ConfigError::TooManyLinkPeers`].
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
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
