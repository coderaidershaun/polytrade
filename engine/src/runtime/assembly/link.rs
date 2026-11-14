//! Link setup: bind the socket, hash the identity, and resolve each peer subscription the config
//! file gave, in preflight, before anything spawns.

use std::net::UdpSocket;

use crate::hot::strategy::Strategy;
use crate::link::{
    FrameGuard, LINK_NAME_LEN, LinkHash, LinkIdentity, PeerFeed, TopicId, schema_hash_of_fields,
};
use crate::msg::time::wall_clock_now;
use crate::runtime::config::{Config, RunIdentity};
use crate::runtime::registry::Registry;

use super::StartError;

pub(super) const LINK_RING_CAPACITY: usize = 1_024;

pub(super) struct BoundLink {
    /// A plain socket that needs no runtime, so a refused start closes it by dropping it. The start
    /// turns it into a tokio socket on the runtime that runs the link actor.
    pub socket: UdpSocket,
    pub identity: LinkIdentity,
    pub guard: FrameGuard,
    pub peers: Vec<PeerFeed>,
    pub topic_count: usize,
    pub feature_count: u16,
}

/// Bind socket + settle identity, guard and peer set, or Ok(None) with no link: block.
///
/// # Errors
/// [`StartError::LinkNameTooLong`] for a name the catalog frame cannot carry,
/// [`StartError::LinkBind`] for a taken or unavailable address, and
/// [`ConfigError`] via [`StartError::Config`] for an unresolvable or over-long peer topic list.
///
/// [`ConfigError`]: crate::runtime::config::ConfigError
#[expect(
    clippy::disallowed_methods,
    reason = "the link boot stamp tells peers that this engine restarted, and is never message time"
)]
pub(super) fn link_bring_up<P>(
    config: &Config<P>,
    identity: &RunIdentity,
    strategy: &dyn Strategy,
    registry: &Registry,
) -> Result<Option<BoundLink>, StartError> {
    let Some(link_config) = &config.link else {
        return Ok(None);
    };
    let feature_count = u16::try_from(strategy.features().len())
        .expect("strategy declares more than 65536 features — feature id overflow");
    // Feature names + displays in catalog frames; over-length caught here not mid-run.
    let wire_names = strategy
        .features()
        .iter()
        .map(|name| Box::<str>::from(*name))
        .chain(registry.instruments().iter().map(|row| row.display.clone()));
    for name in wire_names {
        if name.len() > LINK_NAME_LEN {
            return Err(StartError::LinkNameTooLong {
                found: name.len(),
                name,
                max: LINK_NAME_LEN,
            });
        }
    }

    let topics = strategy.link_topics();
    let peers = link_config
        .subscribe
        .iter()
        .map(|peer| PeerFeed::resolve(peer, topics))
        .collect::<Result<Vec<_>, _>>()?;
    let socket = UdpSocket::bind(link_config.bind)
        .and_then(|socket| socket.set_nonblocking(true).map(|()| socket))
        .map_err(|source| StartError::LinkBind {
            bind: link_config.bind.to_string().into_boxed_str(),
            trading_engine: identity.to_string().into_boxed_str(),
            source,
        })?;

    let token = link_config.token.as_deref().unwrap_or_default();
    Ok(Some(BoundLink {
        socket,
        identity: LinkIdentity {
            token_hash: LinkHash::of_name(token),
            strategy_hash: LinkHash::of_name(identity.strategy_id.as_str()),
            sender_te_hash: LinkHash::of_name(&identity.to_string()),
            // Wall-clock boot; restarted peer's seq=0 reads as restart, not stale stream.
            boot_ts_us: wall_clock_now().to_peer(),
        },
        guard: FrameGuard {
            token_hash: LinkHash::of_name(token),
            strategy_hash: LinkHash::of_name(identity.strategy_id.as_str()),
            schema_hash: schema_hash_of_fields(strategy.link_fields()),
        },
        peers,
        topic_count: TopicId::space_len(topics.len()),
        feature_count,
    }))
}
