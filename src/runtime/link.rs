//! Link setup: bind the socket, hash the identity, and resolve each peer subscription the config
//! file gave, before anything spawns.

use tokio::net::UdpSocket;
use tokio::runtime::Runtime;

use crate::config::{Config, RunIdentity};
use crate::hot::strategy::Strategy;
use crate::info;
use crate::link::{
    FrameGuard, LINK_NAME_LEN, LinkHash, LinkIdentity, PeerFeed, TopicId, schema_hash_of_fields,
};
use crate::registry::Registry;
use crate::time::boot_stamp_us;

use super::EngineError;

pub(super) struct BoundLink {
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
/// [`EngineError::LinkNameTooLong`] for a name the catalog frame cannot carry,
/// [`EngineError::LinkBind`] for a taken or unavailable address, and
/// [`ConfigError`] via [`EngineError::Config`] for an unresolvable or over-long peer topic list.
///
/// [`ConfigError`]: crate::config::ConfigError
pub(super) fn link_bring_up<P>(
    config: &Config<P>,
    identity: &RunIdentity,
    strategy: &dyn Strategy,
    registry: &Registry,
    runtime: &Runtime,
) -> Result<Option<BoundLink>, EngineError> {
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
            return Err(EngineError::LinkNameTooLong {
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
    let socket = runtime
        .block_on(UdpSocket::bind(link_config.bind))
        .map_err(|source| EngineError::LinkBind {
            bind: link_config.bind.to_string().into_boxed_str(),
            trading_engine: identity.to_string().into_boxed_str(),
            source,
        })?;
    info!(
        "link bound on {} — {} peer subscription(s)",
        link_config.bind,
        peers.len()
    );

    let token = link_config.token.as_deref().unwrap_or_default();
    Ok(Some(BoundLink {
        socket,
        identity: LinkIdentity {
            token_hash: LinkHash::of_name(token),
            strategy_hash: LinkHash::of_name(identity.strategy_id.as_str()),
            sender_te_hash: LinkHash::of_name(&identity.to_string()),
            // Wall-clock boot; restarted peer's seq=0 reads as restart, not stale stream.
            boot_ts_us: boot_stamp_us(),
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
