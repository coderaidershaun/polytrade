//! Link setup: bind the socket, hash the identity, and resolve each peer subscription the config
//! file gave, in preflight, before anything spawns.

use std::net::UdpSocket;

use rtrb::{Consumer, RingBuffer};
use tokio::runtime::Handle;

use crate::hot::dispatch::LinkWiring;
use crate::hot::sink::LinkSink;
use crate::hot::spawn::{ControlQueueProducer, LinkQueueProducer};
use crate::hot::strategy::Strategy;
use crate::link::{
    FrameGuard, LINK_NAME_LEN, LinkActor, LinkActorSetup, LinkCountsHandle, LinkHash, LinkIdentity,
    OutboundLink, PeerFeed, TopicId, schema_hash_of_fields,
};
use crate::msg::time::{DurationUs, EngineClock, wall_clock_now};
use crate::msg::ui::LatencyReport;
use crate::msg::ui::channel::UiChannels;
use crate::runtime::config::{Config, RunIdentity};
use crate::runtime::registry::Registry;
use crate::runtime::shutdown::{AcknowledgedWriter, DesiredWriter, TaskHandle};

use super::StartError;

pub(super) const LINK_RING_CAPACITY: usize = 1_024;

pub(super) struct BoundLink {
    /// A plain socket that needs no runtime, so a refused start closes it by dropping it. The link
    /// actor turns it into a tokio socket on the runtime that runs it.
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

/// What the link actor needs beside its bound socket. The start makes each part.
pub(super) struct LinkStart<'a, P> {
    pub config: &'a Config<P>,
    pub inbound: Option<LinkQueueProducer>,
    pub markers: Option<ControlQueueProducer>,
    pub outbound: Option<Consumer<OutboundLink>>,
    pub latency: Consumer<LatencyReport>,
    pub channels: UiChannels,
    pub control: DesiredWriter,
    pub clock: EngineClock,
    pub spin_interval: DurationUs,
}

/// The ring that the hot thread fills for the link actor, beside the acknowledged state that it
/// writes. Both are absent when the config has no `link:` block.
pub(super) fn outbound_ring(
    bound_link: &Option<BoundLink>,
    acknowledged: AcknowledgedWriter,
) -> (Option<LinkWiring>, Option<Consumer<OutboundLink>>) {
    match bound_link {
        None => (None, None),
        Some(_) => {
            let (producer, consumer) = RingBuffer::<OutboundLink>::new(LINK_RING_CAPACITY);
            (
                Some(LinkWiring {
                    sink: LinkSink::new(producer),
                    acknowledged,
                }),
                Some(consumer),
            )
        }
    }
}

pub(super) fn spawn_link_actor<P>(
    bound: BoundLink,
    start: LinkStart<'_, P>,
    edge_runtime: &Handle,
) -> TaskHandle {
    let on_controller_loss = start
        .config
        .link
        .as_ref()
        .map_or_else(Default::default, |link| link.on_controller_loss);
    LinkActor::spawn(
        LinkActorSetup {
            socket: bound.socket,
            identity: bound.identity,
            guard: bound.guard,
            peers: bound.peers,
            on_controller_loss,
            inbound: start
                .inbound
                .expect("a bound link always builds its input queue"),
            markers: start
                .markers
                .expect("a bound link always builds its control queue"),
            outbound: start
                .outbound
                .expect("a bound link always builds its outbound ring"),
            latency: start.latency,
            channels: start.channels,
            control: start.control,
            clock: start.clock,
            spin_interval: start.spin_interval,
            topic_count: bound.topic_count,
            feature_count: bound.feature_count,
            placement: start.config.senders.placement,
            flush_period: start.config.senders.link_flush(),
            counts: LinkCountsHandle::new(),
        },
        edge_runtime,
    )
}
