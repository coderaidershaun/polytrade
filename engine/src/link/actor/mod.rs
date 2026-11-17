//! The link actor: one task that owns the UDP socket, the subscriber table and the sequence gate.
//! This root file wires the receive path, the send path and the control plane to one heartbeat.

mod control_plane;
mod inbound;
mod outbound;

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use rtrb::Consumer;
use tokio::net::UdpSocket;
use tokio::runtime::Handle;

use crate::hot::spawn::{ControlQueueProducer, LinkQueueProducer};
use crate::info;
use crate::msg::time::{DurationUs, EngineClock, LocalTs};
use crate::msg::ui::LatencyReport;
use crate::msg::ui::channel::UiChannels;
use crate::runtime::config::{
    ConfigError, ControllerLoss, ExecutionMode, PeerSubscription, SenderPlacement,
};
use crate::runtime::shutdown::{DesiredWriter, TaskHandle};
use crate::venues::edge::sender::SendWait;

use super::control::{LINK_MAX_TOPICS, RunPhase, TopicSet};
use super::envelope::{FrameGuard, LINK_MAX_DATAGRAM, LinkIdentity, TopicId};
use super::frame::{LinkBody, OutboundLink};
use super::subscribers::{LINK_MAX_SUBSCRIBERS, SequenceGate, SubscriberTable};
use inbound::PeerState;

pub use control_plane::{AssertionVerdict, ControlPlane};

/// Catalog, lifecycle, staleness, control repair. 1s granularity for display state.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(1);

/// Inside LINK_SUBSCRIPTION_TTL so 2 lost datagrams don't drop us.
const SUBSCRIBE_INTERVAL: Duration = Duration::from_secs(2);

/// Healthy run silent; nonzero rollup only.
const REPORT_TICKS: u64 = 60;

/// One peer whose feed this engine wants.
pub struct PeerFeed {
    pub address: SocketAddr,
    pub topics: TopicSet,
}

impl PeerFeed {
    /// Resolves the topic names a config file gave against the ones this strategy declares in code.
    pub(crate) fn resolve(
        peer: &PeerSubscription,
        strategy_topics: &[&'static str],
    ) -> Result<Self, ConfigError> {
        Ok(PeerFeed {
            address: peer.address,
            topics: resolve_topics(peer, strategy_topics)?,
        })
    }
}

fn resolve_topics(
    peer: &PeerSubscription,
    strategy_topics: &[&'static str],
) -> Result<TopicSet, ConfigError> {
    if peer.topics.is_empty() {
        return Ok(TopicSet::ALL);
    }
    let mut ids = Vec::with_capacity(peer.topics.len() + 1);
    for name in &peer.topics {
        let id = TopicId::from_name(name, strategy_topics).ok_or_else(|| {
            ConfigError::UnknownLinkTopic {
                address: address_label(peer.address),
                topic: name.clone(),
            }
        })?;
        ids.push(id);
    }
    // The lifecycle topic cannot be opted out of: it carries the heartbeat, and without it a
    // receiver cannot tell a dead feed from a quiet one.
    if !ids.contains(&TopicId::LIFECYCLE) {
        ids.push(TopicId::LIFECYCLE);
    }
    TopicSet::new(&ids).map_err(|_| ConfigError::TooManyPeerTopics {
        address: address_label(peer.address),
        count: ids.len(),
        max: LINK_MAX_TOPICS,
    })
}

fn address_label(address: SocketAddr) -> Box<str> {
    address.to_string().into_boxed_str()
}

/// Everything the link actor owns. Each part is public, so a test can build an actor on loopback
/// with no hot thread.
pub struct LinkActorSetup {
    /// Bound and set non-blocking. The actor registers it with the runtime that runs the actor.
    pub socket: std::net::UdpSocket,
    pub identity: LinkIdentity,
    pub guard: FrameGuard,
    pub peers: Vec<PeerFeed>,
    pub on_controller_loss: ControllerLoss,
    pub inbound: LinkQueueProducer,
    /// Run-control markers only, so a flood of peer frames never keeps a marker out.
    pub markers: ControlQueueProducer,
    pub outbound: Consumer<OutboundLink>,
    pub channels: UiChannels,
    /// The metrics thread's reports, about one a second.
    pub latency: Consumer<LatencyReport>,
    /// The one writer of the desired state, which also reads what the fold acknowledged.
    pub control: DesiredWriter,
    pub clock: EngineClock,
    pub spin_interval: DurationUs,
    pub topic_count: usize,
    pub feature_count: u16,
    pub placement: SenderPlacement,
    /// How often frames go out while the senders are shared. Pinned, they go out on each pass.
    pub flush_period: Duration,
    /// Where the actor publishes its counts at each heartbeat.
    pub counts: LinkCountsHandle,
}

pub struct LinkActor;

impl LinkActor {
    /// The socket turns into a tokio socket inside the task, so its reactor is the runtime `rt`
    /// that runs the actor.
    pub fn spawn(setup: LinkActorSetup, rt: &Handle) -> TaskHandle {
        let join = rt.spawn(crate::log::tag_task("link", async move {
            Actor::new(setup).run().await;
        }));
        TaskHandle::new(join, "link")
    }
}

enum Wake {
    Datagram(io::Result<(usize, SocketAddr)>),
    Flush,
    Announce,
    Refresh,
}

/// What the link actor refused or lost since it started, one count for each cause.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LinkCounts {
    pub decode_failed: u64,
    pub gate_rejected: u64,
    pub topic_ignored: u64,
    pub send_failed: u64,
    pub receive_failed: u64,
    pub table_full_refused: u64,
    pub peer_silenced: u64,
}

/// The counts of the link actor as of its last heartbeat, for a reader on another thread.
#[derive(Debug, Clone, Default)]
pub struct LinkCountsHandle {
    latest: Arc<Mutex<LinkCounts>>,
}

impl LinkCountsHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(&self) -> LinkCounts {
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish(&self, counts: LinkCounts) {
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner) = counts;
    }
}

struct Actor {
    socket: Arc<UdpSocket>,
    identity: LinkIdentity,
    guard: FrameGuard,
    subscribers: SubscriberTable,
    gate: SequenceGate,
    peers: Vec<PeerState>,
    inbound: LinkQueueProducer,
    markers: ControlQueueProducer,
    outbound: Consumer<OutboundLink>,
    channels: UiChannels,
    latency: Consumer<LatencyReport>,
    control: ControlPlane,
    clock: EngineClock,
    spin_interval: DurationUs,
    feature_count: u16,
    placement: SenderPlacement,
    flush_period: Duration,
    phase: RunPhase,
    /// `None` until the run announces its execution mode.
    execution_mode: Option<ExecutionMode>,
    catalog_frames: Vec<(TopicId, LinkBody)>,
    outbound_seq_by_topic: Vec<u64>,
    recipients: Vec<SocketAddr>,
    buffer: Box<[u8; LINK_MAX_DATAGRAM]>,
    announce_ticks: u64,
    counts: LinkCounts,
    published_counts: LinkCountsHandle,
}

impl Actor {
    fn new(setup: LinkActorSetup) -> Self {
        Self {
            socket: Arc::new(
                UdpSocket::from_std(setup.socket)
                    .expect("a bound socket registers with the runtime that runs the link"),
            ),
            identity: setup.identity,
            guard: setup.guard,
            subscribers: SubscriberTable::new(),
            gate: SequenceGate::new(),
            peers: setup
                .peers
                .into_iter()
                .map(|feed| PeerState {
                    feed,
                    last_seen_ts_us: None,
                    is_silent: false,
                })
                .collect(),
            inbound: setup.inbound,
            markers: setup.markers,
            outbound: setup.outbound,
            channels: setup.channels,
            latency: setup.latency,
            control: ControlPlane::new(setup.control, setup.on_controller_loss),
            clock: setup.clock,
            spin_interval: setup.spin_interval,
            feature_count: setup.feature_count,
            placement: setup.placement,
            flush_period: setup.flush_period,
            phase: RunPhase::Starting,
            execution_mode: None,
            catalog_frames: Vec::new(),
            outbound_seq_by_topic: vec![0; setup.topic_count],
            recipients: Vec::with_capacity(LINK_MAX_SUBSCRIBERS),
            buffer: Box::new([0; LINK_MAX_DATAGRAM]),
            announce_ticks: 0,
            counts: LinkCounts::default(),
            published_counts: setup.counts,
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the link actor stamps each peer frame and marker with message time before the tape records it"
    )]
    async fn run(mut self) {
        let socket = Arc::clone(&self.socket);
        let mut flush = SendWait::new(self.placement, tokio::time::interval(self.flush_period));
        let mut announce = tokio::time::interval(ANNOUNCE_INTERVAL);
        let mut refresh = tokio::time::interval(SUBSCRIBE_INTERVAL);
        let mut inbox = [0u8; LINK_MAX_DATAGRAM];
        loop {
            let wake = tokio::select! {
                received = socket.recv_from(&mut inbox) => Wake::Datagram(received),
                () = flush.wait() => Wake::Flush,
                _ = announce.tick() => Wake::Announce,
                _ = refresh.tick() => Wake::Refresh,
            };
            let now = self.clock.now();
            match wake {
                Wake::Datagram(Ok((len, from))) => self.on_datagram(&inbox[..len], from, now),
                Wake::Datagram(Err(error)) => self.on_receive_error(&error),
                Wake::Flush => self.flush_feeds(now),
                Wake::Announce => self.announce(now),
                Wake::Refresh => self.refresh_subscriptions(),
            }
        }
    }

    fn announce(&mut self, now: LocalTs) {
        self.poll_lifecycle(now);
        let lifecycle = LinkBody::Lifecycle(self.lifecycle());
        self.send(TopicId::LIFECYCLE, lifecycle, now);
        for index in 0..self.catalog_frames.len() {
            let (topic, body) = self.catalog_frames[index];
            self.send(topic, body, now);
        }
        // A dead-man park goes out at once. The push after it is the one each heartbeat makes, so a
        // marker that a full queue dropped goes out again.
        if self.control.check_controller(now).is_some() {
            self.push_marker_if_needed(now);
        }
        self.push_marker_if_needed(now);
        self.check_peer_silence(now);
        self.published_counts.publish(self.counts);
        self.announce_ticks += 1;
        if self.announce_ticks.is_multiple_of(REPORT_TICKS) {
            self.report_counts();
        }
    }

    #[cold]
    fn report_counts(&self) {
        let counts = &self.counts;
        let total = counts.decode_failed
            + counts.gate_rejected
            + counts.topic_ignored
            + counts.send_failed
            + counts.receive_failed
            + counts.table_full_refused
            + counts.peer_silenced;
        if total == 0 {
            return;
        }
        info!(
            "link rejected[decode_failed={} gate_rejected={} topic_ignored={} send_failed={} receive_failed={} table_full_refused={} peer_silenced={}] gate{:?}",
            counts.decode_failed,
            counts.gate_rejected,
            counts.topic_ignored,
            counts.send_failed,
            counts.receive_failed,
            counts.table_full_refused,
            counts.peer_silenced,
            self.gate.counts()
        );
    }
}
