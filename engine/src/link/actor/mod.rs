//! The link actor: one tokio task that owns the UDP socket, the subscriber table and the sequence
//! gate. Inbound frames are validated and pushed to the engine like any other input, and outbound
//! frames are drained on timers from the hot thread's UI rings, which nothing else reads.

mod outbound;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rtrb::Consumer;
use tokio::net::UdpSocket;
use tokio::runtime::Handle;

use crate::config::{ConfigError, ControllerLoss, ExecutionMode, PeerSubscription};
use crate::hot::spawn::LinkQueueProducer;
use crate::msg::inbound::{InboundMessage, RunControl};
use crate::msg::ui::UiChannels;
use crate::shutdown::{RunAssertion, RunControlGate, TaskHandle};
use crate::time::{DurationUs, EngineClock, TsUs};
use crate::{info, warn, warn_repeating};

use super::control::{LINK_MAX_TOPICS, RunPhase, RunState, Subscribe, TopicSet};
use super::envelope::{FrameGuard, LINK_MAX_DATAGRAM, LinkDecodeError, LinkIdentity, TopicId};
use super::frame::{
    InboundLink, LinkBody, LinkDatagram, LinkFrame, LinkOrigin, LinkPayload, OutboundLink,
};
use super::subscribers::{
    LINK_MAX_SUBSCRIBERS, LINK_SUBSCRIPTION_TTL, RefreshOutcome, SequenceGate, SubscriberTable,
};

/// Not awaitable; 10ms keeps DOM responsive vs blocking socket's latency+2000 idle syscalls/s.
const FLUSH_INTERVAL: Duration = Duration::from_millis(10);

/// Catalog, lifecycle, staleness, control repair. 1s granularity for display state.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(1);

/// Inside LINK_SUBSCRIPTION_TTL so 2 lost datagrams don't drop us.
const SUBSCRIBE_INTERVAL: Duration = Duration::from_secs(2);

/// Four missed heartbeats. UDP silence indistinguishable from quiet peer (dangerous: in-process ring can't silently lose producer).
const PEER_SILENCE_LIMIT: DurationUs = LINK_SUBSCRIPTION_TTL;

/// Healthy run silent; nonzero rollup only.
const REPORT_TICKS: u64 = 60;

/// One peer whose feed this engine wants.
pub(crate) struct PeerFeed {
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

pub(crate) struct LinkActorSetup {
    pub socket: UdpSocket,
    pub identity: LinkIdentity,
    pub guard: FrameGuard,
    pub peers: Vec<PeerFeed>,
    pub on_controller_loss: ControllerLoss,
    pub inbound: LinkQueueProducer,
    pub outbound: Consumer<OutboundLink>,
    pub channels: UiChannels,
    /// Link writes desired, reads acknowledged; dispatch does reverse, never reads desired.
    pub control: RunControlGate,
    pub clock: EngineClock,
    pub spin_interval: DurationUs,
    pub topic_count: usize,
    pub feature_count: u16,
}

pub(crate) struct LinkActor;

impl LinkActor {
    pub(crate) fn spawn(setup: LinkActorSetup, rt: &Handle) -> TaskHandle {
        let join = rt.spawn(crate::log::tag_task("link", Actor::new(setup).run()));
        TaskHandle::new(join, "link")
    }
}

enum Wake {
    Datagram(io::Result<(usize, SocketAddr)>),
    Flush,
    Announce,
    Refresh,
}

struct PeerState {
    feed: PeerFeed,
    last_seen_ts_us: Option<TsUs>,
    is_silent: bool,
}

#[derive(Default)]
struct RejectCounts {
    decode_failed: u64,
    gate_rejected: u64,
    topic_ignored: u64,
    send_failed: u64,
    receive_failed: u64,
    table_full_refused: u64,
    peer_silenced: u64,
}

struct Actor {
    socket: Arc<UdpSocket>,
    identity: LinkIdentity,
    guard: FrameGuard,
    subscribers: SubscriberTable,
    gate: SequenceGate,
    peers: Vec<PeerState>,
    on_controller_loss: ControllerLoss,
    inbound: LinkQueueProducer,
    outbound: Consumer<OutboundLink>,
    channels: UiChannels,
    control: RunControlGate,
    clock: EngineClock,
    spin_interval: DurationUs,
    feature_count: u16,
    phase: RunPhase,
    /// `None` until the run announces its execution mode.
    execution_mode: Option<ExecutionMode>,
    catalog_frames: Vec<(TopicId, LinkBody)>,
    outbound_seq_by_topic: Vec<u64>,
    recipients: Vec<SocketAddr>,
    buffer: Box<[u8; LINK_MAX_DATAGRAM]>,
    last_control_ts_us: Option<TsUs>,
    is_controller_lost: bool,
    announce_ticks: u64,
    counts: RejectCounts,
}

impl Actor {
    fn new(setup: LinkActorSetup) -> Self {
        Self {
            socket: Arc::new(setup.socket),
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
            on_controller_loss: setup.on_controller_loss,
            inbound: setup.inbound,
            outbound: setup.outbound,
            channels: setup.channels,
            control: setup.control,
            clock: setup.clock,
            spin_interval: setup.spin_interval,
            feature_count: setup.feature_count,
            phase: RunPhase::Starting,
            execution_mode: None,
            catalog_frames: Vec::new(),
            outbound_seq_by_topic: vec![0; setup.topic_count],
            recipients: Vec::with_capacity(LINK_MAX_SUBSCRIBERS),
            buffer: Box::new([0; LINK_MAX_DATAGRAM]),
            last_control_ts_us: None,
            is_controller_lost: false,
            announce_ticks: 0,
            counts: RejectCounts::default(),
        }
    }

    async fn run(mut self) {
        let socket = Arc::clone(&self.socket);
        let mut flush = tokio::time::interval(FLUSH_INTERVAL);
        let mut announce = tokio::time::interval(ANNOUNCE_INTERVAL);
        let mut refresh = tokio::time::interval(SUBSCRIBE_INTERVAL);
        let mut inbox = [0u8; LINK_MAX_DATAGRAM];
        loop {
            let wake = tokio::select! {
                received = socket.recv_from(&mut inbox) => Wake::Datagram(received),
                _ = flush.tick() => Wake::Flush,
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

    fn on_datagram(&mut self, bytes: &[u8], from: SocketAddr, now: TsUs) {
        let datagram = match LinkDatagram::decode(bytes, &self.guard) {
            Ok(datagram) => datagram,
            Err(error) => return self.on_decode_error(&error),
        };
        if !self.gate.admit(&datagram.envelope, now).is_accepted() {
            self.counts.gate_rejected += 1;
            return;
        }
        self.note_peer_seen(from, now);
        match datagram.body {
            LinkBody::Subscribe(subscribe) => self.on_subscribe(from, subscribe, now),
            LinkBody::Payload(payload) => {
                self.on_payload(LinkOrigin::from(&datagram.envelope), payload, now)
            }
            // Engine serves engine topics (never consumes peer's); delivery still proves peer alive
            _ => self.counts.topic_ignored += 1,
        }
    }

    /// Peer frame -> hot path: stamped engine's clock, not sender's (cross-region skew -> reordering).
    fn on_payload(&mut self, origin: LinkOrigin, payload: LinkPayload, received_ts_us: TsUs) {
        self.inbound.push(InboundMessage::Link(InboundLink {
            frame: LinkFrame { origin, payload },
            received_ts_us,
            queued_ts_us: self.clock.now(),
        }));
    }

    fn on_subscribe(&mut self, from: SocketAddr, subscribe: Subscribe, now: TsUs) {
        if self.subscribers.refresh(from, subscribe.topics, now) == RefreshOutcome::Rejected {
            self.on_table_full(from);
        }
        if subscribe.desired_epoch == 0 {
            // Epoch 0: peer subscribes without claiming control
            return;
        }
        self.last_control_ts_us = Some(now);
        self.is_controller_lost = false;
        let assertion = RunAssertion {
            state: subscribe.desired_state,
            epoch: subscribe.desired_epoch,
        };
        if self.control.desired().accept_if_newer(assertion) {
            info!(
                "controller {from} asserts {:?} at epoch {}",
                assertion.state, assertion.epoch
            );
        }
        self.push_marker_if_needed(now);
    }

    /// Level-triggered: drop+count queue means edge-triggered push alone leaves adapters down on full slot. Dups harmless (dedup on epoch).
    fn push_marker_if_needed(&mut self, now: TsUs) {
        let Some(desired) = self.control.pending() else {
            return;
        };
        self.inbound.push(InboundMessage::RunControl(RunControl {
            desired,
            received_ts_us: now,
            queued_ts_us: now,
        }));
    }

    fn note_peer_seen(&mut self, from: SocketAddr, now: TsUs) {
        let Some(peer) = self.peers.iter_mut().find(|peer| peer.feed.address == from) else {
            return;
        };
        peer.last_seen_ts_us = Some(now);
        if peer.is_silent {
            peer.is_silent = false;
            info!("link peer {from} is live again");
        }
    }

    fn check_peer_silence(&mut self, now: TsUs) {
        for peer in &mut self.peers {
            let is_silent = peer
                .last_seen_ts_us
                .is_none_or(|seen| now.diff(seen) >= PEER_SILENCE_LIMIT);
            if !is_silent || peer.is_silent {
                continue;
            }
            let address = peer.feed.address;
            peer.is_silent = true;
            self.counts.peer_silenced += 1;
            warn!(
                "link peer {address} silent for {}s — its feed is stale, not merely quiet",
                PEER_SILENCE_LIMIT.to_secs()
            );
        }
    }

    /// Dead controller must not decide fate; holding default, dead-man opt-in.
    fn check_controller(&mut self, now: TsUs) {
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

    #[cold]
    fn on_decode_error(&mut self, error: &LinkDecodeError) {
        warn_repeating!(
            self.counts.decode_failed,
            "link rejected {} datagrams (latest: {error})",
            self.counts.decode_failed
        );
    }

    #[cold]
    fn on_receive_error(&mut self, error: &io::Error) {
        warn_repeating!(
            self.counts.receive_failed,
            "link socket receive failed {} times (latest: {error})",
            self.counts.receive_failed
        );
    }

    #[cold]
    fn on_table_full(&mut self, from: SocketAddr) {
        warn_repeating!(
            self.counts.table_full_refused,
            "link subscriber table full at {LINK_MAX_SUBSCRIBERS} — refused {} subscriptions (latest {from})",
            self.counts.table_full_refused
        );
    }
}
