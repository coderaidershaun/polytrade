//! The workstation's link loop as a state machine with no socket, clock, thread or channel:
//! datagram bytes and the caller's time go in, deliveries, status and subscribe bytes come out. The
//! browser page and a native test drive it the same way.

use std::net::SocketAddr;

use crate::config::StrategyId;
use crate::link::{
    Envelope, FrameGuard, LINK_MAX_DATAGRAM, Lifecycle, LinkBody, LinkDatagram, LinkDecodeError,
    LinkHash, LinkIdentity, RunState, SequenceGate, Subscribe, TopicId, TopicSet,
};
use crate::msg::ui::{UiBookSnapshot, UiCatalog, UiEvent};
use crate::time::{DurationUs, TsUs};
use crate::{info, warn, warn_repeating};

use super::link_model::{
    CatalogAssembly, ConnectionState, Controller, LinkStatus, PEER_SILENCE_LIMIT, RemoteRunGate,
    RunObservation,
};

/// Comfortably inside the engine's subscription TTL, so two consecutive lost refreshes do not drop
/// this workstation off its subscriber table.
const SUBSCRIBE_INTERVAL: DurationUs = DurationUs::from_secs(2);

/// Matches the engine link actor's own rollup cadence, so the two sides of one link report at the
/// same rhythm.
const REPORT_INTERVAL: DurationUs = DurationUs::from_secs(60);

/// Enumerated rather than [`TopicSet::ALL`]: "all" means every topic the sender offers, which would
/// have an engine unicast strategy payload frames at a process that declares no link fields and can
/// only count them as rejects.
const WANTED_TOPICS: [TopicId; 5] = [
    TopicId::BOOKS,
    TopicId::EVENTS,
    TopicId::CATALOG_INSTRUMENTS,
    TopicId::CATALOG_FEATURES,
    TopicId::LIFECYCLE,
];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LinkTarget {
    pub strategy: StrategyId,
    /// Engines the operator can move between; the first is attached at start.
    pub peers: Vec<SocketAddr>,
    /// The token itself never reaches the session.
    pub token_hash: LinkHash,
    /// Tells apart workstations that start in the same instant. The caller draws it, because the
    /// session owns no randomness.
    pub sender_nonce: u64,
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkSessionError {
    #[error("the link target names no trading engine address - give the session at least one peer")]
    NoPeers,
}

// UiBookSnapshot is large, but boxing it would cost a heap allocation per datagram and the caller
// consumes each delivery at once.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Delivery {
    Book(UiBookSnapshot),
    Event(UiEvent),
    Catalog(UiCatalog),
}

#[derive(Debug, Default)]
struct RejectCounts {
    decode: u64,
    gated: u64,
    foreign: u64,
}

#[derive(Debug, Default)]
struct DeliveredCounts {
    books: u64,
    events: u64,
    catalog: u64,
    lifecycle: u64,
}

/// One trading engine at a time. Everything about that engine (the sequence gate, the catalog, the
/// control assertion) resets when the operator moves to another.
#[derive(Debug)]
pub struct LinkSession {
    peers: Vec<SocketAddr>,
    active: usize,
    session: u64,
    strategy_id: StrategyId,
    identity: LinkIdentity,
    guard: FrameGuard,
    topics: TopicSet,
    gate: SequenceGate,
    run_gate: RemoteRunGate,
    catalog: CatalogAssembly,
    published_catalog_ts_us: Option<TsUs>,
    controller: Controller,
    reported: Option<Lifecycle>,
    last_seen_ts_us: Option<TsUs>,
    last_subscribe_ts_us: Option<TsUs>,
    subscribe_seq: u64,
    outbox: [u8; LINK_MAX_DATAGRAM],
    last_report_ts_us: TsUs,
    is_engine_silent: bool,
    rejected: RejectCounts,
    delivered: DeliveredCounts,
}

impl LinkSession {
    /// `now` is a reading of the engine clock: wall-anchored microseconds that never step
    /// backwards. The first reading doubles as this run's boot stamp, which the engine compares
    /// across runs and which seeds the control epochs, so a counter that starts at zero would rank
    /// below an earlier run of the same workstation.
    ///
    /// # Errors
    /// [`LinkSessionError::NoPeers`] when the target names no engine to attach to.
    pub fn new(target: LinkTarget, now: TsUs) -> Result<Self, LinkSessionError> {
        if target.peers.is_empty() {
            return Err(LinkSessionError::NoPeers);
        }
        let token_hash = target.token_hash;
        let strategy_hash = LinkHash::of_name(target.strategy.as_str());
        // A workstation is not a trading engine, so it has no te-id. The boot stamp and the nonce
        // together keep two workstations, or one that restarts, from presenting to the engine as
        // one sender.
        let sender = format!("polysim-ui-{}-{}", now.micros(), target.sender_nonce);
        let session = Self {
            peers: target.peers,
            active: 0,
            session: 0,
            strategy_id: target.strategy,
            identity: LinkIdentity {
                token_hash,
                strategy_hash,
                sender_te_hash: LinkHash::of_name(&sender),
                boot_ts_us: now,
            },
            guard: FrameGuard {
                token_hash,
                strategy_hash,
                // The workstation declares no link fields, so a strategy payload frame reaching it
                // is another engine's traffic and this rejects it.
                schema_hash: LinkHash::of_fields(&[]),
            },
            topics: TopicSet::new(&WANTED_TOPICS).expect("the wanted topic list is a code literal"),
            gate: SequenceGate::new(),
            run_gate: RemoteRunGate::default(),
            catalog: CatalogAssembly::new(),
            published_catalog_ts_us: None,
            controller: Controller::new(now),
            reported: None,
            last_seen_ts_us: None,
            last_subscribe_ts_us: None,
            subscribe_seq: 0,
            outbox: [0; LINK_MAX_DATAGRAM],
            last_report_ts_us: now,
            is_engine_silent: false,
            rejected: RejectCounts::default(),
            delivered: DeliveredCounts::default(),
        };
        info!("workstation attaching to trading engine {}", session.peer());
        Ok(session)
    }

    /// The session number in [`status`](Self::status) is already raised when this returns, whether
    /// or not the frame yields a delivery. Compare it after every datagram: when it moved, start
    /// the model over first, then apply the delivery.
    pub fn on_datagram(&mut self, bytes: &[u8], now: TsUs) -> Option<Delivery> {
        let datagram = match LinkDatagram::decode(bytes, &self.guard) {
            Ok(datagram) => datagram,
            Err(error) => {
                self.on_decode_error(&error);
                return None;
            }
        };
        if !self.gate.admit(&datagram.envelope, now).is_accepted() {
            self.rejected.gated += 1;
            return None;
        }
        match self.run_gate.observe(&datagram.envelope) {
            RunObservation::First | RunObservation::Same => {}
            RunObservation::Stale => {
                self.rejected.gated += 1;
                return None;
            }
            RunObservation::Restarted => self.begin_engine_run(),
        }
        self.note_engine_heard(now);
        match datagram.body {
            LinkBody::Book(snapshot) => {
                self.delivered.books += 1;
                Some(Delivery::Book(snapshot))
            }
            LinkBody::Event(event) => {
                self.delivered.events += 1;
                Some(Delivery::Event(event))
            }
            LinkBody::CatalogInstrument(frame) => {
                self.delivered.catalog += 1;
                let held = self.catalog.catalog_ts_us();
                self.catalog.accept_instrument(frame);
                self.on_catalog_frame(held)
            }
            LinkBody::CatalogFeature(frame) => {
                self.delivered.catalog += 1;
                let held = self.catalog.catalog_ts_us();
                self.catalog.accept_feature(frame);
                self.on_catalog_frame(held)
            }
            LinkBody::Lifecycle(lifecycle) => {
                self.delivered.lifecycle += 1;
                self.reported = Some(lifecycle);
                self.publish_catalog()
            }
            LinkBody::Subscribe(_) | LinkBody::Payload(_) => {
                self.rejected.foreign += 1;
                None
            }
        }
    }

    /// The datagram to send to the active peer, when one is due: at start, every two seconds, and
    /// at once after an assert or a move to another peer. A due subscribe is spent when it is
    /// returned, so call this only while the socket can carry it. The silence warning and the
    /// session tally ride on the same call.
    // SMELL: the silence warning and the periodic report run only when a caller asks for a
    // subscribe, and the browser asks only while its relay socket is open. A closed relay socket
    // therefore silences the warning too. They want a call of their own.
    pub fn subscribe_due(&mut self, now: TsUs) -> Option<&[u8]> {
        self.check_engine_silence(now);
        self.report_if_due(now);
        let is_due = self
            .last_subscribe_ts_us
            .is_none_or(|at| now.diff(at) >= SUBSCRIBE_INTERVAL);
        if !is_due {
            return None;
        }
        self.last_subscribe_ts_us = Some(now);
        let assertion = self.controller.assertion();
        self.subscribe_seq += 1;
        let datagram = LinkDatagram {
            envelope: Envelope::new(self.identity, TopicId::SUBSCRIBE, self.subscribe_seq),
            body: LinkBody::Subscribe(Subscribe {
                topics: self.topics,
                desired_state: assertion.state,
                desired_epoch: assertion.epoch,
            }),
        };
        let len = datagram.encode(&mut self.outbox);
        Some(&self.outbox[..len])
    }

    pub fn assert_run_state(&mut self, state: RunState) {
        self.controller.assert(state);
        let assertion = self.controller.assertion();
        info!(
            "asserting {:?} on {} at epoch {}",
            assertion.state,
            self.peer(),
            assertion.epoch
        );
        self.last_subscribe_ts_us = None;
    }

    /// Moves to the next engine in the target's list and starts everything about the last one
    /// over. `false`, with nothing changed, when there is no other engine.
    pub fn attach_to_next_peer(&mut self) -> bool {
        if self.peers.len() < 2 {
            return false;
        }
        self.active = (self.active + 1) % self.peers.len();
        self.session += 1;
        self.gate = SequenceGate::new();
        self.run_gate.clear();
        self.catalog = CatalogAssembly::new();
        self.published_catalog_ts_us = None;
        self.controller.release();
        self.reported = None;
        self.last_seen_ts_us = None;
        self.is_engine_silent = false;
        self.last_subscribe_ts_us = None;
        // The subscribe count is kept: an engine remembers the highest one it heard from this
        // sender, so a count restarted on coming back to it would be refused as stale.
        info!("workstation attaching to trading engine {}", self.peer());
        true
    }

    pub fn active_peer(&self) -> usize {
        self.active
    }

    pub fn status(&self, now: TsUs) -> LinkStatus {
        LinkStatus {
            peer: self.peer(),
            peer_index: self.active,
            peer_count: self.peers.len(),
            session: self.session,
            connection: ConnectionState::from_silence(self.last_seen_ts_us.map(|at| now.diff(at))),
            phase: self.reported.map(|reported| reported.phase),
            reported_state: self.reported.map(|reported| reported.run_state),
            asserted_state: self.controller.asserted(),
            control: self.controller.verdict(self.reported),
        }
    }

    fn peer(&self) -> SocketAddr {
        self.peers[self.active]
    }

    fn begin_engine_run(&mut self) {
        self.session += 1;
        self.catalog = CatalogAssembly::new();
        self.published_catalog_ts_us = None;
        self.reported = None;
    }

    fn on_catalog_frame(&mut self, previous_catalog_ts_us: Option<TsUs>) -> Option<Delivery> {
        if previous_catalog_ts_us.is_some()
            && previous_catalog_ts_us != self.catalog.catalog_ts_us()
        {
            self.session += 1;
        }
        self.publish_catalog()
    }

    fn publish_catalog(&mut self) -> Option<Delivery> {
        let catalog_ts_us = self.catalog.catalog_ts_us()?;
        if self.published_catalog_ts_us == Some(catalog_ts_us) {
            return None;
        }
        let catalog = self
            .catalog
            .build(self.strategy_id.as_str(), self.peer(), self.reported?)?;
        self.published_catalog_ts_us = Some(catalog_ts_us);
        info!("catalog complete from {}", self.peer());
        Some(Delivery::Catalog(catalog))
    }

    fn note_engine_heard(&mut self, now: TsUs) {
        self.last_seen_ts_us = Some(now);
        if self.is_engine_silent {
            self.is_engine_silent = false;
            info!("trading engine {} is answering again", self.peer());
        }
    }

    fn check_engine_silence(&mut self, now: TsUs) {
        let is_silent = self
            .last_seen_ts_us
            .is_some_and(|at| now.diff(at) >= PEER_SILENCE_LIMIT);
        if !is_silent || self.is_engine_silent {
            return;
        }
        self.is_engine_silent = true;
        warn!(
            "trading engine {} silent for {}s - the feed on screen is stale, not merely quiet",
            self.peer(),
            PEER_SILENCE_LIMIT.to_secs()
        );
    }

    fn report_if_due(&mut self, now: TsUs) {
        if now.diff(self.last_report_ts_us) < REPORT_INTERVAL {
            return;
        }
        self.last_report_ts_us = now;
        let rejected = &self.rejected;
        let delivered = &self.delivered;
        info!(
            "link session {} delivered[books={} events={} catalog={} lifecycle={}] rejected[decode={} gated={} foreign={}] gate{:?}",
            self.peer(),
            delivered.books,
            delivered.events,
            delivered.catalog,
            delivered.lifecycle,
            rejected.decode,
            rejected.gated,
            rejected.foreign,
            self.gate.counts()
        );
    }

    #[cold]
    fn on_decode_error(&mut self, error: &LinkDecodeError) {
        warn_repeating!(
            self.rejected.decode,
            "workstation rejected {} datagrams from {} (latest: {error})",
            self.rejected.decode,
            self.peer()
        );
    }
}
