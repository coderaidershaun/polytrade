//! The receiving half of the link: it checks each datagram, hands peer frames and controller
//! assertions on, and watches each peer this engine subscribes to for silence.

use std::io;
use std::net::SocketAddr;

use crate::msg::inbound::{InboundMessage, MessageBody, MessageHeader};
use crate::msg::run_state::RunAssertion;
use crate::msg::time::{DurationUs, LocalTs};
use crate::{info, warn, warn_repeating};

use super::super::control::Subscribe;
use super::super::envelope::LinkDecodeError;
use super::super::frame::{LinkBody, LinkDatagram, LinkFrame, LinkOrigin, LinkPayload};
use super::super::subscribers::{LINK_MAX_SUBSCRIBERS, LINK_SUBSCRIPTION_TTL, RefreshOutcome};
use super::control_plane::AssertionVerdict;
use super::{Actor, PeerFeed};

/// Four missed heartbeats. UDP silence indistinguishable from quiet peer (dangerous: in-process ring can't silently lose producer).
pub(super) const PEER_SILENCE_LIMIT: DurationUs = LINK_SUBSCRIPTION_TTL;

pub(super) struct PeerState {
    pub(super) feed: PeerFeed,
    pub(super) last_seen_ts_us: Option<LocalTs>,
    pub(super) is_silent: bool,
}

impl Actor {
    pub(super) fn on_datagram(&mut self, bytes: &[u8], from: SocketAddr, now: LocalTs) {
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
    #[expect(
        clippy::disallowed_methods,
        reason = "the link actor stamps each peer frame and marker with message time before the tape records it"
    )]
    fn on_payload(&mut self, origin: LinkOrigin, payload: LinkPayload, received_ts_us: LocalTs) {
        self.inbound.push(InboundMessage {
            header: MessageHeader {
                received_ts_us,
                queued_ts_us: self.clock.now(),
            },
            body: MessageBody::Link(LinkFrame { origin, payload }),
        });
    }

    fn on_subscribe(&mut self, from: SocketAddr, subscribe: Subscribe, now: LocalTs) {
        if self.subscribers.refresh(from, subscribe.topics, now) == RefreshOutcome::Rejected {
            self.on_table_full(from);
        }
        let assertion = RunAssertion {
            state: subscribe.desired_state,
            epoch: subscribe.desired_epoch,
        };
        if self.control.on_assertion(from, assertion, now) != AssertionVerdict::NoClaim {
            self.push_marker_if_needed(now);
        }
    }

    fn note_peer_seen(&mut self, from: SocketAddr, now: LocalTs) {
        let Some(peer) = self.peers.iter_mut().find(|peer| peer.feed.address == from) else {
            return;
        };
        peer.last_seen_ts_us = Some(now);
        if peer.is_silent {
            peer.is_silent = false;
            info!("link peer {from} is live again");
        }
    }

    // SMELL: a silent peer only gets a warning line and a count, and the strategy is never told, so
    // it keeps acting on the last frame of that peer. Reconnects of a venue edge also have no cap.
    pub(super) fn check_peer_silence(&mut self, now: LocalTs) {
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

    #[cold]
    fn on_decode_error(&mut self, error: &LinkDecodeError) {
        warn_repeating!(
            self.counts.decode_failed,
            "link rejected {} datagrams (latest: {error})",
            self.counts.decode_failed
        );
    }

    #[cold]
    pub(super) fn on_receive_error(&mut self, error: &io::Error) {
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
