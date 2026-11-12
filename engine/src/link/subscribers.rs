//! Actor-side soft state: fixed-capacity subscriber table + sequence gate (drop dupes/reorder at edge).
//! Both take current time parameter (neither reads clock, no wall-clock drag inward).

use std::net::SocketAddr;

use crate::time::{DurationUs, TsUs};

use super::control::TopicSet;
use super::envelope::{Envelope, LinkHash, TopicId};

/// Live subscribers one sender serves. Capacity hit is designed, never silent growth.
pub const LINK_MAX_SUBSCRIBERS: usize = 16;

/// Sequence-gate slots — one per `(sender, topic)` stream.
pub const LINK_MAX_GATE_KEYS: usize = LINK_MAX_SUBSCRIBERS * 4;

/// TTL -> rides out ~2 lost datagrams.
pub const LINK_SUBSCRIPTION_TTL: DurationUs = DurationUs::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SubscriberEntry {
    address: SocketAddr,
    topics: TopicSet,
    expires_ts_us: TsUs,
}

/// Soft-state subscription: datagram inserts/renews address, aged entries expire. Subscribe opens NAT.
#[derive(Debug)]
pub struct SubscriberTable {
    entries: Vec<SubscriberEntry>,
}

impl SubscriberTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(LINK_MAX_SUBSCRIBERS),
        }
    }

    pub fn refresh(&mut self, address: SocketAddr, topics: TopicSet, now: TsUs) -> RefreshOutcome {
        self.entries.retain(|entry| entry.expires_ts_us > now);
        let expires_ts_us = now + LINK_SUBSCRIPTION_TTL;
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.address == address)
        {
            entry.topics = topics;
            entry.expires_ts_us = expires_ts_us;
            return RefreshOutcome::Renewed;
        }
        if self.entries.len() == LINK_MAX_SUBSCRIBERS {
            return RefreshOutcome::Rejected;
        }
        self.entries.push(SubscriberEntry {
            address,
            topics,
            expires_ts_us,
        });
        RefreshOutcome::Added
    }

    pub fn recipients(&self, topic: TopicId, now: TsUs) -> impl Iterator<Item = SocketAddr> + '_ {
        self.entries
            .iter()
            .filter(move |entry| entry.expires_ts_us > now && entry.topics.is_wanted(topic))
            .map(|entry| entry.address)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for SubscriberTable {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    Added,
    Renewed,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GateSlot {
    sender_te_hash: LinkHash,
    topic: TopicId,
    boot_ts_us: TsUs,
    last_seq: u64,
    last_seen_ts_us: TsUs,
}

/// Drops dupe/reorder at edge. Slot = (sender, topic) + boot. Full gate refuses new (restart only).
#[derive(Debug)]
pub struct SequenceGate {
    slots: Vec<GateSlot>,
    counts: GateCounts,
}

impl SequenceGate {
    pub fn new() -> Self {
        Self {
            slots: Vec::with_capacity(LINK_MAX_GATE_KEYS),
            counts: GateCounts::default(),
        }
    }

    pub fn admit(&mut self, envelope: &Envelope, now: TsUs) -> GateVerdict {
        let found = self.slots.iter().position(|slot| {
            slot.sender_te_hash == envelope.sender_te_hash && slot.topic == envelope.topic
        });
        let Some(index) = found else {
            return self.track(envelope, now);
        };
        self.slots[index].last_seen_ts_us = now;
        let tracked = self.slots[index];
        if envelope.boot_ts_us < tracked.boot_ts_us {
            self.counts.stale_boots += 1;
            return GateVerdict::StaleBoot {
                tracked_boot_ts_us: tracked.boot_ts_us,
            };
        }
        if envelope.boot_ts_us > tracked.boot_ts_us {
            self.slots[index].boot_ts_us = envelope.boot_ts_us;
            self.slots[index].last_seq = envelope.seq;
            self.counts.restarts += 1;
            return GateVerdict::Restarted;
        }
        if envelope.seq <= tracked.last_seq {
            self.counts.stale += 1;
            return GateVerdict::Stale {
                last_seq: tracked.last_seq,
            };
        }
        self.slots[index].last_seq = envelope.seq;
        GateVerdict::Accepted
    }

    pub fn counts(&self) -> GateCounts {
        self.counts
    }

    #[cold]
    fn track(&mut self, envelope: &Envelope, now: TsUs) -> GateVerdict {
        let slot = GateSlot {
            sender_te_hash: envelope.sender_te_hash,
            topic: envelope.topic,
            boot_ts_us: envelope.boot_ts_us,
            last_seq: envelope.seq,
            last_seen_ts_us: now,
        };
        if self.slots.len() < LINK_MAX_GATE_KEYS {
            self.slots.push(slot);
            return GateVerdict::Accepted;
        }
        let Some(stalest) = self.stalest_expired_slot(now) else {
            self.counts.untracked += 1;
            return GateVerdict::Untracked;
        };
        self.slots[stalest] = slot;
        self.counts.evicted += 1;
        GateVerdict::Accepted
    }

    fn stalest_expired_slot(&self, now: TsUs) -> Option<usize> {
        let (index, slot) = self
            .slots
            .iter()
            .enumerate()
            .min_by_key(|(_, slot)| slot.last_seen_ts_us)?;
        (now.diff(slot.last_seen_ts_us) >= LINK_SUBSCRIPTION_TTL).then_some(index)
    }
}

impl Default for SequenceGate {
    fn default() -> Self {
        Self::new()
    }
}

/// One counter per verdict. Operator distinguishes lossy network (stale), collided senders (stale_boots), full gate (untracked). evicted = real churn.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateCounts {
    pub restarts: u64,
    pub stale: u64,
    pub stale_boots: u64,
    pub evicted: u64,
    pub untracked: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateVerdict {
    Accepted,
    Restarted,
    Stale { last_seq: u64 },
    StaleBoot { tracked_boot_ts_us: TsUs },
    Untracked,
}

impl GateVerdict {
    #[inline]
    pub fn is_accepted(self) -> bool {
        matches!(self, GateVerdict::Accepted | GateVerdict::Restarted)
    }
}
