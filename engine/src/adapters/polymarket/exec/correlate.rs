//! Correlation of engine orders to venue order ids. Polymarket mints the order id in the placement
//! answer, so between request and answer, the order exists at the venue under an unknown name.
//! Stream frames naming unknown orders are held (not dropped) pending the mapping. Cancels are
//! withheld during the delayed hold window (venue + wire latency) to avoid refusals. Held frames
//! with no mapping eventually trigger a loud alert (fill not yet seen in the ledger).

use crate::adapters::exec::{ExecEffect, ExecRequest, MirroredOrder, RequestId};
use crate::ids::ClientOrderId;
use crate::msg::exec::{ExecEvent, ExecKind, Provenance};
use crate::time::{DurationUs, TsUs};

use super::codec::UnmappedOrder;

// Held frames pending order id mappings. Small because the window is one HTTP round trip; excess signals a venue storm.
pub const PENDING_CAPACITY: usize = 128;

// TTL for held frames. Long enough for placement answer + recovery resync; short enough to report unmapped fills while actionable.
pub const PENDING_TTL: DurationUs = DurationUs::from_micros(10_000_000);

// Venue taker hold + wire latency. Cancels sent during this window are refused (hard reject cost).
pub const DELAYED_HOLD: DurationUs = DurationUs::from_micros(300_000);

/// A user-stream frame whose order this run cannot yet name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldFrame {
    pub text: String,
    pub held_since: TsUs,
}

/// Frames held for a mapping, oldest first.
#[derive(Debug)]
pub struct PendingFrames {
    frames: Vec<HeldFrame>,
    dropped: u64,
    abandoned: u64,
}

impl Default for PendingFrames {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingFrames {
    pub fn new() -> Self {
        Self {
            frames: Vec::with_capacity(PENDING_CAPACITY),
            dropped: 0,
            abandoned: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn abandoned(&self) -> u64 {
        self.abandoned
    }

    // Evicts oldest frame when full (newer frames describe more recent state; oldest is least likely to map).
    pub fn hold(&mut self, text: String, now: TsUs) {
        self.re_hold(HeldFrame {
            text,
            held_since: now,
        });
    }

    /// Every held frame, for a re-read against a mapping that has since landed. Frames that are
    /// still unattributable are handed back by [`PendingFrames::hold`].
    pub fn drain(&mut self) -> Vec<HeldFrame> {
        std::mem::take(&mut self.frames)
    }

    pub fn expired(&mut self, now: TsUs) -> Vec<HeldFrame> {
        let mut expired = Vec::new();
        self.frames.retain(|frame| {
            let is_live = now.diff(frame.held_since) < PENDING_TTL;
            if !is_live {
                expired.push(frame.clone());
            }
            is_live
        });
        self.abandoned += expired.len() as u64;
        expired
    }

    pub fn re_hold(&mut self, frame: HeldFrame) {
        if self.frames.len() >= PENDING_CAPACITY {
            self.frames.remove(0);
            self.dropped += 1;
        }
        self.frames.push(frame);
    }
}

/// Orders inside the venue's taker hold, and the cancels waiting for it to lapse.
#[derive(Debug, Default)]
pub struct DelayedOrders {
    holds: Vec<DelayedHold>,
    withheld: Vec<WithheldCancel>,
    withholds: u64,
}

/// One order the venue accepted and will not cancel until its hold lapses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DelayedHold {
    client_id: ClientOrderId,
    release_at: TsUs,
}

/// A cancel the core already decided on and the venue would refuse. The whole effect is kept, not
/// just its subject: re-deciding later would mint a second request id for an order the core already
/// believes has one cancel out, and the never-retry rule depends on there being exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WithheldCancel {
    pub request_id: RequestId,
    pub request: ExecRequest,
    pub recon_seq: u64,
    pub release_at: TsUs,
}

impl WithheldCancel {
    /// Restored to the shape the dispatcher takes.
    pub fn into_effect(self) -> ExecEffect {
        ExecEffect::Send {
            request_id: self.request_id,
            request: self.request,
        }
    }
}

impl DelayedOrders {
    pub fn withholds(&self) -> u64 {
        self.withholds
    }

    /// The venue accepted a placement it will not match for another 250 ms and will not cancel
    /// during.
    pub fn on_delayed(&mut self, client_id: ClientOrderId, answered_at: TsUs) {
        let release_at = answered_at + DELAYED_HOLD;
        match self
            .holds
            .iter_mut()
            .find(|hold| hold.client_id == client_id)
        {
            Some(hold) => hold.release_at = release_at,
            None => self.holds.push(DelayedHold {
                client_id,
                release_at,
            }),
        }
    }

    pub fn forget(&mut self, client_id: ClientOrderId) {
        self.holds.retain(|hold| hold.client_id != client_id);
    }

    /// When this order may be cancelled, if it may not be cancelled yet.
    pub fn held_until(&self, client_id: ClientOrderId, now: TsUs) -> Option<TsUs> {
        self.holds
            .iter()
            .find(|hold| hold.client_id == client_id)
            .map(|hold| hold.release_at)
            .filter(|release_at| now < *release_at)
    }

    pub fn withhold(&mut self, cancel: WithheldCancel) {
        self.withholds += 1;
        self.withheld.push(cancel);
    }

    /// Cancels whose hold has lapsed, in the order they were decided.
    pub fn released(&mut self, now: TsUs) -> Vec<WithheldCancel> {
        let mut released = Vec::new();
        self.withheld.retain(|cancel| {
            let is_waiting = now < cancel.release_at;
            if !is_waiting {
                released.push(*cancel);
            }
            is_waiting
        });
        released
    }
}

/// Whether this event obliges the edge to re-read what the account holds.
///
/// Not a freshness preference. The hot account table releases a fill's reservation only against a
/// balance stamped LATER than the reservation was taken, so an edge that reports a fill and no new
/// balance leaves that reservation held forever — and the next flatten starves at the funds gate
/// with a wallet that is demonstrably full.
///
/// Only the two payloads that REPORT a fill qualify. A snapshot carries a filled size too, and
/// treating it as a fill would make every resync trigger the read that triggers the next resync.
pub fn restates_balances(event: &ExecEvent) -> bool {
    match event.kind {
        // A maker fill: the venue's order update is the only report of it that exists.
        ExecKind::ReportTrade => true,
        // A taker fill: the placement answer reports it once and never again.
        ExecKind::AckPlaced => event.last_qty.0 > 0,
        _ => false,
    }
}

/// What to do with an open order resting under an id this run never recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnmappedVerdict {
    /// This run placed it and lost the answer; adopt it under this client id to regain the ability
    /// to cancel it.
    Adopt(ClientOrderId),
    /// A placement on this side is still in flight, so the order its answer will name cannot be told
    /// apart from a person's own order on the same side. Neither is classified until the place
    /// answers or times out — the resync pass re-reads and decides then.
    Defer,
    /// Nothing this run placed explains it. On this venue the same credentials reach the website's
    /// own order entry, and cancelling a person's order is not a recovery.
    LeaveAlone,
}

/// Classify an order resting under an id this run never recorded.
///
/// The deferral is the whole point. A placement whose POST is still outstanding has a mirror entry
/// that is [`Provenance::Mine`], carries no venue id yet, and is NOT ambiguous — its answer is still
/// expected. While such an order exists on the side, an unmapped resting order there could be it OR
/// a person's order at the venue, and adopting either is wrong: adopting the person's binds a second
/// venue id to our slot the moment our own answer lands, double-folding the position. Only once a
/// placement's answer will never come — it timed out or the transport failed, which marks the mirror
/// entry ambiguous — is an unmapped order on the side safe to adopt.
pub fn classify_unmapped(
    mirror: &[MirroredOrder],
    unmapped: &UnmappedOrder,
    has_venue_id: impl Fn(ClientOrderId) -> bool,
) -> UnmappedVerdict {
    let place_in_flight = mirror.iter().any(|order| {
        order.instrument == unmapped.instrument
            && order.side == unmapped.side
            && order.provenance == Provenance::Mine
            && !order.is_ambiguous
            && !has_venue_id(order.client_id)
    });
    if place_in_flight {
        return UnmappedVerdict::Defer;
    }
    match adoption_candidate(mirror, unmapped, has_venue_id) {
        Some(client_id) => UnmappedVerdict::Adopt(client_id),
        None => UnmappedVerdict::LeaveAlone,
    }
}

/// Which of this run's orders an unmapped resting order might BE.
///
/// A candidate is one this run placed on the same instrument and side whose venue id never came
/// back — that is exactly the order a lost placement answer leaves behind, and adopting it is the
/// only way the engine regains the ability to cancel it. Price and size break a tie, because a
/// ladder can hold more than one such slot.
///
/// `None` means nothing this run placed explains the order. The gate above admits only slots whose
/// answer is no longer coming, so a live in-flight placement never reaches here.
fn adoption_candidate(
    mirror: &[MirroredOrder],
    unmapped: &UnmappedOrder,
    has_venue_id: impl Fn(ClientOrderId) -> bool,
) -> Option<ClientOrderId> {
    let candidates: Vec<&MirroredOrder> = mirror
        .iter()
        .filter(|order| {
            order.instrument == unmapped.instrument
                && order.side == unmapped.side
                && order.provenance == Provenance::Mine
                && !has_venue_id(order.client_id)
        })
        .collect();
    candidates
        .iter()
        .find(|order| order.price == unmapped.price && order.qty == unmapped.qty)
        .or_else(|| candidates.first())
        .map(|order| order.client_id)
}
