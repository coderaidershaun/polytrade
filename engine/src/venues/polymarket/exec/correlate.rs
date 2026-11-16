//! Matches engine orders to venue order ids, which the venue names only in its answer. Stream
//! frames for an order not yet named wait here, and an alert fires when one waits too long.

use crate::msg::exec::{ExecEvent, ExecKind, Provenance};
use crate::msg::ids::{ClientOrderId, InstrumentId};
use crate::msg::time::LocalTs;
use crate::venues::exec::{ExecEffect, ExecRequest, MirroredOrder, RequestId, blank};

use super::codec::{UnmappedOrder, venue_order_id_digest};
use super::facts::POLYMARKET_FACTS;

/// A user-stream frame whose order this run cannot yet name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldFrame {
    pub text: String,
    pub held_since: LocalTs,
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
            frames: Vec::with_capacity(POLYMARKET_FACTS.held_frame_capacity),
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
    pub fn hold(&mut self, text: String, now: LocalTs) {
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

    pub fn expired(&mut self, now: LocalTs) -> Vec<HeldFrame> {
        let mut expired = Vec::new();
        self.frames.retain(|frame| {
            let is_live = now.diff(frame.held_since) < POLYMARKET_FACTS.held_frame_lifetime;
            if !is_live {
                expired.push(frame.clone());
            }
            is_live
        });
        self.abandoned += expired.len() as u64;
        expired
    }

    pub fn re_hold(&mut self, frame: HeldFrame) {
        if self.frames.len() >= POLYMARKET_FACTS.held_frame_capacity {
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
    release_at: LocalTs,
}

/// A cancel the core already decided on and the venue would refuse. The whole effect is kept, not
/// just its subject: re-deciding later would mint a second request id for an order the core already
/// believes has one cancel out, and the never-retry rule depends on there being exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WithheldCancel {
    pub request_id: RequestId,
    pub request: ExecRequest,
    pub recon_seq: u64,
    pub release_at: LocalTs,
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

    /// The venue accepted a placement that it will not match until its taker hold passes, and will
    /// not cancel before then.
    pub fn on_delayed(&mut self, client_id: ClientOrderId, answered_at: LocalTs) {
        let release_at = answered_at + POLYMARKET_FACTS.taker_hold + POLYMARKET_FACTS.wire_margin;
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
    pub fn held_until(&self, client_id: ClientOrderId, now: LocalTs) -> Option<LocalTs> {
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
    pub fn released(&mut self, now: LocalTs) -> Vec<WithheldCancel> {
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
        ExecKind::AckPlaced => event.last_qty.mantissa() > 0,
        _ => false,
    }
}

/// Which read of its token named the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenRead {
    /// A read of the token from its binding until its first read ends. Only a fresh-binding read
    /// of the token, while it is still bound and with no order deferred, ends it ([`FirstReads`]).
    First,
    /// A read of the token after its first read ended.
    Quoted,
}

/// The tokens whose first read has not ended. A first read ends only at a read of the token in
/// which no unknown order deferred: a deferred order waits for the answer of a placement of this
/// run, so the read after that answer is the first read that can tell it unproven.
#[derive(Debug, Default)]
pub struct FirstReads {
    tokens: Vec<(InstrumentId, FirstRead)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirstRead {
    Due,
    /// A first read deferred an order, so the next answer of a placement there asks for a read.
    Deferred,
}

impl FirstReads {
    /// A newly bound token is due its first read: orders can rest on it from before the binding.
    pub fn on_bound(&mut self, instrument: InstrumentId) {
        match self.state_mut(instrument) {
            Some(state) => *state = FirstRead::Due,
            None => self.tokens.push((instrument, FirstRead::Due)),
        }
    }

    pub fn read_of(&self, instrument: InstrumentId) -> TokenRead {
        match self.tokens.iter().any(|(due, _)| *due == instrument) {
            true => TokenRead::First,
            false => TokenRead::Quoted,
        }
    }

    /// `verdicts` holds the verdict on each unknown order of the token that the read named.
    pub fn on_read_answered(&mut self, instrument: InstrumentId, verdicts: &[UnmappedVerdict]) {
        if !verdicts.contains(&UnmappedVerdict::Defer) {
            self.tokens.retain(|(due, _)| *due != instrument);
            return;
        }
        if let Some(state) = self.state_mut(instrument) {
            *state = FirstRead::Deferred;
        }
    }

    /// Whether the answer of a placement on this instrument asks for one more read of its token:
    /// once after each first read that deferred an order.
    pub fn take_read_after_answer(&mut self, instrument: InstrumentId) -> bool {
        match self.state_mut(instrument) {
            Some(state) if *state == FirstRead::Deferred => {
                *state = FirstRead::Due;
                true
            }
            _ => false,
        }
    }

    fn state_mut(&mut self, instrument: InstrumentId) -> Option<&mut FirstRead> {
        self.tokens
            .iter_mut()
            .find(|(due, _)| *due == instrument)
            .map(|(_, state)| state)
    }
}

/// What to do with an open order resting under an id this run never recorded. On this venue the
/// same credentials reach the website's own order entry, so no verdict cancels the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnmappedVerdict {
    /// This run placed it and lost the answer: exactly one lost slot equals it in instrument, side,
    /// price, and original size. Adopt it under that client id to regain the ability to cancel it.
    Adopt(ClientOrderId),
    /// A placement on this side is still in flight, so the order its answer will name cannot be told
    /// apart from a person's own order on the same side. Neither is classified until the place
    /// answers or times out — the resync pass re-reads and decides then. At a first read, the
    /// answer of the place also asks for one more read of the token, which is still a first read.
    Defer,
    /// A read of a token already quoted, and no lost slot equals the order, or more than one does.
    /// A lost slot stays ambiguous, and its status probes settle it.
    LeaveAlone,
    /// The first read of its token, and no placement of this run explains the order. It may be an
    /// order of an earlier run of this engine, whose fills the fold could not book, or a person's.
    /// Quoting halts, and a person decides.
    Unproven,
}

/// Classify an order resting under an id this run never recorded. The rules apply in order: a
/// placement in flight on the side defers, one equal lost slot adopts, and any other order is
/// unproven at the first read of its token and left alone at a later one.
///
/// The deferral is the whole point. A placement whose POST is still outstanding has a mirror entry
/// that is [`Provenance::Mine`], carries no venue id yet, and is NOT ambiguous — its answer is still
/// expected. While such an order exists on the side, an unmapped resting order there could be it OR
/// a person's order at the venue, and adopting either is wrong: adopting the person's binds a second
/// venue id to our slot the moment our own answer lands, double-folding the position. Only once a
/// placement's answer will never come — it timed out or the transport failed, which marks the mirror
/// entry ambiguous — is an unmapped order on the side safe to adopt, and then only by an exact match
/// to one such entry.
pub fn classify_unmapped(
    mirror: &[MirroredOrder],
    unmapped: &UnmappedOrder,
    has_venue_id: impl Fn(ClientOrderId) -> bool,
    read: TokenRead,
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
    match (adoption_candidate(mirror, unmapped, has_venue_id), read) {
        (Some(client_id), _) => UnmappedVerdict::Adopt(client_id),
        (None, TokenRead::First) => UnmappedVerdict::Unproven,
        (None, TokenRead::Quoted) => UnmappedVerdict::LeaveAlone,
    }
}

/// The message that halts quoting for an unproven order. It names the order by its venue id, side,
/// price, and size, and by no client id, since this run minted none for it.
pub fn unproven_order_event(unmapped: &UnmappedOrder, received_ts_us: LocalTs) -> ExecEvent {
    ExecEvent {
        venue_order_id: Some(venue_order_id_digest(&unmapped.venue_order_id)),
        provenance: Provenance::Foreign,
        side: unmapped.side,
        price: unmapped.price,
        qty: unmapped.qty,
        cumulative_qty: unmapped.filled,
        ..blank(
            ExecKind::UnprovenOrder,
            Some(unmapped.instrument),
            ClientOrderId(0),
            received_ts_us,
        )
    }
}

/// The one lost slot of this run that an unmapped resting order IS, when exactly one can be.
///
/// A candidate is an order this run placed whose venue id never came back, on the same instrument
/// and side, at the same price and original size: the order that a lost placement answer leaves
/// behind. Adopting it is the only way the engine regains the ability to cancel it.
///
/// `None` when no slot equals the order, or when two or more do. A near match can be a person's
/// order, and a pick between two equal slots binds the fills of one order to the slot of another.
/// The gate above admits only slots whose answer is no longer coming, so a live in-flight placement
/// never reaches here.
fn adoption_candidate(
    mirror: &[MirroredOrder],
    unmapped: &UnmappedOrder,
    has_venue_id: impl Fn(ClientOrderId) -> bool,
) -> Option<ClientOrderId> {
    let mut equal = mirror.iter().filter(|order| {
        order.instrument == unmapped.instrument
            && order.side == unmapped.side
            && order.provenance == Provenance::Mine
            && !has_venue_id(order.client_id)
            && order.price == unmapped.price
            && order.qty == unmapped.qty
    });
    let only = equal.next()?;
    equal.next().is_none().then_some(only.client_id)
}
