//! Each reason and state keeps its own wire slot, each event body survives its padded tail, and the
//! sequence gate drops repeats and lets a restarted peer in, so two builds never misread each other.

use std::net::SocketAddr;

use engine::hot::exec::{OrderState, QuoteLevel, ReadinessGap, RejectReason};
use engine::link::{
    GateVerdict, LINK_MAX_DATAGRAM, LINK_MAX_GATE_KEYS, LINK_MAX_SUBSCRIBERS,
    LINK_SUBSCRIPTION_TTL, LinkBody, LinkDatagram, LinkHash, RefreshOutcome, SequenceGate,
    SubscriberTable, TopicId, TopicSet,
};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use engine::msg::time::{DurationUs, LocalTs, VenueTs};
use engine::msg::ui::{DomQuote, UiEvent, UiWorkingOrder};

use super::wire_frames::{
    EVERY_ORDER_STATE, REJECT_REASONS, encode, envelope, event_datagram, order_snapshot,
    round_trip, working_order,
};

/// The slot each refusal claims in [`REJECT_REASONS`], which is what stops that list rotting into a
/// subset of the enum. A new `RejectReason` lands here as a missing arm, and the arm it needs
/// indexes one past the array, so it will not compile until the array grows with it and the
/// round trip of each frame kind starts generating it.
///
/// The `const` blocks are load-bearing, not decoration: a bare `REJECT_REASONS[20]` compiles, since
/// the out-of-bounds lint does not reach a const of this shape. Only const evaluation refuses it.
fn listed_as(reason: RejectReason) -> RejectReason {
    match reason {
        RejectReason::QtyBelowMin => const { REJECT_REASONS[0] },
        RejectReason::NotionalBelowMin => const { REJECT_REASONS[1] },
        RejectReason::NotionalAboveMax => const { REJECT_REASONS[2] },
        RejectReason::WouldCross => const { REJECT_REASONS[3] },
        RejectReason::OutsideBand => const { REJECT_REASONS[4] },
        RejectReason::Underfunded => const { REJECT_REASONS[5] },
        RejectReason::NotReady(ReadinessGap::Stream) => const { REJECT_REASONS[6] },
        RejectReason::NotReady(ReadinessGap::Balances) => const { REJECT_REASONS[7] },
        RejectReason::NotReady(ReadinessGap::OpenOrders) => const { REJECT_REASONS[8] },
        RejectReason::Halted => const { REJECT_REASONS[9] },
        RejectReason::SessionReducingOnly => const { REJECT_REASONS[10] },
        RejectReason::ExposureCeiling => const { REJECT_REASONS[11] },
        RejectReason::NoQuoteDeclared => const { REJECT_REASONS[12] },
        RejectReason::BookNotQuotable => const { REJECT_REASONS[13] },
        RejectReason::DuplicatePrice => const { REJECT_REASONS[14] },
        RejectReason::OrderLimit => const { REJECT_REASONS[15] },
        RejectReason::OutsideWindow => const { REJECT_REASONS[16] },
        RejectReason::RateBudget => const { REJECT_REASONS[17] },
        RejectReason::Warming => const { REJECT_REASONS[18] },
        RejectReason::IdsSpent => const { REJECT_REASONS[19] },
    }
}

#[test]
fn every_reject_reason_claims_its_own_slot() {
    for (slot, reason) in REJECT_REASONS.into_iter().enumerate() {
        assert_eq!(
            listed_as(reason),
            reason,
            "reject reason at slot {slot} disagrees with the slot it claims, so at least one \
             refusal is generated twice and another not at all"
        );
    }
}

/// The link actor indexes its per-topic sequence array by raw topic id, unchecked, on the send
/// path. Every id the engine can stamp — the fixed engine topics, and one per topic the strategy
/// declares — must land inside the array that the same declaration sized, or a send panics inside
/// the actor instead of the config being refused at startup.
#[test]
fn every_topic_the_engine_can_stamp_indexes_inside_its_sequence_array() {
    for declared in [0, 1, 2, 8, 1024] {
        let slots = TopicId::space_len(declared);
        for engine in [
            TopicId::SUBSCRIBE,
            TopicId::BOOKS,
            TopicId::EVENTS,
            TopicId::CATALOG_INSTRUMENTS,
            TopicId::CATALOG_FEATURES,
            TopicId::LIFECYCLE,
            TopicId::LATENCY,
        ] {
            assert!(
                usize::from(engine.0) < slots,
                "engine topic {engine:?} falls outside the {slots} slots sized for {declared} strategy topics"
            );
        }
        for index in 0..declared {
            assert!(
                usize::from(TopicId::strategy(index).0) < slots,
                "strategy topic {index} falls outside the {slots} slots sized for {declared} strategy topics"
            );
        }
    }
}

/// Saturated, so a field written narrower than it is read cannot hide in a small value.
fn saturated_order_update(state: OrderState) -> UiEvent {
    UiEvent::OrderUpdate {
        instrument: InstrumentId(u16::MAX),
        seq: u64::MAX,
        event_ts_us: VenueTs::from_micros(i64::MAX),
        client_id: ClientOrderId(u64::MAX),
        quote_level: Some(QuoteLevel::new(7).expect("valid quote level")),
        side: Side::Sell,
        state,
        price: Price::from_mantissa(i64::MIN),
        qty: Qty::from_mantissa(i64::MAX),
        filled: Qty::from_mantissa(i64::MIN),
    }
}

fn full_quote_ladder() -> UiEvent {
    let mut quote = DomQuote::default();
    for level in 0..8 {
        quote.bids[level] = Some((
            Price::from_mantissa(100 - level as i64),
            Qty::from_mantissa(level as i64 + 1),
        ));
        quote.asks[level] = Some((
            Price::from_mantissa(101 + level as i64),
            Qty::from_mantissa(level as i64 + 11),
        ));
    }
    UiEvent::Quote {
        instrument: InstrumentId(3),
        seq: 9,
        event_ts_us: VenueTs::from_micros(77),
        quote,
    }
}

/// FITNESS: every `OrderState`, and every body that fills its fixed capacity, survives the tail on
/// every run.
///
/// Each event kind pads to the widest kind, the order snapshot, so a field written narrower than it
/// is read is hidden by the padding unless its value is saturated. The wire flattens `Closed(_)`
/// into the same byte as the open states, which turns five close reasons into five wire
/// discriminants in one eleven-state table — and a table of eleven near-identical tags is where a
/// copy-pasted tag hides.
///
/// `each_frame_kind_round_trips` generates these states too, so this is deliberately NOT a second
/// round-trip: it is the DETERMINISM the generator cannot give. One state sits behind four nested
/// choices there — datagram kind, then market-vs-exec, then event kind, then the state itself — so a
/// given state arrives in roughly one case in 900, and 256 cases reach it about a quarter of the
/// time. Measured, not assumed: against a mutant mapping `Closed(ReconciledGone)` to the `Expired`
/// tag, the proptest passed 6 clean runs out of 6 while this failed all 6.
///
/// A quarter of the time is a test that passes CI and loses the distinction between "we reconciled
/// it away" and "the venue took our order away for a reason we did not choose".
#[test]
fn every_event_body_survives_the_event_tail() {
    let complete_snapshot = order_snapshot(
        2,
        5,
        &[
            working_order(1, OrderState::Live),
            UiWorkingOrder {
                quote_level: None,
                ..working_order(2, OrderState::Unknown)
            },
        ],
    );
    let states = EVERY_ORDER_STATE.map(|state| {
        (
            format!("a saturated OrderUpdate in {state:?}"),
            saturated_order_update(state),
        )
    });
    let bodies = [
        (
            "the full sixteen-level quote ladder".to_string(),
            full_quote_ladder(),
        ),
        (
            "a complete snapshot whose working count overflows its details".to_string(),
            complete_snapshot,
        ),
    ];

    for (name, event) in states.into_iter().chain(bodies) {
        let decoded = round_trip(&event_datagram(event)).expect("a canonical event decodes");
        assert_eq!(
            decoded.body,
            LinkBody::Event(event),
            "case {name}: did not survive the event tail"
        );
    }
}

/// A reused send buffer must not leak the previous datagram into an event frame's unused tail:
/// identical events would encode to different bytes, and a future MAC would reject them.
#[test]
fn event_encoding_is_independent_of_buffer_reuse() {
    let rotation = LinkDatagram {
        envelope: envelope(TopicId::EVENTS, 2),
        body: LinkBody::Event(UiEvent::Rotation {
            instrument: InstrumentId(1),
            seq: 5,
            event_ts_us: VenueTs::from_micros(11),
        }),
    };
    let quote = LinkDatagram {
        envelope: envelope(TopicId::EVENTS, 1),
        body: LinkBody::Event(UiEvent::Quote {
            instrument: InstrumentId(1),
            seq: 4,
            event_ts_us: VenueTs::from_micros(10),
            quote: DomQuote::top(
                Some((Price::from_mantissa(i64::MAX), Qty::from_mantissa(i64::MAX))),
                Some((Price::from_mantissa(i64::MIN), Qty::from_mantissa(i64::MIN))),
            ),
        }),
    };
    let mut dirty = [0xff; LINK_MAX_DATAGRAM];
    quote.encode(&mut dirty);
    let reused = rotation.encode(&mut dirty);
    assert_eq!(&dirty[..reused], encode(&rotation).as_slice());
}

/// The gate ages its slots, so every fixture stamps from one instant unless it is deliberately
/// walking the clock past the TTL.
const GATE_START: LocalTs = LocalTs::from_micros(1_000_000);

fn admit_from(gate: &mut SequenceGate, sender: u64, seq: u64, now: LocalTs) -> GateVerdict {
    let mut envelope = envelope(TopicId::FIRST_STRATEGY, seq);
    envelope.sender_te_hash = LinkHash(sender);
    gate.admit(&envelope, now)
}

fn admit_boot(gate: &mut SequenceGate, boot_ts_us: i64, topic: TopicId, seq: u64) -> bool {
    let mut envelope = envelope(topic, seq);
    envelope.boot_ts_us = VenueTs::from_micros(boot_ts_us);
    gate.admit(&envelope, GATE_START).is_accepted()
}

/// FITNESS: a slot is keyed on `(sender, boot, topic)` and the consumed sequence is the record, so a
/// duplicate or reordered datagram must never reach the hot thread — a replayed tape would then not
/// reproduce the run.
///
/// Both halves of the key earn their place. Collapsing the TOPIC out of it needs no error to do
/// damage: two interleaved streams each look like a reorder of the other, the gate drops most of
/// both, and a topic simply goes quiet with every frame accounted for as stale. Dropping the BOOT
/// stamp costs the other direction — a restarted peer's `seq` returns to 0, so it would stay dark
/// until its seq climbed past the old high-water mark, hours on a slow topic.
#[test]
fn gate_sequences_each_stream_on_its_sender_boot_and_topic() {
    for (name, steps, stale, restarts, stale_boots) in [
        (
            "one peer's topics each carry their own sequence",
            &[
                (100, TopicId::FIRST_STRATEGY, 1, true),
                (100, TopicId::BOOKS, 1, true),
                (100, TopicId::EVENTS, 1, true),
                (100, TopicId::BOOKS, 1, false),
                (100, TopicId::FIRST_STRATEGY, 2, true),
            ][..],
            1,
            0,
            0,
        ),
        (
            "a duplicate and a reorder are both dropped, and the stream carries on",
            &[
                (100, TopicId::FIRST_STRATEGY, 1, true),
                (100, TopicId::FIRST_STRATEGY, 2, true),
                (100, TopicId::FIRST_STRATEGY, 3, true),
                (100, TopicId::FIRST_STRATEGY, 3, false),
                (100, TopicId::FIRST_STRATEGY, 2, false),
                (100, TopicId::FIRST_STRATEGY, 4, true),
            ][..],
            2,
            0,
            0,
        ),
        (
            "a restarted peer is let back in and the boot it replaced is not",
            &[
                (100, TopicId::FIRST_STRATEGY, 5_000, true),
                (200, TopicId::FIRST_STRATEGY, 0, true),
                (200, TopicId::FIRST_STRATEGY, 1, true),
                (100, TopicId::FIRST_STRATEGY, 9_999, false),
            ][..],
            0,
            1,
            1,
        ),
    ] {
        let mut gate = SequenceGate::new();
        for &(boot_ts_us, topic, seq, accepted) in steps {
            assert_eq!(
                admit_boot(&mut gate, boot_ts_us, topic, seq),
                accepted,
                "case {name}: seq {seq} on {topic:?} from boot {boot_ts_us}"
            );
        }
        let counts = gate.counts();
        assert_eq!(counts.stale, stale, "case {name}: stale count");
        assert_eq!(counts.restarts, restarts, "case {name}: restart count");
        assert_eq!(
            counts.stale_boots, stale_boots,
            "case {name}: stale boot count"
        );
    }
}

/// FITNESS: every workstation that attaches is a NEW `(sender, topic)` stream, so a gate that never
/// released a DEAD slot would refuse every peer after the capacity-th one for the rest of the run —
/// leaving a restart of the trading engine as the only recovery, which is the thing running the UI
/// in its own process exists to avoid. Eviction reclaims DEAD slots only: dropping a stream still
/// carrying traffic would trade a visible lockout for silent gaps in a feed nobody was told about,
/// so a live sender keeps its slot right up to the TTL boundary and loses it exactly at.
#[test]
fn gate_eviction_is_gated_on_the_subscription_ttl() {
    let newcomer = LINK_MAX_GATE_KEYS as u64;
    for (name, now, sender_still_live, expect_admitted) in [
        (
            "just inside the ttl, every slot still live",
            GATE_START + LINK_SUBSCRIPTION_TTL - DurationUs::from_micros(1),
            false,
            false,
        ),
        (
            "at the ttl, a quiet slot is reclaimed",
            GATE_START + LINK_SUBSCRIPTION_TTL,
            true,
            true,
        ),
    ] {
        let mut gate = SequenceGate::new();
        for sender in 0..LINK_MAX_GATE_KEYS as u64 {
            assert!(
                admit_from(&mut gate, sender, 1, GATE_START).is_accepted(),
                "case {name}: filling capacity"
            );
        }
        if sender_still_live {
            assert!(
                admit_from(&mut gate, 0, 2, now).is_accepted(),
                "case {name}: refreshing sender 0 keeps its slot live"
            );
        }
        assert_eq!(
            admit_from(&mut gate, newcomer, 1, now).is_accepted(),
            expect_admitted,
            "case {name}: newcomer admission"
        );
        assert_eq!(
            gate.counts().evicted,
            expect_admitted as u64,
            "case {name}: evicted count"
        );
        assert_eq!(
            gate.counts().untracked,
            !expect_admitted as u64,
            "case {name}: untracked count"
        );
        if sender_still_live {
            // Sender 0 kept its slot, so its next frame is gated on the seq that stream actually
            // reached rather than admitted as a fresh one.
            assert!(
                !admit_from(&mut gate, 0, 2, now).is_accepted(),
                "case {name}: the spared sender is gated on its real seq, not readmitted"
            );
            assert_eq!(gate.counts().stale, 1, "case {name}: stale count");
        }
    }
}

fn subscriber(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}")
        .parse()
        .expect("a literal loopback address")
}

/// Soft state means a dead subscriber needs no teardown — but it also means the feed stops silently,
/// so the expiry boundary is the thing the staleness reporting is built on. A renewal has to land on
/// the SAME slot with the newly asserted topic set, since a workstation narrowing its interest and a
/// workstation that died look alike from here.
#[test]
fn a_subscription_renews_in_place_and_expires_at_its_ttl() {
    let mut table = SubscriberTable::new();
    let start = LocalTs::from_micros(1_000_000);
    assert_eq!(
        table.refresh(subscriber(9310), TopicSet::ALL, start),
        RefreshOutcome::Added
    );
    assert_eq!(
        table.refresh(
            subscriber(9310),
            TopicSet::new(&[TopicId::BOOKS]).expect("one topic"),
            start
        ),
        RefreshOutcome::Renewed
    );
    assert_eq!(table.len(), 1);
    assert_eq!(table.recipients(TopicId::BOOKS, start).count(), 1);
    assert_eq!(table.recipients(TopicId::EVENTS, start).count(), 0);

    let alive = start + LINK_SUBSCRIPTION_TTL - DurationUs::from_micros(1);
    assert_eq!(table.recipients(TopicId::BOOKS, alive).count(), 1);
    let expired = start + LINK_SUBSCRIPTION_TTL;
    assert_eq!(table.recipients(TopicId::BOOKS, expired).count(), 0);

    assert_eq!(
        table.refresh(subscriber(9310), TopicSet::ALL, expired),
        RefreshOutcome::Added
    );
    assert_eq!(table.len(), 1);
}

/// A capacity hit is a designed event: the table refuses the subscription and reports the refusal
/// for the actor to count, rather than growing without bound because a port is reachable.
#[test]
fn full_subscriber_table_rejects_and_counts() {
    let mut table = SubscriberTable::new();
    let now = LocalTs::from_micros(1_000_000);
    for port in 0..LINK_MAX_SUBSCRIBERS as u16 {
        assert_eq!(
            table.refresh(subscriber(9310 + port), TopicSet::ALL, now),
            RefreshOutcome::Added
        );
    }
    let overflow = subscriber(9310 + LINK_MAX_SUBSCRIBERS as u16);
    assert_eq!(
        table.refresh(overflow, TopicSet::ALL, now),
        RefreshOutcome::Rejected
    );
    assert_eq!(table.len(), LINK_MAX_SUBSCRIBERS);

    let after_ttl = now + LINK_SUBSCRIPTION_TTL;
    assert_eq!(
        table.refresh(overflow, TopicSet::ALL, after_ttl),
        RefreshOutcome::Added
    );
    assert_eq!(table.len(), 1);
}
