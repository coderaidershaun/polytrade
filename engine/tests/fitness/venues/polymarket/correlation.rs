//! Polymarket names an order only in its placement answer, and reports only running filled totals.
//! These tests hold how the edge ties a venue id to an order of this run, and how it reads a fill.

use std::cell::Cell;
use std::sync::{Arc, Mutex};

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{ClientIdLayout, HaltReason, side_base};
use engine::hot::strategy::{Fill, Strategy as EngineStrategy, StrategyCtx};
use engine::link::{
    Envelope, FrameGuard, LINK_MAX_DATAGRAM, LinkBody, LinkDatagram, LinkHash, LinkIdentity,
    TopicId,
};

use engine::msg::exec::{
    CancelReason, ExecCommand, ExecEvent, ExecKind, OrderStyle, Provenance, VenueOrderStatus,
};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Notional, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, LocalTs, VenueTs};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::venues::exec::{
    ExecCore, ExecEffect, ExecRequest, ExecSession, MirroredOrder, RequestId, SessionSetup,
};
use engine::venues::polymarket::exec::codec::{
    DecodeContext, HttpAnswer, IgnoredReason, KnownOrder, OrderIndex, OrdersRead,
    PlaceRequestContext, StreamEvent, TokenBinding, TokenTable, UnmappedOrder, VenueAnswer,
    decode_orders_page, decode_place, decode_stream_frame, venue_order_id_digest,
};
use engine::venues::polymarket::exec::correlate::{
    DelayedOrders, FirstReads, HeldFrame, PendingFrames, TokenRead, UnmappedVerdict,
    WithheldCancel, classify_unmapped, unproven_order_event,
};
use engine::venues::polymarket::exec::facts::POLYMARKET_FACTS;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use engine::runtime::config::TrackerSpec;

use crate::engine_support::{
    ONE, engine_without_warmup, exec_event, instrument_row, persist_ring, strategy_log_ring,
    ts as at,
};
use crate::exec_support::{
    ASK, BID, CEILING, SentEvents, drain_commands, is_placing, make_ready, quoting_engine_with_ui,
    reseat_book, spin_at,
};
use crate::venues::exec::obligations::{Session, send};

const UP: InstrumentId = InstrumentId(0);
const DOWN: InstrumentId = InstrumentId(1);

fn cents(count: i64) -> Price {
    Price::from_mantissa(count * FIXED_SCALE / 100)
}

fn shares(count: i64) -> Qty {
    Qty::from_mantissa(count * FIXED_SCALE)
}

/// One order of the mirror, and whether the answer to its placement has named its venue id.
#[derive(Debug, Clone, Copy)]
struct Slot {
    order: MirroredOrder,
    has_venue_id: bool,
}

/// A placement of this run whose answer is still expected.
fn in_flight(client_id: u64, side: Side, price: Price, qty: Qty) -> Slot {
    Slot {
        order: MirroredOrder {
            instrument: UP,
            client_id: ClientOrderId(client_id),
            side,
            price,
            qty,
            provenance: Provenance::Mine,
            has_sent_cancel: false,
            is_ambiguous: false,
        },
        has_venue_id: false,
    }
}

/// A placement of this run whose answer will never come: it timed out or its transport failed,
/// which marks the mirror entry ambiguous.
fn lost(client_id: u64, side: Side, price: Price, qty: Qty) -> Slot {
    let mut slot = in_flight(client_id, side, price, qty);
    slot.order.is_ambiguous = true;
    slot
}

fn answered(client_id: u64, side: Side, price: Price, qty: Qty) -> Slot {
    Slot {
        has_venue_id: true,
        ..in_flight(client_id, side, price, qty)
    }
}

fn resting(instrument: InstrumentId, side: Side, price: Price, qty: Qty) -> UnmappedOrder {
    UnmappedOrder {
        instrument,
        venue_order_id: "0xvenue".into(),
        side,
        price,
        qty,
        filled: Qty::from_mantissa(0),
        status: VenueOrderStatus::New,
    }
}

fn verdict(slots: &[Slot], resting: &UnmappedOrder, read: TokenRead) -> UnmappedVerdict {
    let mirror: Vec<MirroredOrder> = slots.iter().map(|slot| slot.order).collect();
    classify_unmapped(
        &mirror,
        resting,
        |client_id| {
            slots
                .iter()
                .any(|slot| slot.order.client_id == client_id && slot.has_venue_id)
        },
        read,
    )
}

/// The rule in its own words. A placement of this run in flight on the side defers, because its
/// answer is still coming. Otherwise the order is adopted only when exactly one lost slot of this
/// run equals it in instrument, side, price, and original size. Anything else is left alone.
fn expected(slots: &[Slot], resting: &UnmappedOrder) -> UnmappedVerdict {
    let unnamed_on_side = || {
        slots.iter().filter(|slot| {
            slot.order.instrument == resting.instrument
                && slot.order.side == resting.side
                && slot.order.provenance == Provenance::Mine
                && !slot.has_venue_id
        })
    };
    if unnamed_on_side().any(|slot| !slot.order.is_ambiguous) {
        return UnmappedVerdict::Defer;
    }
    let equal: Vec<ClientOrderId> = unnamed_on_side()
        .filter(|slot| slot.order.price == resting.price && slot.order.qty == resting.qty)
        .map(|slot| slot.order.client_id)
        .collect();
    match equal.as_slice() {
        [only] => UnmappedVerdict::Adopt(*only),
        _ => UnmappedVerdict::LeaveAlone,
    }
}

struct Case {
    name: &'static str,
    slots: Vec<Slot>,
    resting: UnmappedOrder,
    expected: UnmappedVerdict,
}

fn named_cases() -> Vec<Case> {
    let five = shares(5);
    vec![
        Case {
            name: "a lost slot that differs in price and size explains nothing",
            slots: vec![lost(0xa1, Side::Buy, cents(42), shares(10))],
            resting: resting(UP, Side::Buy, cents(40), shares(25)),
            expected: UnmappedVerdict::LeaveAlone,
        },
        Case {
            name: "the one lost slot with its price and size is adopted",
            slots: vec![lost(0xa1, Side::Buy, cents(42), shares(10))],
            resting: resting(UP, Side::Buy, cents(42), shares(10)),
            expected: UnmappedVerdict::Adopt(ClientOrderId(0xa1)),
        },
        Case {
            name: "two lost slots with its price and size are a guess, so neither is adopted",
            slots: vec![
                lost(0xa1, Side::Buy, cents(42), shares(10)),
                lost(0xa2, Side::Buy, cents(42), shares(10)),
            ],
            resting: resting(UP, Side::Buy, cents(42), shares(10)),
            expected: UnmappedVerdict::LeaveAlone,
        },
        Case {
            name: "a lost placement's answer is adopted by the order it lost",
            slots: vec![lost(0xa1, Side::Buy, cents(45), five)],
            resting: resting(UP, Side::Buy, cents(45), five),
            expected: UnmappedVerdict::Adopt(ClientOrderId(0xa1)),
        },
        Case {
            name: "an exact price and size match still defers: the answer decides, not a guess",
            slots: vec![in_flight(0xa1, Side::Buy, cents(45), five)],
            resting: resting(UP, Side::Buy, cents(45), five),
            expected: UnmappedVerdict::Defer,
        },
        Case {
            name: "deferral is keyed on the side, not on a match: a different order on it waits",
            slots: vec![in_flight(0xa1, Side::Buy, cents(45), five)],
            resting: resting(UP, Side::Buy, cents(44), shares(7)),
            expected: UnmappedVerdict::Defer,
        },
        Case {
            name: "the other side has no placement in flight, so an order there is not deferred",
            slots: vec![in_flight(0xa1, Side::Buy, cents(45), five)],
            resting: resting(UP, Side::Sell, cents(45), five),
            expected: UnmappedVerdict::LeaveAlone,
        },
        Case {
            name: "a placement whose venue id arrived blocks nothing and explains nothing",
            slots: vec![answered(0xa1, Side::Buy, cents(45), five)],
            resting: resting(UP, Side::Buy, cents(45), five),
            expected: UnmappedVerdict::LeaveAlone,
        },
        Case {
            name: "a ladder can leave more than one lost slot; price and size pick between them",
            slots: vec![
                lost(0xa1, Side::Buy, cents(45), five),
                lost(0xa2, Side::Buy, cents(44), shares(7)),
            ],
            resting: resting(UP, Side::Buy, cents(44), shares(7)),
            expected: UnmappedVerdict::Adopt(ClientOrderId(0xa2)),
        },
        Case {
            name: "a prior run's order carries no client id this run could mint, so it is never \
                   adopted",
            slots: vec![Slot {
                order: MirroredOrder {
                    provenance: Provenance::PriorRun,
                    ..lost(0xa1, Side::Buy, cents(45), five).order
                },
                has_venue_id: false,
            }],
            resting: resting(UP, Side::Buy, cents(45), five),
            expected: UnmappedVerdict::LeaveAlone,
        },
    ]
}

/// Slots and an open order drawn from few prices and sizes, so that equal and near orders are
/// common.
fn mirror_and_resting() -> impl Strategy<Value = (Vec<Slot>, UnmappedOrder)> {
    let instrument = || prop_oneof![Just(UP), Just(DOWN)];
    let side = || prop_oneof![Just(Side::Buy), Just(Side::Sell)];
    let price = || prop::sample::select(vec![40_i64, 42, 45]).prop_map(cents);
    let qty = || prop::sample::select(vec![10_i64, 25]).prop_map(shares);
    let provenance = prop_oneof![4 => Just(Provenance::Mine), 1 => Just(Provenance::PriorRun)];
    let slot = (
        instrument(),
        side(),
        price(),
        qty(),
        provenance,
        prop::bool::weighted(0.75),
        prop::bool::weighted(0.2),
    );
    let slots = prop::collection::vec(slot, 0..6).prop_map(|drawn| {
        drawn
            .into_iter()
            .enumerate()
            .map(|(index, drawn)| numbered(index, drawn))
            .collect()
    });
    let order = (instrument(), side(), price(), qty())
        .prop_map(|(instrument, side, price, qty)| resting(instrument, side, price, qty));
    (slots, order)
}

type DrawnSlot = (InstrumentId, Side, Price, Qty, Provenance, bool, bool);

fn numbered(index: usize, drawn: DrawnSlot) -> Slot {
    let (instrument, side, price, qty, provenance, is_ambiguous, has_venue_id) = drawn;
    Slot {
        order: MirroredOrder {
            instrument,
            client_id: ClientOrderId(0xa0 + index as u64),
            side,
            price,
            qty,
            provenance,
            has_sent_cancel: false,
            is_ambiguous,
        },
        has_venue_id,
    }
}

/// FITNESS: on this venue the same credentials reach the website's own order entry, so an open
/// order with an id this run never recorded can be a person's. Adopting it binds a person's order
/// to a slot of this engine, which then folds its fills and cancels it. Nothing reports that
/// afterwards. So an order is adopted only when exactly one lost slot of this run equals it in
/// instrument, side, price, and original size; none, or two and more, leave it alone, and a
/// placement still in flight on the side defers.
#[test]
fn order_with_no_exact_match_is_never_adopted() {
    for case in named_cases() {
        assert_eq!(
            verdict(&case.slots, &case.resting, TokenRead::Quoted),
            case.expected,
            "case: {}",
            case.name
        );
    }

    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let (adopted, deferred, left_alone) = (Cell::new(0), Cell::new(0), Cell::new(0));
    let outcome = runner.run(&mirror_and_resting(), |(slots, resting)| {
        let wanted = expected(&slots, &resting);
        match wanted {
            UnmappedVerdict::Adopt(_) => adopted.set(adopted.get() + 1),
            UnmappedVerdict::Defer => deferred.set(deferred.get() + 1),
            UnmappedVerdict::LeaveAlone => left_alone.set(left_alone.get() + 1),
            UnmappedVerdict::Unproven => unreachable!("the rule in its own words proves no order"),
        }
        prop_assert_eq!(
            verdict(&slots, &resting, TokenRead::Quoted),
            wanted,
            "slots {:?}",
            slots
        );
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("an open order was classified against the exact-match rule: {failure}");
    }
    assert!(
        adopted.get() > 0 && deferred.get() > 0 && left_alone.get() > 0,
        "the generated mirrors must reach each verdict: {} adopted, {} deferred, {} left alone",
        adopted.get(),
        deferred.get(),
        left_alone.get()
    );
}

fn held(frames: &mut PendingFrames, text: &str, now: i64) {
    frames.hold(text.to_owned(), at(now));
}

fn texts(frames: &[HeldFrame]) -> Vec<&str> {
    frames.iter().map(|frame| frame.text.as_str()).collect()
}

/// Request ids are the core's to mint, so the one under test is a real one: the identity that must
/// survive the wait is the identity the core issued, not a number the test invented.
fn minted_cancel(client_id: ClientOrderId) -> (RequestId, ExecRequest) {
    let mut core = ExecCore::with_limits(4, 16);
    // Cancels are admitted from any connected phase; a core still Down would refuse this one.
    core.on_connected(&mut |_| {});
    let mut minted = None;
    core.on_command(
        ExecCommand::Cancel {
            instrument: UP,
            client_id,
        },
        &mut |effect| {
            if let ExecEffect::Send {
                request_id,
                request,
            } = effect
            {
                minted = Some((request_id, request));
            }
        },
    );
    minted.expect("a cancel for an unmirrored order is sent straight through")
}

/// A frame that names an order before its placement answer is held, so a fill is not lost, and
/// the buffer is bounded: the oldest frame leaves first, and a frame that no answer ever explains
/// leaves at its lifetime, so the edge reads the account again. A marketable order sits in the
/// venue's taker hold and cannot be cancelled there, so its cancel waits and goes out one time.
/// Each bound and each wait comes from the facts of the venue.
#[test]
fn held_frames_and_taker_hold_obey_the_named_facts() {
    let facts = POLYMARKET_FACTS;
    assert_eq!(
        (
            facts.held_frame_capacity,
            facts.held_frame_lifetime.micros(),
            facts.taker_hold.micros(),
            facts.wire_margin.micros()
        ),
        (128, 10_000_000, 250_000, 50_000),
        "the facts of the venue keep their values"
    );
    let capacity = facts.held_frame_capacity;
    let lifetime = facts.held_frame_lifetime;

    let mut frames = PendingFrames::new();
    held(&mut frames, "{\"event_type\":\"order\"}", 0);
    assert_eq!(frames.len(), 1);
    let drained = frames.drain();
    assert_eq!(texts(&drained), ["{\"event_type\":\"order\"}"]);
    assert_eq!(
        frames.len(),
        0,
        "a drain empties the buffer for the re-read"
    );
    frames.re_hold(drained[0].clone());
    assert_eq!(
        frames.len(),
        1,
        "a frame still unattributable is handed back"
    );

    let mut frames = PendingFrames::new();
    for index in 0..capacity {
        held(&mut frames, &format!("frame-{index}"), index as i64);
    }
    assert_eq!(frames.dropped(), 0);
    held(&mut frames, &format!("frame-{capacity}"), capacity as i64);
    assert_eq!(frames.len(), capacity, "capacity is a cap, not a hint");
    assert_eq!(frames.dropped(), 1);
    let kept: Vec<String> = (1..=capacity)
        .map(|index| format!("frame-{index}"))
        .collect();
    assert_eq!(
        texts(&frames.drain()),
        kept,
        "the oldest frame is the one that left, and the rest come back intact, oldest first"
    );

    let mut frames = PendingFrames::new();
    held(&mut frames, "alone", 0);
    assert!(frames.expired(at(lifetime.micros() - 1)).is_empty());
    assert_eq!(texts(&frames.expired(at(lifetime.micros()))), ["alone"]);
    assert_eq!(
        frames.abandoned(),
        1,
        "a frame that leaves at its lifetime is counted"
    );

    let mut frames = PendingFrames::new();
    held(&mut frames, "early", 0);
    held(&mut frames, "late", 5_000_000);
    assert!(
        frames.expired(at(lifetime.micros() - 1)).is_empty(),
        "nothing expires before its lifetime"
    );
    assert_eq!(texts(&frames.expired(at(lifetime.micros() + 1))), ["early"]);
    assert_eq!(frames.abandoned(), 1);
    assert_eq!(frames.len(), 1, "the younger frame keeps waiting");

    let client_id = ClientOrderId(0x5150);
    let release_at = at(1_000_000) + facts.taker_hold + facts.wire_margin;
    let mut delayed = DelayedOrders::default();
    delayed.on_delayed(client_id, at(1_000_000));
    assert_eq!(
        delayed.held_until(client_id, at(1_000_000)),
        Some(release_at)
    );
    assert_eq!(
        delayed.held_until(client_id, at(release_at.micros() - 1)),
        Some(release_at)
    );
    assert_eq!(
        delayed.held_until(client_id, release_at),
        None,
        "the hold ends at its stamp, it does not linger"
    );

    let (minted_id, minted_request) = minted_cancel(client_id);
    delayed.withhold(WithheldCancel {
        request_id: minted_id,
        request: minted_request,
        recon_seq: 3,
        release_at,
    });
    assert!(
        delayed.released(at(1_100_000)).is_empty(),
        "the hold is still running"
    );
    assert!(delayed.released(at(release_at.micros() - 1)).is_empty());
    let released = delayed.released(release_at);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].recon_seq, 3);
    assert!(
        delayed.released(at(1_400_000)).is_empty() && delayed.released(at(9_999_999)).is_empty(),
        "a released cancel is sent ONCE: the rule of no resend survives the wait"
    );
    // Re-deciding would mint a second request id for an order that the core believes has one
    // cancel outstanding.
    assert_eq!(
        released[0].into_effect(),
        ExecEffect::Send {
            request_id: minted_id,
            request: ExecRequest::Cancel {
                instrument: UP,
                client_id,
            },
        }
    );

    // The hold belongs to the order: once the order is gone, a later id in its slot must not
    // inherit a wait it never earned.
    let mut delayed = DelayedOrders::default();
    delayed.on_delayed(client_id, at(1_000_000));
    delayed.forget(client_id);
    assert_eq!(delayed.held_until(client_id, at(1_000_000)), None);
    let other = ClientOrderId(0x5151);
    delayed.on_delayed(other, at(0));
    delayed.forget(other);
    assert_eq!(delayed.held_until(other, at(1)), None);
}

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!(
            "../../../../fixtures/polymarket/exec/",
            $name,
            ".json"
        ))
    };
}

/// The names that the committed payloads of the venue carry: the token of the up outcome, the venue
/// id of the order, and the api key that each payload names as its owner, which is no key.
const UP_TOKEN: &str =
    "112440684578249703625547365250874526064395690274269051885028145282946008600017";
const VENUE_ORDER: &str = "0x3f1c9b4a72e5d08a1c6b3fd2094e7a5b81c02d6e9fa473b5c8e10d92a4f67b3c";
const API_KEY: &str = "b1f2c3d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d";

const OURS: ClientOrderId = ClientOrderId(0x6a64_f040_000c_0001);
const PLACED_AT: LocalTs = LocalTs::from_micros(1_786_046_862_000_000);
const INFLIGHT_TIMEOUT: DurationUs = DurationUs::from_millis(5_000);

fn after(micros: i64) -> LocalTs {
    PLACED_AT + DurationUs::from_micros(micros)
}

/// The buy of 10 at 0.52 that each payload is written around.
fn place_request() -> PlaceRequestContext {
    PlaceRequestContext {
        instrument: UP,
        client_id: OURS,
        side: Side::Buy,
        price: cents(52),
        qty: shares(10),
    }
}

/// The edge as its actor drives it: the codec, the index of venue ids, the held frames, and the
/// session, each called in the order that the actor calls them.
struct Edge {
    tokens: TokenTable,
    orders: OrderIndex,
    pending: PendingFrames,
    session: Session,
    /// Each event that the edge folds into its session and forwards to its input queue.
    folded: Vec<ExecEvent>,
}

impl Edge {
    /// Quoting, with the placement of [`OURS`] sent and its answer not here.
    fn with_placement_sent() -> Self {
        let mut tokens = TokenTable::with_retired_capacity(4);
        tokens.bind(TokenBinding {
            instrument: UP,
            token_id: UP_TOKEN.into(),
            tick: cents(1),
            is_neg_risk: false,
        });
        let mut session = ExecSession::new(SessionSetup {
            venue: "polymarket",
            core: ExecCore::with_limits(8, 32),
            instruments: vec![UP],
            events: SentEvents::default(),
            inflight_timeout: INFLIGHT_TIMEOUT,
        });
        assert_eq!(
            session.connect().len(),
            1,
            "a connect subscribes the stream"
        );
        let resync_seq = session.begin_pass(1);
        assert!(session.resync.on_read(resync_seq));
        session.finish_pass();
        assert!(session.arm_readiness(PLACED_AT));
        let request = place_request();
        let place = ExecCommand::Place {
            instrument: request.instrument,
            client_id: request.client_id,
            side: request.side,
            price: request.price,
            qty: request.qty,
            style: OrderStyle::PostOnly,
        };
        assert_eq!(send(&mut session, place, PLACED_AT).len(), 1);
        Self {
            tokens,
            orders: OrderIndex::with_capacity(16),
            pending: PendingFrames::new(),
            session,
            folded: Vec::new(),
        }
    }

    fn context(&self, at: LocalTs) -> DecodeContext<'_> {
        DecodeContext {
            tokens: &self.tokens,
            orders: &self.orders,
            api_key: API_KEY,
            received_ts_us: at,
        }
    }

    /// A frame of the user stream: folded when it names a known order, else held.
    fn on_frame(&mut self, text: &str, at: LocalTs) {
        if self.consume(text, at) {
            self.pending.hold(text.to_owned(), at);
        }
    }

    /// Whether the frame names an order that the index cannot map yet.
    fn consume(&mut self, text: &str, at: LocalTs) -> bool {
        match decode_stream_frame(text, &self.context(at)).expect("committed payload decodes") {
            StreamEvent::Order(event) => {
                self.fold(event);
                false
            }
            StreamEvent::Ignored(IgnoredReason::UnknownOrder) => true,
            other => panic!("an order frame names a known or an unknown order, got {other:?}"),
        }
    }

    /// The only answer that names the order: bind the venue id, re-read each held frame, then fold
    /// the answer itself.
    fn on_place_answer(&mut self, at: LocalTs) {
        let answer = HttpAnswer {
            status: 200,
            body: fixture!("place_live"),
        };
        let decoded = decode_place(answer, &place_request(), &self.context(at))
            .expect("committed payload decodes");
        let VenueAnswer::Answered(outcome) = decoded else {
            panic!("a placement that rests is answered, not refused as venue state");
        };
        let placed = outcome.placed.expect("a live placement names its order");
        assert!(
            self.session
                .inflight
                .take_where(|entry| matches!(
                    entry.request,
                    ExecRequest::Place { client_id, .. } if client_id == placed.client_id
                ))
                .is_some(),
            "the answer settles the placement in flight"
        );
        let known = KnownOrder {
            client_id: placed.client_id,
            instrument: UP,
        };
        self.orders
            .record(&placed.venue_order_id, known)
            .expect("the index has room");
        for frame in self.pending.drain() {
            if self.consume(&frame.text, at) {
                self.pending.re_hold(frame);
            }
        }
        self.fold(outcome.event);
    }

    fn fold(&mut self, event: ExecEvent) {
        let mut outgoing = Vec::new();
        self.session
            .fold(&event, 0, &mut outgoing)
            .expect("the mirror holds the order");
        self.folded.push(event);
    }

    /// The cumulative fill of [`OURS`] that each folded report states.
    fn fills(&self) -> Vec<Qty> {
        self.folded
            .iter()
            .filter(|event| event.kind == ExecKind::ReportTrade && event.client_id == OURS)
            .map(|event| event.cumulative_qty)
            .collect()
    }

    fn acks(&self) -> usize {
        self.folded
            .iter()
            .filter(|event| event.kind == ExecKind::AckPlaced && event.client_id == OURS)
            .count()
    }
}

/// The venue can report a fill of an order before its placement answer names the order, because
/// the stream and the answer travel apart. A frame that the edge dropped would lose the fill, and a
/// frame that it folded two times from its hold would state the fill again. So a frame of an
/// order with no mapping is held, comes back one time when the answer binds the order, and folds
/// once; and a frame that no answer ever explains leaves at its lifetime, with a count, so the read
/// of open orders decides.
#[test]
fn fill_frame_before_place_answer_folds_once() {
    let update = fixture!("ws_order_update_partial");
    let four = shares(4);

    let mut frame_first = Edge::with_placement_sent();
    frame_first.on_frame(update, after(1_000));
    assert_eq!(
        frame_first.pending.len(),
        1,
        "the frame names no mapping yet, so it is held"
    );
    assert!(frame_first.fills().is_empty());
    frame_first.on_place_answer(after(2_000));
    assert_eq!(
        frame_first
            .orders
            .resolve(VENUE_ORDER)
            .map(|known| known.client_id),
        Some(OURS)
    );
    assert!(
        frame_first.pending.is_empty(),
        "the held frame comes back when the answer binds it"
    );
    assert_eq!(frame_first.fills(), [four], "one fill folds for the order");
    assert_eq!(frame_first.acks(), 1);

    let mut answer_first = Edge::with_placement_sent();
    answer_first.on_place_answer(after(1_000));
    answer_first.on_frame(update, after(2_000));
    assert!(
        answer_first.pending.is_empty(),
        "a frame of a mapped order is never held"
    );
    assert_eq!(answer_first.fills(), [four]);
    assert_eq!(answer_first.acks(), 1);

    let mut twice = Edge::with_placement_sent();
    twice.on_frame(update, after(1_000));
    twice.on_frame(update, after(1_500));
    assert_eq!(twice.pending.len(), 2);
    twice.on_place_answer(after(2_000));
    assert!(twice.pending.is_empty());
    assert_eq!(
        twice.fills(),
        [four, four],
        "each delivery folds once and restates the venue's total, so the fill of 4 counts once"
    );
    assert_eq!((twice.pending.dropped(), twice.pending.abandoned()), (0, 0));

    let lifetime = POLYMARKET_FACTS.held_frame_lifetime.micros();
    let mut frame_only = Edge::with_placement_sent();
    frame_only.on_frame(update, after(1_000));
    let expired = frame_only.session.expire(after(INFLIGHT_TIMEOUT.micros()));
    assert!(
        expired.has_lost_placement,
        "the answer never came, so the placement is in doubt"
    );
    assert!(
        frame_only
            .pending
            .expired(after(1_000 + lifetime - 1))
            .is_empty()
    );
    assert_eq!(frame_only.pending.expired(after(1_000 + lifetime)).len(), 1);
    assert_eq!(
        frame_only.pending.abandoned(),
        1,
        "a frame that leaves at its lifetime is counted"
    );
    assert!(
        frame_only.fills().is_empty(),
        "no answer bound it, so it folded nothing"
    );

    let read = HttpAnswer {
        status: 200,
        body: fixture!("data_orders_wrapped"),
    };
    let orders_read = OrdersRead {
        instrument: UP,
        recon_seq: 0,
    };
    let VenueAnswer::Answered(page) = decode_orders_page(
        read,
        orders_read,
        &frame_only.context(after(1_000 + lifetime)),
    )
    .expect("committed payload decodes") else {
        panic!("a page of open orders is answered, not refused as venue state");
    };
    let [resting] = page.unmapped.as_slice() else {
        panic!(
            "the read names one order of a bound token with no mapping: {:?}",
            page.unmapped
        );
    };
    assert_eq!(
        (&*resting.venue_order_id, resting.filled),
        (VENUE_ORDER, four)
    );
    let verdict = classify_unmapped(
        frame_only.session.core.mirror(),
        resting,
        |client_id| frame_only.orders.venue_order_id(client_id).is_some(),
        TokenRead::Quoted,
    );
    assert_eq!(
        verdict,
        UnmappedVerdict::Adopt(OURS),
        "the read decides: the one lost placement equals the order the venue holds"
    );
}

/// The codec of the edge with the token of the payloads bound to [`UP`], and its index naming the
/// order of the payloads as `client_id`.
struct MakerCodec {
    tokens: TokenTable,
    orders: OrderIndex,
}

impl MakerCodec {
    fn naming(client_id: ClientOrderId) -> Self {
        let mut tokens = TokenTable::with_retired_capacity(4);
        tokens.bind(TokenBinding {
            instrument: UP,
            token_id: UP_TOKEN.into(),
            tick: cents(1),
            is_neg_risk: false,
        });
        let mut orders = OrderIndex::with_capacity(4);
        let known = KnownOrder {
            client_id,
            instrument: UP,
        };
        orders
            .record(VENUE_ORDER, known)
            .expect("the index has room");
        Self { tokens, orders }
    }

    fn context(&self, at: LocalTs) -> DecodeContext<'_> {
        DecodeContext {
            tokens: &self.tokens,
            orders: &self.orders,
            api_key: API_KEY,
            received_ts_us: at,
        }
    }

    fn report(&self, frame: &str, at: LocalTs) -> ExecEvent {
        match decode_stream_frame(frame, &self.context(at)).expect("committed payload decodes") {
            StreamEvent::Order(event) => event,
            other => panic!("an UPDATE of a known order is an order event, got {other:?}"),
        }
    }
}

/// A fill as a strategy sees it: the position that the ledger shows inside the fill callback, which
/// the engine fires only after it folded the fill, and the price and the quote amount of the fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeenFill {
    position: Qty,
    price: Price,
    notional: Notional,
}

struct SeenFills(Arc<Mutex<Vec<SeenFill>>>);

impl EngineStrategy for SeenFills {
    fn on_fill(&mut self, ctx: &mut StrategyCtx<'_>, fill: &Fill) {
        let seen = SeenFill {
            position: ctx.position_base(fill.instrument),
            price: fill.price,
            notional: fill.notional,
        };
        self.0
            .lock()
            .expect("no reader panics while it holds the list")
            .push(seen);
    }
}

/// An engine with one row, and the fills that its strategy saw.
fn fill_engine() -> (HotEngine, Arc<Mutex<Vec<SeenFill>>>) {
    let fills = Arc::new(Mutex::new(Vec::new()));
    let (persistence, _rows) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(64);
    let engine = engine_without_warmup(
        &[instrument_row(0, TrackerSpec::default(), 64)],
        Box::new(SeenFills(Arc::clone(&fills))),
        persistence,
        log_sink,
    );
    (engine, fills)
}

/// The fill of `step` shares of the resting buy at 0.52, after which the ledger holds `position`.
fn fill_at_52(step: i64, position: i64) -> SeenFill {
    SeenFill {
        position: shares(position),
        price: cents(52),
        notional: cents(52).notional(shares(step)),
    }
}

/// CONTRACT: the Polymarket codec, its index, and the fold meet at the maker fill. The venue's
/// order UPDATE states only the running total of the order. A report with no quantity of its own
/// trade leaves a fee-gap record for each fill, and such a record must mean a real gap, not one
/// that the venue's own total already explains. So the codec states the step above the total that
/// the edge has folded for the order, and the fold books the fill with no gap.
#[test]
fn a_maker_fill_of_polymarket_books_no_fee_gap() {
    let (mut engine, fills) = fill_engine();
    for message in reseat_book(51 * ONE / 100, 53 * ONE / 100, 0) {
        engine.dispatch(&message);
    }
    let ours = ClientIdLayout { run_nonce: 0 }.encode(side_base(UP, Side::Buy), 1);
    let resting = ExecEvent {
        kind: ExecKind::SnapshotOrder,
        status: Some(VenueOrderStatus::New),
        qty: shares(10),
        ..exec_event(UP, ours, Side::Buy, 52 * ONE / 100, 1)
    };
    engine.dispatch(&InboundMessage::new(at(1), MessageBody::Exec(resting)));

    let mut codec = MakerCodec::naming(ours);
    let steps = [
        (
            fixture!("ws_order_update_partial"),
            4,
            4,
            vec![fill_at_52(4, 4)],
        ),
        (fixture!("ws_order_update_partial"), 4, 0, vec![]),
        (
            fixture!("ws_order_update_filled"),
            10,
            6,
            vec![fill_at_52(6, 10)],
        ),
    ];
    for (index, (frame, cumulative, own_step, expected)) in steps.into_iter().enumerate() {
        let when = 1_000 * (index as i64 + 2);
        let report = codec.report(frame, at(when));
        assert_eq!(
            (report.kind, report.cumulative_qty, report.last_qty),
            (ExecKind::ReportTrade, shares(cumulative), shares(own_step)),
            "report {index}: the venue's total, and the step of its own trade"
        );
        let trade_price = if own_step > 0 { cents(52) } else { Price::from_mantissa(0) };
        assert_eq!(
            report.last_price, trade_price,
            "report {index}: a resting order fills at its own limit, which the frame names"
        );
        engine.dispatch(&InboundMessage::new(at(when), MessageBody::Exec(report)));
        // As the actor does after each event that it folds.
        codec
            .orders
            .record_matched(report.client_id, report.cumulative_qty);
        let seen = std::mem::take(&mut *fills.lock().expect("no writer panics"));
        assert_eq!(
            seen, expected,
            "report {index}: the ledger moves by the step, once, at the price of the fill"
        );
    }
    let counters = engine.counters();
    assert_eq!(
        (counters.fee_gaps, counters.unbooked_fees),
        (0, 0),
        "a maker fill whose step the venue's total explains leaves no gap and books no fee"
    );

    let mut took = MakerCodec::naming(ours);
    let answer = HttpAnswer {
        status: 200,
        body: &format!(
            r#"{{"success":true,"errorMsg":"","orderID":"{VENUE_ORDER}","status":"live","makingAmount":"1.04","takingAmount":"2.0","transactionsHashes":[],"tradeIDs":[]}}"#
        ),
    };
    let request = PlaceRequestContext {
        client_id: ours,
        ..place_request()
    };
    let VenueAnswer::Answered(placed) =
        decode_place(answer, &request, &took.context(at(1))).expect("the answer decodes")
    else {
        panic!("a placement that rests is answered, not refused as venue state");
    };
    assert_eq!(
        (placed.event.cumulative_qty, placed.event.last_qty),
        (shares(2), shares(2)),
        "the placement took 2 and rests the rest"
    );
    took.orders
        .record_matched(ours, placed.event.cumulative_qty);
    let report = took.report(fixture!("ws_order_update_partial"), at(2));
    assert_eq!(
        (report.cumulative_qty, report.last_qty),
        (shares(4), shares(2)),
        "after a placement that took liquidity, an UPDATE states only its own step"
    );
}

/// An order that the first read of the token of UP finds, and one on a token that no binding names.
/// Neither is an order whose venue id this run recorded.
const PERSON: &str = "0xperson";
const OTHER: &str = "0xother";
const OTHER_TOKEN: &str = "a-token-of-another-market";

/// The account as the first read of the token of UP reads it.
fn first_read_of_up() -> String {
    format!(
        r#"{{"data":[{{"id":"{PERSON}","asset_id":"{UP_TOKEN}","side":"BUY","price":"0.45","original_size":"10","size_matched":"0","status":"LIVE","created_at":1}},{{"id":"{OTHER}","asset_id":"{OTHER_TOKEN}","side":"SELL","price":"0.62","original_size":"5","size_matched":"0","status":"LIVE","created_at":1}}]}}"#
    )
}

/// The halt that the last execution frame of the workstation shows, if one shows a halt.
fn shown_halt(ui_events: &mut rtrb::Consumer<UiEvent>) -> Option<UiEvent> {
    std::iter::from_fn(|| ui_events.pop().ok())
        .filter(|event| matches!(event, UiEvent::Execution { .. }))
        .last()
        .filter(|event| {
            matches!(
                event,
                UiEvent::Execution {
                    halt: UiHalt::Halted { .. },
                    ..
                }
            )
        })
}

/// A workstation frame as a peer reads it from the link.
fn over_the_link(event: UiEvent) -> UiEvent {
    let token_hash = LinkHash::of_name("fitness-run-token");
    let strategy_hash = LinkHash::of_name("strat-micro-recorder");
    let identity = LinkIdentity {
        token_hash,
        strategy_hash,
        sender_te_hash: LinkHash::of_name("te-polymarket-btc-updown-5m"),
        boot_ts_us: VenueTs::from_micros(1_786_046_800_000_000),
    };
    let datagram = LinkDatagram {
        envelope: Envelope::new(identity, TopicId::EVENTS, 1),
        body: LinkBody::Event(event),
    };
    let mut buffer = [0; LINK_MAX_DATAGRAM];
    let len = datagram.encode(&mut buffer);
    let guard = FrameGuard {
        token_hash,
        strategy_hash,
        schema_hash: LinkHash::of_fields(&[]),
    };
    match LinkDatagram::decode(&buffer[..len], &guard).expect("the frame decodes") {
        LinkDatagram {
            body: LinkBody::Event(event),
            ..
        } => event,
        other => panic!("an event frame decodes to an event, got {other:?}"),
    }
}

/// CONTRACT: Polymarket echoes no client id, so an open order whose venue id this run never recorded
/// can be an order of an earlier run of this engine, or a person's, placed at the website with the
/// same credentials. The engine never touches an order that it did not place, so it cancels
/// neither. At the first read of a token, such an order on a bound token is unproven: the fold
/// could book none of its fills, so quoting halts at that one message and a person decides. A
/// later read of a token already quoted leaves the order alone, and an order on a token that no
/// binding names halts nothing.
#[test]
fn a_cold_start_read_naming_an_unknown_order_sends_no_cancel() {
    let mut tokens = TokenTable::with_retired_capacity(4);
    tokens.bind(TokenBinding {
        instrument: UP,
        token_id: UP_TOKEN.into(),
        tick: cents(1),
        is_neg_risk: false,
    });
    let orders = OrderIndex::with_capacity(16);
    let context = DecodeContext {
        tokens: &tokens,
        orders: &orders,
        api_key: API_KEY,
        received_ts_us: at(5),
    };
    let page = first_read_of_up();
    let read = OrdersRead {
        instrument: UP,
        recon_seq: 0,
    };
    let VenueAnswer::Answered(decoded) = decode_orders_page(
        HttpAnswer {
            status: 200,
            body: &page,
        },
        read,
        &context,
    )
    .expect("the page decodes") else {
        panic!("a page of open orders is answered, not refused as venue state");
    };
    let unmapped: Vec<&str> = decoded
        .unmapped
        .iter()
        .map(|order| &*order.venue_order_id)
        .collect();
    let unattributable: Vec<&str> = decoded
        .unattributable
        .iter()
        .map(|order| &*order.venue_order_id)
        .collect();
    assert_eq!(
        (unmapped, unattributable),
        (vec![PERSON], vec![OTHER]),
        "step 1: the order on the bound token is unmapped, and the other is unattributable"
    );
    let person = &decoded.unmapped[0];

    assert_eq!(
        verdict(&[], person, TokenRead::First),
        UnmappedVerdict::Unproven,
        "step 2: at the first read of its token no placement of this run explains the order"
    );
    assert_eq!(
        verdict(&[], person, TokenRead::Quoted),
        UnmappedVerdict::LeaveAlone,
        "a read of a token already quoted leaves the order alone"
    );
    let rules_before = [
        (
            in_flight(0xa1, Side::Buy, cents(44), shares(3)),
            UnmappedVerdict::Defer,
        ),
        (
            lost(0xa1, Side::Buy, cents(45), shares(10)),
            UnmappedVerdict::Adopt(ClientOrderId(0xa1)),
        ),
        (
            answered(0xa1, Side::Buy, cents(45), shares(10)),
            UnmappedVerdict::Unproven,
        ),
    ];
    for (slot, expected) in rules_before {
        assert_eq!(
            verdict(&[slot], person, TokenRead::First),
            expected,
            "a placement in flight defers and one equal lost slot adopts, at a first read too: \
             {slot:?}"
        );
    }

    let event = unproven_order_event(person, at(5));
    assert_eq!(
        (
            event.kind,
            event.instrument,
            event.side,
            event.price,
            event.qty,
            event.venue_order_id,
            event.client_id,
            event.provenance,
        ),
        (
            ExecKind::UnprovenOrder,
            Some(UP),
            Side::Buy,
            cents(45),
            shares(10),
            Some(venue_order_id_digest(PERSON)),
            ClientOrderId(0),
            Provenance::Foreign,
        ),
        "step 3: the edge names the order by its venue id, side, price, and size, and by no client \
         id of this run"
    );
    let mut session = ExecSession::new(SessionSetup {
        venue: "polymarket",
        core: ExecCore::with_limits(8, 32),
        instruments: vec![UP],
        events: SentEvents::default(),
        inflight_timeout: INFLIGHT_TIMEOUT,
    });
    let mut outgoing = Vec::new();
    session
        .fold(&event, 0, &mut outgoing)
        .expect("the event reveals no order to retain");
    assert!(
        outgoing.is_empty(),
        "step 3: the edge sends no request for the order, so no cancel: {outgoing:?}"
    );
    assert!(
        session.core.mirror().is_empty(),
        "step 3: nothing is mirrored, so no sweep of this engine can reach the order"
    );
    assert_eq!(
        session.core.foreign_orders(),
        1,
        "step 3: the order is counted with the orders that this engine cannot claim"
    );

    let unproven = InboundMessage::new(at(5), MessageBody::Exec(event));
    let cold_start = |with_unproven: bool| {
        let (mut engine, mut commands, mut ui_events) = quoting_engine_with_ui(CEILING, &[]);
        let mut digests = Vec::new();
        for message in reseat_book(BID, ASK, 1) {
            engine.dispatch(&message);
            digests.push(engine.state_digest());
        }
        spin_at(&mut engine, 0, 2);
        assert_eq!(
            shown_halt(&mut ui_events),
            None,
            "nothing halts before the read"
        );
        if with_unproven {
            engine.dispatch(&unproven);
            digests.push(engine.state_digest());
        }
        make_ready(&mut engine, 6);
        spin_at(&mut engine, 1, 7);
        digests.push(engine.state_digest());
        (
            drain_commands(&mut commands),
            shown_halt(&mut ui_events),
            digests,
        )
    };
    let (commands, _, _) = cold_start(false);
    assert!(
        is_placing(&commands, Side::Buy) && is_placing(&commands, Side::Sell),
        "with no unproven order the ready engine quotes, so the halt below is what stops it"
    );
    let (commands, halt, digests) = cold_start(true);
    let halt = halt.expect("step 4: an unproven order halts quoting");
    assert!(
        matches!(
            halt,
            UiEvent::Execution {
                halt: UiHalt::Halted {
                    reason: HaltReason::UnprovenOrderResting,
                    halted_ts_us,
                },
                ..
            } if halted_ts_us == at(5).to_peer()
        ),
        "step 4: quoting halts at the message of the unproven order: {halt:?}"
    );
    assert_eq!(
        commands,
        [ExecCommand::CancelOurs {
            instrument: UP,
            reason: CancelReason::Halt,
        }],
        "step 4: the strategy places nothing, and the halt sweeps only the orders of this run, of \
         which the event made none"
    );
    assert_eq!(
        cold_start(true).2,
        digests,
        "a replay of the same messages gives the same state after each, so it halts at the same \
         message"
    );
    assert_eq!(
        over_the_link(halt),
        halt,
        "the reason reaches a workstation over the link"
    );

    let (mut engine, mut commands, mut ui_events) = quoting_engine_with_ui(CEILING, &[]);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }
    make_ready(&mut engine, 2);
    spin_at(&mut engine, 0, 3);
    let quoted = drain_commands(&mut commands);
    assert!(is_placing(&quoted, Side::Buy) && is_placing(&quoted, Side::Sell));
    let ours: Vec<Slot> = quoted
        .iter()
        .filter_map(|command| match *command {
            ExecCommand::Place {
                client_id,
                side,
                price,
                qty,
                ..
            } => Some(answered(client_id.0, side, price, qty)),
            _ => None,
        })
        .collect();
    assert_eq!(
        verdict(&ours, person, TokenRead::First),
        UnmappedVerdict::Unproven,
        "step 5: the first read of a newly bound token gives the same verdict"
    );
    engine.dispatch(&InboundMessage::new(
        at(9),
        MessageBody::Exec(unproven_order_event(person, at(9))),
    ));
    spin_at(&mut engine, 1, 10);
    let halt = shown_halt(&mut ui_events).expect("step 5: the same halt");
    assert!(
        matches!(
            halt,
            UiEvent::Execution {
                halt: UiHalt::Halted {
                    reason: HaltReason::UnprovenOrderResting,
                    halted_ts_us,
                },
                ..
            } if halted_ts_us == at(9).to_peer()
        ),
        "step 5: quoting halts at the message of the unproven order: {halt:?}"
    );
    let swept = drain_commands(&mut commands);
    assert!(
        !is_placing(&swept, Side::Buy) && !is_placing(&swept, Side::Sell),
        "step 5: the strategy places nothing more: {swept:?}"
    );
    assert!(
        swept
            .iter()
            .any(|command| matches!(command, ExecCommand::CancelOurs { instrument, .. } if *instrument == UP)),
        "step 5: the halt sweeps the orders of this run: {swept:?}"
    );
}

impl Edge {
    /// The open orders of [`first_read_of_up`] as this edge decodes them: the unknown order on UP.
    fn read_up(&self, at: LocalTs) -> UnmappedOrder {
        let page = first_read_of_up();
        let read = OrdersRead {
            instrument: UP,
            recon_seq: 0,
        };
        let answer = HttpAnswer {
            status: 200,
            body: &page,
        };
        let VenueAnswer::Answered(decoded) =
            decode_orders_page(answer, read, &self.context(at)).expect("the page decodes")
        else {
            panic!("a page of open orders is answered, not refused as venue state");
        };
        let [unknown] = decoded.unmapped.as_slice() else {
            panic!(
                "the read names one unknown order on UP: {:?}",
                decoded.unmapped
            );
        };
        unknown.clone()
    }

    /// The verdict of the edge on an unknown order, at the read kind that `first_reads` gives.
    fn verdict_on(&self, unknown: &UnmappedOrder, first_reads: &FirstReads) -> UnmappedVerdict {
        classify_unmapped(
            self.session.core.mirror(),
            unknown,
            |client_id| self.orders.venue_order_id(client_id).is_some(),
            first_reads.read_of(unknown.instrument),
        )
    }
}

/// CONTRACT: the first read of a token can find an unknown order on a side where a placement of
/// this run waits for its answer. The order defers, since that answer can still name it. A first
/// read that deferred does not end, so the token is read again when the placement answers, and
/// that read is still a first read: an order that the answer did not name is unproven, and quoting
/// halts at its one message. Ending the first read at the read that deferred would leave such an
/// order alone, and quote beside it, at the binding of a token, where an order of an earlier run of
/// this engine rests.
#[test]
fn a_first_read_that_defers_stays_first_until_the_placement_answers() {
    let mut first_reads = FirstReads::default();
    assert_eq!(first_reads.read_of(UP), TokenRead::Quoted);
    first_reads.on_bound(UP);
    assert_eq!(
        first_reads.read_of(UP),
        TokenRead::First,
        "a token that a binding names is due its first read"
    );

    let (mut engine, mut commands, mut ui_events) = quoting_engine_with_ui(CEILING, &[]);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }
    make_ready(&mut engine, 2);
    spin_at(&mut engine, 0, 3);
    drain_commands(&mut commands);

    let mut edge = Edge::with_placement_sent();
    let unknown = edge.read_up(after(100));
    assert_eq!(&*unknown.venue_order_id, PERSON);
    assert_eq!(
        edge.verdict_on(&unknown, &first_reads),
        UnmappedVerdict::Defer,
        "step 1: a placement of this run is in flight on the side, so its answer can still name \
         the order"
    );
    first_reads.on_read_answered(UP, &[UnmappedVerdict::Defer]);
    spin_at(&mut engine, 1, 4);
    assert_eq!(
        shown_halt(&mut ui_events),
        None,
        "step 1: a deferred order sends the fold nothing, so nothing halts"
    );
    assert_eq!(
        first_reads.read_of(UP),
        TokenRead::First,
        "step 1: a first read that deferred does not end"
    );
    assert!(
        !first_reads.take_read_after_answer(DOWN),
        "an answer on another instrument asks for no read"
    );

    edge.on_place_answer(after(200));
    assert_eq!(
        edge.orders.venue_order_id(OURS),
        Some(VENUE_ORDER),
        "step 2: the placement answers with another venue id"
    );
    assert!(
        first_reads.take_read_after_answer(UP),
        "step 3: the answer asks for one more read of the token"
    );
    assert!(
        !first_reads.take_read_after_answer(UP),
        "step 3: one read for each read that deferred"
    );

    let unknown = edge.read_up(after(300));
    assert_eq!(
        first_reads.read_of(UP),
        TokenRead::First,
        "step 4: the next read of the token is still a first read"
    );
    let verdict = edge.verdict_on(&unknown, &first_reads);
    assert_eq!(
        verdict,
        UnmappedVerdict::Unproven,
        "step 5: the answer named an order of this run, and the order that it did not name is \
         unproven"
    );
    let event = unproven_order_event(&unknown, after(300));
    let mut outgoing = Vec::new();
    edge.session
        .fold(&event, 0, &mut outgoing)
        .expect("the event reveals no order to retain");
    assert!(
        outgoing.is_empty(),
        "step 6: the edge sends no request for the unknown venue id, so no cancel: {outgoing:?}"
    );
    first_reads.on_read_answered(UP, &[verdict]);
    assert_eq!(
        first_reads.read_of(UP),
        TokenRead::Quoted,
        "a first read in which no order deferred ends"
    );
    assert!(!first_reads.take_read_after_answer(UP));

    engine.dispatch(&InboundMessage::new(at(5), MessageBody::Exec(event)));
    spin_at(&mut engine, 2, 6);
    let halt = shown_halt(&mut ui_events).expect("step 5: an unproven order halts quoting");
    assert!(
        matches!(
            halt,
            UiEvent::Execution {
                halt: UiHalt::Halted {
                    reason: HaltReason::UnprovenOrderResting,
                    halted_ts_us,
                },
                ..
            } if halted_ts_us == at(5).to_peer()
        ),
        "step 5: quoting halts at the message of the unproven order: {halt:?}"
    );
    let swept = drain_commands(&mut commands);
    assert!(
        !is_placing(&swept, Side::Buy) && !is_placing(&swept, Side::Sell),
        "step 5: the strategy places nothing more: {swept:?}"
    );
}
