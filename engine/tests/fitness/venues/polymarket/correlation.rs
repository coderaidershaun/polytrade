//! Polymarket names an order only in its placement answer, and reports only running filled totals.
//! These tests hold how the edge ties a venue id to an order of this run, and how it reads a fill.

use std::cell::Cell;
use std::sync::{Arc, Mutex};

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{ClientIdLayout, side_base};
use engine::hot::strategy::{Fill, Strategy as EngineStrategy, StrategyCtx};

use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, OrderStyle, Provenance, VenueOrderStatus,
};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, LocalTs};
use engine::venues::exec::{
    ExecCore, ExecEffect, ExecRequest, ExecSession, MirroredOrder, RequestId, SessionSetup,
};
use engine::venues::polymarket::exec::codec::{
    DecodeContext, HttpAnswer, IgnoredReason, KnownOrder, OrderIndex, OrdersRead,
    PlaceRequestContext, StreamEvent, TokenBinding, TokenTable, UnmappedOrder, VenueAnswer,
    decode_orders_page, decode_place, decode_stream_frame,
};
use engine::venues::polymarket::exec::correlate::{
    DelayedOrders, HeldFrame, PendingFrames, UnmappedVerdict, WithheldCancel, classify_unmapped,
};
use engine::venues::polymarket::exec::facts::POLYMARKET_FACTS;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use engine::runtime::config::TrackerSpec;

use crate::engine_support::{
    ONE, engine_without_warmup, exec_event, instrument_row, persist_ring, strategy_log_ring,
    ts as at,
};
use crate::exec_support::{SentEvents, reseat_book};
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

fn verdict(slots: &[Slot], resting: &UnmappedOrder) -> UnmappedVerdict {
    let mirror: Vec<MirroredOrder> = slots.iter().map(|slot| slot.order).collect();
    classify_unmapped(&mirror, resting, |client_id| {
        slots
            .iter()
            .any(|slot| slot.order.client_id == client_id && slot.has_venue_id)
    })
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
/// placement still in flight on the side defers, as before.
#[test]
fn order_with_no_exact_match_is_never_adopted() {
    for case in named_cases() {
        assert_eq!(
            verdict(&case.slots, &case.resting),
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
        }
        prop_assert_eq!(verdict(&slots, &resting), wanted, "slots {:?}", slots);
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
    let verdict = classify_unmapped(frame_only.session.core.mirror(), resting, |client_id| {
        frame_only.orders.venue_order_id(client_id).is_some()
    });
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

/// The position that the ledger shows inside each fill callback, which the engine fires only after
/// it folded the fill.
struct FillPositions(Arc<Mutex<Vec<Qty>>>);

impl EngineStrategy for FillPositions {
    fn on_fill(&mut self, ctx: &mut StrategyCtx<'_>, fill: &Fill) {
        let held = ctx.position_base(fill.instrument);
        self.0
            .lock()
            .expect("no reader panics while it holds the list")
            .push(held);
    }
}

/// An engine with one row, and the positions that its fills showed.
fn fill_engine() -> (HotEngine, Arc<Mutex<Vec<Qty>>>) {
    let positions = Arc::new(Mutex::new(Vec::new()));
    let (persistence, _rows) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(64);
    let engine = engine_without_warmup(
        &[instrument_row(0, TrackerSpec::default(), 64)],
        Box::new(FillPositions(Arc::clone(&positions))),
        persistence,
        log_sink,
    );
    (engine, positions)
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
        (fixture!("ws_order_update_partial"), 4, 4, vec![4]),
        (fixture!("ws_order_update_partial"), 4, 0, vec![]),
        (fixture!("ws_order_update_filled"), 10, 6, vec![10]),
    ];
    for (index, (frame, cumulative, own_step, positions)) in steps.into_iter().enumerate() {
        let when = 1_000 * (index as i64 + 2);
        let report = codec.report(frame, at(when));
        assert_eq!(
            (report.kind, report.cumulative_qty, report.last_qty),
            (ExecKind::ReportTrade, shares(cumulative), shares(own_step)),
            "report {index}: the venue's total, and the step of its own trade"
        );
        engine.dispatch(&InboundMessage::new(at(when), MessageBody::Exec(report)));
        // As the actor does after each event that it folds.
        codec
            .orders
            .record_matched(report.client_id, report.cumulative_qty);
        let held = std::mem::take(&mut *fills.lock().expect("no writer panics"));
        let expected: Vec<Qty> = positions.into_iter().map(shares).collect();
        assert_eq!(
            held, expected,
            "report {index}: the ledger moves by the step, once"
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
