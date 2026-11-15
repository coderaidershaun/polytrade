//! Polymarket names an order only in its placement answer. These tests hold how the edge ties a
//! venue id to an order of this run, and that it never takes an order it cannot tie to exactly one.

use std::cell::Cell;

use engine::msg::exec::{ExecCommand, Provenance, VenueOrderStatus};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use engine::venues::exec::{ExecCore, ExecEffect, ExecRequest, MirroredOrder, RequestId};
use engine::venues::polymarket::exec::codec::UnmappedOrder;
use engine::venues::polymarket::exec::correlate::{
    DELAYED_HOLD, DelayedOrders, HeldFrame, PENDING_CAPACITY, PENDING_TTL, PendingFrames,
    UnmappedVerdict, WithheldCancel, classify_unmapped,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use crate::engine_support::ts as at;

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
    let capacity = PENDING_CAPACITY;
    let lifetime = PENDING_TTL;
    let cancel_hold = DELAYED_HOLD;
    assert_eq!(
        (capacity, lifetime.micros(), cancel_hold.micros()),
        (128, 10_000_000, 300_000),
        "the facts of the venue keep their values"
    );

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
    let release_at = at(1_000_000) + cancel_hold;
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
