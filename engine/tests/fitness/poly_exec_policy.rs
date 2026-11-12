//! Polymarket execution POLICY: the decisions the driver makes that no wire format dictates.
//!
//! Every one of these exists because the venue mints the only order id there is, and mints it in
//! the answer to the placement. That single fact produces a held-frame buffer, an adopt-or-leave
//! rule for orders nobody can name, a cancel that has to wait out the venue's own hold, and a token
//! binding that is only tradeable once three separate reads agree. Socket behaviour stays out;
//! what is pinned here is what the driver DECIDES.

use polysim::adapters::exec::{ExecCore, ExecEffect, ExecRequest, MirroredOrder, RequestId};
use polysim::adapters::polymarket::exec::binding::{
    BindingStep, Bindings, ENRICHMENT_RETRY, EnrichmentRead, MAX_ENRICHMENT_ATTEMPTS,
};
use polysim::adapters::polymarket::exec::codec::{
    DecodeContext, HttpAnswer, OrderIndex, OrdersRead, TokenTable, UnmappedOrder, VenueAnswer,
    decode_heartbeat, decode_neg_risk, decode_orders_page,
};
use polysim::adapters::polymarket::exec::correlate::{
    DELAYED_HOLD, DelayedOrders, PENDING_CAPACITY, PENDING_TTL, PendingFrames, UnmappedVerdict,
    WithheldCancel, classify_unmapped, restates_balances,
};
use polysim::adapters::polymarket::rotation::{OutcomeLeg, TokenId, WindowAssignment};
use polysim::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{ExecCommand, ExecEvent, ExecKind, Provenance, VenueOrderStatus};
use polysim::time::DurationUs;

use crate::engine_support::{exec_event, ts as at};

const UP: InstrumentId = InstrumentId(0);
const DOWN: InstrumentId = InstrumentId(1);
const CONDITION: &str = "0xcondition";
const UP_TOKEN: &str = "111";
const DOWN_TOKEN: &str = "222";

fn assignment(open: i64, close: i64) -> WindowAssignment {
    WindowAssignment {
        up: OutcomeLeg {
            instrument: UP,
            token: TokenId::from(UP_TOKEN),
        },
        down: OutcomeLeg {
            instrument: DOWN,
            token: TokenId::from(DOWN_TOKEN),
        },
        window_open_ts_us: at(open),
        window_close_ts_us: at(close),
        condition_id: std::sync::Arc::from(CONDITION),
    }
}

fn ready_bindings(
    step: BindingStep,
) -> Vec<polysim::adapters::polymarket::exec::binding::ReadyBinding> {
    match step {
        BindingStep::Ready(ready) => ready,
        other => panic!("expected a completed binding, got {other:?}"),
    }
}

/// A window is not tradeable when it arrives. Tick size and the neg-risk flag are separate reads,
/// and the flag picks the exchange contract the signature is checked against — binding early would
/// sign every order for the wrong one. The venue is the authority on this per-token flag, so it
/// must reach the binding unchanged for both polarities.
#[test]
fn a_binding_is_incomplete_until_the_tick_and_both_neg_risk_flags_land() {
    let mut bindings = Bindings::default();

    let step = bindings.on_assignment(&assignment(0, 300_000_000));
    let BindingStep::Enrich {
        condition_id,
        tokens,
    } = step
    else {
        panic!("a fresh assignment must ask for its enrichment reads");
    };
    assert_eq!(&*condition_id, CONDITION);
    assert_eq!(tokens.len(), 2, "one neg-risk read per outcome token");

    assert_eq!(
        bindings.on_market(CONDITION, Price(1_000_000)),
        BindingStep::Wait,
        "tick alone is not enough"
    );
    assert_eq!(
        bindings.on_neg_risk(CONDITION, UP, false),
        BindingStep::Wait,
        "one leg's flag is not enough — the other leg is a separate order"
    );

    let ready = ready_bindings(bindings.on_neg_risk(CONDITION, DOWN, false));
    assert_eq!(ready.len(), 2);
    assert!(ready.iter().all(|entry| entry.tick == Price(1_000_000)));
    assert!(
        ready.iter().all(|entry| !entry.is_neg_risk),
        "a false answer must reach the binding unchanged"
    );

    let mut true_flag = Bindings::default();
    true_flag.on_assignment(&assignment(0, 300_000_000));
    true_flag.on_market(CONDITION, Price(1_000_000));
    true_flag.on_neg_risk(CONDITION, UP, true);
    let ready_true = ready_bindings(true_flag.on_neg_risk(CONDITION, DOWN, true));
    assert!(
        ready_true.iter().all(|entry| entry.is_neg_risk),
        "a true answer must reach the binding unchanged too"
    );
}

/// The two enrichment reads leave on the assignment, but a single transient failure of either would
/// leave the instrument unbound for the whole five-minute window — every placement refused
/// `UnboundInstrument`. The retry re-issues only the read still outstanding, so a failed market read
/// followed by a success still yields a usable binding. It is bounded: a market that never answers
/// is given up rather than re-read for the rest of the window, which would only burn the read
/// budget.
#[test]
fn an_enrichment_read_is_retried_alone_then_given_up_when_it_never_answers() {
    let mut bindings = Bindings::default();
    let start = at(0);
    let step = bindings.on_assignment(&assignment(0, 300_000_000));
    assert!(matches!(step, BindingStep::Enrich { .. }));

    // The neg-risk reads land for both legs; the market read fails (nothing calls on_market).
    bindings.on_neg_risk(CONDITION, UP, false);
    bindings.on_neg_risk(CONDITION, DOWN, false);

    // The first poll only arms the retry timer — the initial reads already went out on the
    // assignment, so re-issuing now would double them.
    assert!(
        bindings.due_enrichment_reads(start).is_empty(),
        "the first poll arms, it does not re-issue"
    );
    assert!(
        bindings.due_enrichment_reads(start).is_empty(),
        "nothing is due before the retry interval elapses"
    );

    let due = bindings.due_enrichment_reads(start + ENRICHMENT_RETRY);
    assert_eq!(due.len(), 1, "only the read still missing is re-issued");
    assert!(
        matches!(&due[0], EnrichmentRead::Market { condition_id } if &**condition_id == CONDITION),
        "the neg-risk reads already landed, so only the market read comes back"
    );

    // The retry succeeds and the binding completes.
    let ready = ready_bindings(bindings.on_market(CONDITION, Price(1_000_000)));
    assert_eq!(
        ready.len(),
        2,
        "the window is usable after a transient blip"
    );

    let mut bindings = Bindings::default();
    bindings.on_assignment(&assignment(0, 300_000_000));

    let mut now = at(0);
    bindings.due_enrichment_reads(now); // arms the timer
    for _ in 0..MAX_ENRICHMENT_ATTEMPTS + 3 {
        now = now + ENRICHMENT_RETRY;
        bindings.due_enrichment_reads(now);
    }
    assert!(
        bindings
            .due_enrichment_reads(now + ENRICHMENT_RETRY)
            .is_empty(),
        "past the attempt cap the binding is gone, so no read is due"
    );
    assert_eq!(
        bindings.on_market(CONDITION, Price(1_000_000)),
        BindingStep::Wait,
        "a late answer for a given-up binding settles nothing"
    );
    assert!(bindings.refused() >= 1, "the given-up binding is counted");
}

/// The edge sweep is a backstop, and a backstop that fires every housekeeping tick would spend the
/// venue's cancel bucket on an empty book. The next window re-arms it: it is a different market
/// with a different close.
#[test]
fn the_close_margin_backstop_fires_once_per_window_and_rearms_for_the_next() {
    let close = 300_000_000;
    let margin = DurationUs::from_micros(3_000_000);
    let mut bindings = Bindings::default();
    bindings.on_assignment(&assignment(0, close));
    bindings.on_market(CONDITION, Price(1_000_000));
    bindings.on_neg_risk(CONDITION, UP, false);
    bindings.on_neg_risk(CONDITION, DOWN, false);

    assert!(
        bindings
            .close_margin_reached(at(close - 10_000_000), margin)
            .is_empty(),
        "ten seconds out is not inside a three second margin"
    );
    let reached = bindings.close_margin_reached(at(close - 1_000_000), margin);
    assert_eq!(reached.len(), 2, "both legs of the window expire together");
    assert!(reached.iter().any(|leg| &*leg.token_id == UP_TOKEN));
    assert!(
        bindings.close_margin_reached(at(close), margin).is_empty(),
        "the same window must not be swept twice"
    );

    let next = WindowAssignment {
        condition_id: std::sync::Arc::from("0xnext"),
        ..assignment(300_000_000, 600_000_000)
    };
    bindings.on_assignment(&next);
    bindings.on_market("0xnext", Price(1_000_000));
    bindings.on_neg_risk("0xnext", UP, false);
    bindings.on_neg_risk("0xnext", DOWN, false);
    assert_eq!(
        bindings.close_margin_reached(at(599_000_000), margin).len(),
        2,
        "a new window's close is a new sweep"
    );
}

/// A sell against a token whose CLOB allowance cache is cold is refused as an empty wallet however
/// approved the chain is, so the edge tracks warmth per token rather than per account. The refresh
/// endpoint allows 50 calls per ten seconds and a withheld sell is re-decided every spin, so asking
/// again while an answer is outstanding would spend that budget on one question. A claim only ever
/// clears on an answer, so a refresh that never reached the venue would hold the token marked
/// in-flight forever — and every sell on it is withheld waiting for an answer nobody asked for.
#[test]
fn allowance_warmth_and_refresh_claims_are_tracked_per_token() {
    let mut bindings = Bindings::default();
    assert!(!bindings.is_allowance_warm(UP_TOKEN));
    assert!(
        bindings.claim_allowance_refresh(UP_TOKEN),
        "the first ask is sent"
    );
    assert!(
        !bindings.claim_allowance_refresh(UP_TOKEN),
        "a second ask while the first is unanswered is not"
    );
    assert!(
        bindings.claim_allowance_refresh(DOWN_TOKEN),
        "a different token is a different question"
    );

    // A refusal has to leave the token askable, or a single failed refresh withholds its sells for
    // the rest of the run.
    bindings.on_allowance_answered(UP_TOKEN, false);
    assert!(!bindings.is_allowance_warm(UP_TOKEN));
    assert!(bindings.claim_allowance_refresh(UP_TOKEN));

    bindings.on_allowance_answered(UP_TOKEN, true);
    assert!(bindings.is_allowance_warm(UP_TOKEN));
    assert!(
        !bindings.is_allowance_warm(DOWN_TOKEN),
        "the other outcome is a different token with its own cache entry"
    );
    assert!(
        !bindings.claim_allowance_refresh(UP_TOKEN),
        "a warm cache is asked about no further"
    );

    // The driver hands the claim back when the request is dropped.
    let mut bindings = Bindings::default();
    assert!(bindings.claim_allowance_refresh(UP_TOKEN));
    bindings.release_allowance_refresh(UP_TOKEN);
    assert!(
        bindings.claim_allowance_refresh(UP_TOKEN),
        "a dropped refresh must not withhold this token's sells for the rest of the run"
    );
}

fn held(frames: &mut PendingFrames, text: &str, now: i64) {
    frames.hold(text.to_owned(), at(now));
}

/// The window between a placement's bytes leaving and its answer landing is one in which the venue
/// can already be reporting fills on it. Dropping those frames loses a fill, so a frame naming an
/// order nobody can map is held and handed back intact. The buffer is bounded by design: the oldest
/// goes because a newer frame describes a more recent state. And a frame no mapping ever explains
/// describes a fill the ledger has not seen — it leaves on a deadline so the driver can force a
/// re-read instead of waiting forever. Both losses are counted rather than silent.
#[test]
fn a_held_frame_is_handed_back_intact_dropped_oldest_first_and_expired_on_its_deadline() {
    let mut frames = PendingFrames::new();
    held(&mut frames, "{\"event_type\":\"order\"}", 0);
    assert_eq!(frames.len(), 1);

    let drained = frames.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].text, "{\"event_type\":\"order\"}");
    assert_eq!(
        frames.len(),
        0,
        "a drain empties the buffer for the re-read"
    );

    // Still unattributable: the caller hands it back rather than losing it.
    frames.re_hold(drained[0].clone());
    assert_eq!(frames.len(), 1);

    let mut frames = PendingFrames::new();
    for index in 0..PENDING_CAPACITY {
        held(&mut frames, &format!("frame-{index}"), index as i64);
    }
    assert_eq!(frames.dropped(), 0);

    held(&mut frames, "frame-overflow", PENDING_CAPACITY as i64);
    assert_eq!(
        frames.len(),
        PENDING_CAPACITY,
        "capacity is a cap, not a hint"
    );
    assert_eq!(frames.dropped(), 1);
    let remaining = frames.drain();
    assert_eq!(
        remaining.first().map(|frame| frame.text.as_str()),
        Some("frame-1"),
        "the oldest frame is the one that left"
    );
    assert_eq!(
        remaining.last().map(|frame| frame.text.as_str()),
        Some("frame-overflow")
    );

    let mut frames = PendingFrames::new();
    held(&mut frames, "early", 0);
    held(&mut frames, "late", 5_000_000);

    assert!(
        frames.expired(at(PENDING_TTL.micros() - 1)).is_empty(),
        "nothing expires before the ttl"
    );
    let expired = frames.expired(at(PENDING_TTL.micros() + 1));
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].text, "early");
    assert_eq!(frames.abandoned(), 1);
    assert_eq!(frames.len(), 1, "the younger frame keeps waiting");
}

fn cancel_request(client_id: ClientOrderId) -> ExecRequest {
    ExecRequest::Cancel {
        instrument: UP,
        client_id,
    }
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

/// A marketable order is held 250 ms at the venue and CANNOT be cancelled during it. Sending the
/// cancel anyway earns a refusal that counts against the hard-reject streak, so it waits. The hold
/// belongs to the order, not to the run: once the order is gone the stamp must go with it or a later
/// id reusing the slot inherits a wait it never earned.
#[test]
fn a_cancel_inside_the_taker_hold_is_withheld_and_released_once() {
    let client_id = ClientOrderId(0x5150);
    let mut delayed = DelayedOrders::default();
    delayed.on_delayed(client_id, at(1_000_000));

    let release_at = delayed
        .held_until(client_id, at(1_000_000))
        .expect("an order inside its hold reports when it leaves it");
    assert_eq!(release_at, at(1_000_000) + DELAYED_HOLD);
    assert!(
        delayed
            .held_until(client_id, at(1_000_000) + DELAYED_HOLD)
            .is_none(),
        "the hold ends at the stamp, it does not linger"
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

    let released = delayed.released(release_at);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].recon_seq, 3);
    assert!(
        delayed.released(at(9_999_999)).is_empty(),
        "a released cancel is sent ONCE — never-retry survives the delay"
    );

    // The identity the core minted rides through the wait: re-deciding would mint a second request
    // id for an order the core already believes has one cancel outstanding.
    let ExecEffect::Send {
        request_id,
        request,
    } = released[0].into_effect()
    else {
        panic!("a released cancel is a send");
    };
    assert_eq!(request_id, minted_id);
    assert_eq!(request, cancel_request(client_id));

    let mut delayed = DelayedOrders::default();
    delayed.on_delayed(client_id, at(1_000_000));
    delayed.forget(client_id);
    assert!(delayed.held_until(client_id, at(1_000_000)).is_none());
}

/// A placement whose POST is still outstanding: it is ours, has no venue id and is not yet
/// ambiguous.
fn mine(client_id: u64, side: Side, price: i64, qty: i64) -> MirroredOrder {
    MirroredOrder {
        instrument: UP,
        client_id: ClientOrderId(client_id),
        side,
        price: Price(price),
        qty: Qty(qty),
        provenance: Provenance::Mine,
        has_sent_cancel: false,
        is_ambiguous: false,
    }
}

/// A placement whose answer will never come — it timed out or its transport failed, which marks the
/// mirror entry ambiguous. Only such a slot is adoptable; a live in-flight one is deferred.
fn lost(client_id: u64, side: Side, price: i64, qty: i64) -> MirroredOrder {
    MirroredOrder {
        is_ambiguous: true,
        ..mine(client_id, side, price, qty)
    }
}

fn resting(side: Side, price: i64, qty: i64) -> UnmappedOrder {
    UnmappedOrder {
        instrument: UP,
        venue_order_id: "0xvenue".into(),
        side,
        price: Price(price),
        qty: Qty(qty),
        filled: Qty(0),
        status: VenueOrderStatus::New,
    }
}

/// `classify_unmapped` decides what an order the venue reports but this run cannot name means,
/// keyed on whether a placement for that (instrument, side) is still in flight. Adopting a resting
/// order while our own placement on the side has no venue id yet would bind a SECOND venue id to our
/// slot the moment our own answer lands, double-folding the position — so it defers instead. Once a
/// placement is lost (ambiguous, no venue id ever coming) it becomes the only explanation for such
/// an order and is adopted; a prior run's order carries no client id this run could mint, so it can
/// never be an adoption — the boot sweep cancels it by venue id instead.
#[test]
fn classify_unmapped_decides_by_placement_state_and_match() {
    struct Case {
        name: &'static str,
        mirror: Vec<MirroredOrder>,
        resting: UnmappedOrder,
        venue_id_landed: bool,
        expected: UnmappedVerdict,
    }

    let cases = [
        Case {
            name: "a lost placement's answer is adopted by the order it lost",
            mirror: vec![lost(0xa1, Side::Buy, 45_000_000, 500_000_000)],
            resting: resting(Side::Buy, 45_000_000, 500_000_000),
            venue_id_landed: false,
            expected: UnmappedVerdict::Adopt(ClientOrderId(0xa1)),
        },
        Case {
            name: "an exact price and size match still defers — the answer decides, not a guess",
            mirror: vec![mine(0xa1, Side::Buy, 45_000_000, 500_000_000)],
            resting: resting(Side::Buy, 45_000_000, 500_000_000),
            venue_id_landed: false,
            expected: UnmappedVerdict::Defer,
        },
        Case {
            name: "deferral is keyed on the side, not on a match: a different order on it still waits",
            mirror: vec![mine(0xa1, Side::Buy, 45_000_000, 500_000_000)],
            resting: resting(Side::Buy, 44_000_000, 700_000_000),
            venue_id_landed: false,
            expected: UnmappedVerdict::Defer,
        },
        Case {
            name: "the OTHER side has no placement in flight, so an order there is not deferred",
            mirror: vec![mine(0xa1, Side::Buy, 45_000_000, 500_000_000)],
            resting: resting(Side::Sell, 45_000_000, 500_000_000),
            venue_id_landed: false,
            expected: UnmappedVerdict::LeaveAlone,
        },
        Case {
            name: "once our own placement's venue id has arrived it blocks nothing, and the resting \
                   order is then unexplained — left alone, never adopted",
            mirror: vec![mine(0xa1, Side::Buy, 45_000_000, 500_000_000)],
            resting: resting(Side::Buy, 45_000_000, 500_000_000),
            venue_id_landed: true,
            expected: UnmappedVerdict::LeaveAlone,
        },
        Case {
            name: "a ladder can leave more than one unanswered slot; price and size pick between them",
            mirror: vec![
                lost(0xa1, Side::Buy, 45_000_000, 500_000_000),
                lost(0xa2, Side::Buy, 44_000_000, 700_000_000),
            ],
            resting: resting(Side::Buy, 44_000_000, 700_000_000),
            venue_id_landed: false,
            expected: UnmappedVerdict::Adopt(ClientOrderId(0xa2)),
        },
        Case {
            name: "a prior run's order carries no client id this run could mint, so it is never adopted",
            mirror: vec![MirroredOrder {
                provenance: Provenance::PriorRun,
                ..lost(0xa1, Side::Buy, 45_000_000, 500_000_000)
            }],
            resting: resting(Side::Buy, 45_000_000, 500_000_000),
            venue_id_landed: false,
            expected: UnmappedVerdict::LeaveAlone,
        },
    ];

    for case in cases {
        let got = classify_unmapped(&case.mirror, &case.resting, |_| case.venue_id_landed);
        assert_eq!(got, case.expected, "case: {}", case.name);
    }
}

/// At boot the token table is empty until the first rotation lands, so a prior run's order rests on
/// a token this run cannot map. Dropped, it would fill behind an armed engine spending real balance
/// the ledger never sees. The codec must SURFACE it — id and side, which is all a cancel needs — so
/// the boot branch can sweep it by venue id instead of never seeing it.
#[test]
fn an_order_on_an_unbound_token_is_surfaced_for_the_boot_sweep_not_dropped() {
    let tokens = TokenTable::with_retired_capacity(4);
    let orders = OrderIndex::with_capacity(16);
    let context = DecodeContext {
        tokens: &tokens,
        orders: &orders,
        api_key: "apikey",
        received_ts_us: at(0),
    };
    let body = r#"{"data":[{"id":"0xprior","asset_id":"unbound-token","side":"SELL","price":"0.62","original_size":"5","size_matched":"0","status":"LIVE","created_at":1}]}"#;
    let VenueAnswer::Answered(decoded) = decode_orders_page(
        HttpAnswer { status: 200, body },
        OrdersRead {
            instrument: UP,
            recon_seq: 0,
        },
        &context,
    )
    .expect("a well-formed page decodes") else {
        panic!("a 200 page is answered, not unavailable");
    };
    assert!(
        decoded.unmapped.is_empty(),
        "the token is bound to nothing, so this is not a bound-token unmapped order"
    );
    assert_eq!(
        decoded.unattributable.len(),
        1,
        "an order on an unbound token is surfaced, never silently dropped"
    );
    assert_eq!(&*decoded.unattributable[0].venue_order_id, "0xprior");
    assert_eq!(decoded.unattributable[0].side, Side::Sell);
    assert!(
        decoded
            .events
            .iter()
            .all(|event| event.kind == ExecKind::SnapshotEnd),
        "nothing was mappable, so the only event is the readiness marker"
    );
}

/// The hot account table releases a fill's reservation only against a balance stamped LATER than
/// the reservation. An edge that reports a fill and no new balance holds that reservation forever,
/// and the next flatten starves at the funds gate against a wallet that is demonstrably full. A
/// placement that rested without matching moves no money, and a snapshot is a READ of a fill that
/// was already accounted for — restating on either would make a resync trigger the read that
/// triggers the next resync.
#[test]
fn restates_balances_classifies_every_exec_kind() {
    struct Case {
        name: &'static str,
        kind: ExecKind,
        last_qty: i64,
        cumulative_qty: i64,
        expected: bool,
    }

    let cases = [
        Case {
            name: "a maker fill: the order update is the only report of it that exists",
            kind: ExecKind::ReportTrade,
            last_qty: 0,
            cumulative_qty: 200_000_000,
            expected: true,
        },
        Case {
            name: "a taker fill: the placement answer reports it once and never again",
            kind: ExecKind::AckPlaced,
            last_qty: 200_000_000,
            cumulative_qty: 200_000_000,
            expected: true,
        },
        Case {
            name: "a placement that rested without matching moves no money",
            kind: ExecKind::AckPlaced,
            last_qty: 0,
            cumulative_qty: 0,
            expected: false,
        },
        Case {
            name: "a snapshot carries a filled size too — that is exactly the trap",
            kind: ExecKind::SnapshotOrder,
            last_qty: 0,
            cumulative_qty: 200_000_000,
            expected: false,
        },
    ];

    let base = || exec_event(UP, ClientOrderId(0xa1), Side::Buy, 45_000_000, 0);
    for case in cases {
        let event = ExecEvent {
            kind: case.kind,
            last_qty: Qty(case.last_qty),
            cumulative_qty: Qty(case.cumulative_qty),
            ..base()
        };
        assert_eq!(
            restates_balances(&event),
            case.expected,
            "case: {}",
            case.name
        );
    }

    for quiet in [
        ExecKind::AckCanceled,
        ExecKind::AckFailed,
        ExecKind::ReportNew,
        ExecKind::ReportCanceled,
        ExecKind::PlaceNotSent,
        ExecKind::AmendNotSent,
        ExecKind::SnapshotEnd,
        ExecKind::StreamReady,
        ExecKind::StreamReset,
    ] {
        assert!(
            !restates_balances(&ExecEvent {
                kind: quiet,
                ..base()
            }),
            "{quiet:?} moves nothing"
        );
    }
}

/// The dead man's switch chains: each call echoes the id the last answer carried. A stale id is
/// refused WITH the expected id in the body, so the refusal is itself the recovery — one reader
/// serves both, or a single stale id ends the chain and the venue cancels the book.
#[test]
fn the_heartbeat_id_is_read_from_a_refusal_as_readily_as_from_a_success() {
    assert_eq!(
        &*decode_heartbeat(r#"{"heartbeat_id":"1a2b"}"#).expect("a success carries the next id"),
        "1a2b"
    );
    assert_eq!(
        &*decode_heartbeat(r#"{"error_msg":"Invalid Heartbeat ID","heartbeat_id":"3c4d"}"#)
            .expect("a stale-id refusal carries the id the venue expected"),
        "3c4d"
    );
    assert!(
        decode_heartbeat(r#"{"error_msg":"Unauthorized"}"#).is_err(),
        "an answer with no id at all cannot continue the chain"
    );
}

/// `/clob-markets` does not publish this flag; it has its own endpoint, and the answer picks which
/// exchange contract every order on the token is signed against.
#[test]
fn the_neg_risk_read_is_a_bare_flag() {
    assert!(!decode_neg_risk(r#"{"neg_risk":false}"#).expect("the documented shape"));
    assert!(decode_neg_risk(r#"{"neg_risk":true}"#).expect("the documented shape"));
    assert!(
        !decode_neg_risk("{}").expect("an absent flag reads as the standard exchange"),
        "absence is the common case and must not be a parse failure"
    );
}
