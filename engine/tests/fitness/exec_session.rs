//! Execution-session fitness: what every venue's edge does when a request cannot be sent, may have
//! been sent, or never gets an answer, and when a connection drops or comes back. Each rule keeps a
//! hot order slot from waiting forever or an order from being sent twice, and each one fails
//! SILENTLY if broken: a swallowed refusal has the strategy quoting around an order nobody placed,
//! a resent request can leave two live orders where one was meant, and an inherited order taken for
//! this run's own is never cancelled.

use polysim::adapters::exec::{
    ExecCore, ExecEffect, ExecRequest, ExecSession, InFlightRequest, MAX_RESYNC_ATTEMPTS,
    MirroredOrder, Outgoing, Phase, PlaceNotSentReason, REQUEST_TIMEOUT, ResyncPoll, SessionSetup,
    TimeoutFallout,
};
use polysim::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{
    CancelReason, ExecCommand, ExecEvent, ExecKind, OrderStyle, Provenance, RejectClass,
    VenueOrderStatus,
};
use polysim::shutdown::FatalSignal;
use polysim::time::{DurationUs, TsUs};

type Session = ExecSession<Vec<ExecEvent>>;

const VENUE: &str = "test-venue";
const INSTRUMENT: InstrumentId = InstrumentId(0);
const OTHER_INSTRUMENT: InstrumentId = InstrumentId(1);
const NOW: TsUs = TsUs::from_micros(1_785_000_000_000_000);
const PRICE: Price = Price(11_800_000_000_000);

fn new_session() -> (Session, FatalSignal) {
    let fatal = FatalSignal::new();
    let session = ExecSession::new(SessionSetup {
        venue: VENUE,
        core: ExecCore::with_limits(8, 32),
        instruments: vec![INSTRUMENT, OTHER_INSTRUMENT],
        events: Vec::new(),
        fatal: fatal.clone(),
    });
    (session, fatal)
}

/// Connected, its subscribe in flight, its pass finished and readiness armed: the only state that
/// quotes.
fn quoting_session() -> (Session, FatalSignal) {
    let (mut session, fatal) = new_session();
    let subscribe = session.connect();
    record(&mut session, subscribe, NOW);
    let resync_seq = session.begin_pass(1);
    assert!(session.resync.on_read(resync_seq));
    session.finish_pass();
    assert!(session.arm_readiness(NOW));
    session.events.clear();
    (session, fatal)
}

/// What a venue's dispatch does with the core's decision: every request it sends is recorded as in
/// flight under the hot sequence it answers.
fn send(session: &mut Session, command: ExecCommand, sent_at: TsUs) -> Vec<ExecRequest> {
    let mut outgoing = Vec::new();
    session.core.on_command(
        command,
        &mut Outgoing::sink(&mut outgoing, command.recon_seq()),
    );
    record(session, outgoing, sent_at)
}

fn record(session: &mut Session, outgoing: Vec<Outgoing>, sent_at: TsUs) -> Vec<ExecRequest> {
    let mut sent = Vec::new();
    for Outgoing { effect, recon_seq } in outgoing {
        if let ExecEffect::Send {
            request_id,
            request,
        } = effect
        {
            session.inflight.record(InFlightRequest {
                request_id,
                request,
                sent_at,
                recon_seq,
            });
            sent.push(request);
        }
    }
    sent
}

fn place(client_id: u64, side: Side) -> ExecCommand {
    ExecCommand::Place {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        side,
        price: PRICE,
        qty: Qty(8_000),
        style: OrderStyle::PostOnly,
    }
}

fn place_request(client_id: u64, side: Side) -> ExecRequest {
    ExecRequest::Place {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        side,
        price: PRICE,
        qty: Qty(8_000),
        style: OrderStyle::PostOnly,
    }
}

fn cancel(client_id: u64) -> ExecCommand {
    ExecCommand::Cancel {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
    }
}

fn prior_run_order(client_id: u64) -> MirroredOrder {
    MirroredOrder {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        side: Side::Buy,
        price: PRICE,
        qty: Qty(8_000),
        provenance: Provenance::PriorRun,
        has_sent_cancel: false,
        is_ambiguous: false,
    }
}

/// A venue report of a resting order the codec attributed to this run by its client id.
fn resting_report(client_id: u64) -> ExecEvent {
    ExecEvent {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        venue_order_id: None,
        trade_id: None,
        kind: ExecKind::ReportNew,
        status: Some(VenueOrderStatus::New),
        reject: None,
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: None,
        price: PRICE,
        qty: Qty(8_000),
        last_price: Price(0),
        last_qty: Qty(0),
        cumulative_qty: Qty(0),
        cumulative_quote: 0,
        commission: 0,
        commission_asset: AssetId::UNKNOWN,
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: NOW,
        request_sent_ts_us: None,
        received_ts_us: NOW,
        queued_ts_us: NOW,
    }
}

fn kinds(events: &[ExecEvent]) -> Vec<ExecKind> {
    events.iter().map(|event| event.kind).collect()
}

fn provenance_of(session: &Session, client_id: u64) -> Option<Provenance> {
    session
        .core
        .mirrored(ClientOrderId(client_id))
        .map(|order| order.provenance)
}

fn sent(outgoing: &[Outgoing]) -> Vec<(ExecRequest, u64)> {
    outgoing
        .iter()
        .filter_map(|sent| match sent.effect {
            ExecEffect::Send { request, .. } => Some((request, sent.recon_seq)),
            _ => None,
        })
        .collect()
}

fn cancel_request(client_id: u64) -> ExecRequest {
    ExecRequest::Cancel {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
    }
}

fn status_request(client_id: u64) -> ExecRequest {
    ExecRequest::OrderStatus {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
    }
}

fn assert_not_quoting(session: &Session, step: &str) {
    assert_ne!(
        session.core.phase(),
        Phase::Quoting,
        "{step} opened quoting"
    );
    assert!(
        !session.core.has_opened_quoting(),
        "{step} latched quoting open"
    );
}

/// FITNESS: a placement the edge could not send releases this run's reservation and nothing else,
/// always answers the hot slot, and stops the run only when the refusal proves the local state
/// corrupt.
///
/// A prior run's order under the same id is still resting on the venue, so releasing it would
/// leave an order no sweep can reach. The core's own refusal of a duplicate id is answered the same
/// way: the first order still owns that reservation. Stopping on an ordinary refusal would end a run
/// over a dropped socket; carrying on after a duplicate id or a full mirror would place orders the
/// engine cannot account for.
#[test]
fn a_refused_placement_releases_only_this_runs_reservation_and_trips_only_on_corrupt_state() {
    let (mut ours, ours_fatal) = quoting_session();
    send(&mut ours, place(1, Side::Sell), NOW);
    ours.refuse(
        place_request(1, Side::Sell),
        PlaceNotSentReason::NoTransport,
        NOW,
    );
    assert_eq!(
        provenance_of(&ours, 1),
        None,
        "this run's reservation stayed"
    );
    assert_eq!(kinds(&ours.events), [ExecKind::PlaceNotSent]);
    assert_eq!(ours.events[0].client_id, ClientOrderId(1));
    assert_eq!(ours.events[0].side, Side::Sell);
    assert!(!ours_fatal.is_tripped());

    let (mut inherited, _) = quoting_session();
    inherited
        .core
        .observe_venue_order(prior_run_order(2))
        .expect("one prior-run order fits the mirror");
    inherited.refuse(
        place_request(2, Side::Buy),
        PlaceNotSentReason::NoTransport,
        NOW,
    );
    assert_eq!(
        provenance_of(&inherited, 2),
        Some(Provenance::PriorRun),
        "a refused placement released a prior run's order that is still resting"
    );
    assert_eq!(kinds(&inherited.events), [ExecKind::PlaceNotSent]);

    let (mut duplicated, duplicated_fatal) = quoting_session();
    send(&mut duplicated, place(3, Side::Buy), NOW);
    let mut effects = Vec::new();
    duplicated
        .core
        .on_command(place(3, Side::Buy), &mut |effect| effects.push(effect));
    for effect in &effects {
        duplicated.on_unsent(effect, NOW);
    }
    assert!(
        duplicated_fatal.is_tripped(),
        "a duplicate client id ran on"
    );
    assert_eq!(kinds(&duplicated.events), [ExecKind::PlaceNotSent]);
    assert_eq!(
        provenance_of(&duplicated, 3),
        Some(Provenance::Mine),
        "the duplicate's refusal released the first order's reservation"
    );

    for (reason, is_fatal) in [
        (PlaceNotSentReason::PhaseClosed, false),
        (PlaceNotSentReason::SideCapacity, false),
        (PlaceNotSentReason::DuplicateClientId, true),
        (PlaceNotSentReason::MirrorStorage, true),
        (PlaceNotSentReason::Encoding, false),
        (PlaceNotSentReason::NoTransport, false),
    ] {
        let (mut session, fatal) = quoting_session();
        session.refuse(place_request(4, Side::Buy), reason, NOW);
        assert_eq!(fatal.is_tripped(), is_fatal, "{reason:?}");
        assert_eq!(
            kinds(&session.events),
            [ExecKind::PlaceNotSent],
            "{reason:?} left the hot slot waiting"
        );
        if is_fatal {
            assert!(
                fatal.reason().is_some_and(|text| text.contains(VENUE)),
                "{reason:?} stopped the run without naming the venue"
            );
        }
    }
}

/// FITNESS: a cancel the edge could not send re-arms its latch, and an amend it could not send is
/// reported back with the order left as it was.
///
/// A cancel left latched is skipped by every later sweep, the shutdown sweep included, so the order
/// would outlive the process. An amend that never left changes nothing on the venue, and without the
/// report the hot slot waits out its in-flight timeout before the side can quote again.
#[test]
fn a_refused_cancel_rearms_and_a_refused_amend_frees_the_slot() {
    let (mut session, fatal) = quoting_session();
    send(&mut session, place(1, Side::Buy), NOW);
    assert_eq!(send(&mut session, cancel(1), NOW).len(), 1);
    assert!(
        session
            .core
            .mirrored(ClientOrderId(1))
            .is_some_and(|order| order.has_sent_cancel)
    );

    session.refuse(cancel_request(1), PlaceNotSentReason::NoTransport, NOW);
    assert!(
        session
            .core
            .mirrored(ClientOrderId(1))
            .is_some_and(|order| !order.has_sent_cancel),
        "a refused cancel stayed latched"
    );
    assert_eq!(
        send(&mut session, cancel(1), NOW),
        [cancel_request(1)],
        "the next cancel command was skipped as already in flight"
    );
    assert!(session.events.is_empty());

    let resting = session.core.mirror().to_vec();
    let refused_amend = ExecRequest::AmendQty {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(1),
        qty: Qty(4_000),
    };
    session.refuse(refused_amend, PlaceNotSentReason::NoTransport, NOW);
    assert_eq!(
        session.core.mirror(),
        resting.as_slice(),
        "an amend that never left changed the mirror"
    );
    assert_eq!(kinds(&session.events), [ExecKind::AmendNotSent]);
    assert_eq!(session.events[0].client_id, ClientOrderId(1));
    assert!(!fatal.is_tripped());
}

/// FITNESS: a request whose bytes may have reached the venue leaves its order ambiguous and answers
/// the hot slot as ambiguous, and nothing is put back on the wire.
///
/// The order may exist, so a resend could leave two live orders and a release could strand one;
/// only a read can settle it. The event carries the side the mirror knows, because the hot side
/// matches the slot on it.
#[test]
fn a_request_the_wire_may_have_seen_is_in_doubt_and_never_resent() {
    let (mut session, _) = quoting_session();
    session.core.on_command(place(1, Side::Sell), &mut |_| {});

    session.mark_in_doubt(place_request(1, Side::Sell), NOW);
    assert!(
        session
            .core
            .mirrored(ClientOrderId(1))
            .is_some_and(|order| order.is_ambiguous)
    );
    let [in_doubt] = session.events.as_slice() else {
        panic!("expected one event, got {:?}", session.events);
    };
    assert_eq!(in_doubt.kind, ExecKind::AckFailed);
    assert_eq!(in_doubt.reject, Some(RejectClass::Ambiguous));
    assert_eq!(in_doubt.client_id, ClientOrderId(1));
    assert_eq!(in_doubt.side, Side::Sell);
    assert_eq!(in_doubt.exchange_ts_us, NOW);

    session.events.clear();
    session.mark_in_doubt(status_request(7), NOW);
    assert_eq!(kinds(&session.events), [ExecKind::AckFailed]);
    assert_eq!(session.events[0].side, Side::Buy);

    session.events.clear();
    session.mark_in_doubt(
        ExecRequest::OpenOrders {
            instrument: INSTRUMENT,
        },
        NOW,
    );
    session.mark_in_doubt(ExecRequest::SubscribeUserStream, NOW);
    assert!(
        session.events.is_empty(),
        "a read that may have been seen names no order: {:?}",
        session.events
    );
    assert_eq!(
        session.inflight.take_all().len(),
        1,
        "only the subscribe sent at connect is in flight; nothing was resent"
    );
}

/// FITNESS: a lost connection makes every outstanding order request ambiguous, then resets the
/// stream, and the reset is the last thing the hot thread hears.
///
/// The answers died with the socket, so each request is exactly as unresolved as a timed-out one.
/// The reset invalidates every hot slot and withdraws readiness, so it closes the sequence.
#[test]
fn losing_the_connection_dooms_every_outstanding_request_then_resets_the_stream_last() {
    let (mut session, _) = quoting_session();
    send(&mut session, place(1, Side::Sell), NOW);
    send(&mut session, place(2, Side::Buy), NOW);
    send(&mut session, cancel(2), NOW);
    let reconcile = ExecCommand::ReconcileOpenOrders {
        instrument: INSTRUMENT,
        recon_seq: 5,
    };
    send(&mut session, reconcile, NOW);

    session.lose_connection(NOW);

    assert_eq!(
        kinds(&session.events),
        [
            ExecKind::AckFailed,
            ExecKind::AckFailed,
            ExecKind::AckFailed,
            ExecKind::StreamReset,
        ]
    );
    let doomed: Vec<(ClientOrderId, Side, Option<RejectClass>)> = session.events[..3]
        .iter()
        .map(|event| (event.client_id, event.side, event.reject))
        .collect();
    let ambiguous = Some(RejectClass::Ambiguous);
    assert_eq!(
        doomed,
        [
            (ClientOrderId(1), Side::Sell, ambiguous),
            (ClientOrderId(2), Side::Buy, ambiguous),
            (ClientOrderId(2), Side::Buy, ambiguous),
        ]
    );
    assert!(session.inflight.take_all().is_empty());
    assert_eq!(session.core.phase(), Phase::Down);
    assert!(session.core.mirror().iter().all(|order| order.is_ambiguous));
}

/// FITNESS: a request past its deadline is reconciled with a status probe under the hot sequence
/// it belonged to, never resent; an unanswered read is handed back to the venue to decide; and only
/// a lost placement is flagged as one.
///
/// Resending risks a second live order. The probe's answer must land in the reconcile pass that
/// asked, or the hot side settles the wrong pass. A lost placement may sit on the venue under an id
/// this run never learned, which is why a venue may need to look for it.
#[test]
fn an_unanswered_request_is_reconciled_with_a_status_probe_under_its_own_hot_sequence() {
    let (mut session, _) = quoting_session();
    session.core.on_command(place(1, Side::Sell), &mut |_| {});
    let reconcile_order = ExecCommand::ReconcileOrder {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(1),
        recon_seq: 41,
    };
    send(&mut session, reconcile_order, NOW);
    let reconcile_open = ExecCommand::ReconcileOpenOrders {
        instrument: INSTRUMENT,
        recon_seq: 42,
    };
    send(&mut session, reconcile_open, NOW);

    let early = session.expire(NOW + REQUEST_TIMEOUT - DurationUs::from_micros(1));
    assert!(early.outgoing.is_empty() && early.unanswered_reads.is_empty());
    assert!(session.events.is_empty());

    let expired = session.expire(NOW + REQUEST_TIMEOUT);
    assert_eq!(sent(&expired.outgoing), [(status_request(1), 41)]);
    assert_eq!(
        expired.unanswered_reads,
        [
            TimeoutFallout::StreamUnusable,
            TimeoutFallout::ReadAbandoned
        ]
    );
    assert!(!expired.has_lost_placement);
    assert_eq!(kinds(&session.events), [ExecKind::AckFailed]);
    assert_eq!(session.events[0].side, Side::Sell);
    assert!(
        session
            .core
            .mirrored(ClientOrderId(1))
            .is_some_and(|order| order.is_ambiguous)
    );

    let (mut placing, _) = quoting_session();
    send(&mut placing, place(2, Side::Buy), NOW);
    let lost = placing.expire(NOW + REQUEST_TIMEOUT);
    assert!(lost.has_lost_placement);
    assert_eq!(sent(&lost.outgoing), [(status_request(2), 0)]);
}

/// FITNESS: a failed resync pass is re-read under a fresh sequence once its backoff is due, and the
/// connection is given up at the attempt cap.
///
/// A stale read from an abandoned pass must never settle the new one, so each re-read is numbered
/// afresh. Without the cap a connection that cannot read its state would stay unready forever
/// instead of being dropped and rebuilt.
#[test]
fn a_stalled_pass_retries_with_a_new_sequence_then_gives_up_at_the_attempt_cap() {
    let (mut session, _) = new_session();
    let mut current = session.begin_pass(3);
    assert_eq!(session.poll_resync(NOW, 3), ResyncPoll::Wait);

    let mut rereads = 0;
    let mut failed_at = NOW;
    loop {
        let retry_at = failed_at + DurationUs::from_secs(1);
        session.resync.on_failure(current, retry_at);
        assert_eq!(
            session.poll_resync(retry_at - DurationUs::from_micros(1), 3),
            ResyncPoll::Wait,
            "a retry ran before its backoff"
        );
        match session.poll_resync(retry_at, 3) {
            ResyncPoll::Reread { resync_seq } => {
                assert!(resync_seq > current, "a re-read reused a sequence");
                current = resync_seq;
                rereads += 1;
            }
            ResyncPoll::GiveUp => break,
            ResyncPoll::Wait => panic!("a due retry waited"),
        }
        failed_at = retry_at;
        assert!(rereads <= MAX_RESYNC_ATTEMPTS, "the pass never gave up");
    }
    assert_eq!(rereads, MAX_RESYNC_ATTEMPTS - 1);
    assert!(session.resync.is_outstanding());
}

/// FITNESS: readiness arms only after a pass has finished, at most once per pass, and ends with one
/// open-orders snapshot end per instrument.
///
/// The engine stays unready until every instrument's snapshot end lands, so a missing one leaves an
/// instrument unquotable and an early one quotes against a mirror nobody has verified. An order a
/// prior run left resting holds readiness back without spending the pass.
#[test]
fn readiness_arms_once_after_the_pass_finishes_with_one_snapshot_end_per_instrument() {
    let (mut session, _) = new_session();
    let subscribe = session.connect();
    assert!(matches!(
        subscribe.as_slice(),
        [Outgoing {
            effect: ExecEffect::Send {
                request: ExecRequest::SubscribeUserStream,
                ..
            },
            recon_seq: 0,
        }]
    ));
    assert!(!session.arm_readiness(NOW), "armed before any pass ran");
    let first = session.begin_pass(3);
    assert!(!session.arm_readiness(NOW), "armed mid-pass");
    session.finish_pass();
    let second = session.begin_pass(3);
    assert!(second > first);
    assert!(
        !session.arm_readiness(NOW),
        "a new pass kept the last one's licence"
    );
    session.finish_pass();

    assert!(session.arm_readiness(NOW));
    assert_eq!(session.core.phase(), Phase::Quoting);
    let ends: Vec<(ExecKind, InstrumentId, TsUs)> = session
        .events
        .iter()
        .map(|event| (event.kind, event.instrument, event.exchange_ts_us))
        .collect();
    assert_eq!(
        ends,
        [
            (ExecKind::SnapshotEnd, INSTRUMENT, NOW),
            (ExecKind::SnapshotEnd, OTHER_INSTRUMENT, NOW),
        ]
    );
    assert!(!session.arm_readiness(NOW), "readiness armed twice");
    assert_eq!(session.events.len(), 2);

    session.lose_connection(NOW);
    let _subscribe = session.connect();
    assert!(
        !session.arm_readiness(NOW),
        "a reconnect armed on the last connection's pass"
    );
    assert_eq!(session.core.phase(), Phase::Resyncing);

    let (mut blocked, _) = new_session();
    let _subscribe = blocked.connect();
    let resync_seq = blocked.begin_pass(1);
    blocked
        .core
        .observe_venue_order(prior_run_order(9))
        .expect("one prior-run order fits the mirror");
    assert!(blocked.resync.on_read(resync_seq));
    blocked.finish_pass();
    assert!(
        !blocked.arm_readiness(NOW),
        "armed over a prior run's order"
    );
    assert_eq!(blocked.core.phase(), Phase::Resyncing);
    assert!(blocked.events.is_empty());
    blocked
        .core
        .on_order_gone(ClientOrderId(9), Qty(0), &mut |_| {});
    assert!(
        blocked.arm_readiness(NOW),
        "the finished pass did not arm once the prior run's order was gone"
    );
}

/// FITNESS: an order found resting before this run first quotes is a prior run's and is cancelled;
/// once the run has quoted, the codec's attribution stands, even across a reconnect; and readiness is
/// the only door to quoting.
///
/// Before the first quote this run has placed nothing, so a resting order with its tag is an
/// earlier process's leftover and must go. After it, the same order may be this run's own, and
/// cancelling it would pull a live quote. A second route to `Quoting` would open the latch without
/// the resync that proves the mirror complete.
#[test]
fn an_order_found_before_quoting_opened_is_prior_run_and_after_it_is_ours() {
    let (mut early, _) = new_session();
    let _subscribe = early.connect();
    assert!(!early.core.has_opened_quoting());
    let mut outgoing = Vec::new();
    early.fold(&resting_report(1), 0, &mut outgoing);
    assert_eq!(provenance_of(&early, 1), Some(Provenance::PriorRun));
    assert!(
        sent(&outgoing).contains(&(cancel_request(1), 0)),
        "an inherited order was left resting: {outgoing:?}"
    );

    let _cancels = early.sweep(CancelReason::Park, Some(INSTRUMENT));
    assert_not_quoting(&early, "a scoped sweep");
    early.lose_connection(NOW);
    assert_not_quoting(&early, "a lost connection");
    let _subscribe = early.connect();
    assert_not_quoting(&early, "a reconnect");
    let mut gone = resting_report(1);
    gone.kind = ExecKind::ReportCanceled;
    gone.status = Some(VenueOrderStatus::Canceled);
    early.fold(&gone, 0, &mut Vec::new());
    assert_not_quoting(&early, "the inherited order leaving");
    early.refuse(
        place_request(5, Side::Buy),
        PlaceNotSentReason::NoTransport,
        NOW,
    );
    assert_not_quoting(&early, "a refused placement");
    early.finish_pass();
    assert!(!early.arm_readiness(NOW), "readiness armed while down");
    assert_not_quoting(&early, "readiness asked for while down");

    let (mut late, _) = quoting_session();
    assert!(late.core.has_opened_quoting());
    let mut outgoing = Vec::new();
    late.fold(&resting_report(2), 0, &mut outgoing);
    assert_eq!(provenance_of(&late, 2), Some(Provenance::Mine));
    assert!(sent(&outgoing).is_empty(), "this run's order was cancelled");

    late.lose_connection(NOW);
    let _subscribe = late.connect();
    assert_eq!(late.core.phase(), Phase::Resyncing);
    assert!(
        late.core.has_opened_quoting(),
        "a reconnect reset the latch"
    );
    let mut outgoing = Vec::new();
    late.fold(&resting_report(3), 0, &mut outgoing);
    assert_eq!(provenance_of(&late, 3), Some(Provenance::Mine));
    assert!(sent(&outgoing).is_empty(), "this run's order was cancelled");
}
