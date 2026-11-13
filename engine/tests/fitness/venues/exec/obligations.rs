//! The nine written obligations of a venue execution edge, each as one test body over a harness: a
//! venue stand-in behind the real codec of that venue. Each venue runs every body, so a new venue
//! cannot skip one.

use engine::msg::exec::{
    AccountChunk, CancelReason, ExecCommand, ExecEvent, ExecKind, OrderStyle, RejectClass,
    VenueOrderStatus,
};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, LocalTs};
use engine::runtime::config::RecordedTables;
use engine::runtime::shutdown::FatalSignal;
use engine::venues::exec::{
    ExecCore, ExecEffect, ExecRequest, ExecSession, InFlightRequest, Outgoing, PlaceNotSentReason,
    SessionSetup,
};

use crate::engine_support::ts;
use crate::exec_support::{
    ASK, BID, CEILING, QuotingSetup, SentEvents, TwoSidedQuoter, built_quoting_engine,
    drain_commands, exec_settings, make_ready, reseat_book, row_with_ceiling, spin_at,
};

pub(crate) type Session = ExecSession<SentEvents>;

/// One report of balances from the venue, in the order the venue sent it.
pub(crate) enum BalanceReport {
    /// Absolute balances, in chunks.
    Chunks(Vec<AccountChunk>),
    /// A change of balance that the edge must not fold.
    Delta,
}

/// A venue execution edge with a stand-in venue behind its real codec.
pub(crate) trait EdgeHarness: Sized {
    const VENUE: &'static str;
    const INSTRUMENT: InstrumentId;
    /// The run nonce of the identity that the stand-in answers to, which a client id of this run
    /// carries in its high bits.
    const RUN_NONCE: u32;
    /// A price on this venue's grid at which a buy rests and does not cross.
    const RESTING_BID: Price;
    const QTY: Qty;

    fn new() -> Self;
    /// What the edge pushes to its input queue for the stand-in's answer to `request`, after the
    /// stand-in applies it.
    fn answer(&mut self, request: ExecRequest, at: LocalTs) -> Vec<InboundMessage>;
    /// What the edge pushes for a fill of `qty` of a resting order.
    fn fill(&mut self, client_id: ClientOrderId, qty: Qty, at: LocalTs) -> Vec<InboundMessage>;
    /// A full read of the balances, then the balance reports of the stream, in venue order.
    fn balance_reports(&mut self) -> Vec<BalanceReport>;
    /// The events of an answer that refuses `request` with a venue code that the codec does not
    /// map.
    fn unmapped_refusal(&mut self, request: ExecRequest, at: LocalTs) -> Vec<ExecEvent>;
    /// The event of a report that says the cancel of `client_id` is pending.
    fn pending_cancel(&mut self, client_id: ClientOrderId, at: LocalTs) -> ExecEvent;
    /// An open order of `client_id` whose price does not decode: its venue order id, and whether
    /// the codec decoded it anyway.
    fn undecodable_open_order(&mut self, client_id: ClientOrderId) -> (i64, bool);
}

pub(crate) const NOW: LocalTs = LocalTs::from_micros(1_785_000_000_000_000);
const INFLIGHT_TIMEOUT: DurationUs = DurationUs::from_millis(5_000);

/// A client id of this run: run nonce, slot, generation 1.
pub(crate) fn own<H: EdgeHarness>(slot: u64) -> ClientOrderId {
    ClientOrderId((u64::from(H::RUN_NONCE) << 32) | (slot << 16) | 1)
}

pub(crate) fn exec_events(messages: &[InboundMessage]) -> Vec<ExecEvent> {
    messages
        .iter()
        .filter_map(|message| match message.body {
            MessageBody::Exec(event) => Some(event),
            _ => None,
        })
        .collect()
}

/// Connected, its pass finished and readiness armed: the only state that quotes.
pub(crate) fn quoting_session<H: EdgeHarness>() -> Session {
    let mut session = ExecSession::new(SessionSetup {
        venue: H::VENUE,
        core: ExecCore::with_limits(8, 32),
        instruments: vec![H::INSTRUMENT],
        events: SentEvents::default(),
        fatal: FatalSignal::new(),
        inflight_timeout: INFLIGHT_TIMEOUT,
    });
    let subscribe = session.connect();
    record(&mut session, subscribe, NOW);
    let resync_seq = session.begin_pass(1);
    assert!(session.resync.on_read(resync_seq));
    session.finish_pass();
    assert!(session.arm_readiness(NOW));
    session.events.clear();
    session
}

fn record(session: &mut Session, outgoing: Vec<Outgoing>, sent_at: LocalTs) -> Vec<ExecRequest> {
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

/// The requests that the core sends for `command`, recorded as in flight.
pub(crate) fn send(session: &mut Session, command: ExecCommand, at: LocalTs) -> Vec<ExecRequest> {
    let mut outgoing = Vec::new();
    session.core.on_command(
        command,
        &mut Outgoing::sink(&mut outgoing, command.recon_seq()),
    );
    record(session, outgoing, at)
}

pub(crate) fn place<H: EdgeHarness>(client_id: ClientOrderId) -> ExecCommand {
    ExecCommand::Place {
        instrument: H::INSTRUMENT,
        client_id,
        side: Side::Buy,
        price: H::RESTING_BID,
        qty: H::QTY,
        style: OrderStyle::PostOnly,
    }
}

fn place_request<H: EdgeHarness>(client_id: ClientOrderId) -> ExecRequest {
    ExecRequest::Place {
        instrument: H::INSTRUMENT,
        client_id,
        side: Side::Buy,
        price: H::RESTING_BID,
        qty: H::QTY,
        style: OrderStyle::PostOnly,
    }
}

fn status_request<H: EdgeHarness>(client_id: ClientOrderId) -> ExecRequest {
    ExecRequest::OrderStatus {
        instrument: H::INSTRUMENT,
        client_id,
    }
}

fn kinds(events: &[ExecEvent]) -> Vec<ExecKind> {
    events.iter().map(|event| event.kind).collect()
}

/// Every command is answered: a refusal, a lost connection, and a lost answer each leave an event
/// for the slot of each order action, so no slot waits for an answer that cannot come.
pub(crate) fn every_command_is_eventually_answered<H: EdgeHarness>() {
    // A refused placement or amend answers its slot with one event.
    for action in [
        place_request::<H>(own::<H>(1)),
        ExecRequest::AmendQty {
            instrument: H::INSTRUMENT,
            client_id: own::<H>(1),
            qty: H::QTY,
        },
    ] {
        let mut session = quoting_session::<H>();
        session.refuse(action, PlaceNotSentReason::NoTransport, NOW);
        assert_eq!(
            session.events.len(),
            1,
            "{action:?} refused: {:?}",
            session.events
        );
    }
    // A refused cancel re-arms its latch, so the next cancel or sweep sends it.
    let mut session = quoting_session::<H>();
    send(&mut session, place::<H>(own::<H>(2)), NOW);
    let cancel = ExecCommand::Cancel {
        instrument: H::INSTRUMENT,
        client_id: own::<H>(2),
    };
    let [refused] = send(&mut session, cancel, NOW)[..] else {
        panic!("one cancel request");
    };
    session.refuse(refused, PlaceNotSentReason::NoTransport, NOW);
    assert!(
        session
            .core
            .mirrored(own::<H>(2))
            .is_some_and(|order| !order.has_sent_cancel)
    );

    let mut session = quoting_session::<H>();
    for slot in 1..=2 {
        send(&mut session, place::<H>(own::<H>(slot)), NOW);
    }
    session.lose_connection(NOW);
    let doomed = kinds(&session.events);
    assert_eq!(
        doomed,
        [
            ExecKind::AckFailed,
            ExecKind::AckFailed,
            ExecKind::StreamReset
        ],
        "each lost placement answers its slot, and the stream reset comes last"
    );

    let mut session = quoting_session::<H>();
    send(&mut session, place::<H>(own::<H>(1)), NOW);
    let expired = session.expire(NOW + INFLIGHT_TIMEOUT);
    assert!(expired.has_lost_placement);
    assert_eq!(kinds(&session.events), [ExecKind::AckFailed]);
}

/// What the edge pushes reaches its one input queue in the order the venue gave it, with received
/// stamps that never go back.
pub(crate) fn one_input_queue_with_monotone_queued_stamps<H: EdgeHarness>() {
    let mut harness = H::new();
    let client_id = own::<H>(1);
    let mut pushed = harness.answer(place_request::<H>(client_id), NOW);
    pushed.extend(harness.fill(client_id, H::QTY, NOW + DurationUs::from_micros(1_000)));
    let events = exec_events(&pushed);
    assert_eq!(
        kinds(&events)[..2],
        [ExecKind::AckPlaced, ExecKind::ReportNew],
        "the answer, then the stream's report of the same order: {events:?}"
    );
    assert_eq!(
        events.last().map(|event| event.kind),
        Some(ExecKind::ReportTrade)
    );
    let stamps: Vec<LocalTs> = pushed
        .iter()
        .map(|message| message.header.received_ts_us)
        .collect();
    assert!(
        stamps.windows(2).all(|pair| pair[0] <= pair[1]),
        "received stamps never go back: {stamps:?}"
    );
}

/// A fill carries the venue's cumulative total for its order, so a report that arrives twice or
/// goes missing never moves the position by the wrong amount.
pub(crate) fn fill_quantities_are_cumulative_venue_totals<H: EdgeHarness>() {
    let mut harness = H::new();
    let client_id = own::<H>(1);
    harness.answer(place_request::<H>(client_id), NOW);
    let third = Qty::from_mantissa(H::QTY.mantissa() / 4);
    let mut cumulative = Vec::new();
    for _ in 0..2 {
        for event in exec_events(&harness.fill(client_id, third, NOW)) {
            assert_eq!(event.last_qty, third);
            cumulative.push(event.cumulative_qty);
        }
    }
    assert_eq!(
        cumulative,
        [third, Qty::from_mantissa(2 * third.mantissa())],
        "each report states the total so far, never a delta"
    );
}

/// Balances arrive as absolute totals whose venue stamp never goes back, and a change of balance
/// is never presented as a balance.
pub(crate) fn balance_chunks_are_absolute_with_a_monotone_venue_stamp<H: EdgeHarness>() {
    let mut stamps = Vec::new();
    let mut deltas = 0;
    for report in H::new().balance_reports() {
        match report {
            BalanceReport::Chunks(chunks) => {
                assert!(chunks.last().is_some_and(|chunk| chunk.is_last_chunk));
                assert!(
                    chunks[..chunks.len() - 1]
                        .iter()
                        .all(|chunk| !chunk.is_last_chunk)
                );
                stamps.extend(chunks.iter().map(|chunk| chunk.venue_update_ts_ms));
            }
            BalanceReport::Delta => deltas += 1,
        }
    }
    assert!(
        stamps.len() >= 2 && deltas >= 1,
        "{stamps:?}, {deltas} deltas"
    );
    assert!(
        stamps.windows(2).all(|pair| pair[0] <= pair[1]),
        "the venue stamp never goes back: {stamps:?}"
    );
}

/// A request that the wire may have seen, or whose answer was lost or ambiguous, is read back and
/// never sent again: the venue may have run it, and a second send could rest a second order.
pub(crate) fn a_timeout_or_ambiguity_reconciles_and_never_resends<H: EdgeHarness>() {
    let mut session = quoting_session::<H>();
    session
        .core
        .on_command(place::<H>(ClientOrderId(1)), &mut |_| {});
    session.mark_in_doubt(place_request::<H>(ClientOrderId(1)), NOW);
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
    assert_eq!(in_doubt.side, Side::Buy);
    assert_eq!(in_doubt.exchange_ts_us, NOW.as_venue_calendar());

    session.events.clear();
    session.mark_in_doubt(status_request::<H>(ClientOrderId(7)), NOW);
    assert_eq!(kinds(&session.events), [ExecKind::AckFailed]);
    assert_eq!(session.events[0].side, Side::Buy);

    session.events.clear();
    session.mark_in_doubt(
        ExecRequest::OpenOrders {
            instrument: H::INSTRUMENT,
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

    for answer in [Answer::Ambiguous, Answer::TimedOut] {
        let mut core = quoting_session::<H>().core;
        core.on_command(place::<H>(ClientOrderId(1)), &mut |_| {});
        let mut sent = Vec::new();
        let mut emit = |effect| {
            if let ExecEffect::Send { request, .. } = effect {
                sent.push(request);
            }
        };
        match answer {
            Answer::Ambiguous => core.on_ambiguous(ClientOrderId(1), &mut emit),
            Answer::TimedOut => core.on_request_timeout(H::INSTRUMENT, ClientOrderId(1), &mut emit),
        }
        assert_eq!(
            sent,
            [status_request::<H>(ClientOrderId(1))],
            "{answer:?}: a status read, never a second placement"
        );
        assert!(core.mirror()[0].is_ambiguous, "{answer:?}");
    }
}

#[derive(Debug, Clone, Copy)]
enum Answer {
    Ambiguous,
    TimedOut,
}

/// A cancel answer is a promise that the order can no longer fill. A venue that cannot promise that
/// yet says the cancel is pending, which ends nothing: the order stays where the edge can see it.
pub(crate) fn a_cancel_answer_is_a_finality_promise<H: EdgeHarness>() {
    let mut harness = H::new();
    let client_id = own::<H>(1);
    let mut session = quoting_session::<H>();
    send(&mut session, place::<H>(client_id), NOW);

    let pending = harness.pending_cancel(client_id, NOW);
    assert_eq!(pending.status, Some(VenueOrderStatus::PendingCancel));
    assert!(!VenueOrderStatus::PendingCancel.is_terminal());
    session.fold(&pending, 0, &mut Vec::new());
    assert!(
        session.core.is_mirrored(client_id),
        "a pending cancel ends nothing"
    );

    harness.answer(place_request::<H>(client_id), NOW);
    let cancel = ExecRequest::Cancel {
        instrument: H::INSTRUMENT,
        client_id,
    };
    let answered = exec_events(&harness.answer(cancel, NOW));
    assert!(
        answered
            .iter()
            .any(|event| event.status.is_some_and(VenueOrderStatus::is_terminal)),
        "a cancel answer is final: {answered:?}"
    );
    for event in &answered {
        session.fold(event, 0, &mut Vec::new());
    }
    assert!(!session.core.is_mirrored(client_id));
}

/// An open order of this run that the codec cannot decode is never skipped: the mirror would be
/// incomplete, and quoting against it can strand an order.
pub(crate) fn an_undecodable_open_order_is_fatal<H: EdgeHarness>() {
    let (_, is_decoded) = H::new().undecodable_open_order(own::<H>(1));
    assert!(!is_decoded, "the codec reads no price it cannot decode");
}

/// A venue code that the codec does not know halts quoting rather than retrying: unknown means
/// unknown.
pub(crate) fn an_unmapped_venue_code_halts<H: EdgeHarness>() {
    let events = H::new().unmapped_refusal(place_request::<H>(own::<H>(1)), NOW);
    let [refused] = events.as_slice() else {
        panic!("one answer for one request: {events:?}");
    };
    assert_eq!(refused.kind, ExecKind::AckFailed);
    assert_eq!(refused.reject, Some(RejectClass::Fatal));
    assert_eq!(refused.client_id, own::<H>(1));

    let mut quoting = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings: exec_settings(),
        tables: RecordedTables::new(&[]),
        run_nonce: H::RUN_NONCE,
    });
    let engine = &mut quoting.engine;
    make_ready(engine, 0);
    for message in reseat_book(BID, ASK, 0) {
        engine.dispatch(&message);
    }
    spin_at(engine, 1, 1_000_000);
    let mut harness = H::new();
    let mut session = quoting_session::<H>();
    for command in drain_commands(&mut quoting.commands) {
        for request in send(&mut session, command, ts(1_000_000)) {
            for event in harness.unmapped_refusal(request, ts(1_000_000)) {
                engine.dispatch(&InboundMessage::new(
                    ts(1_000_000),
                    MessageBody::Exec(event),
                ));
            }
        }
    }
    spin_at(engine, 2, 2_000_000);
    let after = drain_commands(&mut quoting.commands);
    assert!(
        after.iter().any(|command| matches!(
            command,
            ExecCommand::CancelOurs {
                reason: CancelReason::Halt,
                ..
            }
        )),
        "the fold halts quoting: {after:?}"
    );
    assert!(
        !after
            .iter()
            .any(|command| matches!(command, ExecCommand::Place { .. })),
        "no placement after the halt"
    );
}

/// Readiness arms only after the stream is up and the resync pass has read everything, and then
/// once, with one open-orders snapshot end for each instrument.
pub(crate) fn readiness_arms_in_fixed_order<H: EdgeHarness>() {
    let mut session = ExecSession::new(SessionSetup {
        venue: H::VENUE,
        core: ExecCore::with_limits(8, 32),
        instruments: vec![H::INSTRUMENT],
        events: SentEvents::default(),
        fatal: FatalSignal::new(),
        inflight_timeout: INFLIGHT_TIMEOUT,
    });
    let subscribe = session.connect();
    record(&mut session, subscribe, NOW);
    let resync_seq = session.begin_pass(2);
    assert!(
        !session.resync.on_read(resync_seq),
        "one read of two is half a pass"
    );
    assert!(!session.arm_readiness(NOW), "half a pass arms nothing");
    assert!(session.resync.on_read(resync_seq));
    session.finish_pass();
    assert!(session.arm_readiness(NOW));
    assert_eq!(kinds(&session.events), [ExecKind::SnapshotEnd]);
    assert_eq!(session.events[0].instrument, Some(H::INSTRUMENT));
    assert!(!session.arm_readiness(NOW), "readiness arms once");
}

/// One test for each written obligation, for the harness of one venue.
macro_rules! venue_edge_keeps_each_written_obligation {
    ($harness:ty) => {
        #[test]
        fn every_command_is_eventually_answered() {
            $crate::venues::exec::obligations::every_command_is_eventually_answered::<$harness>();
        }

        #[test]
        fn one_input_queue_with_monotone_queued_stamps() {
            $crate::venues::exec::obligations::one_input_queue_with_monotone_queued_stamps::<
                $harness,
            >();
        }

        #[test]
        fn fill_quantities_are_cumulative_venue_totals() {
            $crate::venues::exec::obligations::fill_quantities_are_cumulative_venue_totals::<
                $harness,
            >();
        }

        #[test]
        fn balance_chunks_are_absolute_with_a_monotone_venue_stamp() {
            $crate::venues::exec::obligations::balance_chunks_are_absolute_with_a_monotone_venue_stamp::<$harness>();
        }

        #[test]
        fn a_timeout_or_ambiguity_reconciles_and_never_resends() {
            $crate::venues::exec::obligations::a_timeout_or_ambiguity_reconciles_and_never_resends::<
                $harness,
            >();
        }

        #[test]
        fn a_cancel_answer_is_a_finality_promise() {
            $crate::venues::exec::obligations::a_cancel_answer_is_a_finality_promise::<$harness>();
        }

        #[test]
        fn an_undecodable_open_order_is_fatal() {
            $crate::venues::exec::obligations::an_undecodable_open_order_is_fatal::<$harness>();
        }

        #[test]
        fn an_unmapped_venue_code_halts() {
            $crate::venues::exec::obligations::an_unmapped_venue_code_halts::<$harness>();
        }

        #[test]
        fn readiness_arms_in_fixed_order() {
            $crate::venues::exec::obligations::readiness_arms_in_fixed_order::<$harness>();
        }
    };
}

pub(crate) use venue_edge_keeps_each_written_obligation;
