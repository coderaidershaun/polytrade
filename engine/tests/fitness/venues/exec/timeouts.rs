//! A request that the venue never answers is read back once its in-flight timeout passes, and never
//! sent again: the venue may have run it, and a second send could rest a second order.

use engine::hot::exec::{DesiredQuote, ExecSettings, QuoteLevel};
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{CancelReason, ExecCommand, ExecKind, RejectClass};
use engine::msg::ids::{ClientOrderId, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::{DurationUs, LocalTs};
use engine::runtime::config::RecordedTables;
use engine::venues::exec::{ExecRequest, TimeoutFallout};

use super::exec_session::{
    INFLIGHT_TIMEOUT, INSTRUMENT, NOW, Session, kinds, new_session, place, record, send, sent,
    status_request,
};
use crate::engine_support::ts;
use crate::exec_support::{
    ASK, BID, CEILING, QUOTE_QTY, QuotingSetup, built_quoting_engine, drain_commands,
    exec_settings, make_ready, reseat_book, row_with_ceiling, spin_at,
};

const SECOND: i64 = 1_000_000;

/// Connected, its pass finished and readiness armed at `at`: the only state that quotes.
fn armed_session(at: LocalTs) -> Session {
    let (mut session, _) = new_session();
    let subscribe = session.connect();
    record(&mut session, subscribe, at);
    let resync_seq = session.begin_pass(1);
    assert!(session.resync.on_read(resync_seq));
    session.finish_pass();
    assert!(session.arm_readiness(at));
    session.events.clear();
    session
}

/// Declares one bid at the touch each spin, so the engine has one placement to lose.
struct OneBid;

impl Strategy for OneBid {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let Some(bid) = ctx.book(INSTRUMENT).best_bid() else {
            return;
        };
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: bid.price,
                qty: QUOTE_QTY,
            }),
        );
    }
}

/// What the venue saw and what the engine asked for, over whole-second spins from 1 s to 12 s. The
/// venue answers nothing, so the edge expires the placement at 6 s and its events fold at once.
struct Silence {
    requests: Vec<(i64, ExecRequest)>,
    commands: Vec<(i64, ExecCommand)>,
    probe_at_expiry: Vec<(ExecRequest, u64)>,
    events_at_expiry: Vec<(ExecKind, Option<RejectClass>)>,
}

fn silent_venue(max_consecutive_rejects: u32) -> Silence {
    let mut quoting = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(OneBid),
        restored: &[],
        settings: ExecSettings {
            inflight_timeout: INFLIGHT_TIMEOUT,
            max_consecutive_rejects,
            ..exec_settings()
        },
        tables: RecordedTables::new(&[]),
        run_nonce: 7,
    });
    let engine = &mut quoting.engine;
    make_ready(engine, 0);
    for message in reseat_book(BID, ASK, 0) {
        engine.dispatch(&message);
    }
    let mut session = armed_session(ts(0));
    let mut silence = Silence {
        requests: Vec::new(),
        commands: Vec::new(),
        probe_at_expiry: Vec::new(),
        events_at_expiry: Vec::new(),
    };
    for second in 1..=12 {
        let when = second * SECOND;
        spin_at(engine, second as u64, when);
        for command in drain_commands(&mut quoting.commands) {
            silence.commands.push((when, command));
            for request in send(&mut session, command, ts(when)) {
                silence.requests.push((when, request));
            }
        }
        if when != 6 * SECOND {
            continue;
        }
        let expired = session.expire(ts(when));
        silence.probe_at_expiry = sent(&expired.outgoing);
        for request in record(&mut session, expired.outgoing, ts(when)) {
            silence.requests.push((when, request));
        }
        for event in session.events.drain(..) {
            silence.events_at_expiry.push((event.kind, event.reject));
            engine.dispatch(&InboundMessage::new(ts(when), MessageBody::Exec(event)));
        }
    }
    silence
}

fn is_reconcile_of(command: &ExecCommand, wanted: ClientOrderId) -> bool {
    matches!(command, ExecCommand::ReconcileOrder { client_id, .. } if *client_id == wanted)
}

fn is_halt(command: &ExecCommand) -> bool {
    matches!(
        command,
        ExecCommand::CancelOurs {
            reason: CancelReason::Halt,
            ..
        }
    )
}

/// CONTRACT: the in-flight timeout of `execution.inflight_timeout_ms` is the one deadline of an
/// unanswered request on both sides of the command lane. The edge expires the request at that
/// deadline, reads it back with one status probe under the hot sequence it answers, and tells the
/// fold that its outcome is unknown; the fold blocks the side and asks again one timeout later.
/// Neither side ever sends the request again.
#[test]
fn unanswered_request_gets_one_probe_and_never_a_resend() {
    let mut session = armed_session(NOW);
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

    let early = session.expire(NOW + INFLIGHT_TIMEOUT - DurationUs::from_micros(1));
    assert!(early.outgoing.is_empty() && early.unanswered_reads.is_empty());
    assert!(session.events.is_empty());

    let expired = session.expire(NOW + INFLIGHT_TIMEOUT);
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

    let mut placing = armed_session(NOW);
    send(&mut placing, place(2, Side::Buy), NOW);
    let lost = placing.expire(NOW + INFLIGHT_TIMEOUT);
    assert!(lost.has_lost_placement);
    assert_eq!(sent(&lost.outgoing), [(status_request(2), 0)]);

    let silence = silent_venue(2);
    let Some(&(SECOND, ExecRequest::Place { client_id, .. })) = silence.requests.first() else {
        panic!(
            "the venue first sees one placement at 1 s: {:?}",
            silence.requests
        );
    };
    let status = ExecRequest::OrderStatus {
        instrument: INSTRUMENT,
        client_id,
    };
    assert_eq!(silence.probe_at_expiry, [(status, 0)]);
    assert_eq!(
        silence.events_at_expiry,
        [(ExecKind::AckFailed, Some(RejectClass::Ambiguous))]
    );
    let reconciled: Vec<i64> = silence
        .commands
        .iter()
        .filter(|(_, command)| is_reconcile_of(command, client_id))
        .map(|(when, _)| *when)
        .collect();
    assert_eq!(
        reconciled,
        [12 * SECOND],
        "the fold asks one timeout after the outcome became unknown, not before"
    );
    assert_eq!(
        silence.requests[1..],
        [(6 * SECOND, status), (12 * SECOND, status)],
        "after its one placement the venue sees only reads"
    );
    assert!(
        !silence.commands.iter().any(|(_, command)| is_halt(command)),
        "one unanswered request is one reject of a streak of two"
    );
    assert!(
        silent_venue(1)
            .commands
            .iter()
            .any(|(_, command)| is_halt(command)),
        "one unanswered request counts one reject toward the streak"
    );
}
