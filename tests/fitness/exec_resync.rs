//! Resync fitness: the post-subscribe pass must CONVERGE, and must TELL the hot side that it did.
//!
//! It is the gate between a fresh connection and quoting, and a pass that stalls is silent — the
//! actor keeps its socket, its stream and its heartbeat, and simply drops every order the strategy
//! asks for until the venue drops the connection 23 hours later. The first property forbids that: a
//! pass still waiting on a read always has something outstanding or something scheduled.
//!
//! The second half is the other way the same silence is reached. A pass can converge perfectly and
//! tell nobody: the hot side's readiness gate arms on the pass's end marker and on nothing else, so
//! without the marker a cold start never arms and an armed engine places nothing for the life of the
//! process, with no diagnostic at all. A test that synthesises the marker by hand cannot see that,
//! so these drive the real constructor, and the last of them pins the diagnostic that turns the
//! failure into a one-minute read.

use polysim::adapters::exec::{
    MAX_RESYNC_ATTEMPTS, ResyncPass, ResyncStep, open_orders_snapshot_end,
};
use polysim::hot::exec::{
    OrderClaim, OrderTable, QuoteLevel, ReadinessGap, ReconcilePass, RejectReason,
};
use polysim::ids::{Price, Qty, Side};
use polysim::msg::exec::{ExecKind, OrderStyle};
use polysim::msg::ui::UiEvent;
use polysim::time::TsUs;

use proptest::prelude::*;

use crate::engine_support::{local_refusals, pop, ts as at};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, drain_commands, is_placing, open_orders_snapshot,
    quoting_engine, quoting_engine_with_ui, reseat_book, spin_at, stream_and_balances,
};

const INSTRUMENTS: usize = 2;
const RETRY_DELAY: i64 = 250_000;

/// Later than any instant a test schedules, so `due` at this reads as "is anything scheduled at
/// all" rather than "is it due yet".
const FOREVER: TsUs = TsUs::from_micros(i64::MAX / 2);

/// FITNESS: the undecodable-open-order policy carried all the way to completion, and retries bounded
/// to a finite count.
///
/// The two go together deliberately: both are the same finite-retry machinery read from its two
/// edges. A pass that hit an undecodable order must not be able to REACH complete without re-reading
/// the row it could not decode — `on_failure` deliberately leaves `outstanding` alone, so the failed
/// read stays owed; the retry opens a new seq, and the answer that was in flight for the old one can
/// never retire it. This is the failure class that put a duplicate live order on the book: a pass
/// that believes it saw every resting order, arms readiness, and quotes against a mirror missing the
/// one order it could not parse — nothing errors, the engine simply does not know the order is there.
/// And the retries themselves are finite: a connection that cannot answer the one question quoting
/// depends on is given up on, because reconnecting is what re-subscribes and starts a clean pass —
/// sitting in `Resyncing` forever is the failure this bounds, an expected external failure handled by
/// policy, never by wedging.
#[test]
fn an_undecodable_read_survives_to_completion_and_retries_are_finite() {
    let mut pass = ResyncPass::default();
    let seq = pass.begin(INSTRUMENTS);

    // One instrument answers cleanly; the other carries a row that will not decode.
    assert!(!pass.on_read(seq));
    pass.on_failure(seq, at(RETRY_DELAY));

    assert!(
        pass.is_outstanding(),
        "the undecodable read was forgiven and the pass could complete without it"
    );
    assert_eq!(pass.due(at(RETRY_DELAY)), ResyncStep::Retry);

    // The retry re-asks everything. A late answer to the abandoned read cannot stand in for it.
    let retry = pass.begin_retry(INSTRUMENTS);
    assert!(
        !pass.on_read(seq),
        "an answer from the failed pass retired a read of the new one"
    );
    assert!(pass.is_outstanding());

    // Only re-reading every instrument completes it.
    assert!(!pass.on_read(retry));
    assert!(pass.on_read(retry));
    assert!(!pass.is_outstanding());

    let mut bounded = ResyncPass::default();
    let mut bounded_seq = bounded.begin(INSTRUMENTS);
    let mut now = 0;
    for attempt in 1..=MAX_RESYNC_ATTEMPTS {
        bounded.on_failure(bounded_seq, at(now + RETRY_DELAY));
        now += RETRY_DELAY;
        if attempt < MAX_RESYNC_ATTEMPTS {
            assert_eq!(bounded.due(at(now)), ResyncStep::Retry, "attempt {attempt}");
            bounded_seq = bounded.begin_retry(INSTRUMENTS);
        }
    }
    assert_eq!(bounded.due(at(now)), ResyncStep::GiveUp);
}

/// FITNESS: a cold start must reach the quoting state. The pass does not only settle the mirror —
/// it is the ONLY producer of the marker the hot table's readiness gate arms on, and the read the
/// hot table would ask for itself is issued only once a working order exists, which cannot happen
/// before a first placement readiness refuses. A readiness-dependent test that synthesises the marker
/// by hand hides its absence, so both halves below are driven by the constructor the actor actually
/// sends.
#[test]
fn a_cold_start_quotes_only_once_the_resync_marker_lands() {
    let (mut engine, mut commands) = quoting_engine(CEILING, &[]);
    stream_and_balances(&mut engine, 0);
    for message in reseat_book(BID, ASK, 10) {
        engine.dispatch(pop(0, 0), &message);
    }

    spin_at(&mut engine, 1, 20);
    let before = drain_commands(&mut commands);
    assert!(
        !is_placing(&before, Side::Buy) && !is_placing(&before, Side::Sell),
        "the engine quoted with two of the three readiness legs — the marker is then not load \
         bearing and its absence in production would be undetectable"
    );

    open_orders_snapshot(&mut engine, 30);
    spin_at(&mut engine, 2, 40);
    let after = drain_commands(&mut commands);
    assert!(
        is_placing(&after, Side::Buy) && is_placing(&after, Side::Sell),
        "the marker landed and the engine still placed nothing — an armed run that quotes for the \
         life of the process is exactly what this pins"
    );
}

/// FITNESS: the marker arms readiness and retires NOTHING. An end marker also sweeps the slots its
/// pass did not name, and this pass answers to the actor's mirror rather than to the hot table's own
/// reconciliation — so it carries sequence zero, which is below every slot's `seen_recon_seq` and
/// therefore cannot close a live order. Raise that number and a reconnect silently abandons the
/// orders it just read back.
#[test]
fn the_resync_marker_can_retire_no_slot() {
    let marker = open_orders_snapshot_end(INSTRUMENT, TsUs::from_micros(1_000));
    assert_eq!(marker.kind, ExecKind::SnapshotEnd);
    assert_eq!(marker.instrument, INSTRUMENT);

    let mut orders = OrderTable::new(0);
    let (index, _) = orders
        .claim(OrderClaim {
            instrument: INSTRUMENT,
            side: Side::Buy,
            level: QuoteLevel::ZERO,
            price: Price(100),
            qty: Qty(1),
            style: OrderStyle::PostOnly,
            claimed_ts_us: TsUs::from_micros(0),
            recon_seq: 0,
        })
        .expect("a fresh table has a free slot");

    let mut closed = 0;
    orders.sweep_unseen(
        ReconcilePass {
            instrument: marker.instrument,
            recon_seq: marker.recon_seq,
            recon_ts_us: marker.received_ts_us,
        },
        &mut |_, _| closed += 1,
    );

    assert_eq!(closed, 0, "the resync marker swept a working order");
    assert!(orders.slot(index).state.is_working());
}

/// FITNESS: balances are re-stated on every spin, not only when they change.
///
/// The venue is read for absolute balances at bootstrap and then only when the account MOVES, and
/// the UI feed is UDP with no reliability layer and no replay for a late subscriber. A tee driven by
/// the change therefore spoke once, seconds before a workstation typically attaches, and a run that
/// had not yet filled never spoke again — an account band empty for the life of the process while
/// the engine knew the numbers perfectly well. What makes it a fitness property rather than a fix is
/// the shape: a consumer that joins at spin K must still learn the state, whatever it missed.
#[test]
fn balances_are_restated_every_spin_so_a_late_consumer_learns_them() {
    let (mut engine, _commands, mut events) = quoting_engine_with_ui(CEILING, &[]);
    stream_and_balances(&mut engine, 0);

    // Everything the engine said before the consumer arrived, discarded exactly as the link drops
    // frames nobody is subscribed to yet.
    spin_at(&mut engine, 1, 10);
    while events.pop().is_ok() {}

    spin_at(&mut engine, 2, 20);
    let seen: Vec<_> = std::iter::from_fn(|| events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Balance { asset, free, .. } => Some((asset, free)),
            _ => None,
        })
        .collect();

    assert_eq!(
        seen.len(),
        2,
        "a consumer that attached after the account snapshot learned {} balances, not both",
        seen.len()
    );
    assert!(
        seen.iter().all(|(_, free)| *free > 0),
        "the re-stated balances carry no value: {seen:?}"
    );
}

/// FITNESS: a side that is not quoting says WHY, once per answer.
///
/// The silent-refusal failure is the expensive one — an engine reporting `ARMED` and placing nothing
/// looks identical to an engine with nothing to quote — so every gate names its reason. It has
/// to be edge-triggered to be readable: the spin is one second, so a level-triggered report is two
/// rows a second and the transition that matters drowns in repeats of the one before it.
#[test]
fn a_refusal_is_reported_when_the_reason_changes_and_not_again() {
    let (mut engine, _commands, mut events) = quoting_engine_with_ui(CEILING, &[]);

    // Nothing yet: no stream, no balances, no snapshot. The first gap is the stream.
    spin_at(&mut engine, 1, 10);
    spin_at(&mut engine, 2, 20);
    let first = local_refusals(&mut events);
    assert_eq!(
        first,
        vec![
            RejectReason::NotReady(ReadinessGap::Stream),
            RejectReason::NotReady(ReadinessGap::Stream),
        ],
        "one row per side for the first spin's reason, and nothing for the identical second spin"
    );

    // The stream and the balances land; the open-order read has not. A NEW reason, so a new row.
    stream_and_balances(&mut engine, 30);
    spin_at(&mut engine, 3, 40);
    spin_at(&mut engine, 4, 50);
    let second = local_refusals(&mut events);
    assert_eq!(
        second,
        vec![
            RejectReason::NotReady(ReadinessGap::OpenOrders),
            RejectReason::NotReady(ReadinessGap::OpenOrders),
        ],
        "the reason changed and was not reported, so the panel still shows the old one"
    );

    // Ready and quoting: the latch clears and reports nothing, because the order is the evidence.
    open_orders_snapshot(&mut engine, 60);
    for message in reseat_book(BID, ASK, 60) {
        engine.dispatch(pop(0, 0), &message);
    }
    spin_at(&mut engine, 5, 70);
    assert!(
        local_refusals(&mut events).is_empty(),
        "a side that started quoting reported a refusal"
    );
}

/// One instrument's read, as the driver's loop sees it: which pass asked for it, so an answer can
/// be recognised as stale.
#[derive(Debug, Clone, Copy)]
struct Job {
    seq: u64,
}

#[derive(Debug, Clone, Copy)]
enum Step {
    /// A read landed.
    Read,
    /// A read failed, or the REST queue refused it — the pass cannot tell the two apart, and must
    /// converge either way.
    Failure,
    /// The driver's tick, which is where a scheduled retry is actually issued.
    Tick,
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(
        prop_oneof![Just(Step::Read), Just(Step::Failure), Just(Step::Tick)],
        1..64,
    )
}

proptest! {
    /// FITNESS: the resync never stalls. Whatever order reads land, fail or arrive late in, a pass
    /// still waiting on an instrument always has a read outstanding or a retry scheduled — it is
    /// never simply idle, which is the state that leaves `Resyncing` permanent and every order the
    /// strategy asks for dropped for the life of the connection.
    #[test]
    fn a_resync_pass_is_never_idle_while_it_waits(steps in steps()) {
        let mut pass = ResyncPass::default();
        let mut now = 0;
        let seq = pass.begin(INSTRUMENTS);
        let mut in_flight: Vec<Job> = (0..INSTRUMENTS).map(|_| Job { seq }).collect();
        let mut is_given_up = false;

        for step in steps {
            match step {
                Step::Read => {
                    if let Some(job) = in_flight.pop() {
                        pass.on_read(job.seq);
                    }
                }
                Step::Failure => {
                    if let Some(job) = in_flight.pop() {
                        pass.on_failure(job.seq, at(now + RETRY_DELAY));
                    }
                }
                Step::Tick => {
                    now += RETRY_DELAY;
                    match pass.due(at(now)) {
                        ResyncStep::Wait => {}
                        ResyncStep::Retry => {
                            let seq = pass.begin_retry(INSTRUMENTS);
                            in_flight = (0..INSTRUMENTS).map(|_| Job { seq }).collect();
                        }
                        // The driver drops the connection here, so the pass stops being this
                        // connection's problem.
                        ResyncStep::GiveUp => is_given_up = true,
                    }
                }
            }
            prop_assert!(
                is_given_up
                    || !pass.is_outstanding()
                    || !in_flight.is_empty()
                    || pass.due(FOREVER) != ResyncStep::Wait,
                "the pass is waiting on a read with nothing outstanding and nothing scheduled"
            );
        }
    }
}
