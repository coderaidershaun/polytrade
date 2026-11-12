//! Execution-core fitness: the phase gate, the cancel policy and the sweep. These are the rules
//! standing between a strategy bug and money, and every one of them fails SILENTLY if broken — an
//! order placed while resyncing rests against state the engine has not verified, a cancelled
//! `Foreign` order is a human's order destroyed, and a sweep that reports complete while an order
//! still rests leaves a position nobody is watching after the process exits.

use polysim::adapters::exec::{
    ExecCore, ExecEffect, ExecRequest, MirroredOrder, ObserveOrderError, Phase, PlaceNotSentReason,
    SkipReason,
};
use polysim::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{CancelReason, ExecCommand, OrderStyle, Provenance};

const INSTRUMENT: InstrumentId = InstrumentId(0);
const OTHER_INSTRUMENT: InstrumentId = InstrumentId(1);

fn effects_of(drive: impl FnOnce(&mut dyn FnMut(ExecEffect))) -> Vec<ExecEffect> {
    let mut effects = Vec::new();
    drive(&mut |effect| effects.push(effect));
    effects
}

/// A core that has connected and had its stream confirmed — the only state that quotes.
fn quoting_core() -> ExecCore {
    let mut core = ExecCore::with_limits(8, 32);
    effects_of(|emit| core.on_connected(emit));
    core.on_stream_ready();
    assert_eq!(core.phase(), Phase::Quoting);
    core
}

fn place(client_id: u64) -> ExecCommand {
    ExecCommand::Place {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        side: Side::Buy,
        price: Price(11_800_000_000_000),
        qty: Qty(8_000),
        style: OrderStyle::PostOnly,
    }
}

fn amend(client_id: u64) -> ExecCommand {
    ExecCommand::AmendQty {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        qty: Qty(4_000),
    }
}

fn mirrored(client_id: u64, provenance: Provenance) -> MirroredOrder {
    MirroredOrder {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client_id),
        side: Side::Buy,
        price: Price(11_800_000_000_000),
        qty: Qty(8_000),
        provenance,
        has_sent_cancel: false,
        is_ambiguous: false,
    }
}

fn sent_requests(effects: &[ExecEffect]) -> Vec<ExecRequest> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            ExecEffect::Send { request, .. } => Some(*request),
            _ => None,
        })
        .collect()
}

/// FITNESS: only `Quoting` admits a new order, and every route back to it — reconnect, or a
/// reconnect landing mid-sweep — passes back through the gate rather than around it.
///
/// The three go together deliberately: the same phase gate is exercised from every direction that
/// could bypass it — a direct command in each non-quoting phase, a reconnect that must resync before
/// it quotes again, and a reconnect that lands mid-sweep and must not reopen quoting `Cancelling`
/// already decided to stop. An order placed while resyncing rests against state the engine has not
/// verified; a decision to stop that a dropped socket silently reverses is the same bug from the
/// other side.
#[test]
fn only_the_quoting_phase_admits_a_new_order_from_every_direction() {
    let mut down = ExecCore::with_limits(1, 2);
    assert_eq!(down.phase(), Phase::Down);
    assert!(sent_requests(&effects_of(|emit| down.on_command(place(1), emit))).is_empty());

    let mut resyncing = ExecCore::with_limits(1, 2);
    effects_of(|emit| resyncing.on_connected(emit));
    assert_eq!(resyncing.phase(), Phase::Resyncing);
    assert!(sent_requests(&effects_of(|emit| resyncing.on_command(place(2), emit))).is_empty());

    let mut quoting = quoting_core();
    assert_eq!(
        sent_requests(&effects_of(|emit| quoting.on_command(place(3), emit))).len(),
        1
    );

    let mut settled = quoting_core();
    effects_of(|emit| settled.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(settled.phase(), Phase::Settled);
    assert!(sent_requests(&effects_of(|emit| settled.on_command(place(4), emit))).is_empty());

    let mut reconnecting = quoting_core();
    effects_of(|emit| reconnecting.on_command(place(1), emit));
    reconnecting.on_disconnected();
    assert_eq!(reconnecting.phase(), Phase::Down);
    effects_of(|emit| reconnecting.on_connected(emit));
    assert_eq!(reconnecting.phase(), Phase::Resyncing);
    assert!(sent_requests(&effects_of(|emit| reconnecting.on_command(place(2), emit))).is_empty());
    reconnecting.on_stream_ready();
    assert_eq!(reconnecting.phase(), Phase::Quoting);

    let mut mid_sweep = quoting_core();
    effects_of(|emit| mid_sweep.on_command(place(1), emit));
    effects_of(|emit| mid_sweep.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(mid_sweep.phase(), Phase::Cancelling);
    mid_sweep.on_disconnected();
    effects_of(|emit| mid_sweep.on_connected(emit));
    mid_sweep.on_stream_ready();
    assert_eq!(
        mid_sweep.phase(),
        Phase::Cancelling,
        "a reconnect re-opened a sweep"
    );
    assert!(sent_requests(&effects_of(|emit| mid_sweep.on_command(place(2), emit))).is_empty());
}

/// FITNESS: an amend the phase gate refuses is REPORTED back, never swallowed.
///
/// The hot thread moves the slot to `AmendInFlight` when it banks the command, and a side holding an
/// unanswered order decides nothing further. A refusal that emits nothing therefore has the strategy
/// quoting around a size change no venue was ever asked for, until the in-flight timeout fires
/// seconds later — the same failure `PlaceNotSent` exists to prevent on the placement path.
#[test]
fn a_refused_amend_is_reported_back_rather_than_swallowed() {
    let mut down = ExecCore::with_limits(1, 2);
    let mut cancelling = quoting_core();
    effects_of(|emit| cancelling.on_command(place(1), emit));
    effects_of(|emit| cancelling.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(cancelling.phase(), Phase::Cancelling);

    for core in [&mut down, &mut cancelling] {
        let phase = core.phase();
        let refused = effects_of(|emit| core.on_command(amend(1), emit));
        assert!(
            sent_requests(&refused).is_empty(),
            "{phase:?} sent an amend it does not admit"
        );
        assert_eq!(
            refused,
            [ExecEffect::AmendNotSent {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            }],
            "{phase:?} refused an amend and left the hot slot with nothing to release it"
        );
    }

    let mut quoting = quoting_core();
    effects_of(|emit| quoting.on_command(place(1), emit));
    let admitted = effects_of(|emit| quoting.on_command(amend(1), emit));
    assert_eq!(
        sent_requests(&admitted),
        [ExecRequest::AmendQty {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1),
            qty: Qty(4_000),
        }],
        "the one phase that admits an amend refused it instead"
    );
}

/// FITNESS: the mirror holds exactly what is really resting on the venue — our own orders survive a
/// disconnect because they are still out there, and a human's order never enters it at all.
///
/// The two go together deliberately: both are the same "what belongs in the mirror" boundary, tested
/// from its two edges. Clearing the mirror on disconnect would strand orders the reconnect sweep
/// exists to cancel; admitting a foreign order would collide every one of them onto
/// `ClientOrderId(0)`, since a foreign order carries no parseable client id — only the first kept,
/// all named "order 0", a single retirement dropping the lot.
#[test]
fn the_mirror_holds_only_what_is_really_resting_and_survives_a_disconnect() {
    let mut core = quoting_core();
    effects_of(|emit| core.on_command(place(1), emit));
    effects_of(|emit| {
        core.on_command(
            ExecCommand::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            },
            emit,
        );
    });
    assert!(core.mirror()[0].has_sent_cancel);

    core.on_disconnected();

    assert_eq!(core.mirror().len(), 1, "the order is still on the venue");
    assert!(
        !core.mirror()[0].has_sent_cancel,
        "a cancel sent over a socket that died may never have arrived, so it must be re-sendable"
    );
    assert!(core.mirror()[0].is_ambiguous);

    let mut foreign = quoting_core();
    foreign
        .observe_venue_order(mirrored(99, Provenance::Foreign))
        .unwrap();
    assert!(
        foreign.mirror().is_empty(),
        "a foreign order entered the mirror: {:?}",
        foreign.mirror()
    );
}

/// FITNESS: a sweep sends exactly the right cancels for what it scopes to, resends nothing already
/// in flight, and reports complete ONLY once every order it owns is confirmed gone.
///
/// The four go together deliberately: they are the same completion-gating rule read across the
/// shapes a sweep actually meets in production — nothing to cancel, one leftover from a prior run,
/// several of this run's own orders, and an account holding another instrument's orders the sweep
/// must leave alone. Reporting complete early, or scoping too widely, or resending a cancel already
/// in flight are each a different way the same exit path can leave money resting unattended.
#[test]
fn a_sweep_completes_only_once_its_scoped_orders_are_confirmed_gone() {
    let mut empty = quoting_core();
    let effects = effects_of(|emit| empty.begin_sweep(CancelReason::Fatal, None, emit));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ExecEffect::SweepComplete {
            reason: CancelReason::Fatal
        }
    )));
    assert_eq!(empty.phase(), Phase::Settled);

    let mut prior_run = quoting_core();
    prior_run
        .observe_venue_order(mirrored(7, Provenance::PriorRun))
        .unwrap();
    let first = effects_of(|emit| prior_run.begin_sweep(CancelReason::Startup, None, emit));
    assert_eq!(sent_requests(&first).len(), 1);
    let second = effects_of(|emit| prior_run.begin_sweep(CancelReason::Startup, None, emit));
    assert!(
        sent_requests(&second).is_empty(),
        "a second sweep re-sent a cancel that is already in flight"
    );

    let mut own_orders = quoting_core();
    effects_of(|emit| own_orders.on_command(place(1), emit));
    effects_of(|emit| own_orders.on_command(place(2), emit));
    let opened = effects_of(|emit| own_orders.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(
        sent_requests(&opened).len(),
        2,
        "both orders must be cancelled"
    );
    assert!(
        !opened
            .iter()
            .any(|effect| matches!(effect, ExecEffect::SweepComplete { .. })),
        "the sweep reported complete before either cancel was confirmed"
    );
    let first_gone = effects_of(|emit| own_orders.on_order_gone(ClientOrderId(1), Qty(0), emit));
    assert!(
        !first_gone
            .iter()
            .any(|effect| matches!(effect, ExecEffect::SweepComplete { .. })),
        "the sweep reported complete with one order still resting"
    );
    let second_gone = effects_of(|emit| own_orders.on_order_gone(ClientOrderId(2), Qty(0), emit));
    assert!(second_gone.iter().any(|effect| matches!(
        effect,
        ExecEffect::SweepComplete {
            reason: CancelReason::Shutdown
        }
    )));
    assert_eq!(own_orders.phase(), Phase::Settled);

    let mut scoped = quoting_core();
    effects_of(|emit| scoped.on_command(place(1), emit));
    scoped
        .observe_venue_order(MirroredOrder {
            instrument: OTHER_INSTRUMENT,
            ..mirrored(2, Provenance::Mine)
        })
        .unwrap();
    let scoped_effects = effects_of(|emit| {
        scoped.begin_sweep(CancelReason::Park, Some(INSTRUMENT), emit);
    });
    assert_eq!(
        sent_requests(&scoped_effects),
        [ExecRequest::Cancel {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }]
    );
}

/// FITNESS: an uncertain answer — ambiguous, or simply timed out — resolves by ASKING the venue,
/// never by assuming or by retrying.
///
/// The two go together deliberately: they are the same reconciliation rule triggered by the two ways
/// an answer can go missing. Binance's -2011 reads the same for an order that never existed and one
/// that just filled, so assuming `Gone` on an ambiguous answer would discard a real fill; and
/// re-sending a place whose fate a timeout left unknown is how one intent becomes two live orders.
#[test]
fn an_ambiguous_or_timed_out_answer_reconciles_rather_than_assumes_or_retries() {
    let mut ambiguous = quoting_core();
    effects_of(|emit| ambiguous.on_command(place(1), emit));
    let probe = effects_of(|emit| ambiguous.on_ambiguous(ClientOrderId(1), emit));
    assert_eq!(
        sent_requests(&probe),
        [ExecRequest::OrderStatus {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }]
    );
    assert!(ambiguous.mirror()[0].is_ambiguous);

    let mut timed_out = quoting_core();
    effects_of(|emit| timed_out.on_command(place(1), emit));
    let timeout_effects =
        effects_of(|emit| timed_out.on_request_timeout(INSTRUMENT, ClientOrderId(1), emit));
    assert_eq!(
        sent_requests(&timeout_effects),
        [ExecRequest::OrderStatus {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }],
        "a timed-out request must reconcile, never re-place"
    );
}

/// FITNESS: `on_command`'s admission decisions are asymmetric by design, and fail closed on the
/// expensive side of that asymmetry.
///
/// The four go together deliberately: a cancel for an id the mirror never saw is still forwarded —
/// cancelling something that does not exist costs one routine -2011, which is free, while failing to
/// cancel one that does exist costs an unattended position — and every PLACE-side admission decision
/// fails the other way, refusing rather than guessing: mirror storage exhaustion refuses the NEW
/// order without touching an existing one, a side awaiting an answer keeps its capacity reserved
/// until a definitive terminal fact releases it, and a duplicate client id is refused before wire
/// with the original reservation left intact. Each is "wrong in the recoverable direction" applied to
/// a different admission path.
#[test]
fn on_command_admission_is_permissive_for_cancels_and_fail_closed_for_places() {
    let mut unknown = quoting_core();
    let cancel_effects = effects_of(|emit| {
        unknown.on_command(
            ExecCommand::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1234),
            },
            emit,
        );
    });
    assert_eq!(
        sent_requests(&cancel_effects),
        [ExecRequest::Cancel {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1234)
        }]
    );

    let mut storage = ExecCore::with_limits(2, 1);
    effects_of(|emit| storage.on_connected(emit));
    storage.on_stream_ready();
    effects_of(|emit| storage.on_command(place(1), emit));
    let refused = effects_of(|emit| storage.on_command(place(2), emit));
    assert_eq!(storage.mirror().len(), 1);
    assert!(
        storage
            .mirror()
            .iter()
            .all(|order| order.client_id != ClientOrderId(2))
    );
    assert!(
        storage
            .mirror()
            .iter()
            .any(|order| order.client_id == ClientOrderId(1)),
        "an order already on the venue was evicted"
    );
    assert!(
        refused.iter().any(|effect| matches!(
            effect,
            ExecEffect::PlaceNotSent {
                client_id: ClientOrderId(2),
                reason: PlaceNotSentReason::MirrorStorage,
                ..
            }
        )),
        "storage refusal was not reported as provably unsent"
    );
    assert_eq!(storage.phase(), Phase::Cancelling);

    let mut side_capacity = ExecCore::with_limits(1, 4);
    effects_of(|emit| side_capacity.on_connected(emit));
    side_capacity.on_stream_ready();
    effects_of(|emit| side_capacity.on_command(place(1), emit));
    effects_of(|emit| {
        side_capacity.on_command(
            ExecCommand::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            },
            emit,
        );
    });
    side_capacity.mark_ambiguous(ClientOrderId(1));
    let capacity_refused = effects_of(|emit| side_capacity.on_command(place(2), emit));
    assert!(sent_requests(&capacity_refused).is_empty());
    assert!(capacity_refused.iter().any(|effect| matches!(
        effect,
        ExecEffect::PlaceNotSent {
            reason: PlaceNotSentReason::SideCapacity,
            ..
        }
    )));
    assert_eq!(side_capacity.possibly_live_count(INSTRUMENT, Side::Buy), 1);
    effects_of(|emit| side_capacity.on_order_gone(ClientOrderId(1), Qty(0), emit));
    assert_eq!(
        sent_requests(&effects_of(|emit| side_capacity.on_command(place(2), emit))),
        [ExecRequest::Place {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(2),
            side: Side::Buy,
            price: Price(11_800_000_000_000),
            qty: Qty(8_000),
            style: OrderStyle::PostOnly,
        }]
    );

    let mut duplicate = ExecCore::with_limits(2, 4);
    effects_of(|emit| duplicate.on_connected(emit));
    duplicate.on_stream_ready();
    effects_of(|emit| duplicate.on_command(place(1), emit));
    let duplicate_refused = effects_of(|emit| duplicate.on_command(place(1), emit));
    assert!(sent_requests(&duplicate_refused).is_empty());
    assert_eq!(duplicate.mirror().len(), 1);
    assert!(duplicate_refused.iter().any(|effect| matches!(
        effect,
        ExecEffect::PlaceNotSent {
            reason: PlaceNotSentReason::DuplicateClientId,
            ..
        }
    )));
    assert_eq!(duplicate.phase(), Phase::Cancelling);
}

/// FITNESS: what the venue tells the engine about pre-existing orders is authoritative and fail
/// closed, even when it reveals a limit the engine did not choose to break.
///
/// The three go together deliberately: an order discovered at startup blocks readiness until the
/// venue definitively retires it; discovering TWO possibly-live orders on a side capped at one is
/// retained in full rather than trimmed, because the evidence must not be discarded; and a refresh
/// that would move an already-mirrored identity onto another side is revalidated against the same cap
/// rather than allowed to slip through on the assumption that only insertion needs checking.
#[test]
fn venue_discovered_orders_are_retained_in_full_and_fail_closed_on_the_side_cap() {
    let mut inherited = ExecCore::with_limits(1, 4);
    effects_of(|emit| inherited.on_connected(emit));
    inherited
        .observe_venue_order(mirrored(7, Provenance::PriorRun))
        .unwrap();
    assert!(!inherited.on_stream_ready());
    assert_eq!(inherited.phase(), Phase::Resyncing);
    assert_eq!(inherited.possibly_live_count(INSTRUMENT, Side::Buy), 1);
    effects_of(|emit| inherited.on_order_gone(ClientOrderId(7), Qty(0), emit));
    assert!(inherited.on_stream_ready());
    assert_eq!(inherited.phase(), Phase::Quoting);

    let mut oversubscribed = ExecCore::with_limits(1, 4);
    effects_of(|emit| oversubscribed.on_connected(emit));
    oversubscribed
        .observe_venue_order(mirrored(1, Provenance::PriorRun))
        .unwrap();
    let error = oversubscribed
        .observe_venue_order(mirrored(2, Provenance::PriorRun))
        .expect_err("two possibly-live bids exceed a cap of one");
    assert!(
        matches!(
            error,
            ObserveOrderError::OwnedSideOverLimit {
                count: 2,
                limit: 1,
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(oversubscribed.possibly_live_count(INSTRUMENT, Side::Buy), 2);
    assert_eq!(
        oversubscribed.mirror().len(),
        2,
        "the evidence must not be discarded"
    );
    assert_eq!(oversubscribed.phase(), Phase::Cancelling);

    let mut refreshed = ExecCore::with_limits(1, 4);
    refreshed
        .observe_venue_order(mirrored(1, Provenance::Mine))
        .unwrap();
    refreshed
        .observe_venue_order(MirroredOrder {
            side: Side::Sell,
            ..mirrored(2, Provenance::Mine)
        })
        .unwrap();
    let error = refreshed
        .observe_venue_order(mirrored(2, Provenance::Mine))
        .expect_err("refresh collapsed both identities onto Buy");
    assert!(
        matches!(
            error,
            ObserveOrderError::OwnedSideOverLimit {
                count: 2,
                limit: 1,
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(refreshed.possibly_live_count(INSTRUMENT, Side::Buy), 2);
    assert_eq!(refreshed.phase(), Phase::Cancelling);
}

/// FITNESS: `Settled` is reached by exactly the reasons that end a run, never by an ordinary startup
/// sweep, and once reached nothing — not a reconnect, not a late command — walks it back.
///
/// The three go together deliberately: they are the same terminality rule read from its three edges
/// — what makes `Settled` permanent, what must NOT settle even though it completes over an empty
/// mirror (a startup sweep, which runs before the first quote), and which reasons are recoverable
/// versus which end the run. A trading engine settled by its own startup sweep has never quoted and
/// never will.
#[test]
fn settled_is_reached_only_by_the_reasons_that_end_a_run_and_never_walked_back() {
    let mut settled = quoting_core();
    effects_of(|emit| settled.begin_sweep(CancelReason::Fatal, None, emit));
    assert_eq!(settled.phase(), Phase::Settled);
    effects_of(|emit| settled.on_connected(emit));
    settled.on_stream_ready();
    assert_eq!(settled.phase(), Phase::Settled);
    settled.on_disconnected();
    assert_eq!(settled.phase(), Phase::Settled);
    assert!(sent_requests(&effects_of(|emit| settled.on_command(place(1), emit))).is_empty());

    let mut startup = quoting_core();
    assert!(startup.mirror().is_empty());
    let effects = effects_of(|emit| startup.begin_sweep(CancelReason::Startup, None, emit));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ExecEffect::SweepComplete {
            reason: CancelReason::Startup
        }
    )));
    assert_ne!(
        startup.phase(),
        Phase::Settled,
        "a startup sweep retired the actor"
    );
    assert_eq!(startup.phase(), Phase::Down);
    effects_of(|emit| startup.on_connected(emit));
    startup.on_stream_ready();
    assert_eq!(startup.phase(), Phase::Quoting);
    assert_eq!(
        sent_requests(&effects_of(|emit| startup.on_command(place(1), emit))).len(),
        1
    );

    for reason in [
        CancelReason::Park,
        CancelReason::Disconnect,
        CancelReason::Halt,
    ] {
        let mut core = quoting_core();
        effects_of(|emit| core.begin_sweep(reason, None, emit));
        assert_eq!(core.phase(), Phase::Down, "{reason:?} must be recoverable");
    }
    for reason in [CancelReason::Shutdown, CancelReason::Fatal] {
        let mut core = quoting_core();
        effects_of(|emit| core.begin_sweep(reason, None, emit));
        assert_eq!(core.phase(), Phase::Settled, "{reason:?} ends the run");
    }
}

/// FITNESS: a cancel the venue REFUSED is sent again, whether the retry comes from the hot thread's
/// next command or from a sweep at exit.
///
/// The two go together deliberately: the latch means "a cancel is unanswered", so a refusal that
/// leaves the order resting must clear it — from BOTH callers that can re-arm it. Latched forever
/// instead, the order is excluded from every cancel path including the shutdown sweep, and the
/// process exits leaving it resting on the venue with nothing left that knows it exists.
#[test]
fn a_refused_cancel_is_re_sent_by_either_the_next_command_or_the_next_sweep() {
    let mut commanded = quoting_core();
    effects_of(|emit| commanded.on_command(place(1), emit));
    let cancel = ExecCommand::Cancel {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(1),
    };
    assert_eq!(
        sent_requests(&effects_of(|emit| commanded.on_command(cancel, emit))).len(),
        1
    );
    let while_in_flight = effects_of(|emit| commanded.on_command(cancel, emit));
    assert!(
        sent_requests(&while_in_flight).is_empty(),
        "a second cancel went out while the first was unanswered"
    );
    assert!(
        while_in_flight.iter().any(|effect| matches!(
            effect,
            ExecEffect::Skipped {
                reason: SkipReason::AlreadyCancelling,
                ..
            }
        )),
        "the skip must be reported — silence here is what hid the latch"
    );
    commanded.re_arm_cancel(ClientOrderId(1));
    assert_eq!(
        sent_requests(&effects_of(|emit| commanded.on_command(cancel, emit))),
        [ExecRequest::Cancel {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }],
        "a refused cancel was never retried"
    );

    let mut swept = quoting_core();
    effects_of(|emit| swept.on_command(place(1), emit));
    assert_eq!(
        sent_requests(&effects_of(|emit| {
            swept.begin_sweep(CancelReason::Shutdown, None, emit)
        }))
        .len(),
        1
    );
    swept.re_arm_cancel(ClientOrderId(1));
    let retried = effects_of(|emit| swept.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(
        sent_requests(&retried),
        [ExecRequest::Cancel {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }],
        "the sweep abandoned an order whose cancel the venue refused"
    );
    assert!(
        !retried
            .iter()
            .any(|effect| matches!(effect, ExecEffect::SweepComplete { .. })),
        "a sweep reported complete with an order still resting"
    );
    let settled = effects_of(|emit| swept.on_order_gone(ClientOrderId(1), Qty(0), emit));
    assert!(
        settled
            .iter()
            .any(|effect| matches!(effect, ExecEffect::SweepComplete { .. }))
    );
    assert_eq!(swept.phase(), Phase::Settled);
}

/// FITNESS: an in-flight cancel's latch is touched only by the answer that actually resolves it.
///
/// The four go together deliberately: all guard the SAME latch from being disturbed by an event that
/// looks like it could plausibly answer it but does not. An AMBIGUOUS answer is not a refusal — the
/// cancel may have landed, so re-arming here would put a second cancel on the wire for an order whose
/// state nobody knows yet — and the same open probe must not be asked again while it is still
/// outstanding. A cancel merely awaiting its ordinary answer is not probed at all — the request
/// timeout exists for that, and asking during the ordinary wait would put a second request behind
/// every cancel a sweep sends. A generic open-orders row can predate a cancel sent on another venue
/// pipeline, and refreshing its price must not pretend that cancel was answered. And a probe itself
/// can be lost — refused by the REST queue, or dead with the socket — so at exit, with no hot thread
/// left to re-derive the question, the sweep must ask again itself; nothing else ever would.
#[test]
fn an_outstanding_cancels_latch_is_touched_only_by_its_own_resolving_answer() {
    let mut ambiguous = quoting_core();
    effects_of(|emit| ambiguous.on_command(place(1), emit));
    let cancel = ExecCommand::Cancel {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(1),
    };
    effects_of(|emit| ambiguous.on_command(cancel, emit));
    let probe = effects_of(|emit| ambiguous.on_ambiguous(ClientOrderId(1), emit));
    assert_eq!(
        sent_requests(&probe),
        [ExecRequest::OrderStatus {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }]
    );
    assert!(
        sent_requests(&effects_of(|emit| ambiguous.on_command(cancel, emit))).is_empty(),
        "a cancel went out while the probe that would resolve the order was still open"
    );

    let mut awaiting = quoting_core();
    effects_of(|emit| awaiting.on_command(place(1), emit));
    effects_of(|emit| {
        awaiting.on_command(
            ExecCommand::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            },
            emit,
        );
    });
    let swept = effects_of(|emit| awaiting.begin_sweep(CancelReason::Shutdown, None, emit));
    assert!(
        sent_requests(&swept).is_empty(),
        "the sweep asked about an order whose cancel is still in flight: {:?}",
        sent_requests(&swept)
    );

    let mut snapshot = quoting_core();
    effects_of(|emit| snapshot.on_command(place(1), emit));
    effects_of(|emit| {
        snapshot.on_command(
            ExecCommand::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            },
            emit,
        );
    });
    assert!(snapshot.mirror()[0].has_sent_cancel);
    snapshot
        .observe_venue_order(MirroredOrder {
            price: Price(11_700_000_000_000),
            ..mirrored(1, Provenance::Mine)
        })
        .unwrap();
    assert!(
        snapshot.mirror()[0].has_sent_cancel,
        "a generic snapshot falsely answered a correlated cancel"
    );
    assert_eq!(snapshot.mirror()[0].price, Price(11_700_000_000_000));

    let mut lost_probe = quoting_core();
    effects_of(|emit| lost_probe.on_command(place(1), emit));
    effects_of(|emit| {
        lost_probe.on_command(
            ExecCommand::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            },
            emit,
        );
    });
    // The probe this emits is the one that never reaches the venue.
    effects_of(|emit| lost_probe.on_ambiguous(ClientOrderId(1), emit));
    let swept = effects_of(|emit| lost_probe.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(
        sent_requests(&swept),
        [ExecRequest::OrderStatus {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(1)
        }],
        "the sweep neither re-asked nor cancelled — the order is invisible to it"
    );
    let retried = effects_of(|emit| lost_probe.begin_sweep(CancelReason::Shutdown, None, emit));
    assert_eq!(
        sent_requests(&retried).len(),
        1,
        "the second answer can be lost too, so the second pass must ask again"
    );
    assert!(
        !retried
            .iter()
            .any(|effect| matches!(effect, ExecEffect::SweepComplete { .. })),
        "a sweep reported complete with an unresolved order still mirrored"
    );
    let settled = effects_of(|emit| lost_probe.on_order_gone(ClientOrderId(1), Qty(0), emit));
    assert!(
        settled
            .iter()
            .any(|effect| matches!(effect, ExecEffect::SweepComplete { .. }))
    );
}
