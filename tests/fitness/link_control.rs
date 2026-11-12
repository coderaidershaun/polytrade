//! The two-way control path, and the two bugs it is shaped around.
//!
//! Hot state is a pure function of the ordered input sequence, so the run state a strategy
//! is gated on must come from a RECORDED marker and never from the atomic the link actor latches —
//! an atomic set off-thread makes suppression depend on ITS timing, and replay diverges.
//!
//! The marker's input queue drops and counts rather than failing, because its producer is a remote
//! peer and a peer that can flood the port must not be able to kill the engine. So control
//! is LEVEL-triggered: the actor asks what marker is still outstanding rather than reacting to a
//! transition, and a dropped slot costs one heartbeat instead of leaving the edge parked and the hot
//! thread trading forever.

use polysim::config::{RecordedTables, TableKind};
use polysim::hot::strategy::{Registration, Strategy, StrategyCtx};
use polysim::ids::{InstrumentId, Side};
use polysim::link::RunState;
use polysim::msg::inbound::{InboundMessage, SpinTick, TradeEvent};
use polysim::msg::persist::{FeatureId, LinkRowKind, PersistRecord};
use polysim::shutdown::RunAssertion;
use polysim::time::DurationUs;

use crate::engine_support::{
    LinkedEngine, LinkedSetup, ONE, call_codes, delta_chunk, drain_persist, engine_with_link,
    idle_at, instrument_row, pop, run_control, running_at, snapshot_pair, spin, tracker_spec_all,
    trade,
};

/// One `call` feature per callback, so the drained stream encodes exactly which callbacks fired —
/// which is how a suppression bug shows up as a value rather than as silence.
struct CallProbe {
    call: Option<FeatureId>,
    vol: Option<FeatureId>,
}

impl Strategy for CallProbe {
    fn features(&self) -> &'static [&'static str] {
        &["call", "vol"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
        self.vol = registration.features.get(1).copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        ctx.emit(self.call.expect("registered"), event.instrument, 1.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.emit(self.call.expect("registered"), InstrumentId(0), 6.0);
        if let Some(vol) = ctx.ewma_vol(InstrumentId(0)) {
            ctx.emit(self.vol.expect("registered"), InstrumentId(0), vol);
        }
    }
}

fn probe() -> Box<dyn Strategy> {
    Box::new(CallProbe {
        call: None,
        vol: None,
    })
}

fn linked() -> LinkedEngine {
    linked_with_warmup(DurationUs::ZERO)
}

fn linked_with_warmup(warmup: DurationUs) -> LinkedEngine {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    engine_with_link(LinkedSetup {
        instruments: &instruments,
        strategy: probe(),
        tables: RecordedTables::new(&[TableKind::Features, TableKind::LinkFrames]),
        warmup,
    })
}

/// RUNNING → IDLE → RUNNING, driven by nothing but recorded markers on the ordered sequence.
fn control_sequence() -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 1);
    vec![
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
        InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 2)),
        InboundMessage::SpinTick(spin(1, 3)),
        run_control(idle_at(1), 4),
        // Parked: neither of these may reach the strategy.
        InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 5)),
        InboundMessage::SpinTick(spin(2, 6)),
        run_control(running_at(2), 7),
        InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 8)),
        InboundMessage::SpinTick(spin(3, 9)),
    ]
}

fn run(sequence: &[InboundMessage]) -> Vec<PersistRecord> {
    let mut linked = linked();
    assert!(
        linked.control.desired().accept_if_newer(idle_at(1)),
        "a fresh epoch wins the highest-wins race"
    );
    assert_eq!(linked.control.desired().state(), RunState::Idle);
    for message in sequence {
        linked.engine.dispatch(pop(0, 0), message);
    }
    drain_persist(&mut linked)
}

/// FITNESS: the recorded control markers alone decide what the strategy sees, so the same tape
/// replays to the same hot state.
///
/// This is the test that catches the replay-divergence latch bug. The desired latch is held at IDLE
/// for the whole run and must gate nothing, because the latch is edge-side OUTPUT: if dispatch
/// consulted it instead of the marker, every callback below would be suppressed, and if it consulted
/// both, the RUNNING stretches would be. If dispatch ignored the marker, the parked trade and spin
/// would call back.
#[test]
fn recorded_control_markers_replay_to_identical_hot_state() {
    let first = run(&control_sequence());
    let second = run(&control_sequence());
    assert_eq!(first, second, "identical control replay diverged");

    assert_eq!(
        call_codes(&first),
        vec![1.0, 6.0, 1.0, 6.0],
        "the trade and spin between the IDLE and RUNNING markers reach nothing, and hot state \
         follows the recorded sequence rather than the latch set beside it"
    );

    let kinds: Vec<LinkRowKind> = first
        .iter()
        .filter_map(|record| match record {
            PersistRecord::LinkFrame(row) => Some(row.kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        vec![LinkRowKind::RunIdle, LinkRowKind::RunRunning],
        "both applied markers are in the tape, in order"
    );
}

/// FITNESS: control converges from the LEVEL, so a dropped marker costs one heartbeat rather than
/// leaving the edge parked and the hot thread trading forever, and a repeated marker is deduped on
/// its epoch so a transition's side effects cannot re-run — a second RESUME would otherwise wipe
/// derived state the engine had just rebuilt. The tape still holds every marker that arrived, not
/// every one that changed something.
///
/// This is the test that catches the edge-trigger bug. The marker rides a drop+count queue, so
/// the first push here is simply never dispatched.
#[test]
fn a_dropped_control_marker_self_corrects_on_the_next_heartbeat() {
    let mut linked = linked();
    let park = idle_at(1);
    assert!(
        linked.control.desired().accept_if_newer(park),
        "a fresh epoch wins the highest-wins race"
    );

    let pending = linked.control.pending().expect("a marker is outstanding");
    assert_eq!(pending, park);
    // The marker is never dispatched here, exactly as if its push had been dropped.

    linked.engine.dispatch(pop(0, 0), &run_control(park, 10));
    assert_eq!(
        linked.control.pending(),
        None,
        "the repushed marker converged the pair"
    );
    assert_eq!(
        linked.control.acknowledged().load(),
        park,
        "the hot thread reports what it applied, not what was asked"
    );

    // The heartbeat that raced the acknowledgement pushes the same marker again. Idempotent.
    linked.engine.dispatch(pop(0, 0), &run_control(park, 11));
    assert_eq!(linked.control.pending(), None);
    assert_eq!(linked.control.acknowledged().load(), park);

    let records = drain_persist(&mut linked);
    assert!(
        records.iter().all(|record| !matches!(
            record,
            PersistRecord::Feature(row) if row.feature == FeatureId(0)
        )),
        "no callback fired while parked, duplicate marker included"
    );
    let markers: Vec<RunAssertion> = records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::LinkFrame(row) if row.kind == LinkRowKind::RunIdle => {
                Some(idle_at(row.seq))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        markers,
        vec![park; 2],
        "every marker is recorded — the tape is what arrived, not what changed"
    );
}

/// FITNESS: a stale controller cannot oscillate the toggle. The epoch is monotonic and highest
/// wins, so a second controller — or the same one after a restart at a lower epoch — loses.
#[test]
fn a_lower_epoch_assertion_loses_the_race() {
    let linked = linked();
    assert!(linked.control.desired().accept_if_newer(idle_at(5)));
    assert!(
        !linked.control.desired().accept_if_newer(running_at(3)),
        "a stale controller's assertion is discarded"
    );
    assert!(
        !linked.control.desired().accept_if_newer(running_at(5)),
        "re-asserting at the same epoch is idempotent, not a flip"
    );
    assert_eq!(linked.control.desired().load(), idle_at(5));
    assert!(linked.control.desired().accept_if_newer(running_at(6)));
    assert_eq!(linked.control.desired().load(), running_at(6));
}

/// FITNESS: resuming wipes per-instrument derived state and re-arms warmup.
///
/// An EwmaVol resident, a tracker series or a Hawkes fit carried across a multi-minute park is
/// poison: the first post-resume rows would be polluted research data that reads as ordinary. The
/// `vol` column is the observable — it must be gone after the resume and only return once fresh
/// samples have rebuilt it.
#[test]
fn resuming_resets_derived_state_and_rearms_warmup() {
    // A real span, because a zero-warmup engine has nothing to re-arm; the stamps below straddle it.
    const WARMUP_US: i64 = 10;
    let mut linked = linked_with_warmup(DurationUs::from_micros(WARMUP_US));
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 0);
    for message in [
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 5 * ONE)], 2)),
        InboundMessage::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, 9 * ONE)], 3)),
        InboundMessage::SpinTick(spin(1, 20)),
    ] {
        linked.engine.dispatch(pop(0, 0), &message);
    }
    assert!(
        has_vol(&drain_persist(&mut linked)),
        "the pre-park spin carries an EwmaVol the suppressed prefix warmed"
    );

    for message in [
        run_control(idle_at(1), 30),
        run_control(running_at(2), 40),
        InboundMessage::SpinTick(spin(2, 45)),
    ] {
        linked.engine.dispatch(pop(0, 0), &message);
    }
    assert!(
        call_codes(&drain_persist(&mut linked)).is_empty(),
        "warmup re-armed on the resume, so the spin 5us later reaches no callback"
    );

    linked
        .engine
        .dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(3, 60)));
    let past_warmup = drain_persist(&mut linked);
    assert_eq!(
        call_codes(&past_warmup),
        vec![6.0],
        "a spin past the re-armed span calls back again"
    );
    assert!(
        !has_vol(&past_warmup),
        "the resident EwmaVol was wiped, so no pre-park volatility survives the gap"
    );
}

fn has_vol(records: &[PersistRecord]) -> bool {
    records
        .iter()
        .any(|record| matches!(record, PersistRecord::Feature(row) if row.feature == FeatureId(1)))
}
