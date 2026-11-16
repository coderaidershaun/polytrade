//! The control plane of the link actor, driven with the time passed in, against a real fold: the
//! hot state must follow the highest epoch even when markers are lost on the way.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use engine::hot::strategy::{Registration, Strategy as EngineStrategy, StrategyCtx};
use engine::link::{AssertionVerdict, ControlPlane};
use engine::msg::ids::{InstrumentId, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, RunControl, SpinTick, TradeEvent};
use engine::msg::persist::{FeatureId, LinkRowKind, PersistRecord};
use engine::msg::run_state::{RunAssertion, RunState};
use engine::msg::time::{DurationUs, LocalTs};
use engine::runtime::config::{ControllerLoss, RecordedTables, TableKind};

use crate::engine_support::{
    LinkedEngine, LinkedSetup, ONE, call_codes, drain_persist, engine_with_link, idle_at,
    instrument_row, run_control, running_at, snapshot_pair, spin, tracker_spec_all, trade, ts,
};

const CONTROLLER: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 47_001);

/// One `call` feature for each callback, so the drained rows say which callbacks fired, and a
/// count of resumes, so a resume that runs twice shows as a number.
struct CallProbe {
    call: Option<FeatureId>,
    resumes: Arc<AtomicUsize>,
}

impl EngineStrategy for CallProbe {
    fn features(&self) -> &'static [&'static str] {
        &["call"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        ctx.emit(self.call.expect("registered"), event.instrument, 1.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.emit(self.call.expect("registered"), InstrumentId(0), 6.0);
    }

    fn on_resume(&mut self, _ctx: &mut StrategyCtx<'_>) {
        self.resumes.fetch_add(1, Ordering::Relaxed);
    }
}

/// A fold with a link, the control plane over the same run-state pair, and the markers that
/// reached the fold, in order.
struct Harness {
    linked: LinkedEngine,
    plane: ControlPlane,
    resumes: Arc<AtomicUsize>,
    now: LocalTs,
    dispatched: Vec<RunAssertion>,
}

impl Harness {
    fn new(on_controller_loss: ControllerLoss) -> Self {
        let resumes = Arc::new(AtomicUsize::new(0));
        let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
        let linked = engine_with_link(LinkedSetup {
            instruments: &instruments,
            strategy: Box::new(CallProbe {
                call: None,
                resumes: Arc::clone(&resumes),
            }),
            tables: RecordedTables::new(&[TableKind::Features, TableKind::LinkFrames]),
            warmup: DurationUs::ZERO,
        });
        let plane = ControlPlane::new(linked.control.clone(), on_controller_loss);
        Self {
            linked,
            plane,
            resumes,
            now: ts(1_000_000),
            dispatched: Vec::new(),
        }
    }

    fn wait(&mut self, gap: DurationUs) {
        self.now = self.now + gap;
    }

    /// A subscribe that carries an assertion, as the actor handles it: the plane judges it, and a
    /// claim of control pushes the marker that is still pending.
    fn assert(&mut self, assertion: RunAssertion, marker: Marker) -> AssertionVerdict {
        let verdict = self.plane.on_assertion(CONTROLLER, assertion, self.now);
        if verdict != AssertionVerdict::NoClaim {
            self.push_pending(marker);
        }
        verdict
    }

    /// The heartbeat of the actor: the controller check, then the pending marker again.
    fn announce(&mut self, marker: Marker) -> Option<RunAssertion> {
        let park = self.plane.check_controller(self.now);
        if park.is_some() {
            self.push_pending(marker);
        }
        self.push_pending(marker);
        park
    }

    fn push_pending(&mut self, marker: Marker) {
        let Some(desired) = self.plane.pending() else {
            return;
        };
        if marker == Marker::Dropped {
            return;
        }
        let message =
            InboundMessage::new(self.now, MessageBody::RunControl(RunControl { desired }));
        self.linked.engine.dispatch(&message);
        self.dispatched.push(desired);
    }

    fn acknowledged(&self) -> RunAssertion {
        self.linked.control.acknowledged().load()
    }

    fn desired(&self) -> RunAssertion {
        self.linked.control.desired().load()
    }
}

/// Whether the push of a marker reaches the fold, or is lost as on a full queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Delivered,
    Dropped,
}

#[derive(Debug, Clone, Copy)]
enum Step {
    Assert {
        assertion: RunAssertion,
        marker: Marker,
    },
    Announce {
        marker: Marker,
    },
}

fn link_kinds(records: &[PersistRecord]) -> Vec<LinkRowKind> {
    records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::LinkFrame(row) => Some(row.kind),
            _ => None,
        })
        .collect()
}

fn link_markers(records: &[PersistRecord]) -> Vec<RunAssertion> {
    records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::LinkFrame(row) => Some(RunAssertion {
                state: match row.kind {
                    LinkRowKind::RunIdle => RunState::Idle,
                    _ => RunState::Running,
                },
                epoch: row.seq,
            }),
            _ => None,
        })
        .collect()
}

fn seal_count(records: &[PersistRecord]) -> usize {
    records
        .iter()
        .filter(|record| matches!(record, PersistRecord::SealAll))
        .count()
}

/// RUNNING, then IDLE, then RUNNING, set by nothing but recorded markers.
fn control_sequence() -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 1);
    vec![
        InboundMessage::new(ts(1), MessageBody::Book(bids)),
        InboundMessage::new(ts(1), MessageBody::Book(asks)),
        InboundMessage::new(
            ts(2),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 2)),
        ),
        spin(1, 3),
        run_control(idle_at(1), 4),
        // Parked: neither of these may reach the strategy.
        InboundMessage::new(
            ts(5),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 5)),
        ),
        spin(2, 6),
        run_control(running_at(2), 7),
        InboundMessage::new(
            ts(8),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, 8)),
        ),
        spin(3, 9),
    ]
}

/// The desired state is held at IDLE for the whole run and must gate nothing: only the recorded
/// markers decide what the strategy sees, so the same tape replays to the same hot state.
fn recorded_markers_alone_decide_what_the_strategy_sees() {
    let replay = || {
        let mut harness = Harness::new(ControllerLoss::Hold);
        assert_eq!(
            harness
                .plane
                .on_assertion(CONTROLLER, idle_at(1), harness.now),
            AssertionVerdict::Accepted,
            "a fresh epoch wins"
        );
        assert_eq!(harness.desired().state, RunState::Idle);
        for message in &control_sequence() {
            harness.linked.engine.dispatch(message);
        }
        drain_persist(&mut harness.linked)
    };
    let first = replay();
    assert_eq!(
        first,
        replay(),
        "the same control sequence replayed to a different tape"
    );
    assert_eq!(
        call_codes(&first),
        vec![1.0, 6.0, 1.0, 6.0],
        "the trade and spin between the IDLE and RUNNING markers reach nothing"
    );
    assert_eq!(
        link_kinds(&first),
        vec![LinkRowKind::RunIdle, LinkRowKind::RunRunning],
        "both applied markers are on the tape, in order"
    );
}

/// The first push of the marker is lost. The heartbeat pushes it again, the fold applies it, and
/// the heartbeat that raced the acknowledgement pushes it a second time, which changes nothing.
fn a_dropped_marker_is_pushed_again_and_applied_once() {
    let mut harness = Harness::new(ControllerLoss::Hold);
    let park = idle_at(1);
    assert_eq!(
        harness.assert(park, Marker::Dropped),
        AssertionVerdict::Accepted
    );
    assert_eq!(harness.plane.pending(), Some(park));

    harness.linked.engine.dispatch(&run_control(park, 10));
    assert_eq!(
        harness.plane.pending(),
        None,
        "the pushed marker converged the pair"
    );
    assert_eq!(
        harness.acknowledged(),
        park,
        "the fold reports what it applied"
    );

    harness.linked.engine.dispatch(&run_control(park, 11));
    assert_eq!(harness.plane.pending(), None);
    assert_eq!(harness.acknowledged(), park);

    let records = drain_persist(&mut harness.linked);
    assert!(
        call_codes(&records).is_empty(),
        "no callback fired while parked"
    );
    assert_eq!(
        link_markers(&records),
        vec![park; 2],
        "each marker that arrived is on the tape, not each one that changed something"
    );
}

/// A stale controller cannot make the state oscillate: a lower or an equal epoch loses.
fn a_lower_epoch_loses() {
    let mut harness = Harness::new(ControllerLoss::Hold);
    let now = harness.now;
    let plane = &mut harness.plane;
    assert_eq!(
        plane.on_assertion(CONTROLLER, idle_at(5), now),
        AssertionVerdict::Accepted
    );
    assert_eq!(
        plane.on_assertion(CONTROLLER, running_at(3), now),
        AssertionVerdict::Stale
    );
    assert_eq!(
        plane.on_assertion(CONTROLLER, running_at(5), now),
        AssertionVerdict::Stale
    );
    assert_eq!(harness.desired(), idle_at(5));
    let plane = &mut harness.plane;
    assert_eq!(
        plane.on_assertion(CONTROLLER, running_at(6), now),
        AssertionVerdict::Accepted
    );
    assert_eq!(harness.desired(), running_at(6));
}

fn marker() -> impl Strategy<Value = Marker> {
    prop_oneof![Just(Marker::Delivered), Just(Marker::Dropped)]
}

fn step() -> impl Strategy<Value = Step> {
    let assertion = (any::<bool>(), 0..8u64).prop_map(|(is_idle, epoch)| RunAssertion {
        state: if is_idle { RunState::Idle } else { RunState::Running },
        epoch,
    });
    prop_oneof![
        3 => (assertion, marker()).prop_map(|(assertion, marker)| Step::Assert { assertion, marker }),
        1 => marker().prop_map(|marker| Step::Announce { marker }),
    ]
}

/// Steps 0 to 100 ms apart, at most 40 of them, so no gap reaches the controller silence.
fn run() -> impl Strategy<Value = (bool, Vec<(Step, i64)>)> {
    (
        any::<bool>(),
        prop::collection::vec((step(), 0..100i64), 0..40),
    )
}

/// The state changes that the fold must apply: each marker of a higher epoch than the last one
/// applied, where the state differs.
fn transitions(dispatched: &[RunAssertion]) -> (usize, usize) {
    let mut run = RunAssertion::INITIAL;
    let (mut parks, mut resumes) = (0, 0);
    for marker in dispatched {
        if marker.epoch <= run.epoch {
            continue;
        }
        if marker.state != run.state {
            match marker.state {
                RunState::Idle => parks += 1,
                RunState::Running => resumes += 1,
            }
        }
        run = *marker;
    }
    (parks, resumes)
}

fn converges(is_dead_man: bool, steps: &[(Step, i64)]) -> Result<(), TestCaseError> {
    let on_loss = if is_dead_man { ControllerLoss::Idle } else { ControllerLoss::Hold };
    let mut harness = Harness::new(on_loss);
    let mut highest = RunAssertion::INITIAL;
    for &(step, gap_ms) in steps {
        harness.wait(DurationUs::from_millis(gap_ms));
        match step {
            Step::Assert { assertion, marker } => {
                let expected = match assertion.epoch {
                    0 => AssertionVerdict::NoClaim,
                    epoch if epoch > highest.epoch => AssertionVerdict::Accepted,
                    _ => AssertionVerdict::Stale,
                };
                if expected == AssertionVerdict::Accepted {
                    highest = assertion;
                }
                prop_assert_eq!(
                    harness.assert(assertion, marker),
                    expected,
                    "{:?}",
                    assertion
                );
            }
            Step::Announce { marker } => {
                prop_assert_eq!(
                    harness.announce(marker),
                    None,
                    "a dead-man inside the silence"
                );
            }
        }
    }

    harness.wait(DurationUs::from_millis(1));
    prop_assert_eq!(harness.announce(Marker::Delivered), None);
    prop_assert_eq!(
        harness.plane.pending(),
        None,
        "one announce after the drops converges"
    );
    prop_assert_eq!(
        harness.desired(),
        highest,
        "the highest epoch is the desired state"
    );
    prop_assert_eq!(
        harness.acknowledged(),
        highest,
        "the fold applied the highest epoch"
    );

    harness.wait(ControlPlane::SILENCE_LIMIT);
    let has_claim = highest.epoch > 0;
    let expected_park = (is_dead_man && has_claim).then(|| idle_at(highest.epoch + 1));
    prop_assert_eq!(
        harness.announce(Marker::Delivered),
        expected_park,
        "the dead-man park"
    );
    harness.wait(ControlPlane::SILENCE_LIMIT);
    prop_assert_eq!(
        harness.announce(Marker::Delivered),
        None,
        "the dead-man parks one time"
    );
    prop_assert_eq!(harness.acknowledged(), expected_park.unwrap_or(highest));
    prop_assert_eq!(harness.plane.pending(), None);

    let records = drain_persist(&mut harness.linked);
    prop_assert_eq!(
        link_markers(&records),
        harness.dispatched.clone(),
        "the tape holds each marker that arrived, in order"
    );
    let (parks, resumes) = transitions(&harness.dispatched);
    prop_assert_eq!(seal_count(&records), parks, "each park applied one time");
    prop_assert_eq!(
        harness.resumes.load(Ordering::Relaxed),
        resumes,
        "each resume one time"
    );
    Ok(())
}

/// FITNESS: the marker rides a queue that drops and counts when it is full, so the control path
/// must converge from the level and not from the edge. If it did not, a lost marker would leave
/// the venues parked while the fold trades, or the reverse, and nothing would report it. Random
/// assertions with random lost markers must end, one announce after the losses stop, with the
/// fold at the desired state of the highest epoch, each epoch applied one time, each lower epoch
/// refused, and a dead-man park that comes one time.
#[test]
fn acknowledged_state_reaches_highest_epoch_after_drops_stop() {
    recorded_markers_alone_decide_what_the_strategy_sees();
    a_dropped_marker_is_pushed_again_and_applied_once();
    a_lower_epoch_loses();

    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(&run(), |(is_dead_man, steps)| {
        converges(is_dead_man, &steps)
    });
    if let Err(failure) = outcome {
        panic!("the control path did not converge on the highest epoch: {failure}");
    }
}
