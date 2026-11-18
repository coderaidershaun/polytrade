//! Who may assert the run state of an engine, and which epochs: only a listed controller, and only
//! an epoch that the run-state cell can hold beside its state.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use engine::hot::ingress::IngressQueues;
use engine::hot::spawn::{ControlQueueProducer, touched_input_ring};
use engine::link::{
    AssertionVerdict, ControlPlane, LINK_MAX_DATAGRAM, Lifecycle, LinkBody, LinkDatagram,
    LinkDecodeError, RunPhase, Subscribe, SubscriberTable, TopicId, TopicSet,
};
use engine::msg::ids::QueueId;
use engine::msg::inbound::{InboundMessage, MessageBody, RunControl};
use engine::msg::run_state::{ControllerEpoch, EpochOutOfRange, RunAssertion, RunState};
use engine::msg::time::{DurationUs, LocalTs};
use engine::runtime::config::{ControllerLoss, RecordedTables, TableKind};

use super::wire_frames::{envelope, guard};
use crate::engine_support::{
    Idle, LinkedEngine, LinkedSetup, engine_with_link, instrument_row, tracker_spec_all, ts,
};

const SENTINEL: u64 = 0x0123_4567_89ab_cdef;
const TOP_BIT: u64 = 1 << 63;

fn linked() -> LinkedEngine {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    engine_with_link(LinkedSetup {
        instruments: &instruments,
        strategy: Box::new(Idle),
        tables: RecordedTables::new(&[TableKind::LinkFrames]),
        warmup: DurationUs::ZERO,
    })
}

/// The bytes of `datagram`, with the eight bytes of `SENTINEL` set to `epoch`: a peer can send any
/// 64 bits there, whatever this build can encode.
fn with_epoch(datagram: &LinkDatagram, epoch: u64) -> Vec<u8> {
    let mut buffer = [0; LINK_MAX_DATAGRAM];
    let len = datagram.encode(&mut buffer);
    let mut bytes = buffer[..len].to_vec();
    let sentinel = SENTINEL.to_le_bytes();
    let at = bytes
        .windows(8)
        .position(|window| window == sentinel)
        .expect("the sentinel epoch is in the frame");
    bytes[at..at + 8].copy_from_slice(&epoch.to_le_bytes());
    bytes
}

fn subscribe(state: RunState) -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::SUBSCRIBE, 1),
        body: LinkBody::Subscribe(Subscribe {
            topics: TopicSet::new(&[TopicId::LIFECYCLE]).expect("one topic fits"),
            desired_state: state,
            desired_epoch: ControllerEpoch::new(SENTINEL),
        }),
    }
}

fn lifecycle(state: RunState) -> LinkDatagram {
    LinkDatagram {
        envelope: envelope(TopicId::LIFECYCLE, 1),
        body: LinkBody::Lifecycle(Lifecycle {
            phase: RunPhase::Ready,
            run_state: state,
            execution_mode: None,
            acknowledged_epoch: ControllerEpoch::new(SENTINEL),
            spin_interval_us: DurationUs::from_millis(100),
            feature_count: 3,
        }),
    }
}

/// The assertion that each frame read back, or the refusal of its decode.
fn decoded_epochs(state: RunState, epoch: u64) -> [Result<RunAssertion, LinkDecodeError>; 2] {
    let read = |datagram: LinkDatagram| {
        let decoded = LinkDatagram::decode(&with_epoch(&datagram, epoch), &guard())?;
        Ok(match decoded.body {
            LinkBody::Subscribe(subscribe) => RunAssertion {
                state: subscribe.desired_state,
                epoch: subscribe.desired_epoch,
            },
            LinkBody::Lifecycle(lifecycle) => RunAssertion {
                state: lifecycle.run_state,
                epoch: lifecycle.acknowledged_epoch,
            },
            other => panic!("the frame decoded as another kind: {other:?}"),
        })
    };
    [read(subscribe(state)), read(lifecycle(state))]
}

/// The fold applies the marker and stores what it applied in the cell, and the link reads it back.
fn cell_round_trip(assertion: RunAssertion) -> RunAssertion {
    let mut linked = linked();
    let plane = linked.control_plane(&[], ControllerLoss::Hold);
    let marker = InboundMessage::new(
        ts(1),
        MessageBody::RunControl(RunControl { desired: assertion }),
    );
    linked.engine.dispatch(&marker);
    plane.acknowledged()
}

fn refused_or_round_trips(state: RunState, epoch: u64) -> Result<(), TestCaseError> {
    for decoded in decoded_epochs(state, epoch) {
        let assertion = match decoded {
            Ok(assertion) => assertion,
            Err(refusal) => {
                prop_assert!(epoch >= TOP_BIT, "epoch {epoch} below 2^63 was refused");
                prop_assert_eq!(
                    refusal,
                    LinkDecodeError::EpochOutOfRange { found: epoch },
                    "the refusal names the value seen"
                );
                prop_assert_eq!(
                    ControllerEpoch::try_from(epoch),
                    Err(EpochOutOfRange { found: epoch })
                );
                continue;
            }
        };
        prop_assert_eq!(
            u64::from(assertion.epoch),
            epoch,
            "the epoch reads back as sent"
        );
        prop_assert_eq!(assertion.state, state);
        if epoch > 0 {
            prop_assert_eq!(
                cell_round_trip(assertion),
                assertion,
                "the run-state cell gives back the state and epoch it was given"
            );
        }
        prop_assert!(
            epoch < TOP_BIT,
            "epoch {epoch} of 2^63 or more was accepted"
        );
    }
    Ok(())
}

/// FITNESS: no 64 bits in the epoch of a subscribe or a lifecycle frame give another state or
/// another epoch than the ones that were sent. The run-state cell keeps the state in the top bit of
/// the word of the epoch, so an epoch of 2^63 or more that got in would read back as the other
/// state at another epoch, and a forged frame could park or start the engine.
#[test]
fn each_u64_epoch_is_refused_or_round_trips() {
    let mut refused = 0;
    for epoch in [TOP_BIT - 1, TOP_BIT] {
        for state in [RunState::Running, RunState::Idle] {
            refused += decoded_epochs(state, epoch)
                .iter()
                .filter(|decoded| decoded.is_err())
                .count();
        }
    }
    assert_eq!(
        refused, 4,
        "2^63 is refused in both frames and both states, 2^63 - 1 never"
    );
    top_epoch_leaves_the_dead_man_an_epoch();

    for epoch in [
        0,
        1,
        TOP_BIT - 2,
        TOP_BIT - 1,
        TOP_BIT,
        TOP_BIT + 1,
        u64::MAX,
    ] {
        for state in [RunState::Running, RunState::Idle] {
            if let Err(failure) = refused_or_round_trips(state, epoch) {
                panic!("({state:?}, {epoch}): {failure}");
            }
        }
    }
    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let states = prop_oneof![Just(RunState::Running), Just(RunState::Idle)];
    let outcome = runner.run(&(states, any::<u64>()), |(state, epoch)| {
        refused_or_round_trips(state, epoch)
    });
    if let Err(failure) = outcome {
        panic!("an epoch neither refused nor round-tripped: {failure}");
    }
}

/// The control plane refuses the top epoch from a listed controller and counts it, so the dead-man
/// park always has the desired epoch plus 1.
fn top_epoch_leaves_the_dead_man_an_epoch() {
    let mut linked = linked();
    let mut plane = linked.control_plane(&[LISTED], ControllerLoss::Idle);
    let tab = SocketAddr::new(LISTED, 40_001);
    let top = RunAssertion {
        state: RunState::Running,
        epoch: ControllerEpoch::MAX,
    };
    assert_eq!(
        plane.on_assertion(tab, top, ts(0)),
        AssertionVerdict::EpochTooHigh
    );
    assert_eq!(plane.refused_epochs(), 1);
    assert_eq!(
        plane.desired(),
        RunAssertion::INITIAL,
        "the desired state did not change"
    );
    let below = at(RunState::Running, ControllerEpoch::MAX.get() - 1);
    assert_eq!(
        plane.on_assertion(tab, below, ts(0)),
        AssertionVerdict::Accepted
    );
    let park = plane.check_controller(ts(0) + ControlPlane::SILENCE_LIMIT);
    assert_eq!(park, Some(at(RunState::Idle, ControllerEpoch::MAX.get())));
}

const LISTED: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
const FORGER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)), 40_002);

/// The inbound path of the link actor for a subscribe: the subscriber table, then the control
/// plane, then the pending marker into the control queue and on to the fold.
struct Link {
    linked: LinkedEngine,
    subscribers: SubscriberTable,
    plane: ControlPlane,
    markers: ControlQueueProducer,
    queues: IngressQueues,
}

impl Link {
    fn new(plane: impl FnOnce(&mut LinkedEngine) -> ControlPlane) -> Self {
        let mut linked = linked();
        let plane = plane(&mut linked);
        let (ring, queue) = touched_input_ring(16);
        Self {
            linked,
            subscribers: SubscriberTable::new(),
            plane,
            markers: ControlQueueProducer::new(ring, QueueId::new(0).unwrap()),
            queues: IngressQueues::new(vec![queue]),
        }
    }

    fn subscribe(
        &mut self,
        from: SocketAddr,
        assertion: RunAssertion,
        now: LocalTs,
    ) -> AssertionVerdict {
        let topics = TopicSet::new(&[TopicId::LIFECYCLE]).expect("one topic fits");
        self.subscribers.refresh(from, topics, now);
        let verdict = self.plane.on_assertion(from, assertion, now);
        self.plane.push_pending(&mut self.markers, now);
        self.fold();
        verdict
    }

    fn announce(&mut self, now: LocalTs) -> Option<RunAssertion> {
        let park = self.plane.check_controller(now);
        self.plane.push_pending(&mut self.markers, now);
        self.fold();
        park
    }

    fn fold(&mut self) {
        while let Some((_, message)) = self.queues.pop_next() {
            self.linked.engine.dispatch(&message);
        }
    }
}

fn at(state: RunState, epoch: u64) -> RunAssertion {
    RunAssertion {
        state,
        epoch: ControllerEpoch::new(epoch),
    }
}

fn secs(seconds: i64) -> LocalTs {
    ts(seconds * 1_000_000)
}

/// CONTRACT: an assertion from an address that is not listed changes no state and keeps no engine
/// running. If it did, one forged frame of a high epoch would lock each real controller out until
/// a restart, and a forger that kept sending would keep the dead-man rule from ever parking.
#[test]
fn assertion_from_unlisted_address_changes_nothing() {
    let mut link = Link::new(|linked| linked.control_plane(&[LISTED], ControllerLoss::Idle));
    let first_tab = SocketAddr::new(LISTED, 40_001);
    assert_eq!(
        link.subscribe(first_tab, at(RunState::Running, 5), secs(0)),
        AssertionVerdict::Accepted
    );
    assert_eq!(link.plane.acknowledged(), at(RunState::Running, 5));

    let forged = at(RunState::Idle, 9_000_000_000_000_000_000);
    assert_eq!(
        link.subscribe(FORGER, forged, secs(1)),
        AssertionVerdict::UnlistedSender
    );
    assert_eq!(
        link.subscribers.len(),
        2,
        "the topics of the forger are accepted"
    );
    assert_eq!(
        link.plane.refused_assertions().of(FORGER.ip()),
        1,
        "counted for its address"
    );
    assert_eq!(
        link.plane.desired(),
        at(RunState::Running, 5),
        "the forged assertion changed the desired state"
    );
    assert_eq!(link.plane.pending(), None, "no marker is pending");

    let second_tab = SocketAddr::new(LISTED, 40_003);
    assert_eq!(
        link.subscribe(second_tab, at(RunState::Idle, 6), secs(2)),
        AssertionVerdict::Accepted,
        "a port is no part of the match"
    );
    assert_eq!(link.plane.acknowledged(), at(RunState::Idle, 6));
    link.subscribe(second_tab, at(RunState::Running, 7), secs(3));
    assert_eq!(link.plane.acknowledged(), at(RunState::Running, 7));

    let silence = ControlPlane::SILENCE_LIMIT.to_secs() as i64;
    let mut parks = Vec::new();
    let mut forged_sends = 1;
    for second in 4..3 + silence + 4 {
        if second % 2 == 1 {
            link.subscribe(FORGER, forged, secs(second));
            forged_sends += 1;
        }
        if let Some(park) = link.announce(secs(second)) {
            parks.push((second, park));
        }
    }
    assert_eq!(
        parks,
        vec![(3 + silence, at(RunState::Idle, 8))],
        "the dead-man rule parks one time, at the silence limit after the listed controller"
    );
    assert_eq!(
        link.plane.acknowledged(),
        at(RunState::Idle, 8),
        "the fold is parked"
    );
    let refused = link.plane.refused_assertions();
    assert_eq!(
        refused.of(FORGER.ip()),
        forged_sends,
        "each forged assertion is counted"
    );
    assert_eq!(refused.of(LISTED), 0);
    assert_eq!(refused.total(), forged_sends);
}
