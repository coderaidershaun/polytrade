//! What each lane of the hot thread does when its ring is full. A lane that halted where it should
//! drop, or dropped where it should halt, would lose state or let a stalled consumer stop the run.

use engine::hot::dispatch::{HotEngine, PersistWiring};
use engine::hot::exec::{ExecHalt, HaltReason};
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::{
    ExposureSink, LinkSink, PersistSink, SampleSink, StrategyLogSink, UiBookSink, UiEventSink,
};
use engine::hot::spawn::spawn_hot_thread;
use engine::hot::spawn::{HotThreadConfig, IdleWait, LinkQueueProducer, QueueProducer, SimTapGate};
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{QueueId, Side, SourceId};
use engine::msg::inbound::{InboundMessage, MarketTapItem, MessageBody, SpinTick, VenueMeta};
use engine::msg::latency::StageSample;
use engine::msg::persist::PersistRecord;
use engine::msg::ui::UiEvent;
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::shutdown::{DrainSignal, FatalReason, FatalSignal};
use engine::runtime::{DrainDeadline, DrainOutcome, Exit, Trigger};
use rtrb::{Consumer, RingBuffer};
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::engine_support::{
    Idle, detached_setup, engine_with_ui_and_exec, idle_at, instrument_row, run_control,
    running_at, strategy_log_ring, trade, ts,
};

fn spin_at(seq: u64, when: i64) -> InboundMessage {
    InboundMessage::new(ts(when), MessageBody::SpinTick(SpinTick { seq }))
}

/// CONTRACT: an input queue that the engine feeds halts the engine when it is full, and the link
/// queue, which an untrusted peer fills, drops the frame and counts it. A full input queue that
/// dropped would fold a hole in the order of messages, and a full link queue that halted would let
/// any peer stop the engine.
#[test]
fn each_lane_full_gives_its_class() {
    let queue = QueueId::new(0).unwrap();
    let (producer, consumer) = RingBuffer::<InboundMessage>::new(1);
    let fatal = FatalSignal::new();
    let mut input = QueueProducer::new(producer, fatal.clone(), queue, SourceId(0));
    input.push(spin_at(0, 1_000));
    assert!(!fatal.is_tripped(), "one message fits a ring of one slot");
    input.push(spin_at(1, 2_000));
    let reason = fatal
        .reason()
        .expect("the second push trips the fatal latch");
    assert_eq!(reason, FatalReason::InputQueueFull { queue, capacity: 1 });
    assert_eq!(reason.failure_class(), FailureClass::Halt(Halt::Engine));
    assert_eq!(reason.failure_class(), QueueProducer::WHEN_FULL);
    assert_eq!(
        consumer.slots(),
        1,
        "the producer waited for nothing and added nothing"
    );

    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let engine = HotEngine::new(detached_setup(&instruments, Box::new(Idle)));
    let hot = spawn_hot_thread(
        HotThreadConfig {
            core_id: None,
            tag: "fitness-lanes",
            samples: None,
            idle: IdleWait::Park(std::time::Duration::from_micros(50)),
        },
        IngressQueues::new(vec![consumer]),
        engine,
        fatal.clone(),
        DrainSignal::new(),
    );
    hot.join()
        .expect("the hot thread reads the latch and stops");
    let exit = Exit::decide(DrainOutcome {
        trigger: Trigger::Fatal(reason),
        fatal: fatal.reason(),
        failures: Vec::new(),
        deadline: DrainDeadline::Kept,
    });
    assert_eq!(exit, Exit::Halted(reason));
    assert_ne!(exit.code(), 0);

    let (producer, _peer_frames) = RingBuffer::<InboundMessage>::new(1);
    let mut link = LinkQueueProducer::new(producer, QueueId::new(1).unwrap());
    link.push(spin_at(0, 1_000));
    link.push(spin_at(1, 2_000));
    assert_eq!(
        link.dropped(),
        1,
        "the second frame is dropped and counted, with no trip"
    );

    let (producer, input_ring) = RingBuffer::<InboundMessage>::new(4);
    let (tap_producer, tap_ring) = RingBuffer::<MarketTapItem>::new(1);
    let fatal = FatalSignal::new();
    let mut tapped = QueueProducer::new(producer, fatal.clone(), queue, SourceId(0))
        .with_tap(tap_producer, Arc::new(SimTapGate::new()));
    tapped.push_tap_watermark(ts(500));
    assert!(!fatal.is_tripped(), "one watermark fits a tap of one slot");
    let message = InboundMessage::new(
        ts(1_000),
        MessageBody::Trade(trade(0, 100, 1, Side::Buy, 1_000)),
    );
    tapped.push_tapped(message, VenueMeta::None);
    let reason = fatal.reason().expect("a full tap trips the fatal latch");
    assert_eq!(reason, FatalReason::MarketTapFull { queue, capacity: 1 });
    assert_eq!(reason.failure_class(), QueueProducer::WHEN_FULL);
    assert_eq!(
        input_ring.slots(),
        0,
        "the input ring took nothing that the tap refused"
    );
    assert_eq!(tap_ring.slots(), 1, "the tap holds only its watermark");

    assert_eq!(LinkQueueProducer::WHEN_FULL, FailureClass::Reject);
    let state_lanes = [
        ("ui books", UiBookSink::WHEN_FULL),
        ("ui events", UiEventSink::WHEN_FULL),
        ("outbound link", LinkSink::WHEN_FULL),
        ("strategy log", StrategyLogSink::WHEN_FULL),
        ("latency samples", SampleSink::<StageSample>::WHEN_FULL),
        ("exposure, older snapshot", ExposureSink::WHEN_FULL),
        ("tape rows", PersistSink::ROW_WHEN_FULL),
    ];
    for (lane, class) in state_lanes {
        assert_eq!(class, FailureClass::Reject, "{lane}");
    }
    assert_eq!(PersistSink::SEAL_WHEN_FULL, FailureClass::Retry);
    assert_eq!(
        PersistSink::SEAL_PAST_CAP,
        FailureClass::Halt(Halt::Quoting)
    );
}

/// A tape that records the link-frame table, so that each run-control message writes one row.
struct Parked {
    engine: HotEngine,
    tape: Consumer<PersistRecord>,
    ui_events: Consumer<UiEvent>,
}

impl Parked {
    /// Two run controls fill a tape ring of two slots. The park at epoch 5 asks for a seal that
    /// does not fit, so the seal waits, with a cap on its tries.
    fn with_seal_try_limit(limit: u32) -> Parked {
        let (producer, tape) = RingBuffer::<PersistRecord>::new(2);
        let persistence = PersistWiring {
            sink: PersistSink::new(producer, NonZeroU32::new(limit).unwrap()),
            tables: RecordedTables::new(&[TableKind::LinkFrames]),
        };
        Parked::parked_with(persistence, tape)
    }

    fn parked_with(persistence: PersistWiring, tape: Consumer<PersistRecord>) -> Parked {
        let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
        let (mut engine, _ui_books, ui_events, _commands) = engine_with_ui_and_exec(
            &instruments,
            Box::new(Idle),
            persistence,
            strategy_log_ring(64).0,
            1,
        );
        for message in [
            run_control(running_at(1), 1_000),
            run_control(running_at(2), 2_000),
            run_control(idle_at(5), 3_000),
        ] {
            engine.dispatch(&message);
        }
        Parked {
            engine,
            tape,
            ui_events,
        }
    }

    fn spin(&mut self, seq: u64, when: i64) -> Option<ExecHalt> {
        self.engine.dispatch(&spin_at(seq, when));
        let mut halt = None;
        while let Ok(event) = self.ui_events.pop() {
            if let UiEvent::Execution { halt: latest, .. } = event {
                halt = Some(latest);
            }
        }
        halt
    }

    fn tape(&mut self) -> Vec<PersistRecord> {
        std::iter::from_fn(|| self.tape.pop().ok()).collect()
    }
}

/// CONTRACT: a seal that waits goes first once the writer makes room, ahead of each row after it,
/// and quoting goes on. A row that landed ahead of the seal would be written into a file that the
/// seal had closed.
#[test]
fn pending_seal_goes_first_when_room_returns() {
    let mut parked = Parked::with_seal_try_limit(3);
    assert_eq!(
        parked.engine.dropped_persist_records(),
        1,
        "the row of the park found the ring full and was dropped and counted"
    );
    let first = parked.tape.pop().expect("the writer takes one row");
    assert!(matches!(first, PersistRecord::LinkFrame(_)), "{first:?}");

    let halt = parked.spin(1, 4_000);
    assert_eq!(
        halt.map(ExecHalt::is_halted),
        Some(false),
        "no halt: {halt:?}"
    );
    let tape = parked.tape();
    assert!(
        matches!(
            tape.as_slice(),
            [PersistRecord::LinkFrame(_), PersistRecord::SealAll]
        ),
        "the second row, then the seal at the next spin: {tape:?}"
    );

    for message in [
        run_control(running_at(6), 5_000),
        run_control(idle_at(7), 6_000),
    ] {
        parked.engine.dispatch(&message);
    }
    let halt = parked.spin(2, 7_000);
    assert_eq!(
        halt.map(ExecHalt::is_halted),
        Some(false),
        "the next seal counts its tries from 1 again, so its second try halts nothing: {halt:?}"
    );
}

/// CONTRACT: a seal that the ring refuses is tried at the request and at each spin, and the last try
/// that the cap allows halts quoting. A seal that waited with no end would drop each row of a parked
/// run for as long as the writer stalls, with nothing to say so.
#[test]
fn seal_retry_reaches_cap_then_halts() {
    let mut parked = Parked::with_seal_try_limit(3);
    let halt = parked.spin(1, 4_000);
    assert_eq!(
        halt.map(ExecHalt::is_halted),
        Some(false),
        "try 2 of 3 fails: {halt:?}"
    );
    let halt = parked.spin(2, 5_000);
    assert!(
        matches!(
            halt,
            Some(ExecHalt::Halted {
                reason: HaltReason::SealNotAccepted,
                ..
            })
        ),
        "try 3 of 3 fails, and quoting halts: {halt:?}"
    );

    parked.engine.dispatch(&run_control(idle_at(5), 6_000));
    assert_eq!(
        parked.engine.dropped_persist_records(),
        2,
        "the row of the park and the row that came while the seal waited are dropped and counted"
    );
    let tape = parked.tape();
    assert!(
        matches!(
            tape.as_slice(),
            [PersistRecord::LinkFrame(_), PersistRecord::LinkFrame(_)]
        ),
        "no row lands behind a seal that was not sent: {tape:?}"
    );
}
