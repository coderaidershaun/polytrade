//! What each lane of the hot thread does when its ring is full. A lane that halted where it should
//! drop, or dropped where it should halt, would lose state or let a stalled consumer stop the run.

use engine::hot::dispatch::{HotEngine, PersistWiring};
use engine::hot::exec::HaltReason;
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::{
    AuditBank, AuditSink, ExposureSink, LinkSink, ResearchSink, SampleSink, StrategyLogSink,
    UiBookSink, UiEventSink,
};
use engine::hot::spawn::spawn_hot_thread;
use engine::hot::spawn::{
    ControlQueueProducer, HotDriver, HotLoop, HotThreadConfig, IdleWait, LinkQueueProducer,
    QueueProducer, SimTapGate,
};
use engine::log::open_hot_lane;
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{QueueId, Side, SourceId};
use engine::msg::inbound::{
    AuditBankFull, FileWriter, InboundMessage, MarketTapItem, MessageBody, SpinTick, WriterFailure,
};
use engine::msg::latency::StageSample;
use engine::msg::persist::{AuditRecord, ResearchRecord};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{Config, RecordedTables, TableKind, TrackerSpec};
use engine::runtime::registry::Registry;
use engine::runtime::shutdown::{DrainSignal, FatalReason, FatalSignal};
use engine::runtime::{DrainDeadline, DrainOutcome, Exit, Trigger, pin_to_core};
use rtrb::{Consumer, RingBuffer};
use std::net::{Ipv4Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;
use std::sync::Arc;

use crate::engine_support::{
    FillPen, Idle, ONE, detached_setup, engine_with_ui_and_exec, idle_at, instrument_row,
    run_control, running_at, strategy_log_ring, trade, ts,
};
use crate::micro_strategy::MicroRecorderParams;

fn spin_at(seq: u64, when: i64) -> InboundMessage {
    InboundMessage::new(ts(when), MessageBody::SpinTick(SpinTick { seq }))
}

/// CONTRACT: an input queue that the engine feeds halts the engine when it is full, and the link
/// queue, which an untrusted peer fills, drops the frame and counts it. A full input queue that
/// dropped would fold a hole in the order of messages, and a full link queue that halted would let
/// any peer stop the engine. The writer-status queues are ones that the engine feeds: its own file
/// writers fill them, the exposure writer one and the tape writer the other, and so is the audit
/// status queue, which the audit sink of the hot thread fills. The control queue drops and counts
/// too: its sender is outside the engine, and
/// the link actor pushes the pending marker again at its next heartbeat.
#[test]
fn each_lane_full_gives_its_class() {
    let queue = QueueId::new(0).unwrap();
    full_engine_fed_queue_halts(queue, [spin_at(0, 1_000), spin_at(1, 2_000)]);
    let writer_failed = InboundMessage::new(
        ts(1_000),
        MessageBody::WriterFailed(WriterFailure {
            writer: FileWriter::Exposure,
            consecutive_failures: 3,
        }),
    );
    full_engine_fed_queue_halts(shipped_writer_status_queue(), [writer_failed; 2]);
    let tape_failed = InboundMessage::new(
        ts(1_000),
        MessageBody::WriterFailed(WriterFailure {
            writer: FileWriter::Tape,
            consecutive_failures: 1,
        }),
    );
    full_engine_fed_queue_halts(shipped_tape_status_queue(), [tape_failed; 2]);
    let bank_full = InboundMessage::new(
        ts(1_000),
        MessageBody::AuditBankFull(AuditBankFull {
            banked: 49_152,
            capacity: 65_536,
        }),
    );
    full_engine_fed_queue_halts(shipped_audit_status_queue(), [bank_full; 2]);

    let (producer, _peer_frames) = RingBuffer::<InboundMessage>::new(1);
    let mut link = LinkQueueProducer::new(producer, QueueId::new(1).unwrap());
    let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 47_004));
    link.push(peer, spin_at(0, 1_000));
    link.push(peer, spin_at(1, 2_000));
    assert_eq!(
        link.dropped(),
        1,
        "the second frame is dropped and counted, with no trip"
    );

    let (producer, markers) = RingBuffer::<InboundMessage>::new(1);
    let mut control = ControlQueueProducer::new(producer, QueueId::new(2).unwrap());
    control.push(idle_at(1), ts(1_000));
    control.push(running_at(2), ts(2_000));
    assert_eq!(
        control.dropped(),
        1,
        "the second marker is dropped and counted, with no trip"
    );
    assert_eq!(markers.slots(), 1, "the first marker waits in the ring");
    assert_eq!(ControlQueueProducer::WHEN_FULL, FailureClass::Reject);

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
    tapped.push_tapped(message);
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
        ("research rows", ResearchSink::ROW_WHEN_FULL),
        ("audit rows past the bank", AuditSink::BANK_FULL),
    ];
    for (lane, class) in state_lanes {
        assert_eq!(class, FailureClass::Reject, "{lane}");
    }
    assert_eq!(ResearchSink::SEAL_WHEN_FULL, FailureClass::Retry);
    assert_eq!(AuditSink::WHEN_FULL, FailureClass::Retry);
    for halts_quoting in [
        ResearchSink::SEAL_PAST_CAP,
        AuditSink::SEAL_PAST_CAP,
        AuditSink::BANK_AT_HIGH_MARK,
    ] {
        assert_eq!(halts_quoting, FailureClass::Halt(Halt::Quoting));
    }
}

/// Pushes two messages into an input queue of one slot that the engine feeds. The second trips the
/// fatal latch with no wait and leaves the first in the ring, and a hot thread over the ring reads
/// the latch and stops.
fn full_engine_fed_queue_halts(queue: QueueId, [first, second]: [InboundMessage; 2]) {
    let (producer, consumer) = RingBuffer::<InboundMessage>::new(1);
    let fatal = FatalSignal::new();
    let mut input = QueueProducer::new(producer, fatal.clone(), queue, SourceId(0));
    input.push(first);
    assert!(!fatal.is_tripped(), "one message fits a ring of one slot");
    input.push(second);
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
    let hot_fatal = fatal.clone();
    let hot = spawn_hot_thread(
        HotThreadConfig {
            core_id: None,
            tag: "fitness-lanes",
        },
        pin_to_core,
    )
    .expect("a thread with no core never refuses its pin")
    .start(move || HotLoop {
        driver: HotDriver::new(IngressQueues::new(vec![consumer]), engine, None),
        idle: IdleWait::Park(std::time::Duration::from_micros(50)),
        fatal: hot_fatal,
        drain: DrainSignal::new(),
    });
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
}

/// The writer-status queue of the binance engine that ships, which trades.
fn shipped_writer_status_queue() -> QueueId {
    let config = Config::<MicroRecorderParams>::load(Path::new(
        "../strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml",
    ))
    .expect("the shipped binance config loads");
    Registry::build(&config)
        .expect("the shipped binance registry builds")
        .writer_status_queue_id()
        .expect("an engine that trades has a writer-status queue")
}

/// The status queue of the tape writer of the binance engine that ships, which keeps a tape.
fn shipped_tape_status_queue() -> QueueId {
    let config = Config::<MicroRecorderParams>::load(Path::new(
        "../strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml",
    ))
    .expect("the shipped binance config loads");
    Registry::build(&config)
        .expect("the shipped binance registry builds")
        .tape_status_queue_id()
        .expect("an engine with a tape has a tape status queue")
}

fn shipped_audit_status_queue() -> QueueId {
    let config = Config::<MicroRecorderParams>::load(Path::new(
        "../strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml",
    ))
    .expect("the shipped binance config loads");
    Registry::build(&config)
        .expect("the shipped binance registry builds")
        .audit_status_queue_id()
        .expect("an engine with a tape has an audit status queue")
}

/// A tape that records the link-frame table, so that each run-control message writes one row.
struct Parked {
    engine: HotEngine,
    tape: Consumer<ResearchRecord>,
    ui_events: Consumer<UiEvent>,
}

impl Parked {
    /// Two run controls fill a tape ring of two slots. The park at epoch 5 asks for a seal that
    /// does not fit, so the seal waits, with a cap on its tries.
    fn with_seal_try_limit(limit: u32) -> Parked {
        Parked::with_seal_borders(limit, NonZeroU64::MAX)
    }

    fn with_seal_borders(limit: u32, interval_spins: NonZeroU64) -> Parked {
        let (producer, tape) = RingBuffer::<ResearchRecord>::new(2);
        // The audit ring has room for its seals, so each halt here is the research seal's.
        let (audit_producer, _audit) = RingBuffer::<AuditRecord>::new(64);
        let limit = NonZeroU32::new(limit).unwrap();
        let bank = AuditBank {
            rows: 64,
            high_mark: 64,
        };
        let persistence = PersistWiring {
            audit: AuditSink::new(audit_producer, bank, limit),
            research: ResearchSink::new(producer, limit),
            tables: RecordedTables::new(&[TableKind::LinkFrames]),
            seal_interval_spins: interval_spins,
        };
        Parked::parked_with(persistence, tape)
    }

    fn parked_with(persistence: PersistWiring, tape: Consumer<ResearchRecord>) -> Parked {
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

    fn spin(&mut self, seq: u64, when: i64) -> Option<UiHalt> {
        self.engine.dispatch(&spin_at(seq, when));
        let mut halt = None;
        while let Ok(event) = self.ui_events.pop() {
            if let UiEvent::Execution { halt: latest, .. } = event {
                halt = Some(latest);
            }
        }
        halt
    }

    fn tape(&mut self) -> Vec<ResearchRecord> {
        std::iter::from_fn(|| self.tape.pop().ok()).collect()
    }
}

/// An audit ring of two slots and a bank behind it, with room in the research ring. A fill banks
/// its rows past the first two, and the park after it asks for a seal that waits behind them.
fn audit_parked(limit: u32) -> (Parked, Consumer<AuditRecord>) {
    let (research_producer, tape) = RingBuffer::<ResearchRecord>::new(64);
    let (audit_producer, audit) = RingBuffer::<AuditRecord>::new(2);
    let limit = NonZeroU32::new(limit).unwrap();
    let bank = AuditBank {
        rows: 64,
        high_mark: 64,
    };
    let persistence = PersistWiring {
        audit: AuditSink::new(audit_producer, bank, limit),
        research: ResearchSink::new(research_producer, limit),
        tables: RecordedTables::new(&[TableKind::Orders, TableKind::Fills]),
        seal_interval_spins: NonZeroU64::MAX,
    };
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (mut engine, _ui_books, ui_events, _commands) = engine_with_ui_and_exec(
        &instruments,
        Box::new(Idle),
        persistence,
        strategy_log_ring(64).0,
        0,
    );
    engine.dispatch(&run_control(running_at(1), 1_000));
    for message in FillPen::new(0).fill(Side::Buy, 100 * ONE, ONE, 1_500) {
        engine.dispatch(&message);
    }
    assert!(
        engine.banked_audit_rows() > 0,
        "the fill banks rows past the two slots of the ring"
    );
    engine.dispatch(&run_control(idle_at(5), 3_000));
    let parked = Parked {
        engine,
        tape,
        ui_events,
    };
    (parked, audit)
}

fn audit_rows(audit: &mut Consumer<AuditRecord>) -> Vec<AuditRecord> {
    std::iter::from_fn(|| audit.pop().ok()).collect()
}

/// CONTRACT: a seal that waits goes first once the writer makes room, ahead of each row after it,
/// and quoting goes on. A row that landed ahead of the seal would be written into a file that the
/// seal had closed.
#[test]
fn pending_seal_goes_first_when_room_returns() {
    let mut parked = Parked::with_seal_try_limit(3);
    assert_eq!(
        parked.engine.dropped_research_records().total(),
        1,
        "the row of the park found the ring full and was dropped and counted"
    );
    let first = parked.tape.pop().expect("the writer takes one row");
    assert!(matches!(first, ResearchRecord::LinkFrame(_)), "{first:?}");

    let halt = parked.spin(1, 4_000);
    assert_eq!(
        halt.map(UiHalt::is_halted),
        Some(false),
        "no halt: {halt:?}"
    );
    let tape = parked.tape();
    assert!(
        matches!(
            tape.as_slice(),
            [ResearchRecord::LinkFrame(_), ResearchRecord::SealAll]
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
        halt.map(UiHalt::is_halted),
        Some(false),
        "the next seal counts its tries from 1 again, so its second try halts nothing: {halt:?}"
    );

    // The audit seal waits behind the rows that the bank held when it was asked for, and reaches
    // the writer after each of them and before any row after it.
    let (mut parked, mut audit) = audit_parked(3);
    let banked = parked.engine.banked_audit_rows();
    let mut written = audit_rows(&mut audit);
    let halt = parked.spin(1, 4_000);
    assert_eq!(
        halt.map(UiHalt::is_halted),
        Some(false),
        "try 2 of 3 of the audit seal: {halt:?}"
    );
    written.extend(audit_rows(&mut audit));
    parked.spin(2, 5_000);
    written.extend(audit_rows(&mut audit));
    let seal_at = written
        .iter()
        .position(|record| matches!(record, AuditRecord::SealAll))
        .unwrap_or_else(|| panic!("the audit seal reaches the writer: {written:?}"));
    assert_eq!(
        seal_at,
        2 + banked - 1,
        "the seal comes after the two rows of the ring and the rows of the bank, as the last of \
         the {banked} records that the bank held: {written:?}"
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
        halt.map(UiHalt::is_halted),
        Some(false),
        "try 2 of 3 fails: {halt:?}"
    );
    let halt = parked.spin(2, 5_000);
    assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
                reason: HaltReason::SealNotAccepted,
                ..
            })
        ),
        "try 3 of 3 fails, and quoting halts: {halt:?}"
    );

    parked.engine.dispatch(&run_control(idle_at(5), 6_000));
    assert_eq!(
        parked.engine.dropped_research_records().link_frames,
        2,
        "the row of the park and the row that came while the seal waited are dropped and counted"
    );
    let tape = parked.tape();
    assert!(
        matches!(
            tape.as_slice(),
            [ResearchRecord::LinkFrame(_), ResearchRecord::LinkFrame(_)]
        ),
        "no row lands behind a seal that was not sent: {tape:?}"
    );

    // A cap of 1: the request is the one try, so the next spin pushes nothing and halts, though
    // the writer has made room by then.
    let mut hot_log = open_hot_lane(64);
    let mut parked = Parked::with_seal_try_limit(1);
    let first = parked.tape.pop().expect("the writer takes one row");
    assert!(matches!(first, ResearchRecord::LinkFrame(_)), "{first:?}");
    let halt = parked.spin(1, 4_000);
    assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
                reason: HaltReason::SealNotAccepted,
                ..
            })
        ),
        "try 1 of 1 failed at the request, so the next spin halts: {halt:?}"
    );
    // The spin halts before it writes its own rows, and the first of them, the mid of the book,
    // then sends the seal into the room that the writer made: a seal past the halt is no try.
    let after_halt = parked.tape();
    assert!(
        matches!(
            after_halt.as_slice(),
            [ResearchRecord::LinkFrame(_)]
                | [ResearchRecord::LinkFrame(_), ResearchRecord::SealAll]
        ),
        "no seal is pushed between its cap and the halt: {after_halt:?}"
    );
    let refusals: Vec<String> = std::iter::from_fn(|| hot_log.pop())
        .map(|record| {
            let mut line = String::new();
            record
                .render(&mut line)
                .expect("a string takes every write");
            line
        })
        .filter(|line| line.starts_with("persistence seal refused"))
        .collect();
    assert!(
        matches!(refusals.as_slice(), [line] if line.contains("each of its 1 tries")),
        "one log record, with the true count of tries: {refusals:?}"
    );
    parked.spin(2, 5_000);
    let later = parked.tape();
    let seals = after_halt
        .iter()
        .chain(&later)
        .filter(|record| matches!(record, ResearchRecord::SealAll))
        .count();
    assert_eq!(
        seals, 1,
        "after the halt the seal still goes, once, when room returns: {later:?}"
    );

    // A seal border at each spin asks for a seal while one waits. The waiting seal closes each
    // file once it goes, so it keeps its count, and try 3 of 3 still halts.
    let mut parked = Parked::with_seal_borders(3, NonZeroU64::MIN);
    parked.spin(1, 4_000);
    let halt = parked.spin(2, 5_000);
    assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
                reason: HaltReason::SealNotAccepted,
                ..
            })
        ),
        "a border keeps the count of the waiting seal: {halt:?}"
    );

    // An audit ring that the writer never drains, beside a research ring with room: the research
    // seal goes at the request, so the halt at try 3 of 3 is the audit seal's.
    let (mut parked, _audit) = audit_parked(3);
    assert!(
        parked
            .tape()
            .iter()
            .any(|record| matches!(record, ResearchRecord::SealAll)),
        "the research seal is sent at the request"
    );
    let halt = parked.spin(1, 4_000);
    assert_eq!(
        halt.map(UiHalt::is_halted),
        Some(false),
        "try 2 of 3 of the audit seal fails: {halt:?}"
    );
    let halt = parked.spin(2, 5_000);
    assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
                reason: HaltReason::SealNotAccepted,
                ..
            })
        ),
        "try 3 of 3 of the audit seal fails, and quoting halts: {halt:?}"
    );
}
