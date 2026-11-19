//! Peer frames on their way from the link actor into the fold: what the fold consumed is what its
//! tape holds, whatever the full link queue dropped, and each drop is counted for its sender.

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use engine::hot::ingress::IngressQueues;
use engine::hot::spawn::{ControlQueueProducer, LinkQueueProducer, touched_input_ring};
use engine::hot::strategy::{Registration, Strategy as EngineStrategy, StrategyCtx};
use engine::link::{
    Envelope, FrameGuard, HistoryRing, LINK_MAX_DATAGRAM, LINK_MAX_SENDERS, LinkActor,
    LinkActorSetup, LinkBody, LinkCounts, LinkCountsHandle, LinkDatagram, LinkFrame, LinkHash,
    LinkIdentity, LinkOrigin, LinkPayload, OutboundLink, SenderCount, SequenceGate, TopicId,
    schema_hash_of_fields,
};
use engine::msg::ids::{InstrumentId, QueueId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{FeatureId, LinkFrameRow, LinkRowKind, ResearchRecord};
use engine::msg::time::{DurationUs, EngineClock};
use engine::msg::ui::LatencyReport;
use engine::msg::ui::channel::ui_channel;
use engine::runtime::config::{ControllerLoss, RecordedTables, SenderPlacement, TableKind};
use engine::runtime::metrics::{RollupRow, RollupSink, SenderDropRow};
use engine::runtime::shutdown::RunStateHandles;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use rtrb::RingBuffer;

use crate::engine_support::{
    LinkedEngine, LinkedSetup, drain_persist, engine_with_link, idle_at, instrument_row,
    run_control, running_at, tracker_spec_all, ts, venue_ts,
};
use crate::runtime::metrics::Harness;

const FIELDS: [&str; 2] = ["peer_mid", "peer_confidence"];
const SENDERS: [&str; 2] = ["peer-a", "peer-b"];
const BOOT_US: i64 = 1_000;

/// Records each value a peer sent as a feature, so the strategy's part of the fold shows in rows.
struct EchoProbe {
    received: Option<FeatureId>,
}

impl EngineStrategy for EchoProbe {
    fn features(&self) -> &'static [&'static str] {
        &["received"]
    }

    fn link_fields(&self) -> &'static [&'static str] {
        &FIELDS
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.received = registration.features.first().copied();
    }

    fn on_link(&mut self, ctx: &mut StrategyCtx<'_>, frame: &LinkFrame) {
        for value in frame.payload.values() {
            ctx.emit(self.received.expect("registered"), InstrumentId(0), *value);
        }
    }
}

fn linked() -> LinkedEngine {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    engine_with_link(LinkedSetup {
        instruments: &instruments,
        strategy: Box::new(EchoProbe { received: None }),
        tables: RecordedTables::new(&[TableKind::Features, TableKind::LinkFrames]),
        warmup: DurationUs::ZERO,
    })
}

fn identity(sender: usize) -> LinkIdentity {
    LinkIdentity {
        token_hash: LinkHash::of_name(""),
        strategy_hash: LinkHash::of_name("strat"),
        sender_te_hash: LinkHash::of_name(SENDERS[sender]),
        boot_ts_us: venue_ts(BOOT_US),
    }
}

/// One step of a run: a peer sends a frame, a controller asserts a run state, or the fold pops.
#[derive(Debug, Clone)]
enum Step {
    Frame {
        sender: usize,
        seq: u64,
        values: Vec<f64>,
    },
    Park,
    Resume,
    Pop(usize),
}

/// What one run left behind: what the fold consumed, its tape, and its state.
struct Run {
    admitted: Vec<(usize, u64)>,
    consumed: Vec<InboundMessage>,
    records: Vec<ResearchRecord>,
    digest: engine::hot::dispatch::StateDigest,
}

/// The link actor's half (the sequence gate, then the real link queue, with run-control markers on
/// their own queue), then the fold, which pops the oldest head of the two rings when the script
/// says, and once more at the end until both are empty.
fn run(script: &[Step], link_capacity: usize) -> Run {
    let (link_ring, link_consumer) = touched_input_ring(link_capacity);
    let (control_ring, control_consumer) = touched_input_ring(4);
    let mut link = LinkQueueProducer::new(link_ring, QueueId::new(0).expect("queue 0"));
    let mut markers = ControlQueueProducer::new(control_ring, QueueId::new(1).expect("queue 1"));
    let mut queues = IngressQueues::new(vec![link_consumer, control_consumer]);
    let mut gate = SequenceGate::new();
    let mut linked = linked();
    let mut epoch = 0;
    let mut clock = 100;
    let mut admitted = Vec::new();
    let mut consumed = Vec::new();
    let mut records = Vec::new();
    let mut pop = |linked: &mut LinkedEngine, queues: &mut IngressQueues, count: usize| {
        for _ in 0..count {
            let Some((_, message)) = queues.pop_next() else {
                return;
            };
            linked.engine.dispatch(&message);
            records.extend(drain_persist(linked));
            consumed.push(message);
        }
    };
    for step in script {
        clock += 10;
        match step {
            Step::Frame {
                sender,
                seq,
                values,
            } => {
                let envelope = Envelope::new(identity(*sender), TopicId::FIRST_STRATEGY, *seq);
                if !gate.admit(&envelope, ts(clock)).is_accepted() {
                    continue;
                }
                admitted.push((*sender, *seq));
                let frame = LinkFrame {
                    origin: LinkOrigin::from(&envelope),
                    payload: LinkPayload::new(
                        schema_hash_of_fields(&FIELDS),
                        venue_ts(clock - 1),
                        values,
                    ),
                };
                link.push(
                    address(40_000 + *sender as u16),
                    InboundMessage::new(ts(clock), MessageBody::Link(frame)),
                );
            }
            Step::Park => {
                epoch += 1;
                markers.push(idle_at(epoch), ts(clock));
            }
            Step::Resume => {
                epoch += 1;
                markers.push(running_at(epoch), ts(clock));
            }
            Step::Pop(count) => pop(&mut linked, &mut queues, *count),
        }
    }
    pop(&mut linked, &mut queues, usize::MAX);
    Run {
        admitted,
        consumed,
        records,
        digest: linked.engine.state_digest(),
    }
}

fn link_rows(records: &[ResearchRecord]) -> Vec<LinkFrameRow> {
    records
        .iter()
        .filter_map(|record| match record {
            ResearchRecord::LinkFrame(row) => Some(*row),
            _ => None,
        })
        .collect()
}

/// The messages the tape says the fold consumed, rebuilt from its link rows alone: one payload for
/// each run of value rows of one frame, one marker for each run-state row.
fn messages_of_tape(rows: &[LinkFrameRow]) -> Vec<InboundMessage> {
    let mut messages = Vec::new();
    let mut index = 0;
    while index < rows.len() {
        let row = rows[index];
        let when = row.received_ts_us.micros();
        match row.kind {
            LinkRowKind::RunIdle | LinkRowKind::RunRunning => {
                let assertion = if row.kind == LinkRowKind::RunIdle {
                    idle_at(row.seq)
                } else {
                    running_at(row.seq)
                };
                messages.push(run_control(assertion, when));
                index += 1;
            }
            LinkRowKind::Payload => {
                let count = usize::from(row.count);
                let values: Vec<f64> = rows[index..index + count]
                    .iter()
                    .map(|slot| slot.value)
                    .collect();
                let frame = LinkFrame {
                    origin: LinkOrigin {
                        sender_te_hash: LinkHash(row.sender_te_hash),
                        boot_ts_us: venue_ts(BOOT_US),
                        topic: TopicId(row.topic),
                        seq: row.seq,
                    },
                    payload: LinkPayload::new(
                        schema_hash_of_fields(&FIELDS),
                        row.event_ts_us,
                        &values,
                    ),
                };
                messages.push(InboundMessage::new(ts(when), MessageBody::Link(frame)));
                index += count;
            }
        }
    }
    messages
}

fn refold(messages: &[InboundMessage]) -> Run {
    let mut linked = linked();
    let mut records = Vec::new();
    for message in messages {
        linked.engine.dispatch(message);
        records.extend(drain_persist(&mut linked));
    }
    Run {
        admitted: Vec::new(),
        consumed: messages.to_vec(),
        records,
        digest: linked.engine.state_digest(),
    }
}

/// The claims of one run: the gate kept each duplicate and reorder off the ring, the tape holds
/// exactly what the fold consumed (parked or not), and a second fold of the tape gives the same
/// state and the same rows.
fn holds_for(script: &[Step], link_capacity: usize) -> Run {
    let first = run(script, link_capacity);
    for (sender, name) in SENDERS.iter().enumerate() {
        let seqs: Vec<u64> = first
            .admitted
            .iter()
            .filter(|(each, _)| *each == sender)
            .map(|(_, seq)| *seq)
            .collect();
        assert!(
            seqs.windows(2).all(|pair| pair[0] < pair[1]),
            "a duplicate or a reorder of {name} entered the ring: {seqs:?}"
        );
    }
    let from_tape = messages_of_tape(&link_rows(&first.records));
    assert_eq!(
        from_tape, first.consumed,
        "the link rows of the tape are the frames and markers the fold consumed"
    );
    let second = refold(&from_tape);
    assert_eq!(
        second.records, first.records,
        "a second fold of the tape writes the same rows"
    );
    assert_eq!(
        second.digest, first.digest,
        "a second fold of the tape ends in the same state"
    );
    first
}

fn features(records: &[ResearchRecord]) -> Vec<f64> {
    records
        .iter()
        .filter_map(|record| match record {
            ResearchRecord::Feature(row) => Some(row.value),
            _ => None,
        })
        .collect()
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        6 => (0..SENDERS.len(), 1..12u64, prop::collection::vec(-1e6..1e6f64, 1..=FIELDS.len()))
            .prop_map(|(sender, seq, values)| Step::Frame { sender, seq, values }),
        1 => Just(Step::Park),
        1 => Just(Step::Resume),
        3 => (0..6usize).prop_map(Step::Pop),
    ]
}

/// Fitness: a dropped frame never reaches the tape and a consumed frame always does, so a replay
/// of the tape is the run it describes. A tape written upstream of the drop-and-count link queue
/// would hold frames that the fold never saw, and its replay would diverge in silence.
#[test]
fn fold_state_equals_refold_of_consumed_link_frames() {
    let duplicates_and_reorders: Vec<Step> = [(1, 1.0), (2, 2.0), (2, 22.0), (1, 11.0), (3, 3.0)]
        .into_iter()
        .map(|(seq, value)| Step::Frame {
            sender: 0,
            seq,
            values: vec![value],
        })
        .collect();
    let fixed = holds_for(&duplicates_and_reorders, 8);
    assert_eq!(fixed.admitted, vec![(0, 1), (0, 2), (0, 3)]);
    assert_eq!(features(&fixed.records), vec![1.0, 2.0, 3.0]);
    let seqs: Vec<u64> = link_rows(&fixed.records)
        .iter()
        .map(|row| row.seq)
        .collect();
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "the tape holds exactly what was consumed"
    );

    let cases = if std::env::var("CI_NIGHTLY").is_ok() { 4096 } else { 256 };
    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });
    let scripts = (prop::collection::vec(step(), 0..40), 1..6usize);
    runner
        .run(&scripts, |(script, link_capacity)| {
            holds_for(&script, link_capacity);
            Ok(())
        })
        .unwrap_or_else(|failure| panic!("{failure}"));
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

fn frame_message(seq: u64) -> InboundMessage {
    let envelope = Envelope::new(identity(0), TopicId::FIRST_STRATEGY, seq);
    let frame = LinkFrame {
        origin: LinkOrigin::from(&envelope),
        payload: LinkPayload::new(schema_hash_of_fields(&FIELDS), venue_ts(1), &[1.0]),
    };
    InboundMessage::new(ts(100), MessageBody::Link(frame))
}

fn counted(queue_full: u64, over_rate: u64) -> SenderCount {
    SenderCount {
        queue_full,
        over_rate,
    }
}

/// A link actor on a loopback socket with no hot thread, so its link queue of two slots stays as
/// full as the peers make it.
struct FloodedActor {
    _runtime: tokio::runtime::Runtime,
    address: SocketAddr,
    _inbound: rtrb::Consumer<InboundMessage>,
    counts: LinkCountsHandle,
}

impl FloodedActor {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("a test runtime builds");
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port binds");
        socket
            .set_nonblocking(true)
            .expect("the socket turns non-blocking");
        let address = socket.local_addr().expect("a bound socket has an address");
        let (producer, inbound) = touched_input_ring(2);
        let (marker_producer, _markers) = touched_input_ring(4);
        let (_wiring, channels) = ui_channel();
        let (_outbound_producer, outbound) = RingBuffer::<OutboundLink>::new(4);
        let (_latency_producer, latency) = RingBuffer::<LatencyReport>::new(4);
        let counts = LinkCountsHandle::new();
        LinkActor::spawn(
            LinkActorSetup {
                socket,
                identity: identity(1),
                guard: FrameGuard {
                    token_hash: LinkHash::of_name(""),
                    strategy_hash: LinkHash::of_name("strat"),
                    schema_hash: schema_hash_of_fields(&FIELDS),
                },
                peers: Vec::new(),
                on_controller_loss: ControllerLoss::default(),
                controllers: Vec::new(),
                inbound: LinkQueueProducer::new(producer, QueueId::new(1).expect("queue 1")),
                markers: ControlQueueProducer::new(
                    marker_producer,
                    QueueId::new(2).expect("queue 2"),
                ),
                outbound,
                channels,
                latency,
                control: RunStateHandles::new().desired_writer,
                clock: EngineClock::start(),
                spin_interval: DurationUs::from_micros(1_000_000),
                topic_count: TopicId::space_len(1),
                feature_count: 0,
                placement: SenderPlacement::Shared,
                flush_period: Duration::from_millis(10),
                counts: counts.clone(),
                history: HistoryRing::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
            },
            runtime.handle(),
        );
        Self {
            _runtime: runtime,
            address,
            _inbound: inbound,
            counts,
        }
    }

    /// Sends `count` payload frames of one sender from `peer`, with rising sequence numbers.
    fn flood(&self, peer: &UdpSocket, sender: usize, count: u64) {
        let mut buffer = [0; LINK_MAX_DATAGRAM];
        for seq in 1..=count {
            let datagram = LinkDatagram {
                envelope: Envelope::new(identity(sender), TopicId::FIRST_STRATEGY, seq),
                body: LinkBody::Payload(LinkPayload::new(
                    schema_hash_of_fields(&FIELDS),
                    venue_ts(1),
                    &[seq as f64],
                )),
            };
            let len = datagram.encode(&mut buffer);
            peer.send_to(&buffer[..len], self.address)
                .expect("a loopback frame is sent");
        }
    }

    /// The counts of the actor once its heartbeat published `drops` drops of a full queue.
    fn counts_after(&self, drops: u64) -> LinkCounts {
        for _ in 0..100 {
            let counts = self.counts.load();
            let published: u64 = counts
                .senders
                .rows()
                .map(|(_, count)| count.queue_full)
                .sum();
            if published >= drops {
                return counts;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        self.counts.load()
    }
}

#[derive(Default)]
struct SenderRows(Vec<SenderDropRow>);

impl RollupSink for SenderRows {
    fn write(&mut self, row: RollupRow) {
        if let RollupRow::SenderDrop(row) = row {
            self.0.push(row);
        }
    }
}

/// Contract: a frame that the full link queue drops, or that comes above its sender's rate, is
/// counted for the address that sent it, and the metrics thread writes those counts as one row for
/// each sender. A flood from one peer then shows as that peer, and never as a quiet total.
#[test]
fn link_drops_are_counted_for_each_sender() {
    let (a, b) = (address(40_001), address(40_002));
    let (ring, _consumer) = touched_input_ring(2);
    let mut link = LinkQueueProducer::new(ring, QueueId::new(0).expect("queue 0"));
    for seq in 1..=3 {
        link.push(a, frame_message(seq));
    }
    for seq in 1..=2 {
        link.push(b, frame_message(seq));
    }
    assert_eq!(
        link.senders().of(a),
        counted(1, 0),
        "a full queue drops a's third frame"
    );
    assert_eq!(link.senders().of(b), counted(2, 0), "and each frame of b");
    assert_eq!(link.dropped(), 3, "the total is the sum of the senders");

    let (ring, _consumer) = touched_input_ring(64);
    let rate = NonZeroU32::new(2).expect("two is not zero");
    let mut link =
        LinkQueueProducer::new(ring, QueueId::new(0).expect("queue 0")).with_sender_rate(rate);
    for seq in 1..=3 {
        link.push(a, frame_message(seq));
    }
    link.push(b, frame_message(1));
    assert_eq!(
        link.senders().of(a),
        counted(0, 1),
        "a's third frame of the period is over rate"
    );
    assert_eq!(
        link.senders().of(b),
        counted(0, 0),
        "b keeps a rate of its own"
    );
    link.start_rate_period();
    for seq in 4..=5 {
        link.push(a, frame_message(seq));
    }
    assert_eq!(
        link.senders().of(a),
        counted(0, 1),
        "a new period gives a its rate again"
    );

    let (ring, _consumer) = touched_input_ring(1);
    let mut link = LinkQueueProducer::new(ring, QueueId::new(0).expect("queue 0"));
    link.push(a, frame_message(1));
    let crowd = LINK_MAX_SENDERS as u16 + 3;
    for port in 0..crowd {
        link.push(address(41_000 + port), frame_message(1));
    }
    let others = link
        .senders()
        .rows()
        .find_map(|(sender, count)| sender.is_none().then_some(count));
    assert_eq!(
        others,
        Some(counted(4, 0)),
        "each sender past the table is counted in one row of its own"
    );

    let actor = FloodedActor::start();
    let (peer_a, peer_b) = (
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port binds"),
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port binds"),
    );
    actor.flood(&peer_a, 0, 3);
    actor.flood(&peer_b, 1, 2);
    let counts = actor.counts_after(3);
    let (from_a, from_b) = (
        peer_a.local_addr().expect("bound"),
        peer_b.local_addr().expect("bound"),
    );
    assert_eq!(counts.senders.of(from_a), counted(1, 0), "{counts:?}");
    assert_eq!(counts.senders.of(from_b), counted(2, 0), "{counts:?}");

    let mut harness = Harness::new(1);
    harness.metrics.watch_link_counts(actor.counts.clone());
    let mut rows = SenderRows::default();
    harness.metrics.pass(ts(1), &mut rows);
    harness.metrics.pass(ts(61_000_000), &mut rows);
    let written: Vec<(Option<SocketAddr>, u64, u64)> = rows
        .0
        .iter()
        .map(|row| (row.sender, row.queue_full_total, row.over_rate_total))
        .collect();
    assert!(
        written.contains(&(Some(from_a), 1, 0)) && written.contains(&(Some(from_b), 2, 0)),
        "the metrics thread writes a drop row for each sender: {written:?}"
    );
}
