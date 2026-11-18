//! Peer frames on their way from the link actor into the fold: what the fold consumed is what its
//! tape holds, whatever the full link queue dropped.

use engine::hot::ingress::IngressQueues;
use engine::hot::spawn::{ControlQueueProducer, LinkQueueProducer, touched_input_ring};
use engine::hot::strategy::{Registration, Strategy as EngineStrategy, StrategyCtx};
use engine::link::{
    Envelope, LinkFrame, LinkHash, LinkIdentity, LinkOrigin, LinkPayload, SequenceGate, TopicId,
    schema_hash_of_fields,
};
use engine::msg::ids::{InstrumentId, QueueId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{FeatureId, LinkFrameRow, LinkRowKind, PersistRecord};
use engine::msg::time::DurationUs;
use engine::runtime::config::{RecordedTables, TableKind};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use crate::engine_support::{
    LinkedEngine, LinkedSetup, drain_persist, engine_with_link, idle_at, instrument_row,
    run_control, running_at, tracker_spec_all, ts, venue_ts,
};

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
    records: Vec<PersistRecord>,
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
                link.push(InboundMessage::new(ts(clock), MessageBody::Link(frame)));
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

fn link_rows(records: &[PersistRecord]) -> Vec<LinkFrameRow> {
    records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::LinkFrame(row) => Some(*row),
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

fn features(records: &[PersistRecord]) -> Vec<f64> {
    records
        .iter()
        .filter_map(|record| match record {
            PersistRecord::Feature(row) => Some(row.value),
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
