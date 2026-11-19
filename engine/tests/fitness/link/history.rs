//! A tab that opens during a run gets the recent events of the engine on the history topic, a fixed
//! count each flush, while live frames still leave in the flush of their arrival.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use engine::link::{HistoryRing, LinkBody, LinkDatagram, Subscribe, TopicId, TopicSet};
use engine::msg::ids::InstrumentId;
use engine::msg::run_state::{ControllerEpoch, RunState};
use engine::msg::time::VenueTs;
use engine::msg::ui::UiEvent;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

use super::loopback::LoopbackActor;
use super::wire_frames::{encode, envelope, guard};
use crate::operator::rust_sources;

fn count(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("a test count is not zero")
}

/// An event that carries its number in its sequence, so a receiver can name each frame.
fn event(seq: u64) -> UiEvent {
    UiEvent::Rotation {
        instrument: InstrumentId(0),
        seq,
        event_ts_us: VenueTs::from_micros(1_700_000_000_000_000 + seq as i64),
    }
}

/// The steps of one flush in the order the send path takes them: the history frames due, then each
/// live event, which is sent and then recorded. It returns the history frames that went out.
fn flush(
    ring: &mut HistoryRing,
    subscribers: &[SocketAddr],
    live: &[u64],
) -> Vec<(SocketAddr, u64)> {
    ring.begin_flush(subscribers);
    let mut sent = Vec::new();
    while let Some((to, frame)) = ring.next_frame() {
        sent.push((to, frame.seq()));
    }
    for seq in live {
        ring.record(event(*seq));
    }
    sent
}

fn tab_address(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// A tab on loopback that asks one engine for the events and the history topics.
struct Tab {
    socket: UdpSocket,
    history: Vec<u64>,
    live: Vec<u64>,
}

impl Tab {
    fn subscribe(engine: SocketAddr) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port binds");
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("the read timeout is set");
        let topics = TopicSet::new(&[TopicId::EVENTS, TopicId::HISTORY]).expect("two topics fit");
        let subscribe = LinkDatagram {
            envelope: envelope(TopicId::SUBSCRIBE, 1),
            body: LinkBody::Subscribe(Subscribe {
                topics,
                desired_state: RunState::Running,
                desired_epoch: ControllerEpoch::ZERO,
            }),
        };
        socket
            .send_to(&encode(&subscribe), engine)
            .expect("the subscribe is sent");
        Self {
            socket,
            history: Vec::new(),
            live: Vec::new(),
        }
    }

    /// Reads datagrams until `is_done` holds for what arrived, or five seconds pass.
    fn read_until(&mut self, is_done: impl Fn(&Self) -> bool) {
        let mut buffer = [0_u8; 1_500];
        for _ in 0..100 {
            if is_done(self) {
                return;
            }
            let Ok(len) = self.socket.recv(&mut buffer) else {
                continue;
            };
            let datagram = LinkDatagram::decode(&buffer[..len], &guard())
                .expect("each frame of the engine decodes");
            match datagram.body {
                LinkBody::History(frame) => self.history.push(frame.seq()),
                LinkBody::Event(frame) => self.live.push(frame.seq()),
                _ => {}
            }
        }
    }
}

/// CONTRACT: a subscriber that names the history topic gets each event the ring held when it came,
/// one time each and in order, at most the count of a flush in each flush. Live events go on in the
/// flush of their arrival, and the ring keeps the size it was made with.
#[test]
fn late_subscriber_gets_recent_events_and_live_frames_still_flow() {
    let tab = tab_address(47_100);
    let mut ring = HistoryRing::new(count(8), count(2));
    for seq in 1..=8 {
        ring.record(event(seq));
    }
    let flushes = [
        flush(&mut ring, &[tab], &[9]),
        flush(&mut ring, &[tab], &[10]),
        flush(&mut ring, &[tab], &[]),
        flush(&mut ring, &[tab], &[]),
        flush(&mut ring, &[tab], &[]),
    ];
    let expected: [&[u64]; 5] = [&[1, 2], &[3, 4], &[5, 6], &[7, 8], &[]];
    for (index, (sent, expected)) in flushes.iter().zip(expected).enumerate() {
        let seqs: Vec<u64> = sent.iter().map(|(_, seq)| *seq).collect();
        assert_eq!(seqs, expected, "flush {}: wrong history frames", index + 1);
        assert!(
            sent.iter().all(|(to, _)| *to == tab),
            "flush {}: a frame went elsewhere",
            index + 1
        );
    }
    let held: Vec<u64> = ring.held().map(UiEvent::seq).collect();
    assert_eq!(
        held,
        (3..=10).collect::<Vec<_>>(),
        "the ring holds the newest eight"
    );
    assert_eq!(
        ring.capacity(),
        8,
        "the ring keeps the size it was made with"
    );
    assert_eq!(ring.skipped(), 0, "no frame was evicted before it was sent");

    let mut actor = LoopbackActor::start_with(HistoryRing::new(count(8), count(2)));
    for seq in 1..=8 {
        actor
            .events
            .push(event(seq))
            .expect("the event ring has room");
    }
    // Several flushes of 10 ms record the eight events before any tab subscribes.
    std::thread::sleep(Duration::from_millis(200));
    let mut late = Tab::subscribe(actor.address);
    late.read_until(|tab| !tab.history.is_empty());
    for seq in [9, 10] {
        actor
            .events
            .push(event(seq))
            .expect("the event ring has room");
    }
    late.read_until(|tab| tab.history.len() >= 8 && tab.live.len() >= 2);
    assert_eq!(
        late.history,
        (1..=8).collect::<Vec<_>>(),
        "history frames, in order"
    );
    assert_eq!(late.live, vec![9, 10], "live frames on the events topic");
    let counts = actor.counts_when(|counts| counts.history_sent >= 8);
    assert_eq!(
        counts.history_sent, 8,
        "each history frame is counted: {counts:?}"
    );
    assert_eq!(
        counts.history_failed, 0,
        "no history send failed: {counts:?}"
    );
    assert_eq!(
        counts.history_skipped, 0,
        "no history frame was skipped: {counts:?}"
    );
}

#[derive(Debug, Clone)]
enum Step {
    Record(u64),
    Flush(Vec<bool>),
}

fn arb_steps() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        (1_u64..40).prop_map(Step::Record),
        prop::collection::vec(any::<bool>(), 3).prop_map(Step::Flush),
    ];
    prop::collection::vec(step, 1..60)
}

/// Each flush may send at most `per_flush` frames, the frames each tab gets rise one by one, and a
/// tab gets only frames recorded before it came, so no live frame rides the history topic.
fn holds_its_size_and_order(
    capacity: usize,
    per_flush: usize,
    steps: &[Step],
) -> Result<(), TestCaseError> {
    let tabs = [
        tab_address(47_201),
        tab_address(47_202),
        tab_address(47_203),
    ];
    let mut ring = HistoryRing::new(count(capacity), count(per_flush));
    let mut recorded = 0_u64;
    let mut joined: BTreeMap<SocketAddr, u64> = BTreeMap::new();
    let mut last_sent: BTreeMap<SocketAddr, u64> = BTreeMap::new();
    for step in steps {
        match step {
            Step::Record(events) => {
                for _ in 0..*events {
                    recorded += 1;
                    ring.record(event(recorded));
                }
            }
            Step::Flush(present) => {
                let subscribers: Vec<SocketAddr> = tabs
                    .iter()
                    .zip(present)
                    .filter(|(_, is_present)| **is_present)
                    .map(|(tab, _)| *tab)
                    .collect();
                joined.retain(|tab, _| subscribers.contains(tab));
                last_sent.retain(|tab, _| subscribers.contains(tab));
                for tab in &subscribers {
                    joined.entry(*tab).or_insert(recorded);
                }
                let sent = flush(&mut ring, &subscribers, &[]);
                prop_assert!(
                    sent.len() <= per_flush,
                    "a flush sent {} frames",
                    sent.len()
                );
                for (to, seq) in sent {
                    prop_assert!(
                        seq <= joined[&to],
                        "{to} got frame {seq}, recorded after it came"
                    );
                    let previous = last_sent.insert(to, seq).unwrap_or(0);
                    prop_assert!(
                        seq > previous,
                        "{to} got frame {seq} after frame {previous}"
                    );
                }
            }
        }
        prop_assert_eq!(ring.capacity(), capacity, "the ring changed its size");
        prop_assert!(ring.len() <= capacity);
    }
    Ok(())
}

/// FITNESS: the history ring keeps the size it was made with after any count of events, never sends
/// more than the count of a flush, and no code of the hot thread names it. A ring that grew, or a
/// hot thread that fed it, would turn a late tab into a latency cost of the fold.
#[test]
fn history_ring_never_grows_and_never_reaches_the_hot_thread() {
    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let cases = (1_usize..32, 1_usize..6, arb_steps());
    let outcome = runner.run(&cases, |(capacity, per_flush, steps)| {
        holds_its_size_and_order(capacity, per_flush, &steps)
    });
    if let Err(failure) = outcome {
        panic!("{failure}");
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/hot");
    let mut naming = Vec::new();
    for path in rust_sources(&root) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let names_it = source.lines().any(|line| {
            line.split("//")
                .next()
                .unwrap_or(line)
                .contains("HistoryRing")
        });
        if names_it {
            naming.push(path.display().to_string());
        }
    }
    assert!(
        naming.is_empty(),
        "code of the hot thread names the history ring: {naming:?}"
    );
}
