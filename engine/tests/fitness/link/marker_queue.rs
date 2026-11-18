//! Run-control markers and peer frames reach the fold on separate input queues, so a flood of peer
//! frames that fills the link queue never keeps a marker out, and no edge can write the run state.

use std::net::{Ipv4Addr, SocketAddr};

use engine::hot::ingress::IngressQueues;
use engine::hot::spawn::{ControlQueueProducer, LinkQueueProducer, touched_input_ring};
use engine::link::{AssertionVerdict, LinkFrame, LinkHash, LinkOrigin, LinkPayload, TopicId};
use engine::msg::ids::QueueId;
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{LinkRowKind, PersistRecord};
use engine::msg::run_state::RunState;
use engine::msg::time::DurationUs;
use engine::runtime::config::{ControllerLoss, RecordedTables, TableKind};
use engine::runtime::shutdown::{
    AcknowledgedWriter, DesiredReader, DesiredWriter, RunStateHandles,
};

use crate::engine_support::{
    Idle, LinkedSetup, drain_persist, engine_with_link, idle_at, instrument_row, running_at,
    tracker_spec_all, ts, venue_ts,
};

const CONTROLLER: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 47_002);
const LINK_CAPACITY: usize = 4;
const CONTROL_CAPACITY: usize = 2;

fn payload(seq: u64, when: i64) -> InboundMessage {
    let frame = LinkFrame {
        origin: LinkOrigin {
            sender_te_hash: LinkHash::of_name("peer-te"),
            boot_ts_us: venue_ts(1),
            topic: TopicId::FIRST_STRATEGY,
            seq,
        },
        payload: LinkPayload::new(LinkHash::of_name("peer"), venue_ts(when), &[seq as f64]),
    };
    InboundMessage::new(ts(when), MessageBody::Link(frame))
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

/// CONTRACT: a marker goes into a control queue of its own, so a full link queue keeps no marker
/// out, and the fold pops it after the older messages only. If the marker shared the queue that
/// peers fill, a flood would drop each push of it, and the fold would trade on while the edges
/// park, or the reverse, for as long as the flood lasts.
#[test]
fn payload_flood_never_delays_a_marker_past_its_next_pop() {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let mut linked = engine_with_link(LinkedSetup {
        instruments: &instruments,
        strategy: Box::new(Idle),
        tables: RecordedTables::new(&[TableKind::LinkFrames]),
        warmup: DurationUs::ZERO,
    });
    let mut plane = linked.control_plane(&[CONTROLLER.ip()], ControllerLoss::Hold);
    let (link_ring, link_queue) = touched_input_ring(LINK_CAPACITY);
    let mut link = LinkQueueProducer::new(link_ring, QueueId::new(0).unwrap());
    let (control_ring, control_queue) = touched_input_ring(CONTROL_CAPACITY);
    let mut markers = ControlQueueProducer::new(control_ring, QueueId::new(1).unwrap());
    for seq in 1..=4 {
        link.push(payload(seq, seq as i64 * 10));
    }
    assert_eq!(
        plane.on_assertion(CONTROLLER, idle_at(9), ts(50)),
        AssertionVerdict::Accepted
    );
    assert_eq!(plane.acknowledged(), running_at(0));

    assert_eq!(plane.pending(), Some(idle_at(9)), "the marker is pending");
    plane.push_pending(&mut markers, ts(60));
    assert_eq!(
        markers.dropped(),
        0,
        "the push of the marker succeeds while the link queue is full"
    );
    link.push(payload(5, 70));
    link.push(payload(6, 80));
    assert_eq!(
        link.dropped(),
        2,
        "each payload of the flood is dropped and counted"
    );

    let mut queues = IngressQueues::new(vec![link_queue, control_queue]);
    let mut pops_before_marker = 0;
    while let Some((_, message)) = queues.pop_next() {
        linked.engine.dispatch(&message);
        if matches!(message.body, MessageBody::RunControl(_)) {
            break;
        }
        pops_before_marker += 1;
        // A frame that lands once the marker waits is newer than it, and pops after it.
        link.push(payload(
            10 + pops_before_marker,
            100 + pops_before_marker as i64,
        ));
    }
    assert_eq!(
        pops_before_marker, 4,
        "the marker waited for the 4 older frames, and for no newer one"
    );
    assert_eq!(
        plane.acknowledged(),
        idle_at(9),
        "the fold applied the marker"
    );
    assert_eq!(plane.pending(), None);
    assert_eq!(markers.dropped(), 0);
    let records = drain_persist(&mut linked);
    assert_eq!(
        link_kinds(&records),
        [vec![LinkRowKind::Payload; 4], vec![LinkRowKind::RunIdle]].concat(),
        "the tape holds the four older frames, then the marker"
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record, PersistRecord::SealAll)),
        "the park sealed the tape"
    );
}

/// FITNESS (type test): only the link actor writes the desired state and only the hot thread
/// writes the acknowledged state, by type. An edge holds a reader, which turns into no writer, and
/// neither writer can be copied to a second holder. An edge that wrote the run state could start
/// or park the engine against its controller.
#[test]
fn edge_handle_cannot_write_run_state() {
    assert_lacks!(DesiredReader: Into<DesiredWriter>);
    assert_lacks!(DesiredReader: Into<AcknowledgedWriter>);
    assert_lacks!(DesiredWriter: Clone);
    assert_lacks!(AcknowledgedWriter: Clone);

    let RunStateHandles { desired_reader, .. } = RunStateHandles::new();
    let edge = desired_reader.clone();
    assert_eq!(edge.state(), RunState::Running, "a run starts running");
}
