//! START and STOP from a tab: the bytes of a real workstation session cross the real relay to a
//! real link actor, become a marker that the fold applies, and come back to the tab as acknowledged.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::num::NonZeroUsize;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rtrb::{Consumer, RingBuffer};
use serde_json::Value;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use engine::hot::spawn::{ControlQueueProducer, LinkQueueProducer, touched_input_ring};
use engine::link::{
    Envelope, HistoryRing, LinkActor, LinkActorSetup, LinkBody, LinkCountsHandle, LinkCredential,
    LinkDatagram, LinkHash, OutboundLink, Subscribe, TopicId, schema_hash_of_fields,
};
use engine::msg::ids::QueueId;
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::persist::{LinkRowKind, PersistRecord};
use engine::msg::run_state::{ControllerEpoch, RunState};
use engine::msg::time::{DurationUs, EngineClock, LocalTs, VenueTs};
use engine::msg::ui::LatencyReport;
use engine::msg::ui::channel::ui_channel;
use engine::operator::workstation::link::model::ControlVerdict;
use engine::operator::workstation::link::session::{LinkSession, LinkTarget};
use engine::runtime::config::{
    ControllerLoss, RecordedTables, SenderPlacement, StrategyId, TableKind,
};
use engine::runtime::shutdown::DesiredWriter;

use super::web_http::{DEADLINE, bundle, command, fake_engine, listening};
use crate::engine_support::{
    Idle, LinkedEngine, LinkedSetup, drain_persist, engine_with_link, instrument_row,
    tracker_spec_all,
};

/// The strategy that the relay binary of these tests serves.
const STRATEGY: &str = "web-test";
const TOKEN: &str = "tab-to-fold-token";
const SUBSCRIBE_INTERVAL: DurationUs = DurationUs::from_secs(2);

/// The session's clock starts at 4 µs, so its first assertion has epoch 5 and its second epoch 6.
const SESSION_BOOT: LocalTs = LocalTs::from_micros(4);

type Tab = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("an answer arrives in time")
}

/// Connects the way a browser tab does, with the origin of the page the server served.
async fn open_tab(port: u16, peer: usize) -> Tab {
    let mut request = format!("ws://127.0.0.1:{port}/api/link?peer={peer}")
        .into_client_request()
        .expect("a valid url");
    let origin = HeaderValue::from_str(&format!("http://127.0.0.1:{port}")).expect("a header");
    request.headers_mut().insert("Origin", origin);
    let (tab, response) = within(connect_async(request))
        .await
        .expect("the relay accepts the upgrade");
    assert_eq!(response.status(), 101);
    tab
}

/// The session that the browser builds from the relay's hello, which is the tab's first message.
async fn session_of_tab(tab: &mut Tab, now: LocalTs) -> LinkSession {
    let Some(Ok(Message::Text(text))) = within(tab.next()).await else {
        panic!("the first message of a tab is the hello, as text");
    };
    let hello: Value = serde_json::from_str(text.as_str()).expect("the hello is json");
    let peers = hello["peers"]
        .as_array()
        .expect("the hello lists its peers")
        .iter()
        .map(|peer| {
            peer.as_str()
                .and_then(|text| text.parse::<SocketAddr>().ok())
                .expect("each peer is an address")
        })
        .collect();
    let target = LinkTarget {
        strategy: StrategyId::new(hello["strategy"].as_str().expect("a strategy"))
            .expect("the hello names a valid strategy"),
        peers,
        token_hash: LinkHash(hello["tokenHash"].as_u64().expect("a token hash")),
        sender_nonce: 7,
    };
    LinkSession::new(target, now).expect("the hello names at least one peer")
}

/// A subscribe of the session, as the engine's guard decodes it.
fn decoded_frame(bytes: &[u8]) -> (Envelope, Subscribe) {
    let guard = LinkCredential::new(Some(TOKEN), STRATEGY).guard(schema_hash_of_fields(&[]));
    let datagram = LinkDatagram::decode(bytes, &guard).expect("the engine's guard admits it");
    match datagram.body {
        LinkBody::Subscribe(subscribe) => (datagram.envelope, subscribe),
        other => panic!("a session sends subscribes only, got {other:?}"),
    }
}

fn decoded(bytes: &[u8]) -> Subscribe {
    decoded_frame(bytes).1
}

/// Sends the subscribe that is due at `now` through the tab, and returns it as the engine reads it.
async fn send_due(session: &mut LinkSession, tab: &mut Tab, now: LocalTs) -> (Envelope, Subscribe) {
    let bytes = session
        .subscribe_due(now)
        .expect("a subscribe is due")
        .to_vec();
    within(tab.send(Message::binary(bytes.clone())))
        .await
        .expect("the subscribe is sent");
    decoded_frame(&bytes)
}

/// Hands each datagram that the tab receives to the session, until the verdict of the session
/// reads `verdict`.
async fn read_until(
    session: &mut LinkSession,
    tab: &mut Tab,
    now: LocalTs,
    verdict: ControlVerdict,
) -> ControlVerdict {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let status = session.status(now);
        if status.control == verdict {
            return status.control;
        }
        assert!(
            Instant::now() < deadline,
            "the tab never read {verdict:?}, last {status:?}"
        );
        if let Message::Binary(bytes) = within(tab.next()).await.expect("open").expect("a frame") {
            session.on_datagram(&bytes, now);
        }
    }
}

/// A link actor of the engine on a loopback socket, with 127.0.0.1 on its list of controllers.
/// The test holds the read ends of its queues and plays the hot thread itself.
struct Engine {
    address: SocketAddr,
    markers: Consumer<InboundMessage>,
    _peer_frames: Consumer<InboundMessage>,
    _outbound: rtrb::Producer<OutboundLink>,
    _latency: rtrb::Producer<LatencyReport>,
}

impl Engine {
    fn start(control: DesiredWriter) -> Self {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a loopback port binds");
        socket
            .set_nonblocking(true)
            .expect("the socket turns non-blocking");
        let address = socket.local_addr().expect("a bound socket has an address");
        let (link_ring, peer_frames) = touched_input_ring(16);
        let (marker_ring, markers) = touched_input_ring(16);
        let (_wiring, channels) = ui_channel();
        let (outbound_producer, outbound) = RingBuffer::<OutboundLink>::new(4);
        let (latency_producer, latency) = RingBuffer::<LatencyReport>::new(4);
        let credential = LinkCredential::new(Some(TOKEN), STRATEGY);
        LinkActor::spawn(
            LinkActorSetup {
                socket,
                identity: credential.identity(
                    LinkHash::of_name("te-tab-to-fold"),
                    VenueTs::from_micros(1_000),
                ),
                guard: credential.guard(schema_hash_of_fields(&[])),
                peers: Vec::new(),
                on_controller_loss: ControllerLoss::Hold,
                controllers: vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
                inbound: LinkQueueProducer::new(link_ring, QueueId::new(1).expect("queue 1")),
                markers: ControlQueueProducer::new(marker_ring, QueueId::new(2).expect("queue 2")),
                outbound,
                channels,
                latency,
                control,
                clock: EngineClock::start(),
                spin_interval: DurationUs::from_micros(1_000_000),
                topic_count: TopicId::space_len(0),
                feature_count: 0,
                placement: SenderPlacement::Shared,
                flush_period: Duration::from_millis(10),
                counts: LinkCountsHandle::new(),
                history: HistoryRing::new(NonZeroUsize::MIN, NonZeroUsize::MIN),
            },
            &tokio::runtime::Handle::current(),
        );
        Self {
            address,
            markers,
            _peer_frames: peer_frames,
            _outbound: outbound_producer,
            _latency: latency_producer,
        }
    }

    /// The marker of `desired`, popped as the hot thread pops it. The link actor pushes the
    /// pending marker again at each heartbeat, so each copy of an older marker popped on the way
    /// goes to the fold, which applies each epoch once.
    async fn marker_of(
        &mut self,
        fold: &mut LinkedEngine,
        desired: (RunState, u64),
    ) -> InboundMessage {
        let deadline = Instant::now() + DEADLINE;
        loop {
            match self.markers.pop() {
                Ok(marker) if desired_of(&marker) == desired => return marker,
                Ok(older) => fold.engine.dispatch(&older),
                Err(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "the link actor pushed no marker of {desired:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    /// Each marker left on the control queue.
    fn markers_left(&mut self) -> Vec<(RunState, u64)> {
        let mut left = Vec::new();
        while let Ok(marker) = self.markers.pop() {
            left.push(desired_of(&marker));
        }
        left
    }
}

fn desired_of(marker: &InboundMessage) -> (RunState, u64) {
    let MessageBody::RunControl(control) = marker.body else {
        panic!("the control queue holds run-control markers only, got {marker:?}");
    };
    (control.desired.state, control.desired.epoch.get())
}

fn fold() -> LinkedEngine {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    engine_with_link(LinkedSetup {
        instruments: &instruments,
        strategy: Box::new(Idle),
        tables: RecordedTables::new(&[TableKind::LinkFrames]),
        warmup: DurationUs::ZERO,
    })
}

/// Contract: the one path of run control, end to end. A tab asserts START at epoch 5, then STOP
/// at epoch 6: each subscribe crosses the relay from a socket of its own, passes the guard and the
/// list of controllers, becomes a marker on the control queue, and the fold applies it, parks and
/// seals. The lifecycle frame brings the acknowledged epoch back, so the tab reads Pending and then
/// Applied, and a second tab with a lower epoch reads Lost and moves nothing.
#[tokio::test(flavor = "multi_thread")]
async fn start_then_stop_from_a_tab_parks_the_fold() {
    let mut fold = fold();
    let mut engine = Engine::start(fold.desired_writer());
    let (_spare, spare_address) = fake_engine();
    let site = bundle("tab-to-fold");
    let mut serve = command(site.path(), &[engine.address, spare_address]);
    serve.args(["--token", TOKEN]);
    let (_server, port) = listening(serve);
    let mut tab = open_tab(port, 0).await;
    let mut session = session_of_tab(&mut tab, SESSION_BOOT).await;

    let (envelope, watching) = send_due(&mut session, &mut tab, SESSION_BOOT).await;
    assert_eq!(
        (envelope.topic, envelope.seq, envelope.boot_ts_us),
        (TopicId::SUBSCRIBE, 1, SESSION_BOOT.to_peer()),
        "the first subscribe is number 1, and the first clock reading is the boot stamp"
    );
    assert_eq!(
        watching.desired_epoch,
        ControllerEpoch::ZERO,
        "the subscribe at start asserts nothing, so a tab can watch without taking control"
    );
    assert_eq!(
        watching.topics.topics(),
        [
            TopicId::BOOKS,
            TopicId::EVENTS,
            TopicId::CATALOG_INSTRUMENTS,
            TopicId::CATALOG_FEATURES,
            TopicId::LIFECYCLE,
            TopicId::LATENCY,
            TopicId::HISTORY
        ],
        "the tab names the topics it reads"
    );
    assert_eq!(
        session.status(SESSION_BOOT).control,
        ControlVerdict::NoOpinion
    );
    let just_short = SESSION_BOOT + DurationUs::from_micros(SUBSCRIBE_INTERVAL.micros() - 1);
    assert!(
        session.subscribe_due(just_short).is_none(),
        "one subscribe in each 2 s"
    );
    let refresh = SESSION_BOOT + SUBSCRIBE_INTERVAL;
    let (envelope, _) = send_due(&mut session, &mut tab, refresh).await;
    assert_eq!(
        envelope.seq, 2,
        "each subscribe numbers itself above the last"
    );

    let start_at = refresh + DurationUs::from_millis(500);
    assert!(session.subscribe_due(start_at).is_none());
    session.assert_run_state(RunState::Running);
    let (_, start) = send_due(&mut session, &mut tab, start_at).await;
    assert_eq!(
        (start.desired_state, start.desired_epoch.get()),
        (RunState::Running, 5)
    );
    let start_just_short = start_at + DurationUs::from_micros(SUBSCRIBE_INTERVAL.micros() - 1);
    assert!(
        session.subscribe_due(start_just_short).is_none(),
        "the 2 s interval starts again at the subscribe of the assert"
    );
    assert!(
        session
            .subscribe_due(start_at + SUBSCRIBE_INTERVAL)
            .is_some()
    );
    let marker = engine.marker_of(&mut fold, (RunState::Running, 5)).await;
    fold.engine.dispatch(&marker);
    assert_eq!(
        read_until(&mut session, &mut tab, start_at, ControlVerdict::Applied).await,
        ControlVerdict::Applied,
        "the lifecycle frame acknowledges epoch 5"
    );

    let stop_at = start_at + DurationUs::from_millis(500);
    session.assert_run_state(RunState::Idle);
    assert_eq!(session.status(stop_at).control, ControlVerdict::Pending);
    let (_, stop) = send_due(&mut session, &mut tab, stop_at).await;
    assert_eq!(
        (stop.desired_state, stop.desired_epoch.get()),
        (RunState::Idle, 6)
    );
    let marker = engine.marker_of(&mut fold, (RunState::Idle, 6)).await;
    let deadline = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < deadline {
        if let Some(Ok(Message::Binary(bytes))) =
            tokio::time::timeout(Duration::from_millis(100), tab.next())
                .await
                .ok()
                .flatten()
        {
            session.on_datagram(&bytes, stop_at);
        }
    }
    assert_eq!(
        session.status(stop_at).control,
        ControlVerdict::Pending,
        "before the fold applies epoch 6, the engine acknowledges an older epoch"
    );
    drain_persist(&mut fold);
    fold.engine.dispatch(&marker);
    let records = drain_persist(&mut fold);
    assert!(
        records
            .iter()
            .any(|record| matches!(record, PersistRecord::SealAll)),
        "the park seals the tape: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(record,
            PersistRecord::LinkFrame(row) if row.kind == LinkRowKind::RunIdle && row.seq == 6)),
        "the tape records the marker it consumed"
    );
    read_until(&mut session, &mut tab, stop_at, ControlVerdict::Applied).await;
    let status = session.status(stop_at);
    assert_eq!(
        status.reported_state,
        Some(RunState::Idle),
        "the tab reads a parked engine"
    );

    let mut second_tab = open_tab(port, 0).await;
    let mut second = session_of_tab(&mut second_tab, LocalTs::from_micros(1)).await;
    second.assert_run_state(RunState::Running);
    let (_, lower) = send_due(&mut second, &mut second_tab, LocalTs::from_micros(1)).await;
    assert_eq!(lower.desired_epoch.get(), 2);
    assert_eq!(
        read_until(
            &mut second,
            &mut second_tab,
            LocalTs::from_micros(1),
            ControlVerdict::Lost { holder_epoch: 6 },
        )
        .await,
        ControlVerdict::Lost { holder_epoch: 6 },
        "a lower epoch from a second tab loses"
    );
    assert!(
        engine
            .markers_left()
            .iter()
            .all(|desired| *desired == (RunState::Idle, 6)),
        "the lower epoch reaches no marker, and the fold stays parked"
    );

    let after_stop = stop_at + DurationUs::from_millis(500);
    assert!(
        session.attach_to_next_peer(),
        "the relay names a second engine"
    );
    let status = session.status(after_stop);
    assert_eq!(
        status.control,
        ControlVerdict::NoOpinion,
        "an attach releases the assertion"
    );
    let released = decoded(
        session
            .subscribe_due(after_stop)
            .expect("due at once on attach"),
    );
    assert_eq!(released.desired_epoch, ControllerEpoch::ZERO);
    session.assert_run_state(RunState::Running);
    let held = decoded(
        session
            .subscribe_due(after_stop)
            .expect("due at once after an assert"),
    );
    assert_eq!(
        held.desired_epoch.get(),
        7,
        "an attach never rewinds the epoch"
    );
    let restart_at = LocalTs::from_micros(60_000_000);
    let mut restarted = LinkSession::new(
        LinkTarget {
            strategy: StrategyId::new(STRATEGY).expect("a valid strategy id"),
            peers: vec![engine.address],
            token_hash: LinkCredential::new(Some(TOKEN), STRATEGY).token_hash(),
            sender_nonce: 7,
        },
        restart_at,
    )
    .expect("one peer");
    restarted.assert_run_state(RunState::Idle);
    let outranking = decoded(restarted.subscribe_due(restart_at).expect("due at start"));
    assert!(
        outranking.desired_epoch > held.desired_epoch,
        "a restarted workstation outranks its own previous run"
    );
}
