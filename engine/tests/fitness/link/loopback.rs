//! A link actor of this build on a loopback socket with no hot thread, for the tests that drive
//! the real actor with datagrams and read what it hands on and what it counts.

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::num::NonZeroUsize;
use std::time::Duration;

use engine::hot::spawn::{ControlQueueProducer, LinkQueueProducer, touched_input_ring};
use engine::link::{
    HistoryRing, LinkActor, LinkActorSetup, LinkCounts, LinkCountsHandle, LinkPayload,
    OutboundLink, TopicId,
};
use engine::msg::ids::QueueId;
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, EngineClock};
use engine::msg::ui::channel::ui_channel;
use engine::msg::ui::{LatencyReport, UiEvent};
use engine::runtime::config::{ControllerLoss, SenderPlacement};
use engine::runtime::shutdown::RunStateHandles;
use rtrb::RingBuffer;

use super::wire_frames::{guard, identity};

/// A link actor of this build on a loopback socket, with no hot thread: what it hands on stays in
/// the input ring that the test holds.
pub(super) struct LoopbackActor {
    _runtime: tokio::runtime::Runtime,
    pub(super) address: SocketAddr,
    inbound: rtrb::Consumer<InboundMessage>,
    /// The end of the event ring that the hot thread would push to.
    pub(super) events: rtrb::Producer<UiEvent>,
    counts: LinkCountsHandle,
}

impl LoopbackActor {
    pub(super) fn start() -> Self {
        let small = NonZeroUsize::new(8).expect("8 is not zero");
        Self::start_with(HistoryRing::new(small, small))
    }

    pub(super) fn start_with(history: HistoryRing) -> Self {
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
        let (producer, inbound) = touched_input_ring(64);
        let (marker_producer, _markers) = touched_input_ring(4);
        let (_wiring, mut channels) = ui_channel();
        let (events, event_ring) = RingBuffer::<UiEvent>::new(64);
        channels.events = event_ring;
        let (_outbound_producer, outbound) = RingBuffer::<OutboundLink>::new(4);
        let (_latency_producer, latency) = RingBuffer::<LatencyReport>::new(4);
        let counts = LinkCountsHandle::new();
        LinkActor::spawn(
            LinkActorSetup {
                socket,
                identity: identity(),
                guard: guard(),
                peers: Vec::new(),
                on_controller_loss: ControllerLoss::default(),
                controllers: Vec::new(),
                inbound: LinkQueueProducer::new(producer, QueueId::new(1).expect("queue 1 exists")),
                markers: ControlQueueProducer::new(
                    marker_producer,
                    QueueId::new(2).expect("queue 2 exists"),
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
                history,
            },
            runtime.handle(),
        );
        Self {
            _runtime: runtime,
            address,
            inbound,
            events,
            counts,
        }
    }

    /// The counts of the actor once `is_reached` holds for them, read at its heartbeat.
    pub(super) fn counts_when(&self, is_reached: impl Fn(&LinkCounts) -> bool) -> LinkCounts {
        for _ in 0..100 {
            let counts = self.counts.load();
            if is_reached(&counts) {
                return counts;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        self.counts.load()
    }

    /// The counts of the actor once they reach `decode_failed` refusals, read at its heartbeat.
    pub(super) fn counts_after(&self, decode_failed: u64) -> LinkCounts {
        self.counts_when(|counts| counts.decode_failed >= decode_failed)
    }

    /// Each peer frame that the actor handed on, once at least `count` of them arrived.
    pub(super) fn peer_frames(&mut self, count: usize) -> Vec<LinkPayload> {
        let mut frames = Vec::new();
        for _ in 0..100 {
            while let Ok(message) = self.inbound.pop() {
                if let MessageBody::Link(frame) = message.body {
                    frames.push(frame.payload);
                }
            }
            if frames.len() >= count {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        frames
    }
}
