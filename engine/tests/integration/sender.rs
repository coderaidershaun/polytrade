//! A pinned sender takes a command to the wire as soon as the command lands, with no tick to wait
//! for. It runs in real time on a pinned thread of the host, with a loopback socket for a venue.

use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use engine::hot::sink::SampleSink;
use engine::msg::exec::{ExecCommand, ExecLaneItem, OrderStyle, StampedExecCommand};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, QueueId, Side};
use engine::msg::latency::WireSample;
use engine::msg::time::{
    ClockSource, EngineClock, InvariantCounter, Stamper, TickAnchor, TickScale,
};
use engine::runtime::PinnedSenders;
use engine::runtime::config::{LatencyConfig, SenderPlacement};
use engine::runtime::metrics::{Metrics, MetricsParts, MetricsSetup};
use engine::venues::edge::sender::{CommandTransport, Handover, SendWait, Sender, SenderSetup};
use engine::venues::exec::SessionOutcome;

/// So long that a command that waited for a tick could not arrive while the test runs.
const TICK: Duration = Duration::from_secs(3_600);

/// Far below the tick, and far above the scheduling delay of a pinned thread on a quiet host.
const ARRIVAL_LIMIT: Duration = Duration::from_secs(1);

const CLIENT: ClientOrderId = ClientOrderId(0xA);

/// Sends the client id of each placement to a loopback port, as a venue's request would leave.
struct Loopback {
    socket: UdpSocket,
    venue: SocketAddr,
}

impl CommandTransport for Loopback {
    async fn hand_over(&mut self, command: ExecCommand) -> Result<Handover, SessionOutcome> {
        let ExecCommand::Place { client_id, .. } = command else {
            return Ok(Handover::NotSent);
        };
        self.socket
            .send_to(&client_id.0.to_le_bytes(), self.venue)
            .expect("a loopback send");
        Ok(Handover::Taken)
    }

    async fn flush(&mut self) -> Result<(), SessionOutcome> {
        Ok(())
    }

    fn has_reached_wire(&self, _fed: usize) -> bool {
        false
    }
}

/// The write end of a wire-sample ring, from a metrics core whose scale two real anchors set.
fn wire_samples(stamper: &Stamper) -> SampleSink<WireSample> {
    let clock = EngineClock::start();
    let first = TickAnchor {
        tick: stamper.stamp(),
        at: clock.now(),
    };
    std::thread::sleep(Duration::from_millis(2));
    let last = TickAnchor {
        tick: stamper.stamp(),
        at: clock.now(),
    };
    let MetricsParts { samples, .. } = Metrics::with_rings(MetricsSetup {
        latency: LatencyConfig::default(),
        clock_source: ClockSource::check(InvariantCounter::detect())
            .unwrap_or_else(|refusal| refusal.fallback()),
        queue_count: 1,
        scale: TickScale::between(first, last).expect("the second anchor is later"),
        start_ts_us: clock.now(),
    });
    samples.wires
}

#[test]
#[ignore = "real time on a pinned thread with a loopback socket: a person runs it on a linux host, by the latency runbook"]
fn pinned_sender_sends_without_waiting_for_a_tick() {
    let core = core_affinity::get_core_ids()
        .and_then(|cores| cores.last().copied())
        .map(|core| core.id);
    let senders = PinnedSenders::spawn(core).expect("the pinned senders' thread starts");
    let venue = UdpSocket::bind("127.0.0.1:0").expect("a loopback port");
    venue
        .set_read_timeout(Some(ARRIVAL_LIMIT))
        .expect("a read timeout");
    let mut transport = Loopback {
        socket: UdpSocket::bind("127.0.0.1:0").expect("a loopback port"),
        venue: venue.local_addr().expect("a bound address"),
    };
    let stamper = Stamper::Counter(quanta::Clock::new());
    let wires = wire_samples(&stamper);
    let (mut lane, commands) = rtrb::RingBuffer::new(8);
    let loop_stamper = stamper.clone();
    senders.handle().spawn(async move {
        let mut sender = Sender::new(SenderSetup {
            commands,
            stamper: loop_stamper,
            wires,
            queue: QueueId(0),
            wait: SendWait::new(
                SenderPlacement::Pinned,
                tokio::time::interval_at(tokio::time::Instant::now() + TICK, TICK),
            ),
        });
        loop {
            sender.wait().await;
            if sender.drain(&mut transport).await.is_some() {
                return;
            }
        }
    });

    let pushed_at = Instant::now();
    lane.push(ExecLaneItem::Command(StampedExecCommand {
        command: ExecCommand::Place {
            instrument: InstrumentId(0),
            client_id: CLIENT,
            side: Side::Buy,
            price: Price(1),
            qty: Qty(1),
            style: OrderStyle::PostOnly,
        },
        issued_ts_us: engine::msg::time::TsUs::from_micros(0),
        issue_tick: stamper.stamp(),
    }))
    .expect("room on the lane");
    let mut request = [0_u8; 8];
    let (length, _) = venue
        .recv_from(&mut request)
        .expect("the command reaches the venue long before any tick");
    let waited = pushed_at.elapsed();

    assert_eq!(&request[..length], &CLIENT.0.to_le_bytes());
    assert!(
        waited < ARRIVAL_LIMIT,
        "the pinned sender sent on arrival, after {waited:?}"
    );
}
