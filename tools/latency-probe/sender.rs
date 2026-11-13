//! Mode `sender`: commands enter the lane at a fixed rate, and the engine's sender loop takes them
//! to a loopback socket in place of a venue. The metrics core times each one from lane to socket.

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use rtrb::RingBuffer;
use tokio::runtime::Handle;

use engine::msg::exec::{ExecCommand, ExecLaneItem, OrderStyle, StampedExecCommand};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, QueueId, Side};
use engine::msg::time::DurationUs;
use engine::runtime::PinnedSenders;
use engine::runtime::config::SenderPlacement;
use engine::venues::edge::sender::{CommandTransport, Handover, SendWait, Sender, SenderSetup};
use engine::venues::exec::SessionOutcome;

use crate::clock;
use crate::rig::MetricsRun;

const LANE: usize = 4_096;

/// The workers of a small engine's shared runtime.
const SHARED_WORKERS: usize = 2;

/// The size of one order request on the wire, about what a venue takes.
const REQUEST_BYTES: usize = 256;

/// Long enough that the reader of the loopback socket sees its stop flag soon after the run ends.
const READ_TIMEOUT: Duration = Duration::from_millis(50);

pub struct SenderRun {
    pub placement: SenderPlacement,
    pub commands: usize,
    pub rate_hz: u64,
    pub tick_us: u64,
    pub core: Option<usize>,
}

/// A venue that takes the bytes of each command into a loopback socket at once.
struct LoopbackVenue {
    socket: UdpSocket,
    request: [u8; REQUEST_BYTES],
    refused: Arc<AtomicU64>,
}

impl CommandTransport for LoopbackVenue {
    async fn hand_over(&mut self, _command: ExecCommand) -> Result<Handover, SessionOutcome> {
        if self.socket.send(&self.request).is_err() {
            self.refused.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Handover::Taken)
    }

    async fn flush(&mut self) -> Result<(), SessionOutcome> {
        Ok(())
    }

    fn has_reached_wire(&self, _fed: usize) -> bool {
        false
    }
}

impl SenderRun {
    /// # Errors
    /// When the log, the metrics thread or the sender's runtime cannot start, a pinned sender
    /// among them on Linux.
    pub fn run(&self) -> anyhow::Result<()> {
        let clock = clock::start_engine_clock();
        let (metrics, samples, stamper) = MetricsRun::start(&clock, "latency-probe-sender")?;
        let (mut lane, commands) = RingBuffer::new(LANE);
        let (venue, reader) = loopback();
        let refused = Arc::new(AtomicU64::new(0));
        let is_done = Arc::new(AtomicBool::new(false));

        let reading = Arc::clone(&is_done);
        let drain_socket = std::thread::spawn(move || {
            let mut buffer = [0u8; REQUEST_BYTES];
            while !reading.load(Ordering::Acquire) {
                // The reader only keeps the socket's buffer from filling, so a timeout just ends
                // one wait.
                reader.recv(&mut buffer).ok();
            }
        });

        let wires = samples.wires;
        let sender_stamper = stamper.clone();
        let tick = Duration::from_micros(self.tick_us);
        let wire = LoopbackVenue {
            socket: venue,
            request: [0x5a; REQUEST_BYTES],
            refused: Arc::clone(&refused),
        };
        let home = SenderHome::start(self.placement, self.core)?;
        let placement = self.placement;
        let sending = Arc::clone(&is_done);
        let (finished, has_finished) = std::sync::mpsc::sync_channel(1);
        home.handle().spawn(async move {
            let setup = SenderSetup {
                commands,
                stamper: sender_stamper,
                wires,
                queue: QueueId(0),
                wait: SendWait::new(placement, tokio::time::interval(tick)),
            };
            drive_sender(setup, wire, &sending).await;
            // The harness waits for this before it stops the runtime, so the send cannot fail.
            finished.send(()).ok();
        });

        let interval = DurationUs::from_micros((1_000_000 / self.rate_hz.max(1)).max(1) as i64);
        let mut due = clock::now(&clock);
        for index in 0..self.commands {
            clock::spin_until(&clock, due);
            let mut item = ExecLaneItem::Command(StampedExecCommand {
                command: place(index),
                issued_ts_us: clock::now(&clock),
                issue_tick: clock::stamp(&stamper),
            });
            while let Err(rtrb::PushError::Full(back)) = lane.push(item) {
                if lane.is_abandoned() {
                    break;
                }
                item = back;
                std::hint::spin_loop();
            }
            due = due + interval;
        }
        while !lane.is_abandoned() && lane.slots() < LANE {
            std::hint::spin_loop();
        }
        is_done.store(true, Ordering::Release);
        has_finished
            .recv()
            .context("the sender task of the harness ended before it finished")?;
        drop(home);
        drain_socket
            .join()
            .expect("the socket reader of the harness does not panic");
        let label = match self.placement {
            SenderPlacement::Shared => "sender --placement shared",
            SenderPlacement::Pinned => "sender --placement pinned",
        };
        metrics.finish(label)?;
        println!(
            "development {label}: loopback sends refused={}",
            refused.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

/// Where the sender loop runs: the shared multi-thread runtime of an engine, or the engine's own
/// pinned sender thread.
enum SenderHome {
    Shared(tokio::runtime::Runtime),
    Pinned(PinnedSenders),
}

impl SenderHome {
    /// # Errors
    /// When the runtime cannot be built, or when the pinned thread cannot pin on Linux.
    fn start(placement: SenderPlacement, core: Option<usize>) -> anyhow::Result<SenderHome> {
        match placement {
            SenderPlacement::Shared => tokio::runtime::Builder::new_multi_thread()
                .worker_threads(SHARED_WORKERS)
                .enable_all()
                .build()
                .map(SenderHome::Shared)
                .context("the harness cannot build a shared runtime"),
            SenderPlacement::Pinned => PinnedSenders::spawn(core)
                .map(SenderHome::Pinned)
                .context("the harness cannot start the pinned sender thread"),
        }
    }

    fn handle(&self) -> &Handle {
        match self {
            SenderHome::Shared(runtime) => runtime.handle(),
            SenderHome::Pinned(senders) => senders.handle(),
        }
    }
}

async fn drive_sender(
    setup: SenderSetup<tokio::time::Interval>,
    mut wire: LoopbackVenue,
    is_done: &AtomicBool,
) {
    let mut sender = Sender::new(setup);
    loop {
        let is_last = is_done.load(Ordering::Acquire);
        if let Some(ended) = sender.drain(&mut wire).await {
            eprintln!("the loopback venue ended the session: {ended:?}");
            return;
        }
        if is_last {
            return;
        }
        sender.wait().await;
    }
}

/// A connected pair of loopback sockets: the venue end that the sender writes, and its reader.
fn loopback() -> (UdpSocket, UdpSocket) {
    let reader = UdpSocket::bind("127.0.0.1:0").expect("the harness can bind a loopback socket");
    reader
        .set_read_timeout(Some(READ_TIMEOUT))
        .expect("a loopback socket takes a read timeout");
    let venue = UdpSocket::bind("127.0.0.1:0").expect("the harness can bind a loopback socket");
    venue
        .connect(reader.local_addr().expect("a bound socket has an address"))
        .expect("a loopback socket connects");
    (venue, reader)
}

fn place(index: usize) -> ExecCommand {
    ExecCommand::Place {
        instrument: InstrumentId(0),
        client_id: ClientOrderId(index as u64 + 1),
        side: if index.is_multiple_of(2) { Side::Buy } else { Side::Sell },
        price: Price(65_000 * FIXED_SCALE),
        qty: Qty(1_000),
        style: OrderStyle::PostOnly,
    }
}
