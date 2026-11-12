//! Mode `sender`: commands enter the lane at a fixed rate, and the engine's sender loop takes them
//! to a loopback socket that stands in for a venue. The metrics core times each command from the
//! moment it entered the lane to the moment the socket took its bytes, with the placement given.

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rtrb::RingBuffer;

use engine::msg::exec::{ExecCommand, ExecLaneItem, OrderStyle, StampedExecCommand};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, QueueId, Side};
use engine::msg::latency::{Category, Stage};
use engine::msg::time::DurationUs;
use engine::runtime::config::SenderPlacement;
use engine::venues::edge::sender::{CommandTransport, Handover, SendWait, Sender, SenderSetup};
use engine::venues::exec::SessionOutcome;

use crate::clock;
use crate::rig::{self, MetricsRun};
use crate::tally;

const LANE: usize = 4_096;

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
}

impl SenderRun {
    /// # Errors
    /// When a pinned sender cannot pin on Linux.
    pub fn run(&self) -> anyhow::Result<()> {
        let clock = clock::start_engine_clock();
        let (metrics, samples, stamper) = MetricsRun::start(&clock);
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
        let sending = Arc::clone(&is_done);
        let placement = self.placement;
        let core = self.core;
        let sender = std::thread::spawn(move || -> anyhow::Result<()> {
            if placement == SenderPlacement::Pinned {
                rig::pin(core)?;
            }
            let runtime = match placement {
                SenderPlacement::Shared => tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_time()
                    .build(),
                SenderPlacement::Pinned => tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build(),
            }
            .expect("the harness can build a tokio runtime");
            runtime.block_on(async {
                let setup = SenderSetup {
                    commands,
                    stamper: sender_stamper,
                    wires,
                    queue: QueueId(0),
                    wait: SendWait::new(placement, tokio::time::interval(tick)),
                };
                drive_sender(setup, wire, &sending).await;
            });
            Ok(())
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
        let sent = sender
            .join()
            .expect("the sender thread of the harness does not panic");
        drain_socket
            .join()
            .expect("the socket reader of the harness does not panic");
        sent?;
        let rows = metrics.finish();
        let label = match self.placement {
            SenderPlacement::Shared => "sender --placement shared",
            SenderPlacement::Pinned => "sender --placement pinned",
        };
        tally::print_rows(
            label,
            &rows,
            Category::Command,
            &[Stage::DecisionToWire, Stage::BankAndLaneWait, Stage::Send],
        );
        println!(
            "development {label}: loopback sends refused={}",
            refused.load(Ordering::Relaxed)
        );
        Ok(())
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
