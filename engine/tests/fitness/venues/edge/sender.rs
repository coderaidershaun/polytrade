//! The sender between the command lane and a venue: each command leaves once and in lane order,
//! stamped at its pop and at the wire, and each placement waits in its own way between wakes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, QuoteLevel};
use engine::hot::sink::{ExecSink, SampleSink};
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{ExecCommand, ExecKind, ExecLaneItem, OrderStyle, StampedExecCommand};
use engine::msg::ids::{ClientOrderId, Price, QueueId, Side};
use engine::msg::inbound::SpinTick;
use engine::msg::latency::{Category, Stage, WireSample};
use engine::msg::time::{DurationUs, LocalTs};
use engine::runtime::config::SenderPlacement;
use engine::runtime::metrics::{Metrics, SampleProducers};
use engine::venues::edge::sender::{
    CommandTransport, Handover, SendWait, Sender, SenderSetup, TickSource,
};
use engine::venues::exec::{
    ExecCore, ExecEffect, ExecRequest, ExecSession, InFlightRequest, Outgoing, PlaceNotSentReason,
    RequestId, SessionOutcome, SessionSetup,
};
use futures_util::FutureExt;
use rtrb::{Consumer, Producer, RingBuffer};

use crate::engine_support::detached_setup;
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, SentEvents, exec_settings, make_ready, reseat_book,
    row_with_ceiling, spin_at,
};
use crate::runtime::metrics::{Counter, Harness, Rows};

type Session = ExecSession<SentEvents>;

const NOW: LocalTs = LocalTs::from_micros(1_785_000_000_000_000);
const INTERVAL_US: i64 = 60_000_000;
const LANE_CAPACITY: usize = 8;
const EXEC_QUEUE: QueueId = match QueueId::new(3) {
    Ok(queue_id) => queue_id,
    Err(_) => panic!("queue 3 is below the cap"),
};

const ISSUE: u64 = 100;
const POP: u64 = 150;
const WIRE: u64 = 400;

/// Earlier than [`ISSUE`], so a wire sample of B would be the largest decision-to-wire time.
const B_ISSUE: u64 = 50;

/// Declares one bid at the touch, so a spin banks exactly one placement.
struct OneBid;

impl Strategy for OneBid {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let Some(bid) = ctx.book(INSTRUMENT).best_bid() else {
            return;
        };
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: bid.price,
                qty: QUOTE_QTY,
            }),
        );
    }
}

/// A fold whose command bank reads `counter`: the bank stamps the one placement of the spin at
/// [`ISSUE`], and the drain of that spin puts the command and then the watermark on the lane.
fn lane_after_one_banked_placement(counter: &Counter) -> Consumer<ExecLaneItem> {
    let instruments = [row_with_ceiling(CEILING)];
    let (commands, lane) = RingBuffer::<ExecLaneItem>::new(LANE_CAPACITY);
    let mut engine = HotEngine::new(HotEngineSetup {
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands, counter.stamper.clone()),
            settings: exec_settings(),
            run_nonce: 0,
        }],
        ..detached_setup(&instruments, Box::new(OneBid))
    });
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, ASK, 1) {
        engine.dispatch(&message);
    }
    counter.at(ISSUE);
    spin_at(&mut engine, 1, 10);
    lane
}

/// A session that quotes: connected, its subscribe in flight, its pass finished, readiness armed.
fn quoting_session() -> Session {
    let mut session = ExecSession::new(SessionSetup {
        venue: "test-venue",
        core: ExecCore::with_limits(8, 32),
        instruments: vec![INSTRUMENT],
        events: SentEvents::default(),
        inflight_timeout: DurationUs::from_millis(5_000),
    });
    for Outgoing { effect, recon_seq } in session.connect() {
        if let ExecEffect::Send {
            request_id,
            request,
        } = effect
        {
            session.inflight.record(InFlightRequest {
                request_id,
                request,
                sent_at: NOW,
                recon_seq,
            });
        }
    }
    let resync_seq = session.begin_pass(1);
    assert!(session.resync.on_read(resync_seq));
    session.finish_pass();
    assert!(session.arm_readiness(NOW));
    session.events.clear();
    session
}

fn decide(session: &mut Session, command: ExecCommand) -> Vec<Outgoing> {
    let mut outgoing = Vec::new();
    session.core.on_command(
        command,
        &mut Outgoing::sink(&mut outgoing, command.recon_seq()),
    );
    outgoing
}

fn move_counter(mock: &quanta::Mock, value: u64) {
    let current = mock.value();
    if value >= current {
        mock.increment(value - current);
    } else {
        mock.decrement(current - value);
    }
}

/// What a test reads back from a venue stand-in.
trait Venue: CommandTransport {
    fn wire(&self) -> &[ExecRequest];
    fn session(&self) -> &Session;
}

/// The shape of a venue that encodes and feeds each request at the flush and puts a wake's
/// requests on the wire with that one flush. With its socket down it refuses a placement and still
/// sends a cancel. A request of `unencodable` fails to encode at the flush and is answered as not
/// sent there.
struct SocketVenue {
    session: Session,
    mock: Arc<quanta::Mock>,
    is_socket_up: bool,
    unencodable: Option<ClientOrderId>,
    fed: Vec<Outgoing>,
    /// The requests of each command of this wake that was handed over as fed.
    fed_requests: Vec<Vec<RequestId>>,
    wire: Vec<ExecRequest>,
}

impl SocketVenue {
    fn new(mock: Arc<quanta::Mock>, is_socket_up: bool) -> SocketVenue {
        SocketVenue {
            session: quoting_session(),
            mock,
            is_socket_up,
            unencodable: None,
            fed: Vec::new(),
            fed_requests: Vec::new(),
            wire: Vec::new(),
        }
    }

    fn failing_to_encode(mock: Arc<quanta::Mock>, client: ClientOrderId) -> SocketVenue {
        SocketVenue {
            unencodable: Some(client),
            ..SocketVenue::new(mock, true)
        }
    }

    fn is_unencodable(&self, request: &ExecRequest) -> bool {
        let client = match *request {
            ExecRequest::Place { client_id, .. }
            | ExecRequest::Cancel { client_id, .. }
            | ExecRequest::AmendQty { client_id, .. }
            | ExecRequest::OrderStatus { client_id, .. } => Some(client_id),
            ExecRequest::OpenOrders { .. } | ExecRequest::SubscribeUserStream => None,
        };
        client.is_some() && client == self.unencodable
    }
}

impl CommandTransport for SocketVenue {
    async fn hand_over(&mut self, command: ExecCommand) -> Result<Handover, SessionOutcome> {
        if self.fed.is_empty() {
            self.fed_requests.clear();
        }
        let mut requests = Vec::new();
        for Outgoing { effect, recon_seq } in decide(&mut self.session, command) {
            let ExecEffect::Send {
                request_id,
                request,
            } = effect
            else {
                self.session
                    .on_unsent(&effect, NOW)
                    .expect("the session raises no fault here");
                continue;
            };
            if !self.is_socket_up && matches!(request, ExecRequest::Place { .. }) {
                self.session
                    .refuse(request, PlaceNotSentReason::NoTransport, NOW)
                    .expect("the session raises no fault here");
                continue;
            }
            self.fed.push(Outgoing {
                effect: ExecEffect::Send {
                    request_id,
                    request,
                },
                recon_seq,
            });
            requests.push(request_id);
        }
        if requests.is_empty() {
            return Ok(Handover::NotSent);
        }
        self.fed_requests.push(requests);
        Ok(Handover::Fed)
    }

    async fn flush(&mut self) -> Result<(), SessionOutcome> {
        if self.fed.is_empty() {
            return Ok(());
        }
        for Outgoing { effect, recon_seq } in std::mem::take(&mut self.fed) {
            let ExecEffect::Send {
                request_id,
                request,
            } = effect
            else {
                continue;
            };
            if self.is_unencodable(&request) {
                self.session
                    .refuse(request, PlaceNotSentReason::Encoding, NOW)
                    .expect("the session raises no fault here");
                continue;
            }
            self.session.inflight.record(InFlightRequest {
                request_id,
                request,
                sent_at: NOW,
                recon_seq,
            });
            self.wire.push(request);
        }
        move_counter(&self.mock, WIRE);
        Ok(())
    }

    fn has_reached_wire(&self, fed: usize) -> bool {
        self.fed_requests.get(fed).is_some_and(|requests| {
            requests
                .iter()
                .any(|&request| self.session.inflight.holds(request))
        })
    }
}

impl Venue for SocketVenue {
    fn wire(&self) -> &[ExecRequest] {
        &self.wire
    }

    fn session(&self) -> &Session {
        &self.session
    }
}

/// The shape of a venue that hands each request to a bounded lane of its own. A full lane refuses.
struct LaneVenue {
    session: Session,
    mock: Arc<quanta::Mock>,
    room: usize,
    lane: Vec<ExecRequest>,
}

impl LaneVenue {
    fn new(mock: Arc<quanta::Mock>, room: usize) -> LaneVenue {
        LaneVenue {
            session: quoting_session(),
            mock,
            room,
            lane: Vec::new(),
        }
    }
}

impl CommandTransport for LaneVenue {
    async fn hand_over(&mut self, command: ExecCommand) -> Result<Handover, SessionOutcome> {
        let mut handover = Handover::NotSent;
        for Outgoing { effect, recon_seq } in decide(&mut self.session, command) {
            let ExecEffect::Send {
                request_id,
                request,
            } = effect
            else {
                self.session
                    .on_unsent(&effect, NOW)
                    .expect("the session raises no fault here");
                continue;
            };
            if self.lane.len() == self.room {
                self.session
                    .refuse(request, PlaceNotSentReason::NoTransport, NOW)
                    .expect("the session raises no fault here");
                continue;
            }
            self.session.inflight.record(InFlightRequest {
                request_id,
                request,
                sent_at: NOW,
                recon_seq,
            });
            self.lane.push(request);
            move_counter(&self.mock, WIRE);
            handover = Handover::Taken;
        }
        Ok(handover)
    }

    async fn flush(&mut self) -> Result<(), SessionOutcome> {
        Ok(())
    }

    fn has_reached_wire(&self, _fed: usize) -> bool {
        false
    }
}

impl Venue for LaneVenue {
    fn wire(&self) -> &[ExecRequest] {
        &self.lane
    }

    fn session(&self) -> &Session {
        &self.session
    }
}

/// Fires only when the test sends it a tick, and remembers whether anything waited on it.
struct TestTicks {
    fired: tokio::sync::mpsc::UnboundedReceiver<()>,
    is_awaited: Arc<AtomicBool>,
}

impl TickSource for TestTicks {
    async fn tick(&mut self) {
        self.is_awaited.store(true, Ordering::Relaxed);
        self.fired.recv().await;
    }
}

struct Ticks {
    fire: tokio::sync::mpsc::UnboundedSender<()>,
    is_awaited: Arc<AtomicBool>,
}

impl Ticks {
    fn fire(&self) {
        self.fire
            .send(())
            .expect("the sender holds the tick source");
    }

    fn is_awaited(&self) -> bool {
        self.is_awaited.load(Ordering::Relaxed)
    }
}

fn built_sender(
    commands: Consumer<ExecLaneItem>,
    counter: &Counter,
    wires: SampleSink<WireSample>,
    placement: SenderPlacement,
) -> (Sender<TestTicks>, Ticks) {
    let (fire, fired) = tokio::sync::mpsc::unbounded_channel();
    let is_awaited = Arc::new(AtomicBool::new(false));
    let ticks = TestTicks {
        fired,
        is_awaited: Arc::clone(&is_awaited),
    };
    let sender = Sender::new(SenderSetup {
        commands,
        stamper: counter.stamper.clone(),
        wires,
        queue: EXEC_QUEUE,
        wait: SendWait::new(placement, ticks),
    });
    (sender, Ticks { fire, is_awaited })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a test runtime")
}

/// One wake of the loop: the wait of its placement, then the drain of the lane.
async fn pass<T: CommandTransport>(sender: &mut Sender<TestTicks>, venue: &mut T) {
    sender.wait().await;
    assert_eq!(sender.drain(venue).await, None, "the venue stays connected");
}

fn stage_of(rows: &Rows, stage: Stage) -> Option<(u64, u64)> {
    rows.stage(Category::Command, stage)
        .map(|row| (row.count, row.max_ns))
}

fn lane_item(counter: &Counter, command: ExecCommand) -> ExecLaneItem {
    lane_item_at(counter, command, ISSUE)
}

fn lane_item_at(counter: &Counter, command: ExecCommand, issue: u64) -> ExecLaneItem {
    ExecLaneItem::Command(StampedExecCommand {
        command,
        issued_ts_us: LocalTs::from_micros(10),
        issue_tick: counter.at(issue),
    })
}

fn place(client: u64) -> ExecCommand {
    ExecCommand::Place {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client),
        side: Side::Buy,
        price: Price::from_mantissa(BID),
        qty: QUOTE_QTY,
        style: OrderStyle::PostOnly,
    }
}

fn place_a() -> ExecCommand {
    place(0xA)
}

fn cancel_b() -> ExecCommand {
    ExecCommand::Cancel {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(0xB),
    }
}

/// A lane that holds `Place` A, `Cancel` B and a watermark, in that order.
fn lane_of_a_b_watermark(counter: &Counter) -> (Producer<ExecLaneItem>, Consumer<ExecLaneItem>) {
    let (mut producer, consumer) = RingBuffer::new(LANE_CAPACITY);
    for item in [
        lane_item(counter, place_a()),
        lane_item(counter, cancel_b()),
        ExecLaneItem::Watermark(LocalTs::from_micros(10)),
    ] {
        producer.push(item).expect("room on the lane");
    }
    (producer, consumer)
}

/// A lane that holds `Place` A, `Place` B and `Place` C, with B banked at [`B_ISSUE`].
fn lane_of_places_a_b_c(counter: &Counter) -> (Producer<ExecLaneItem>, Consumer<ExecLaneItem>) {
    let (mut producer, consumer) = RingBuffer::new(LANE_CAPACITY);
    for item in [
        lane_item(counter, place(0xA)),
        lane_item_at(counter, place(0xB), B_ISSUE),
        lane_item(counter, place(0xC)),
    ] {
        producer.push(item).expect("room on the lane");
    }
    (producer, consumer)
}

fn is_place(request: &ExecRequest, client: u64) -> bool {
    matches!(request, ExecRequest::Place { client_id, .. } if client_id.0 == client)
}

fn is_cancel(request: &ExecRequest, client: u64) -> bool {
    matches!(request, ExecRequest::Cancel { client_id, .. } if client_id.0 == client)
}

fn answered_not_sent(session: &Session, client: u64) -> bool {
    session
        .events
        .iter()
        .any(|event| event.kind == ExecKind::PlaceNotSent && event.client_id.0 == client)
}

/// Runs one banked placement through the sender to `venue`, then reads the stages of its sample.
fn one_placement_through<V: Venue>(make_venue: impl FnOnce(Arc<quanta::Mock>) -> V) {
    let Harness {
        counter,
        mut metrics,
        samples,
        mut rows,
        ..
    } = Harness::new(10);
    let SampleProducers { wires, .. } = samples;
    let lane = lane_after_one_banked_placement(&counter);
    let mut venue = make_venue(Arc::clone(&counter.mock));
    let (mut sender, ticks) = built_sender(lane, &counter, wires, SenderPlacement::Shared);

    counter.at(POP);
    ticks.fire();
    runtime().block_on(pass(&mut sender, &mut venue));

    assert_eq!(venue.wire().len(), 1, "one request: {:?}", venue.wire());
    assert!(matches!(venue.wire()[0], ExecRequest::Place { .. }));
    pass_interval(&mut metrics, &mut rows);
    assert_eq!(
        stage_of(&rows, Stage::BankAndLaneWait),
        Some((1, POP - ISSUE))
    );
    assert_eq!(stage_of(&rows, Stage::Send), Some((1, WIRE - POP)));
    assert_eq!(
        stage_of(&rows, Stage::DecisionToWire),
        Some((1, WIRE - ISSUE))
    );
}

fn pass_interval(metrics: &mut Metrics, rows: &mut Rows) {
    metrics.pass(LocalTs::from_micros(INTERVAL_US), rows);
}

#[test]
fn command_is_stamped_at_bank_and_sent_on_arrival() {
    one_placement_through(|mock| SocketVenue::new(mock, true));
    one_placement_through(|mock| LaneVenue::new(mock, LANE_CAPACITY));
}

/// Runs the lane of A, B and a watermark through one wake to `venue`, and returns the venue and
/// how many wire samples the wake wrote.
fn a_b_watermark_through<V: Venue>(make_venue: impl FnOnce(Arc<quanta::Mock>) -> V) -> (V, u64) {
    let (venue, decision_to_wire) = wakes_through(lane_of_a_b_watermark, make_venue);
    (venue, decision_to_wire.map_or(0, |(count, _)| count))
}

/// Runs `lane` through one wake to `venue`, then a second wake with the lane empty, and returns the
/// venue and the count and largest value of the decision-to-wire stage.
fn wakes_through<V: Venue>(
    lane: fn(&Counter) -> (Producer<ExecLaneItem>, Consumer<ExecLaneItem>),
    make_venue: impl FnOnce(Arc<quanta::Mock>) -> V,
) -> (V, Option<(u64, u64)>) {
    let Harness {
        counter,
        mut metrics,
        samples,
        mut rows,
        ..
    } = Harness::new(10);
    let SampleProducers { wires, .. } = samples;
    let (producer, lane) = lane(&counter);
    let mut venue = make_venue(Arc::clone(&counter.mock));
    let (mut sender, ticks) = built_sender(lane, &counter, wires, SenderPlacement::Shared);

    counter.at(POP);
    ticks.fire();
    runtime().block_on(pass(&mut sender, &mut venue));
    assert_eq!(
        producer.slots(),
        LANE_CAPACITY,
        "one wake takes each item off the lane, the watermark too"
    );
    ticks.fire();
    runtime().block_on(pass(&mut sender, &mut venue));

    pass_interval(&mut metrics, &mut rows);
    (venue, stage_of(&rows, Stage::DecisionToWire))
}

#[test]
fn each_lane_command_reaches_the_wire_one_time_in_lane_order() {
    let (socket, samples) = a_b_watermark_through(|mock| SocketVenue::new(mock, true));
    assert_eq!(socket.wire().len(), 2, "{:?}", socket.wire());
    assert!(
        is_place(&socket.wire()[0], 0xA),
        "A first: {:?}",
        socket.wire()
    );
    assert!(
        is_cancel(&socket.wire()[1], 0xB),
        "then B: {:?}",
        socket.wire()
    );
    assert_eq!(samples, 2, "one wire sample for each command on the wire");

    let (lane, samples) = a_b_watermark_through(|mock| LaneVenue::new(mock, LANE_CAPACITY));
    assert_eq!(lane.wire().len(), 2, "{:?}", lane.wire());
    assert!(is_place(&lane.wire()[0], 0xA), "A first: {:?}", lane.wire());
    assert!(is_cancel(&lane.wire()[1], 0xB), "then B: {:?}", lane.wire());
    assert_eq!(samples, 2);

    let (socket_down, samples) = a_b_watermark_through(|mock| SocketVenue::new(mock, false));
    assert!(
        answered_not_sent(socket_down.session(), 0xA),
        "a placement with the socket down is answered as not sent: {:?}",
        socket_down.session().events
    );
    assert_eq!(socket_down.wire().len(), 1, "{:?}", socket_down.wire());
    assert!(is_cancel(&socket_down.wire()[0], 0xB));
    assert_eq!(samples, 1, "a refused command has no wire time");

    let (full_lane, samples) = a_b_watermark_through(|mock| LaneVenue::new(mock, 0));
    assert!(
        answered_not_sent(full_lane.session(), 0xA),
        "a placement that meets a full lane is answered as not sent: {:?}",
        full_lane.session().events
    );
    assert!(full_lane.wire().is_empty(), "{:?}", full_lane.wire());
    assert_eq!(samples, 0, "a refused command has no wire time");

    let (failed_encoding, decision_to_wire) = wakes_through(lane_of_places_a_b_c, |mock| {
        SocketVenue::failing_to_encode(mock, ClientOrderId(0xB))
    });
    let wire = failed_encoding.wire();
    assert_eq!(wire.len(), 2, "{wire:?}");
    assert!(is_place(&wire[0], 0xA), "A first: {wire:?}");
    assert!(is_place(&wire[1], 0xC), "then C: {wire:?}");
    assert!(
        answered_not_sent(failed_encoding.session(), 0xB),
        "a placement that fails to encode at the flush is answered as not sent: {:?}",
        failed_encoding.session().events
    );
    assert_eq!(
        decision_to_wire,
        Some((2, WIRE - ISSUE)),
        "one wire sample for A and one for C, and none for B, whose request never reached the wire"
    );
}

#[test]
fn pinned_wait_form_sends_without_a_tick() {
    let Harness {
        counter,
        mut metrics,
        samples,
        mut rows,
        ..
    } = Harness::new(10);
    let SampleProducers { wires, .. } = samples;
    let (mut producer, lane) = RingBuffer::new(LANE_CAPACITY);
    producer
        .push(lane_item(&counter, place_a()))
        .expect("room on the lane");
    let mut venue = LaneVenue::new(Arc::clone(&counter.mock), LANE_CAPACITY);
    let (mut sender, ticks) = built_sender(lane, &counter, wires, SenderPlacement::Pinned);

    counter.at(POP);
    runtime().block_on(pass(&mut sender, &mut venue));
    assert_eq!(venue.wire().len(), 1, "the command left on the first pass");
    assert!(
        !ticks.is_awaited(),
        "the pinned wait never waited on the tick source"
    );
    pass_interval(&mut metrics, &mut rows);
    assert_eq!(
        stage_of(&rows, Stage::BankAndLaneWait),
        Some((1, POP - ISSUE))
    );
    assert_eq!(stage_of(&rows, Stage::Send), Some((1, WIRE - POP)));
    assert_eq!(
        stage_of(&rows, Stage::DecisionToWire),
        Some((1, WIRE - ISSUE))
    );

    let SampleProducers { wires, .. } = Harness::new(10).samples;
    let (mut producer, lane) = RingBuffer::new(LANE_CAPACITY);
    producer
        .push(lane_item(&counter, place_a()))
        .expect("room on the lane");
    let mut venue = LaneVenue::new(Arc::clone(&counter.mock), LANE_CAPACITY);
    let (mut sender, ticks) = built_sender(lane, &counter, wires, SenderPlacement::Shared);
    let runtime = runtime();
    let before_the_tick = runtime.block_on(async { pass(&mut sender, &mut venue).now_or_never() });
    assert!(
        before_the_tick.is_none(),
        "the shared wait holds for its tick"
    );
    assert!(
        venue.wire().is_empty(),
        "nothing is sent before the tick fires"
    );
    ticks.fire();
    runtime.block_on(pass(&mut sender, &mut venue));
    assert_eq!(
        venue.wire().len(),
        1,
        "the tick wakes the loop and the command leaves"
    );
}
