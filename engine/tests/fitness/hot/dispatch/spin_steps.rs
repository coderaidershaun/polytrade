//! The steps of a spin run in one fixed order, because each relies on the one before it: the
//! strategy reads sampled trackers, and the execution pass sees the bids of the same spin.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, QuoteLevel};
use engine::hot::ingress::IngressQueues;
use engine::hot::metrics::DriverSamples;
use engine::hot::series::FastQueue;
use engine::hot::sink::ExecSink;
use engine::hot::spawn::{HotDriver, Step};
use engine::hot::strategy::{Registration, Strategy, StrategyCtx};
use engine::msg::exec::{ExecCommand, ExecLaneItem};
use engine::msg::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::time::Stamper;
use engine::msg::ui::UiEvent;
use engine::runtime::config::{RecordedTables, SpinField};
use engine::runtime::metrics::{Metrics, SampleProducers, SinkKind};
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, Producer, RingBuffer};

use crate::engine_support::{
    ONE, account_chunk, delta_chunk, detached_setup, idle_at, instrument_row, persist_ring_for,
    run_control, running_at, snapshot_pair, spin, stream_ready, tracker_spec_all, ts,
    ui_event_ring,
};
use crate::exec_support::exec_settings;
use crate::runtime::metrics::{Harness, Rows};

const MARKET: InstrumentId = InstrumentId(0);

const BID: i64 = 99 * ONE;

/// The one spin of the scenario, and its slot.
const SLOT: u64 = 7;
const SPIN_US: i64 = 7_000;

/// Far more than any step writes.
const LANE: usize = 1_024;

/// Each spin it reads how many trade counts the tracker sampled since the resume, declares one
/// bid at the best bid, and banks one feature and one note line.
struct OneBid {
    call: Option<FeatureId>,
    sampled: Arc<Mutex<Vec<usize>>>,
}

impl Strategy for OneBid {
    fn features(&self) -> &'static [&'static str] {
        &["spin"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        let sampled = ctx
            .tracker(MARKET)
            .spin_sampled(SpinField::TradeCount)
            .map_or(0, FastQueue::len);
        self.sampled
            .lock()
            .expect("the probe's list is never poisoned")
            .push(sampled);
        let Some(bid) = ctx.book(MARKET).best_bid() else {
            return;
        };
        let quote = DesiredQuote {
            price: bid.price,
            qty: Qty::from_mantissa(ONE / 10),
        };
        ctx.quote(MARKET, Side::Buy, QuoteLevel::ZERO, Some(quote));
        ctx.emit(self.call.expect("features registered"), MARKET, 1.0);
        engine::note!(ctx, "declared one bid on spin {}", tick.seq);
    }
}

/// One engine behind the real driver, fed one message at a time, and the read ends of its lanes.
struct Run {
    driver: HotDriver,
    input: Producer<InboundMessage>,
    persist: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    ui_events: Consumer<UiEvent>,
    metrics: Metrics,
    rows: Rows,
    sampled: Arc<Mutex<Vec<usize>>>,
}

/// What the spin of the scenario wrote to each lane, and what the strategy saw.
struct SpinOutputs {
    sampled: Vec<usize>,
    tape: Vec<PersistRecord>,
    commands: Vec<ExecLaneItem>,
    events: Vec<UiEvent>,
}

impl Run {
    fn new(ui_event_capacity: usize) -> Run {
        let Harness {
            counter,
            metrics,
            samples,
            rows,
            ..
        } = Harness::new(10);
        let SampleProducers {
            stages, counters, ..
        } = samples;
        let samples = DriverSamples {
            stamper: counter.stamper.clone(),
            stages,
            counters,
        };
        let sampled = Arc::new(Mutex::new(Vec::new()));
        let rows_of_market = [instrument_row(0, tracker_spec_all(1), 64)];
        // A ring of one slot holds one seal, so a second seal waits until the test reads the first.
        // No table is recorded, so nothing else is ever pushed onto it to carry the seal out early.
        let (persistence, persist) = persist_ring_for(1, RecordedTables::new(&[]));
        let (ui_event_sink, ui_events) = ui_event_ring(ui_event_capacity);
        let (command_producer, commands) = RingBuffer::<ExecLaneItem>::new(LANE);
        let strategy = OneBid {
            call: None,
            sampled: Arc::clone(&sampled),
        };
        let engine = HotEngine::new(HotEngineSetup {
            persistence: Some(persistence),
            ui_event_sink,
            exec: Some(ExecWiring {
                sink: ExecSink::new(command_producer, Stamper::Off),
                settings: exec_settings(),
                run_nonce: 0,
            }),
            ..detached_setup(&rows_of_market, Box::new(strategy))
        });
        let (input, queue) = RingBuffer::new(LANE);
        let driver = HotDriver::new(IngressQueues::new(vec![queue]), engine, Some(samples));
        Run {
            driver,
            input,
            persist,
            commands,
            ui_events,
            metrics,
            rows,
            sampled,
        }
    }

    fn fold(&mut self, message: InboundMessage) {
        self.input
            .push(message)
            .expect("the driver empties the input ring at each step");
        assert_eq!(self.driver.step(), Step::Folded);
    }

    fn take<T>(consumer: &mut Consumer<T>) -> Vec<T> {
        std::iter::from_fn(|| consumer.pop().ok()).collect()
    }

    /// A book, a park whose seal fits, a resume, a park whose seal waits, the read of the first
    /// seal, a resume, a delta that sets the top of the book again, and the three facts that let
    /// an order be sent. Each lane is read empty after each message.
    fn set_up(&mut self) {
        let (bids, asks) = snapshot_pair(0, &[(BID, ONE)], &[(101 * ONE, ONE)], 1_000);
        let setup = [
            InboundMessage::new(ts(1_000), MessageBody::Book(bids)),
            InboundMessage::new(ts(1_000), MessageBody::Book(asks)),
            run_control(idle_at(1), 2_000),
            run_control(running_at(2), 2_100),
            run_control(idle_at(3), 3_000),
        ];
        for message in setup {
            self.fold(message);
            self.read_everything();
        }
        let seals = Run::take(&mut self.persist);
        assert!(
            matches!(seals.as_slice(), [PersistRecord::SealAll]),
            "the first seal fits the ring, and the second waits behind it: {seals:?}"
        );
        let late = [
            run_control(running_at(4), 4_000),
            InboundMessage::new(
                ts(5_000),
                MessageBody::Book(delta_chunk(0, Side::Sell, &[(101 * ONE, 2 * ONE)], 5_000)),
            ),
            stream_ready(MARKET, 6_000),
            InboundMessage::new(
                ts(6_000),
                MessageBody::Exec(open_orders_snapshot_end(MARKET, ts(6_000))),
            ),
            InboundMessage::new(
                ts(6_000),
                MessageBody::Account(account_chunk(
                    &[(AssetId(0), 10 * ONE), (AssetId(1), 1_000_000 * ONE)],
                    6_000,
                )),
            ),
        ];
        for message in late {
            self.fold(message);
            Run::take(&mut self.commands);
            Run::take(&mut self.ui_events);
        }
        self.sampled
            .lock()
            .expect("the probe's list is never poisoned")
            .clear();
    }

    fn read_everything(&mut self) {
        Run::take(&mut self.commands);
        Run::take(&mut self.ui_events);
    }

    fn spin(&mut self) -> SpinOutputs {
        self.fold(spin(SLOT, SPIN_US));
        SpinOutputs {
            sampled: self
                .sampled
                .lock()
                .expect("the probe's list is never poisoned")
                .clone(),
            tape: Run::take(&mut self.persist),
            commands: Run::take(&mut self.commands),
            events: Run::take(&mut self.ui_events),
        }
    }
}

/// The lane sequence of the first event of a kind in `events`.
fn seq_of(events: &[UiEvent], is_kind: fn(&UiEvent) -> bool) -> u64 {
    events
        .iter()
        .find(|event| is_kind(event))
        .map(UiEvent::seq)
        .expect("the spin tees an event of each kind that the test reads")
}

/// The client id of the one placement of the spin, and its price.
fn placement(commands: &[ExecLaneItem]) -> (ClientOrderId, Price) {
    let places: Vec<_> = commands
        .iter()
        .filter_map(|item| match item {
            ExecLaneItem::Command(stamped) => match stamped.command {
                ExecCommand::Place {
                    client_id, price, ..
                } => Some((client_id, price)),
                _ => None,
            },
            ExecLaneItem::Watermark(_) => None,
        })
        .collect();
    assert_eq!(
        places.len(),
        1,
        "the spin places exactly its one bid: {commands:?}"
    );
    places[0]
}

/// CONTRACT: one spin runs its steps in the named order. The trackers take the spin, a pending seal
/// is tried, the strategy declares its bid, what it banked drains, the execution pass places that
/// bid in the same spin, the execution state goes out, then the positions, and the driver's
/// snapshot of the counters comes last.
#[test]
fn spin_steps_run_in_named_order() {
    let mut run = Run::new(LANE);
    run.set_up();
    let SpinOutputs {
        sampled,
        tape,
        commands,
        events,
    } = run.spin();

    assert_eq!(
        sampled,
        vec![1],
        "the trackers took the spin before the strategy read them: one sample since the resume"
    );
    assert!(
        matches!(tape.as_slice(), [PersistRecord::SealAll]),
        "the spin tried the pending seal, and it went: {tape:?}"
    );
    let (client_id, price) = placement(&commands);
    assert_eq!(
        price,
        Price::from_mantissa(BID),
        "the execution pass placed the bid that the strategy declared in this spin"
    );

    let drained = seq_of(&events, |event| matches!(event, UiEvent::Feature { .. }));
    let execution = seq_of(&events, |event| matches!(event, UiEvent::Execution { .. }));
    let position = seq_of(&events, |event| matches!(event, UiEvent::Position { .. }));
    assert!(
        drained < execution && execution < position,
        "the drain, then the execution state, then the positions: {drained}, {execution}, {position}"
    );
    let snapshot = events
        .iter()
        .find_map(|event| match event {
            UiEvent::OrderSnapshot {
                side: Side::Buy,
                seq,
                detail_len,
                orders,
                ..
            } => Some((*seq, orders[..usize::from(*detail_len)].to_vec())),
            _ => None,
        })
        .expect("the execution state holds the bid side");
    assert!(
        snapshot.1.iter().any(|order| order.client_id == client_id),
        "the execution state shows the order that the execution pass placed: {:?}",
        snapshot.1
    );
    let last_order_event = events
        .iter()
        .filter(|event| matches!(event, UiEvent::OrderSnapshot { .. }))
        .map(UiEvent::seq)
        .max()
        .expect("the spin tees its order snapshots");
    assert!(
        last_order_event < position,
        "no position event comes before an order event of the spin"
    );

    // The driver snapshots the counters after the fold returns. With room on the event ring for
    // each event of the spin but the positions, the snapshot counts the position event that the
    // full ring dropped, so it was taken after the positions went out.
    let room_before_positions = events.iter().filter(|event| event.seq() < position).count();
    let mut tight = Run::new(room_before_positions);
    tight.set_up();
    tight.spin();
    tight.metrics.pass(ts(60_000_000), &mut tight.rows);
    assert_eq!(
        tight.rows.dropped_total(SinkKind::UiEvents),
        Some(1),
        "the counter snapshot of the spin counts the position event that the ring dropped"
    );
}
