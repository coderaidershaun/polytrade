//! Fold time never steps back: a tick that pops after a newer message leaves the strategy and each
//! deadline at the later time, while the rows of that tick keep the tick's own stamp.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, QuoteLevel};
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::ExecSink;
use engine::hot::spawn::{HotDriver, Step};
use engine::hot::strategy::{Fill, OrderReject, OrderUpdate, Registration, Strategy, StrategyCtx};
use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, RejectClass, VenueOrderStatus,
};
use engine::msg::ids::{InstrumentId, Price, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::persist::{FeatureId, OrderTransition, PersistRecord};
use engine::msg::time::{LocalTs, Stamper};
use engine::runtime::config::{RecordedTables, TableKind};
use proptest::prelude::{any, prop};
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, Producer, RingBuffer};

use crate::engine_support::{
    ONE, detached_setup, exec_event, instrument_row, persist_ring_for, snapshot_pair, spin,
    tracker_spec_all, trade, ts,
};
use crate::exec_support::{QUOTE_QTY, drain_commands, exec_settings, make_ready};

const LANE: usize = 1_024;

/// The callback that saw a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Callback {
    Trade,
    Spin,
}

/// What one callback saw: the received stamp of its message and the time of the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Seen {
    callback: Callback,
    received: LocalTs,
    now: LocalTs,
}

/// Keeps what each trade and spin callback saw, and writes one feature row on each spin.
struct Clock {
    call: Option<FeatureId>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Clock {
    fn see(&self, ctx: &StrategyCtx<'_>, callback: Callback) {
        self.seen
            .lock()
            .expect("the probe's list is never poisoned")
            .push(Seen {
                callback,
                received: ctx.event_ts(),
                now: ctx.now().as_local(),
            });
    }
}

impl Strategy for Clock {
    fn features(&self) -> &'static [&'static str] {
        &["spin"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.call = registration.features.first().copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, _event: &TradeEvent) {
        self.see(ctx, Callback::Trade);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        self.see(ctx, Callback::Spin);
        ctx.emit(
            self.call.expect("features registered"),
            InstrumentId(0),
            1.0,
        );
    }
}

/// An engine behind the real driver with a market queue and a timer queue, and its lanes.
struct Folds {
    driver: HotDriver,
    market: Producer<InboundMessage>,
    timer: Producer<InboundMessage>,
    persist: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Folds {
    fn new() -> Folds {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let rows = [instrument_row(0, tracker_spec_all(1), 64)];
        let (persistence, persist) =
            persist_ring_for(LANE, RecordedTables::new(&[TableKind::Features]));
        let (command_producer, commands) = RingBuffer::<ExecLaneItem>::new(LANE);
        let strategy = Clock {
            call: None,
            seen: Arc::clone(&seen),
        };
        let engine = HotEngine::new(HotEngineSetup {
            persistence: Some(persistence),
            exec: vec![ExecWiring {
                sink: ExecSink::new(command_producer, Stamper::Off),
                settings: exec_settings(),
                run_nonce: 0,
            }],
            ..detached_setup(&rows, Box::new(strategy))
        });
        let (market, market_queue) = RingBuffer::new(LANE);
        let (timer, timer_queue) = RingBuffer::new(LANE);
        let queues = IngressQueues::new(vec![market_queue, timer_queue]);
        Folds {
            driver: HotDriver::new(queues, engine, None),
            market,
            timer,
            persist,
            commands,
            seen,
        }
    }

    fn push_trade(&mut self, received: i64) {
        let body = MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, received));
        self.market
            .push(InboundMessage::new(ts(received), body))
            .expect("the market ring has room");
    }

    fn push_tick(&mut self, seq: u64, received: i64) {
        self.timer
            .push(spin(seq, received))
            .expect("the timer ring has room");
    }

    fn fold_all(&mut self) {
        while self.driver.step() == Step::Folded {}
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .expect("the probe's list is never poisoned")
            .clone()
    }

    /// The stamp of each watermark on the command lane: the time that the execution pass of each
    /// spin read for each of its deadlines.
    fn watermarks(&mut self) -> Vec<LocalTs> {
        std::iter::from_fn(|| self.commands.pop().ok())
            .filter_map(|item| match item {
                ExecLaneItem::Watermark(at) => Some(at),
                ExecLaneItem::Command(_) => None,
            })
            .collect()
    }

    /// The stamp of each feature row on the tape.
    fn row_stamps(&mut self) -> Vec<LocalTs> {
        std::iter::from_fn(|| self.persist.pop().ok())
            .filter_map(|record| match record {
                PersistRecord::Feature(row) => Some(row.event_ts_us),
                _ => None,
            })
            .collect()
    }
}

/// Checks one run against the stamps in the order that they popped: each callback reads the latest
/// stamp so far, the execution pass of each spin reads that time too, and the row of each spin
/// keeps the stamp of that spin.
fn check(folds: &mut Folds) -> Result<(), String> {
    let seen = folds.seen();
    let mut latest = LocalTs::from_micros(i64::MIN);
    for (index, each) in seen.iter().enumerate() {
        latest = latest.max(each.received);
        if each.now != latest {
            return Err(format!(
                "callback {index} read the fold time {:?}, but the latest stamp so far is \
                 {latest:?}: {seen:?}",
                each.now
            ));
        }
    }
    let spins: Vec<Seen> = seen
        .into_iter()
        .filter(|each| each.callback == Callback::Spin)
        .collect();
    let read_by_spins: Vec<LocalTs> = spins.iter().map(|each| each.now).collect();
    let watermarks = folds.watermarks();
    if watermarks != read_by_spins {
        return Err(format!(
            "the execution passes read {watermarks:?}, the spins read {read_by_spins:?}"
        ));
    }
    let spin_stamps: Vec<LocalTs> = spins.iter().map(|each| each.received).collect();
    let row_stamps = folds.row_stamps();
    if row_stamps != spin_stamps {
        return Err(format!(
            "the rows of the spins carry {row_stamps:?}, the spins were received at {spin_stamps:?}"
        ));
    }
    Ok(())
}

/// The value that an execution callback handed the strategy: its kind, the stamp that the value
/// carries, and the stamp of the message under fold.
type ValueStamps = Arc<Mutex<Vec<(&'static str, LocalTs, LocalTs)>>>;

/// Quotes a bid at the touch of market 0 on each spin, and keeps the stamps of each execution value.
struct Quoter {
    values: ValueStamps,
}

impl Quoter {
    fn keep(&self, ctx: &StrategyCtx<'_>, kind: &'static str, value_ts: LocalTs) {
        let mut values = self.values.lock().expect("one thread folds");
        values.push((kind, value_ts, ctx.event_ts()));
    }
}

impl Strategy for Quoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let market = InstrumentId(0);
        let Some(bid) = ctx.book(market).best_bid() else {
            return;
        };
        let quote = DesiredQuote {
            price: bid.price,
            qty: QUOTE_QTY,
        };
        ctx.quote(market, Side::Buy, QuoteLevel::ZERO, Some(quote));
    }

    fn on_fill(&mut self, ctx: &mut StrategyCtx<'_>, fill: &Fill) {
        self.keep(ctx, "fill", fill.event_ts_us);
    }

    fn on_order_update(&mut self, ctx: &mut StrategyCtx<'_>, update: &OrderUpdate) {
        self.keep(ctx, "update", update.event_ts_us);
    }

    fn on_reject(&mut self, ctx: &mut StrategyCtx<'_>, reject: &OrderReject) {
        self.keep(ctx, "reject", reject.event_ts_us);
    }
}

/// An order row or a fill row that reached the tape, by its received stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Row {
    /// The row of a placement that the execution pass of a spin made.
    Placed(LocalTs),
    Order(LocalTs),
    Fill(LocalTs),
}

/// The order and fill rows that reached the tape while one message folded. The rows that the
/// execution pass of a spin writes wait in the bank and reach the tape with the next message.
fn rows_after(
    engine: &mut HotEngine,
    persist: &mut Consumer<PersistRecord>,
    message: &InboundMessage,
) -> Vec<Row> {
    engine.dispatch(message);
    std::iter::from_fn(|| persist.pop().ok())
        .filter_map(|record| match record {
            PersistRecord::Order(row) if row.transition == OrderTransition::Placed => {
                Some(Row::Placed(row.received_ts_us))
            }
            PersistRecord::Order(row) => Some(Row::Order(row.received_ts_us)),
            PersistRecord::Fill(row) => Some(Row::Fill(row.received_ts_us)),
            _ => None,
        })
        .collect()
}

/// After a trade at 2,000,000 the fold time stays there while a late tick and three late answers
/// of the venue fold. The order row that the tick's quote writes keeps the tick's stamp, and each
/// row and each callback value of an answer keeps the stamp of that answer.
fn execution_rows_keep_the_stamp_of_their_message() {
    let values = ValueStamps::default();
    let rows = [instrument_row(0, tracker_spec_all(1), 64)];
    let tables = RecordedTables::new(&[TableKind::Orders, TableKind::Fills]);
    let (persistence, mut persist) = persist_ring_for(LANE, tables);
    let (command_producer, mut commands) = RingBuffer::<ExecLaneItem>::new(LANE);
    let strategy = Quoter {
        values: Arc::clone(&values),
    };
    let mut engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: vec![ExecWiring {
            sink: ExecSink::new(command_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: 0,
        }],
        ..detached_setup(&rows, Box::new(strategy))
    });
    make_ready(&mut engine, 1_000);
    let (bids, asks) = snapshot_pair(0, &[(99 * ONE, ONE)], &[(101 * ONE, ONE)], 1_000);
    engine.dispatch(&InboundMessage::new(ts(1_000), MessageBody::Book(bids)));
    engine.dispatch(&InboundMessage::new(ts(1_000), MessageBody::Book(asks)));
    let newer = trade(0, 100 * ONE, ONE, Side::Buy, 2_000_000);
    engine.dispatch(&InboundMessage::new(
        ts(2_000_000),
        MessageBody::Trade(newer),
    ));
    while persist.pop().is_ok() {}

    let mut rows = rows_after(&mut engine, &mut persist, &spin(1, 1_500_000));
    let client_id = drain_commands(&mut commands)
        .into_iter()
        .find_map(|command| match command {
            ExecCommand::Place { client_id, .. } => Some(client_id),
            _ => None,
        })
        .expect("the late tick placed the quote");
    let price = 99 * ONE;
    let answer = |kind, when| ExecEvent {
        kind,
        ..exec_event(InstrumentId(0), client_id, Side::Buy, price, when)
    };
    let ack = ExecEvent {
        status: Some(VenueOrderStatus::New),
        ..answer(ExecKind::AckPlaced, 1_600_000)
    };
    let refused = ExecEvent {
        reject: Some(RejectClass::StillLive),
        ..answer(ExecKind::AckFailed, 1_700_000)
    };
    let fill = ExecEvent {
        status: Some(VenueOrderStatus::Filled),
        qty: QUOTE_QTY,
        last_price: Price::from_mantissa(price),
        last_qty: QUOTE_QTY,
        cumulative_qty: QUOTE_QTY,
        cumulative_quote: Price::from_mantissa(price).notional(QUOTE_QTY),
        ..answer(ExecKind::ReportTrade, 1_800_000)
    };
    for (event, when) in [(ack, 1_600_000), (refused, 1_700_000), (fill, 1_800_000)] {
        let message = InboundMessage::new(ts(when), MessageBody::Exec(event));
        let answered = rows_after(&mut engine, &mut persist, &message);
        for row in &answered {
            let is_kept = match row {
                Row::Placed(stamp) => *stamp == ts(1_500_000),
                Row::Order(stamp) | Row::Fill(stamp) => *stamp == ts(when),
            };
            assert!(
                is_kept,
                "a placement keeps the stamp 1,500,000 of the late tick, and each row of the \
                 answer received at {when} keeps that stamp: {answered:?}"
            );
        }
        rows.extend(answered);
    }
    let placed = rows
        .iter()
        .filter(|row| matches!(row, Row::Placed(_)))
        .count();
    let fills = rows
        .iter()
        .filter(|row| matches!(row, Row::Fill(_)))
        .count();
    assert_eq!(
        (placed, fills),
        (1, 1),
        "one placement and one fill reached the tape: {rows:?}"
    );
    assert_eq!(
        *values.lock().expect("the fold is done"),
        vec![
            ("update", ts(1_600_000), ts(1_600_000)),
            ("reject", ts(1_700_000), ts(1_700_000)),
            ("fill", ts(1_800_000), ts(1_800_000)),
        ],
        "each execution value that the strategy receives keeps the stamp of its message"
    );
}

/// FITNESS: the fold time never decreases. It is the latest received stamp of the messages popped
/// so far, from any queue. The strategy reads it, the execution pass reads it for each deadline,
/// and a row that a message writes keeps the stamp of that message.
#[test]
fn fold_time_never_decreases() {
    let mut folds = Folds::new();
    folds.push_trade(1_000_500);
    assert_eq!(folds.driver.step(), Step::Folded);
    folds.push_tick(1, 1_000_000);
    assert_eq!(folds.driver.step(), Step::Folded);

    assert_eq!(
        folds.seen(),
        vec![
            Seen {
                callback: Callback::Trade,
                received: ts(1_000_500),
                now: ts(1_000_500),
            },
            Seen {
                callback: Callback::Spin,
                received: ts(1_000_000),
                now: ts(1_000_500),
            },
        ],
        "after the trade the fold time is 1,000,500, and the late tick leaves it there"
    );
    assert_eq!(
        folds.watermarks(),
        vec![ts(1_000_500)],
        "the execution pass of the late tick reads its deadlines at the fold time"
    );
    assert_eq!(
        folds.row_stamps(),
        vec![ts(1_000_000)],
        "the row of the tick keeps the stamp of the tick"
    );
    execution_rows_keep_the_stamp_of_their_message();

    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    let messages = prop::collection::vec((any::<bool>(), 0..1_000_i64), 1..60);
    let outcome = runner.run(&messages, |messages| {
        let mut folds = Folds::new();
        let mut seq = 0;
        for (is_spin, offset) in messages {
            let received = 1_000_000 + offset;
            if is_spin {
                seq += 1;
                folds.push_tick(seq, received);
            } else {
                folds.push_trade(received);
            }
        }
        folds.fold_all();
        check(&mut folds).map_err(TestCaseError::fail)
    });
    if let Err(failure) = outcome {
        panic!("the fold time stepped back: {failure}");
    }
}
