//! Fold time never steps back: a tick that pops after a newer message leaves the strategy and each
//! deadline at the later time, while the rows of that tick keep the tick's own stamp.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::ingress::IngressQueues;
use engine::hot::sink::ExecSink;
use engine::hot::spawn::{HotDriver, Step};
use engine::hot::strategy::{Registration, Strategy, StrategyCtx};
use engine::msg::exec::ExecLaneItem;
use engine::msg::ids::{InstrumentId, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick, TradeEvent};
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::time::{LocalTs, Stamper};
use engine::runtime::config::{RecordedTables, TableKind};
use proptest::prelude::{any, prop};
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, Producer, RingBuffer};

use crate::engine_support::{
    ONE, detached_setup, instrument_row, persist_ring_for, spin, tracker_spec_all, trade, ts,
};
use crate::exec_support::exec_settings;

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
            exec: Some(ExecWiring {
                sink: ExecSink::new(command_producer, Stamper::Off),
                settings: exec_settings(),
                run_nonce: 0,
            }),
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
