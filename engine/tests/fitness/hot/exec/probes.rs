//! An order that the venue never answers is asked about a few times, then the open orders of its
//! instrument are read once, and an order still unknown after that halts quoting.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, ExecSettings, HaltReason, OrderState, QuoteLevel};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{CancelReason, ExecCommand, ExecEvent, ExecLaneItem};
use engine::msg::ids::{Price, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::{DurationUs, Stamper};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::TrackerSpec;
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{detached_setup, instrument_row, spin, ts, ui_event_ring, venue_ts};
use crate::exec_support::{
    ASK, BID, INSTRUMENT, QUOTE_QTY, drain_commands, exec_settings, make_ready, reseat_book,
};

const SECOND: i64 = 1_000_000;

/// The state of the resting bid, as the strategy read it at the spin of each received stamp.
type SeenBids = Arc<Mutex<Vec<(i64, Option<OrderState>)>>>;

/// Declares one bid at the touch each spin, and keeps the state of the bid that rests there as it
/// reads it before the execution pass of that spin.
struct OneBid {
    seen: SeenBids,
}

impl Strategy for OneBid {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let resting = ctx
            .resting(INSTRUMENT, Side::Buy, QuoteLevel::ZERO)
            .map(|order| order.state);
        self.seen
            .lock()
            .expect("seen mutex poisoned")
            .push((ctx.event_ts().micros(), resting));
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: Price::from_mantissa(BID),
                qty: QUOTE_QTY,
            }),
        );
    }
}

/// The engine, the lane that nobody answers, and what was sent on it, with the second of each send.
struct Unanswered {
    engine: HotEngine,
    lane: Consumer<ExecLaneItem>,
    ui_events: Consumer<UiEvent>,
    seen: SeenBids,
    sent: Vec<(i64, &'static str, u64)>,
}

impl Unanswered {
    /// Ready on a two-sided book, with an in-flight timeout of 5 s and 3 probes.
    fn new() -> Self {
        let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
        let (producer, lane) = RingBuffer::<ExecLaneItem>::new(256);
        let (ui_event_sink, ui_events) = ui_event_ring(4_096);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut engine = HotEngine::new(HotEngineSetup {
            ui_event_sink,
            exec: vec![ExecWiring {
                sink: ExecSink::new(producer, Stamper::Off),
                settings: ExecSettings {
                    inflight_timeout: DurationUs::from_millis(5_000),
                    max_status_probes: 3,
                    ..exec_settings()
                },
                run_nonce: 0,
            }],
            ..detached_setup(
                &instruments,
                Box::new(OneBid {
                    seen: Arc::clone(&seen),
                }),
            )
        });
        make_ready(&mut engine, 0);
        for message in reseat_book(BID, ASK, 1) {
            engine.dispatch(&message);
        }
        Self {
            engine,
            lane,
            ui_events,
            seen,
            sent: Vec::new(),
        }
    }

    fn spin_at_second(&mut self, second: i64) {
        self.engine.dispatch(&spin(second as u64, second * SECOND));
        for command in drain_commands(&mut self.lane) {
            let kind = match command {
                ExecCommand::Place { .. } => "place",
                ExecCommand::ReconcileOrder { .. } => "probe",
                ExecCommand::ReconcileOpenOrders { .. } => "read",
                ExecCommand::CancelOurs {
                    reason: CancelReason::Halt,
                    ..
                } => "halt sweep",
                _ => "other",
            };
            self.sent.push((second, kind, command.recon_seq()));
        }
    }

    /// The bid as the strategy read it at the spin of `second`.
    fn bid_at(&self, second: i64) -> Option<OrderState> {
        self.seen
            .lock()
            .expect("seen mutex poisoned")
            .iter()
            .find(|(at, _)| *at == second * SECOND)
            .unwrap_or_else(|| panic!("no spin ran at second {second}"))
            .1
    }

    fn halt(&mut self) -> Option<UiHalt> {
        std::iter::from_fn(|| self.ui_events.pop().ok())
            .filter_map(|event| match event {
                UiEvent::Execution { halt, .. } => Some(halt),
                _ => None,
            })
            .last()
    }
}

/// One placement at 1 s, and spins each whole second up to the read of open orders at 25 s.
fn placed_and_unanswered_until_the_read() -> Unanswered {
    let mut run = Unanswered::new();
    for second in 1..=25 {
        run.spin_at_second(second);
    }
    run
}

/// CONTRACT: an order whose request the venue never answers is asked about at most
/// `execution.max_status_probes` times, one in-flight timeout apart, and never sent again. Then the
/// open orders of its instrument are read once. A read that does not name the order closes it and
/// frees its side. With no answer, the order is still unknown at the next timeout, and quoting
/// halts: an engine that kept probing would trade on with an order it cannot account for.
#[test]
fn spent_probes_read_open_orders_then_halt() {
    let mut read = placed_and_unanswered_until_the_read();
    let until_the_read = [
        (1, "place", 0),
        (7, "probe", 1),
        (13, "probe", 2),
        (19, "probe", 3),
        (25, "read", 4),
    ];
    assert_eq!(
        read.sent, until_the_read,
        "three probes one timeout apart, then one read of open orders in place of a fourth probe"
    );
    assert_eq!(read.bid_at(25), Some(OrderState::Unknown));

    // Case a: the read ends and does not name the order.
    let answer = ExecEvent {
        recon_seq: 4,
        ..open_orders_snapshot_end(INSTRUMENT, ts(25 * SECOND + 500_000))
    };
    read.engine.dispatch(&InboundMessage::new(
        ts(25 * SECOND + 500_000),
        MessageBody::Exec(answer),
    ));
    read.spin_at_second(26);
    assert_eq!(
        read.bid_at(26),
        None,
        "a read that does not name the order closes it, and its side is free"
    );
    assert!(
        !read.halt().is_some_and(|halt| halt.is_halted()),
        "a read that answered leaves quoting on"
    );

    // Case b: no answer at all.
    let mut silent = placed_and_unanswered_until_the_read();
    for second in 26..=31 {
        silent.spin_at_second(second);
    }
    let mut until_the_halt = until_the_read.to_vec();
    until_the_halt.push((31, "halt sweep", 0));
    assert_eq!(
        silent.sent, until_the_halt,
        "no fourth probe and no second read; at the next timeout after the read, the halt sweep"
    );
    assert_eq!(silent.bid_at(31), Some(OrderState::Unknown));
    assert_eq!(
        silent.halt(),
        Some(UiHalt::Halted {
            reason: HaltReason::StatusProbesSpent,
            halted_ts_us: venue_ts(31 * SECOND),
        }),
        "an order still unknown after its probes and the read halts quoting at that fold time"
    );
}
