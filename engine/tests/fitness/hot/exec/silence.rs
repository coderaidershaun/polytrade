//! An order stream that says nothing while an order works is checked by one read of open orders at
//! each silence. A quiet stream never halts, and a stream that keeps missing what reads show does.

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, ExecSettings, QuoteLevel};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    CancelReason, ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus,
};
use engine::msg::ids::{ClientOrderId, Price, Qty, Side, VenueOrderId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::Stamper;
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::TrackerSpec;
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, detached_setup, exec_event, instrument_row, spin, ts, ui_event_ring,
};
use crate::exec_support::{
    ASK, BID, INSTRUMENT, drain_commands, exec_settings, make_ready, reseat_book,
};

const SECOND: i64 = 1_000_000;
const ORDER_QTY: i64 = 10 * ONE;
/// How long after its read the stand-in's answer arrives: before the next spin.
const ANSWER_DELAY: i64 = 500_000;

/// Declares one bid of 10 at the touch each spin, so the order that rests there is never replaced.
struct OneBidOfTen;

impl Strategy for OneBidOfTen {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: Price::from_mantissa(BID),
                qty: Qty::from_mantissa(ORDER_QTY),
            }),
        );
    }
}

/// An engine with one working order A of 10 that the stream last reported at stamp 0, and a venue
/// stand-in that answers each read of open orders and sends no stream event.
struct Quiet {
    engine: HotEngine,
    lane: Consumer<ExecLaneItem>,
    ui_events: Consumer<UiEvent>,
    order: ClientOrderId,
    /// Each command that reached the lane, with the second of the spin that drained it.
    sent: Vec<(i64, &'static str)>,
    reads: usize,
}

impl Quiet {
    fn new(settings: ExecSettings) -> Self {
        let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
        let (producer, mut lane) = RingBuffer::<ExecLaneItem>::new(256);
        let (ui_event_sink, ui_events) = ui_event_ring(4_096);
        let mut engine = HotEngine::new(HotEngineSetup {
            ui_event_sink,
            exec: Some(ExecWiring {
                sink: ExecSink::new(producer, Stamper::Off),
                settings,
                run_nonce: 0,
            }),
            ..detached_setup(&instruments, Box::new(OneBidOfTen))
        });
        make_ready(&mut engine, 0);
        for message in reseat_book(BID, ASK, 0) {
            engine.dispatch(&message);
        }
        engine.dispatch(&spin(0, 0));
        let order = match drain_commands(&mut lane).as_slice() {
            [ExecCommand::Place { client_id, .. }] => *client_id,
            other => panic!("the first spin places order A and nothing else: {other:?}"),
        };
        let mut quiet = Self {
            engine,
            lane,
            ui_events,
            order,
            sent: vec![(0, "place")],
            reads: 0,
        };
        let report = ExecEvent {
            kind: ExecKind::ReportNew,
            status: Some(VenueOrderStatus::New),
            ..quiet.order_event(0, 0)
        };
        quiet.dispatch(0, report);
        quiet
    }

    /// Order A as the venue holds it, with `filled` of it filled.
    fn order_event(&self, at: i64, filled: i64) -> ExecEvent {
        let filled = Qty::from_mantissa(filled);
        ExecEvent {
            venue_order_id: Some(VenueOrderId(1)),
            qty: Qty::from_mantissa(ORDER_QTY),
            cumulative_qty: filled,
            cumulative_quote: Price::from_mantissa(BID).notional(filled),
            ..exec_event(INSTRUMENT, self.order, Side::Buy, BID, at)
        }
    }

    fn dispatch(&mut self, at: i64, event: ExecEvent) {
        self.engine
            .dispatch(&InboundMessage::new(ts(at), MessageBody::Exec(event)));
    }

    /// One spin at `second`. The stand-in answers each read that the spin sends half a second
    /// later, and shows order A with the fill that `filled` gives for the number of that read.
    fn spin_at(&mut self, second: i64, filled: impl Fn(usize) -> i64) {
        self.engine.dispatch(&spin(second as u64, second * SECOND));
        for command in drain_commands(&mut self.lane) {
            let kind = match command {
                ExecCommand::Place { .. } => "place",
                ExecCommand::ReconcileOpenOrders { recon_seq, .. } => {
                    self.reads += 1;
                    let at = second * SECOND + ANSWER_DELAY;
                    self.answer_read(at, recon_seq, filled(self.reads));
                    "read"
                }
                ExecCommand::CancelOurs {
                    reason: CancelReason::Halt,
                    ..
                } => "halt sweep",
                _ => "other",
            };
            self.sent.push((second, kind));
        }
    }

    fn answer_read(&mut self, at: i64, recon_seq: u64, filled: i64) {
        let status = match filled {
            0 => VenueOrderStatus::New,
            ORDER_QTY => VenueOrderStatus::Filled,
            _ => VenueOrderStatus::PartiallyFilled,
        };
        let order = ExecEvent {
            kind: ExecKind::SnapshotOrder,
            status: Some(status),
            recon_seq,
            ..self.order_event(at, filled)
        };
        self.dispatch(at, order);
        let end = ExecEvent {
            recon_seq,
            ..open_orders_snapshot_end(INSTRUMENT, ts(at))
        };
        self.dispatch(at, end);
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

/// A silence span of 30 s.
fn silence_settings() -> ExecSettings {
    ExecSettings {
        exec_silence_spins: 30,
        ..exec_settings()
    }
}

/// CONTRACT: a stream that stays quiet while an order rests gets one read of open orders at each
/// silence, and a read that agrees with the order table raises nothing: no new subscription and no
/// halt, however long the quiet lasts. An engine that halted on silence alone would stop each
/// healthy run whose order rests with no fill.
#[test]
fn quiet_stream_with_a_resting_order_never_halts() {
    let mut quiet = Quiet::new(silence_settings());
    for second in 1..=600 {
        quiet.spin_at(second, |_| 0);
    }
    let mut expected = vec![(0, "place")];
    expected.extend((1..=20).map(|span| (span * 30, "read")));
    assert_eq!(
        quiet.sent, expected,
        "one read at each 30 s of silence, and nothing else in 600 s"
    );
    assert!(
        !quiet.halt().is_some_and(|halt| halt.is_halted()),
        "a quiet stream whose reads agree never halts"
    );
}
