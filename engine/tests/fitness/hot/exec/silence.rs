//! An order stream that says nothing while an order works is checked by one read of open orders at
//! each silence. A quiet stream never halts, and a stream that keeps missing what reads show does.

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{DesiredQuote, ExecSettings, HaltReason, QuoteLevel};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    CancelReason, ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus,
};
use engine::msg::ids::{ClientOrderId, Price, Qty, Side, VenueOrderId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::{DurationUs, Stamper};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::TrackerSpec;
use engine::venues::exec::open_orders_snapshot_end;
use proptest::prelude::*;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, detached_setup, exec_event, instrument_row, spin, ts, ui_event_ring, venue_ts,
};
use crate::exec_support::{
    ASK, BID, INSTRUMENT, drain_commands, exec_settings, make_ready, reseat_book,
};

const SECOND: i64 = 1_000_000;
const SPAN: DurationUs = DurationUs::from_secs(30);
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
    /// Each command that reached the lane, with the stamp of the spin that drained it.
    sent: Vec<(i64, &'static str)>,
    reads: usize,
    spins: u64,
    /// The answer of a status probe of the edge's own resync: the second of the read that it comes
    /// just before, the fill it shows, and its recon number.
    probe: Option<(i64, i64, u64)>,
}

impl Quiet {
    fn new(settings: ExecSettings) -> Self {
        let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
        let (producer, mut lane) = RingBuffer::<ExecLaneItem>::new(256);
        let (ui_event_sink, ui_events) = ui_event_ring(4_096);
        let mut engine = HotEngine::new(HotEngineSetup {
            ui_event_sink,
            exec: vec![ExecWiring {
                sink: ExecSink::new(producer, Stamper::Off),
                settings,
                run_nonce: 0,
            }],
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
            spins: 0,
            probe: None,
        };
        quiet.report(0, ExecKind::ReportNew, 0, 0);
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

    /// A report of the order stream on order A, which has `filled` of it filled, `last` of that by
    /// this report.
    fn report(&mut self, at: i64, kind: ExecKind, filled: i64, last: i64) {
        let status =
            if filled == 0 { VenueOrderStatus::New } else { VenueOrderStatus::PartiallyFilled };
        let report = ExecEvent {
            kind,
            status: Some(status),
            last_price: Price::from_mantissa(BID),
            last_qty: Qty::from_mantissa(last),
            ..self.order_event(at, filled)
        };
        self.dispatch(at, report);
    }

    fn spin_at(&mut self, second: i64, filled: impl Fn(usize) -> i64) {
        self.spin_at_micros(second * SECOND, filled);
    }

    /// One spin at `at`. The stand-in answers each read that the spin sends half a second later,
    /// and shows order A with the fill that `filled` gives for the number of that read.
    fn spin_at_micros(&mut self, at: i64, filled: impl Fn(usize) -> i64) {
        self.spins += 1;
        self.engine.dispatch(&spin(self.spins, at));
        for command in drain_commands(&mut self.lane) {
            let kind = match command {
                ExecCommand::Place { .. } => "place",
                ExecCommand::ReconcileOpenOrders { recon_seq, .. } => {
                    if let Some((_, filled, probe_seq)) =
                        self.probe.take_if(|(second, ..)| *second * SECOND == at)
                    {
                        let answer = ExecEvent {
                            kind: ExecKind::SnapshotOrder,
                            status: Some(VenueOrderStatus::PartiallyFilled),
                            recon_seq: probe_seq,
                            ..self.order_event(at + ANSWER_DELAY / 2, filled)
                        };
                        self.dispatch(at + ANSWER_DELAY / 2, answer);
                    }
                    self.reads += 1;
                    self.answer_read(at + ANSWER_DELAY, recon_seq, filled(self.reads));
                    "read"
                }
                ExecCommand::ResubscribeStream => "resubscribe",
                ExecCommand::CancelOurs {
                    reason: CancelReason::Halt,
                    ..
                } => "halt sweep",
                _ => "other",
            };
            self.sent.push((at, kind));
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

fn silence_settings(span: DurationUs) -> ExecSettings {
    ExecSettings {
        stream_silence: span,
        max_stream_resets: 2,
        ..exec_settings()
    }
}

fn seconds(sent: &[(i64, &'static str)]) -> Vec<(i64, &'static str)> {
    sent.iter().map(|&(at, kind)| (at / SECOND, kind)).collect()
}

/// CONTRACT: a stream that stays quiet while an order rests gets one read of open orders at each
/// silence, and a read that agrees with the order table raises nothing: no new subscription and no
/// halt, however long the quiet lasts. An engine that halted on silence alone would stop each
/// healthy run whose order rests with no fill.
#[test]
fn quiet_stream_with_a_resting_order_never_halts() {
    let mut quiet = Quiet::new(silence_settings(SPAN));
    for second in 1..=600 {
        quiet.spin_at(second, |_| 0);
    }
    let mut expected = vec![(0, "place")];
    expected.extend((1..=20).map(|span| (span * 30, "read")));
    assert_eq!(
        seconds(&quiet.sent),
        expected,
        "one read at each 30 s of silence, and nothing else in 600 s"
    );
    assert!(
        !quiet.halt().is_some_and(|halt| halt.is_halted()),
        "a quiet stream whose reads agree never halts"
    );
}

/// CONTRACT: a read that shows a fill the stream never reported is one missed event. The fold
/// takes the fill of the read and asks the edge for a new subscription. Past the cap of missed
/// events with no stream event between, quoting halts. Only a report of the stream sets the count
/// back to 0, never the answer to an own read. A stream that died while reads still answer would
/// otherwise go unnoticed for the rest of the run.
#[test]
fn silent_stream_reads_then_resubscribes_then_halts() {
    let filled_by_read = |read: usize| [0, 4 * ONE, 7 * ONE, ORDER_QTY][read.min(4) - 1];
    let mut silent = Quiet::new(silence_settings(SPAN));
    for second in 1..=121 {
        silent.spin_at(second, filled_by_read);
    }
    assert_eq!(
        seconds(&silent.sent),
        [
            (0, "place"),
            (30, "read"),
            (60, "read"),
            (61, "resubscribe"),
            (90, "read"),
            (91, "resubscribe"),
            (120, "read"),
            (121, "halt sweep"),
        ],
        "read 1 agrees; reads 2 and 3 each miss an event and subscribe again; read 4 is past the cap"
    );
    assert_eq!(
        silent.halt(),
        Some(UiHalt::Halted {
            reason: HaltReason::StreamSilent,
            halted_ts_us: venue_ts(120 * SECOND + ANSWER_DELAY),
        }),
        "the third missed event halts quoting at the fold time of its read"
    );
    assert_eq!(
        silent.engine.counters().fee_gaps,
        3,
        "each fill that only a read shows names no last quantity, so its step is a fee gap"
    );

    // The status probe of an edge's own resync answers with recon number 0. Its fill books, and it
    // counts toward no read of the silence: the read that follows shows what the table holds.
    let mut probed = Quiet::new(silence_settings(SPAN));
    probed.probe = Some((30, 4 * ONE, 0));
    for second in 1..=31 {
        probed.spin_at(second, |_| 4 * ONE);
    }
    assert_eq!(
        seconds(&probed.sent),
        [(0, "place"), (30, "read")],
        "a probe answer of recon number 0 is no part of the read, which then agrees"
    );

    // A report of the stream after the second missed event sets the count back to 0.
    let filled_by_read = |read: usize| [0, 4 * ONE, 7 * ONE, 9 * ONE][read.min(4) - 1];
    let mut reported = Quiet::new(silence_settings(SPAN));
    for second in 1..=91 {
        reported.spin_at(second, filled_by_read);
    }
    reported.report(95 * SECOND, ExecKind::ReportTrade, 8 * ONE, ONE);
    for second in 92..=140 {
        reported.spin_at(second, filled_by_read);
    }
    assert_eq!(
        seconds(&reported.sent)[6..],
        [(125, "read"), (126, "resubscribe")],
        "the next silence is a span after the report, and its missed event is the first again"
    );
    assert!(
        !reported.halt().is_some_and(|halt| halt.is_halted()),
        "a stream that reports between missed events never reaches the cap"
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    /// FITNESS: the silence deadline is a difference of message stamps. The first read of open
    /// orders goes out at the first spin whose stamp is a whole span past the last event of the
    /// order stream, whatever the cadence of the spins. A rule that counted spins would read early
    /// when the timer fires fast and late when it skips slots, so a dead stream would go unread for
    /// longer than its key says.
    #[test]
    fn silence_deadline_is_a_difference_of_message_stamps(
        span_secs in 1i64..=60,
        gaps in prop::collection::vec(1_000i64..=7 * SECOND, 1..120),
        report_before in prop::option::of(0usize..120),
    ) {
        let span = DurationUs::from_secs(span_secs);
        let mut quiet = Quiet::new(silence_settings(span));
        let mut at = 0;
        let mut last_report = 0;
        let mut expected = None;
        for (index, gap) in gaps.iter().enumerate() {
            at += gap;
            if report_before == Some(index) {
                last_report = at - 1;
                quiet.report(last_report, ExecKind::ReportNew, 0, 0);
            }
            quiet.spin_at_micros(at, |_| 0);
            if at - last_report >= span.micros() {
                expected = Some((at, "read"));
                break;
            }
        }
        let first_read = quiet.sent.iter().copied().find(|&(_, kind)| kind == "read");
        prop_assert_eq!(first_read, expected);
    }
}
