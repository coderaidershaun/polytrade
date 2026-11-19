//! An audit row is the record of money, so no flood of research rows and no stalled writer loses
//! one while the engine quotes: the audit ring banks each row it cannot take, and a bank at its
//! high mark halts quoting by one message of the input.

use std::num::{NonZeroU32, NonZeroU64};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup, PersistWiring};
use engine::hot::exec::{ClientIdLayout, HaltReason, side_base};
use engine::hot::sink::{AuditBank, AuditSink, ExecSink, ResearchSink};
use engine::hot::spawn::QueueProducer;
use engine::hot::strategy::{Registration, Strategy, StrategyCtx};
use engine::msg::exec::{ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus};
use engine::msg::ids::{InstrumentId, Notional, Price, Qty, QueueId, Side, SourceId, TradeId};
use engine::msg::inbound::{AuditBankFull, InboundMessage, MessageBody, TradeEvent};
use engine::msg::persist::{AuditRecord, FeatureId, ResearchRecord};
use engine::msg::time::{LocalTs, Stamper};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::shutdown::FatalSignal;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, detached_setup, exec_event, instrument_row, spin, trade, ts, ui_event_ring,
};
use crate::exec_support::{drain_commands, exec_settings, is_placing, reseat_book};

const RUN_NONCE: u32 = 7;
const BASE_TS_US: i64 = 1_784_410_640_000_000;

/// Emits as many feature rows on each trade as the trade's quantity mantissa says, so a test sets
/// the research load of each message.
#[derive(Default)]
struct Flood {
    feature: Option<FeatureId>,
}

impl Strategy for Flood {
    fn features(&self) -> &'static [&'static str] {
        &["flood"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.feature = registration.features.first().copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        let Some(feature) = self.feature else {
            return;
        };
        for index in 0..event.qty.mantissa() {
            ctx.emit(feature, event.instrument, index as f64);
        }
    }
}

/// The read ends of the two rings of the fold, of the audit status queue, of the workstation
/// events and of the commands.
struct Rings {
    audit: Consumer<AuditRecord>,
    research: Consumer<ResearchRecord>,
    status: Consumer<InboundMessage>,
    ui_events: Consumer<UiEvent>,
    commands: Consumer<ExecLaneItem>,
}

struct Sizes {
    research_ring: usize,
    audit_ring: usize,
    bank: AuditBank,
}

fn engine(sizes: Sizes, strategy: Box<dyn Strategy>, tables: &[TableKind]) -> (HotEngine, Rings) {
    let (research_producer, research) = RingBuffer::<ResearchRecord>::new(sizes.research_ring);
    let (audit_producer, audit) = RingBuffer::<AuditRecord>::new(sizes.audit_ring);
    let (status_producer, status) = RingBuffer::<InboundMessage>::new(4);
    let audit_status = QueueProducer::new(
        status_producer,
        FatalSignal::new(),
        QueueId::new(1).expect("queue 1 exists"),
        SourceId(1),
    );
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (ui_event_sink, ui_events) = ui_event_ring(65_536);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(1_024);
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(PersistWiring {
            audit: AuditSink::new(audit_producer, sizes.bank, NonZeroU32::MAX)
                .reporting_to(audit_status),
            research: ResearchSink::new(research_producer, NonZeroU32::MAX),
            tables: RecordedTables::new(tables),
            seal_interval_spins: NonZeroU64::MAX,
        }),
        ui_event_sink,
        exec: vec![ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: exec_settings(),
            run_nonce: RUN_NONCE,
        }],
        ..detached_setup(&instruments, strategy)
    });
    let rings = Rings {
        audit,
        research,
        status,
        ui_events,
        commands,
    };
    (engine, rings)
}

/// A fill of an own order that no slot holds: one incident row, the record of money it leaves.
fn orphan_fill(index: u64, when: i64) -> InboundMessage {
    let client_id = ClientIdLayout {
        run_nonce: RUN_NONCE,
    }
    .encode(12, index as u16 + 1);
    let filled = Qty::from_mantissa(ONE / 10);
    let event = ExecEvent {
        kind: ExecKind::ReportTrade,
        status: Some(VenueOrderStatus::PartiallyFilled),
        trade_id: Some(TradeId(index as i64 + 1)),
        qty: Qty::from_mantissa(ONE),
        last_price: Price::from_mantissa(100 * ONE),
        last_qty: filled,
        cumulative_qty: filled,
        cumulative_quote: Price::from_mantissa(100 * ONE).notional(filled),
        ..exec_event(InstrumentId(0), client_id, Side::Buy, 100 * ONE, when)
    };
    InboundMessage::new(ts(when), MessageBody::Exec(event))
}

/// Trades that ask for `research` feature rows, no trade more than 1,000, with `audit` orphan fills
/// spread between them.
fn flood(audit: u64, research: u64) -> Vec<InboundMessage> {
    let trades = audit.max(research.div_ceil(1_000));
    let mut messages = Vec::new();
    let mut when = BASE_TS_US;
    let mut orphans = 0;
    for step in 0..trades {
        let rows = research * (step + 1) / trades - research * step / trades;
        let event = trade(0, 100 * ONE, rows as i64, Side::Buy, when);
        messages.push(InboundMessage::new(ts(when), MessageBody::Trade(event)));
        when += 1_000;
        while orphans < audit * (step + 1) / trades {
            messages.push(orphan_fill(orphans, when));
            orphans += 1;
            when += 1_000;
        }
    }
    messages
}

/// What a writer that resumes takes: the rows in the ring, then each row that the bank still held,
/// which the sink hands over when the hot thread ends.
fn resume_writer(engine: HotEngine, mut audit: Consumer<AuditRecord>) -> Vec<AuditRecord> {
    std::thread::scope(|scope| {
        let reader = scope.spawn(move || {
            let mut rows = Vec::new();
            loop {
                match audit.pop() {
                    Ok(row) => rows.push(row),
                    Err(_) if audit.is_abandoned() => break,
                    Err(_) => std::thread::yield_now(),
                }
            }
            rows.extend(std::iter::from_fn(|| audit.pop().ok()));
            rows
        });
        drop(engine);
        reader.join().expect("the reader thread ends")
    })
}

fn flood_drops_no_audit_row(audit: u64, research: u64) -> Result<(), TestCaseError> {
    let messages = flood(audit, research);
    let roomy = Sizes {
        research_ring: 65_536,
        audit_ring: 65_536,
        bank: AuditBank {
            rows: 1_024,
            high_mark: 768,
        },
    };
    let (mut reference, mut room) =
        engine(roomy, Box::new(Flood::default()), &[TableKind::Features]);
    let mut written = Vec::new();
    let mut features = 0_u64;
    for message in &messages {
        reference.dispatch(message);
        written.extend(std::iter::from_fn(|| room.audit.pop().ok()));
        features += std::iter::from_fn(|| room.research.pop().ok()).count() as u64;
    }
    proptest::prop_assert_eq!(
        (written.len() as u64, features),
        (audit, research),
        "the run writes one incident row for each orphan fill and each feature row asked for"
    );

    let tight = Sizes {
        research_ring: 8,
        audit_ring: 8,
        bank: AuditBank {
            rows: 1_024,
            high_mark: 768,
        },
    };
    let (mut flooded, mut stalled) =
        engine(tight, Box::new(Flood::default()), &[TableKind::Features]);
    for message in &messages {
        flooded.dispatch(message);
    }
    let counters = flooded.counters();
    proptest::prop_assert_eq!(
        (counters.audit_banked_peak, counters.audit_lost),
        (audit.saturating_sub(8), 0),
        "the ring holds 8 audit rows and the bank each one after, with none lost"
    );
    proptest::prop_assert_eq!(
        counters.research_dropped.features,
        research.saturating_sub(8),
        "the research ring holds 8 feature rows and drops and counts each one after"
    );
    proptest::prop_assert!(
        stalled.status.pop().is_err(),
        "a bank below its high mark posts no message"
    );
    let kept = std::iter::from_fn(|| stalled.research.pop().ok()).count() as u64;
    proptest::prop_assert_eq!(kept + counters.research_dropped.features, research);
    let read = resume_writer(flooded, stalled.audit);
    proptest::prop_assert_eq!(
        read,
        written,
        "the writer takes each audit row that the run wrote, in the order it wrote them"
    );
    Ok(())
}

/// FITNESS: research pressure never costs an audit row. With research and audit rings of 8 slots
/// and the writer stalled, 5,000 feature rows and 20 incident rows fold: 8 feature rows stay in
/// the ring and 4,992 are dropped and counted, 8 audit rows sit in the ring and 12 in the bank,
/// and once the writer resumes it takes all 20 in order. The same holds for each case of 1 to 200
/// audit rows and 0 to 10,000 research rows. One ring for both lost fill rows to a feature flood.
#[test]
fn research_flood_drops_no_audit_row() {
    flood_drops_no_audit_row(20, 5_000).expect("the case of the trace");
    let mut runner = TestRunner::new(Config {
        cases: 24,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&(1..=200_u64, 0..=10_000_u64), |(audit, research)| {
            flood_drops_no_audit_row(audit, research)
        })
        .expect("no flood of research rows loses an audit row");
}

/// One resting buy that the venue named, and the reports that each fill it a little: each report
/// writes an order row and a fill row.
struct Resting {
    order: ExecEvent,
    cumulative: i64,
}

impl Resting {
    fn adopt(engine: &mut HotEngine) -> Resting {
        let client_id = ClientIdLayout {
            run_nonce: RUN_NONCE,
        }
        .encode(side_base(InstrumentId(0), Side::Buy), 1);
        let order = ExecEvent {
            kind: ExecKind::SnapshotOrder,
            status: Some(VenueOrderStatus::New),
            qty: Qty::from_mantissa(ONE),
            ..exec_event(InstrumentId(0), client_id, Side::Buy, 99 * ONE, BASE_TS_US)
        };
        for message in reseat_book(99 * ONE, 101 * ONE, BASE_TS_US) {
            engine.dispatch(&message);
        }
        engine.dispatch(&InboundMessage::new(
            ts(BASE_TS_US),
            MessageBody::Exec(order),
        ));
        Resting {
            order,
            cumulative: 0,
        }
    }

    fn report(&mut self, trade: i64, when: i64) -> InboundMessage {
        self.cumulative += 1;
        let event = ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::PartiallyFilled),
            trade_id: Some(TradeId(trade)),
            last_price: Price::from_mantissa(99 * ONE),
            last_qty: Qty::from_mantissa(1),
            cumulative_qty: Qty::from_mantissa(self.cumulative),
            cumulative_quote: Notional::from_mantissa(self.cumulative * 99),
            ..self.order
        };
        InboundMessage::new(ts(when), MessageBody::Exec(event))
    }
}

/// A quoting engine over an audit ring of 4 slots whose writer is stalled, fed reports until the
/// bank of `bank` reaches its high mark. Its post must name the mark, be stamped with the report
/// that crossed it, and halt quoting when the fold takes it.
fn bank_at_mark_halts_quoting(bank: AuditBank) -> Result<(), TestCaseError> {
    const AUDIT_RING: usize = 4;
    let sizes = Sizes {
        research_ring: 1_024,
        audit_ring: AUDIT_RING,
        bank,
    };
    let (mut engine, mut rings) = engine(
        sizes,
        Box::new(Flood::default()),
        &[TableKind::Orders, TableKind::Fills],
    );
    let mut resting = Resting::adopt(&mut engine);

    let mut when = BASE_TS_US;
    let mut posted = None;
    for trade in 1..=(bank.rows as i64 + 4) {
        when += 1_000;
        let banked_before = engine.banked_audit_rows();
        engine.dispatch(&resting.report(trade, when));
        if let Ok(message) = rings.status.pop() {
            proptest::prop_assert!(
                banked_before < bank.high_mark && engine.banked_audit_rows() >= bank.high_mark,
                "the post comes with the row that brings the bank to its mark"
            );
            posted = Some(message);
            break;
        }
    }
    let Some(message) = posted else {
        return Err(TestCaseError::fail(
            "the bank reached its mark and posted nothing",
        ));
    };
    proptest::prop_assert_eq!(
        message.body,
        MessageBody::AuditBankFull(AuditBankFull {
            banked: bank.high_mark as u32,
            capacity: bank.rows as u32,
        })
    );
    proptest::prop_assert_eq!(
        message.header.received_ts_us,
        LocalTs::from_micros(when),
        "stamped with the received stamp of the row that crossed the mark"
    );

    drain_commands(&mut rings.commands);
    engine.dispatch(&message);
    engine.dispatch(&spin(1, when + 1));
    let halt = std::iter::from_fn(|| rings.ui_events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Execution { halt, .. } => Some(halt),
            _ => None,
        })
        .last();
    proptest::prop_assert!(
        matches!(
            halt,
            Some(UiHalt::Halted {
                reason: HaltReason::AuditBankFull,
                ..
            })
        ),
        "quoting halts at the message: {:?}",
        halt
    );
    let swept = drain_commands(&mut rings.commands);
    proptest::prop_assert!(
        swept
            .iter()
            .any(|command| matches!(command, ExecCommand::CancelOurs { .. })),
        "the halt sweeps the orders the engine holds: {:?}",
        swept
    );
    for seq in 2..=4 {
        engine.dispatch(&spin(seq, when + seq as i64));
    }
    let later = drain_commands(&mut rings.commands);
    proptest::prop_assert!(
        !is_placing(&later, Side::Buy) && !is_placing(&later, Side::Sell),
        "later spins place nothing: {:?}",
        later
    );
    proptest::prop_assert!(
        rings.status.pop().is_err(),
        "the bank posts once for the run"
    );

    let counters = engine.counters();
    proptest::prop_assert_eq!(counters.audit_lost, 0, "no row is lost below capacity");
    let banked = engine.banked_audit_rows();
    let read = resume_writer(engine, rings.audit);
    proptest::prop_assert_eq!(
        read.len(),
        AUDIT_RING + banked,
        "the writer takes the rows of the ring and then each row of the bank"
    );
    let stamps: Vec<LocalTs> = read
        .iter()
        .filter_map(AuditRecord::received_ts_us)
        .collect();
    proptest::prop_assert!(
        stamps.windows(2).all(|pair| pair[0] <= pair[1]),
        "the rows reach the writer in the order they were written"
    );
    Ok(())
}

/// A bank at capacity drops a row and counts it: quoting halted at the mark, before the first one.
fn bank_at_capacity_counts_each_lost_row() {
    let bank = AuditBank {
        rows: 16,
        high_mark: 12,
    };
    let (mut engine, mut rings) = engine(
        Sizes {
            research_ring: 1_024,
            audit_ring: 4,
            bank,
        },
        Box::new(Flood::default()),
        &[TableKind::Orders, TableKind::Fills],
    );
    let mut resting = Resting::adopt(&mut engine);
    for trade in 1..=40 {
        engine.dispatch(&resting.report(trade, BASE_TS_US + trade * 1_000));
    }
    let counters = engine.counters();
    assert_eq!(
        counters.audit_banked_peak, 16,
        "the bank filled to its capacity"
    );
    assert!(counters.audit_lost > 0, "each row past capacity is counted");
    assert!(
        rings.status.pop().is_ok(),
        "the bank posted at its mark first"
    );
    let read = resume_writer(engine, rings.audit);
    assert_eq!(
        read.len(),
        4 + 16,
        "the ring and the full bank reach the writer"
    );
}

/// FITNESS: an audit ring of 4 slots with a stalled writer banks each row after the fourth. At the
/// high mark of the bank the sink posts one `AuditBankFull` on the audit status queue, stamped with
/// the row that crossed the mark; the fold halts quoting at it and sweeps its own orders, whose rows
/// the bank still takes; later spins place nothing; no row is lost; and the writer, once it
/// resumes, takes each row in order. The same holds for each high mark of 1 to 64 with room for
/// the sweep above it. Past capacity a row is dropped and counted.
#[test]
fn full_audit_bank_halts_quoting() {
    bank_at_mark_halts_quoting(AuditBank {
        rows: 16,
        high_mark: 12,
    })
    .expect("the case of the trace");
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&(1..=64_usize), |high_mark| {
            bank_at_mark_halts_quoting(AuditBank {
                rows: high_mark + 16,
                high_mark,
            })
        })
        .expect("each high mark halts quoting the same way");
    bank_at_capacity_counts_each_lost_row();
}
