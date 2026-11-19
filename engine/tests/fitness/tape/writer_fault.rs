//! A tape table that cannot be written stops alone when only research reads it, and halts quoting
//! when it is a record of what the engine did with money.

use std::fs::File;
use std::io::{self, Write};
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rtrb::{Consumer, RingBuffer};

use engine::hot::exec::HaltReason;
use engine::hot::spawn::QueueProducer;
use engine::msg::exec::{ExecCommand, Provenance};
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{
    ClientOrderId, InstrumentId, Notional, Price, Qty, QueueId, Side, SourceId,
};
use engine::msg::inbound::{FileWriter, InboundMessage, MessageBody, SpinTick, WriterFailure};
use engine::msg::persist::{
    AuditRecord, BootRecord, LedgerEntry, LedgerKind, LinkFrameRow, LinkRowKind, OrderLifecycle,
    OrderRow, OrderTransition, ResearchRecord, TapeRecord, TradeRow,
};
use engine::msg::time::{EngineClock, LocalTs, VenueTs};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{RecordedTables, TableKind};
use engine::runtime::durable_file::{DiskFiles, FileSteps, StepFile};
use engine::runtime::shutdown::FatalSignal;
use engine::tape::{
    PersistConfig, PersistHandle, PersistWriter, RunMeta, TapeInput, TapeStatus, TapeTable,
};

use crate::engine_support::ONE;
use crate::exec_support::{
    ASK, BID, CEILING, drain_commands, is_placing, make_ready, quoting_engine_with_ui, reseat_book,
    spin_at,
};
use crate::parquet_readback::{
    BASE_TS_US, Record, TempDir, drain_blocking, parquet_files, preloaded, read_parquet_file,
    run_meta, table_files,
};

/// Rows of each table before the seal and again after it.
const ROWS: usize = 3;
const REPORT_DEADLINE: Duration = Duration::from_secs(10);

/// CONTRACT: the class of a tape fault comes from its table. A fault of the trades or the link
/// frames table, which only research reads, or of the tape, which costs trading nothing, stops
/// that table and names it, while the audit tables
/// go on and seal each file, whether the trades file cannot be written or cannot be opened. A fault
/// of the orders or the ledger table sends one `WriterFailed` message to the fold, which halts
/// quoting at it. A writer that ended at any fault would lose the orders of a run for a research
/// table, and one that told nobody would let the engine trade on with no record of its orders.
#[test]
fn audit_write_fault_stops_quoting_and_research_fault_stops_one_table() {
    a_research_fault_stops_only_its_table(TapeTable::Trades);
    a_research_fault_stops_only_its_table(TapeTable::LinkFrames);
    a_research_fault_stops_only_its_table(TapeTable::Tape);
    a_research_table_that_cannot_open_stops_only_itself();
    an_audit_fault_halts_quoting();
    an_audit_fault_reports_once_and_halts(TapeTable::Ledger);
}

fn a_research_fault_stops_only_its_table(table: TapeTable) {
    let dir = TempDir::new("tape-research-fault");
    let mut writer = Writer::failing_writes_of(table.as_str(), dir.path());
    let stopped = writer.handle.stopped_tables();
    let drained = drain_blocking(writer.handle);

    assert!(
        drained.is_ok(),
        "nothing halts at a fault of {table}: {drained:?}"
    );
    assert_eq!(
        stopped.tables(),
        [table],
        "the {table} table stops, by name"
    );
    assert!(writer.reports.pop().is_err(), "no message goes to the fold");
    assert!(!writer.fatal.is_tripped());
    for audit in ["orders", "ledger"] {
        assert_eq!(
            rows_of_each_file(dir.path(), audit),
            [ROWS, ROWS],
            "the {audit} table goes on, and each open file is sealed"
        );
    }
    assert!(table_files(dir.path(), table.as_str()).is_empty());
}

/// A plain file stands where the trades folder must go, so each open of a trades file fails, for
/// any user, root included: no folder can be made below a file.
fn a_research_table_that_cannot_open_stops_only_itself() {
    let dir = TempDir::new("tape-research-open-fault");
    let meta = run_meta();
    let tape = meta.run_dir(dir.path());
    std::fs::create_dir_all(&tape).expect("the test makes the tape folder");
    std::fs::write(tape.join("trades"), b"not a folder").expect("the test plants a file");
    let mut writer = Writer::spawn(Arc::new(DiskFiles), dir.path(), meta);
    let stopped = writer.handle.stopped_tables();
    let drained = drain_blocking(writer.handle);

    assert!(drained.is_ok(), "nothing halts: {drained:?}");
    assert_eq!(stopped.count(), 1, "the trades table stops, with a count");
    assert!(writer.reports.pop().is_err(), "no message goes to the fold");
    let orders = parquet_files(&tape.join("orders"));
    assert!(
        orders.iter().all(|path| {
            File::open(path)
                .is_ok_and(|file| ParquetRecordBatchReaderBuilder::try_new(file).is_ok())
        }),
        "each file of the healthy table keeps its footer though a sibling table failed"
    );
    assert_eq!(
        rows_of_each_file(dir.path(), "orders"),
        [ROWS, ROWS],
        "each healthy row is sealed into a file with a footer"
    );
}

/// The one message of an audit fault, after which the drain fails and the research tables go on.
fn an_audit_fault_reports_once_and_halts(table: TapeTable) -> InboundMessage {
    let dir = TempDir::new("tape-audit-fault");
    let mut writer = Writer::failing_writes_of(table.as_str(), dir.path());
    let stopped = writer.handle.stopped_tables();
    let report = wait_for_report(&mut writer.reports);
    assert_eq!(
        report.body,
        MessageBody::WriterFailed(WriterFailure {
            writer: FileWriter::Tape,
            consecutive_failures: 1,
        }),
    );
    let drained = drain_blocking(writer.handle);
    let error = drained.expect_err("a run that lost its record of money does not end clean");
    assert_eq!(error.failure_class(), FailureClass::Halt(Halt::Quoting));
    assert!(writer.reports.pop().is_err(), "the writer reports once");
    assert_eq!(
        stopped.tables(),
        [table],
        "the {table} table stops, by name"
    );
    assert_eq!(
        rows_of_each_file(dir.path(), "trades"),
        [ROWS, ROWS],
        "the research table goes on"
    );
    report
}

fn an_audit_fault_halts_quoting() {
    let report = an_audit_fault_reports_once_and_halts(TapeTable::Orders);

    let (mut engine, mut commands, mut ui_events) = quoting_engine_with_ui(CEILING, &[]);
    let when = BASE_TS_US;
    make_ready(&mut engine, when);
    for message in reseat_book(BID, ASK, when) {
        engine.dispatch(&message);
    }
    spin_at(&mut engine, 1, when);
    assert!(
        is_placing(&drain_commands(&mut commands), Side::Buy),
        "the engine quotes before the report"
    );
    engine.dispatch(&report);
    spin_at(&mut engine, 2, when + 1_000_000);
    spin_at(&mut engine, 3, when + 2_000_000);
    let after = drain_commands(&mut commands);
    assert!(
        !is_placing(&after, Side::Buy) && !is_placing(&after, Side::Sell),
        "no quote goes out after the halt: {after:?}"
    );
    assert!(
        matches!(
            halt_of(&mut ui_events),
            Some(UiHalt::Halted {
                reason: HaltReason::WriterFailed,
                ..
            })
        ),
        "quoting halts at the report"
    );
    assert!(
        after
            .iter()
            .any(|command| matches!(command, ExecCommand::CancelOurs { .. })),
        "the halt pulls the orders that the engine holds: {after:?}"
    );
}

/// A tape writer over the orders, ledger, trades and link frames tables, with rows of each, a seal
/// of each ring, and rows of each again, and its own status queue.
struct Writer {
    handle: PersistHandle,
    reports: Consumer<InboundMessage>,
    fatal: FatalSignal,
}

impl Writer {
    fn failing_writes_of(table: &'static str, dir: &Path) -> Writer {
        Writer::spawn(Arc::new(FailsWritesOf { table }), dir, run_meta())
    }

    fn spawn(steps: Arc<dyn FileSteps>, dir: &Path, meta: RunMeta) -> Writer {
        let (status_producer, reports) = RingBuffer::<InboundMessage>::new(4);
        let fatal = FatalSignal::new();
        let status = TapeStatus {
            queue: QueueProducer::new(
                status_producer,
                fatal.clone(),
                QueueId::new(6).unwrap(),
                SourceId(4),
            ),
            clock: EngineClock::start(),
        };
        let handle = PersistWriter::through(steps)
            .reporting_to(status)
            .recording_tape(tape_input())
            .spawn(
                PersistConfig {
                    dir: dir.to_path_buf(),
                    tables: RecordedTables::new(&[
                        TableKind::Orders,
                        TableKind::Trades,
                        TableKind::LinkFrames,
                    ]),
                    retention: Duration::from_hours(6),
                },
                meta,
                preloaded(records()),
            );
        Writer {
            handle,
            reports,
            fatal,
        }
    }
}

/// The tape of the writer: rows of messages, a seal, and rows again.
fn tape_input() -> TapeInput {
    let (mut producer, records) = RingBuffer::<TapeRecord>::new(2 * ROWS + 1);
    let queue = QueueId::new(0).expect("queue 0");
    let message = |seq: usize| TapeRecord::Message {
        seq: seq as u64 + 1,
        queue,
        message: InboundMessage::new(
            LocalTs::from_micros(BASE_TS_US + seq as i64),
            MessageBody::SpinTick(SpinTick { seq: seq as u64 }),
        ),
    };
    let mut tape: Vec<TapeRecord> = (0..ROWS).map(message).collect();
    tape.push(TapeRecord::Seal);
    tape.extend((ROWS..2 * ROWS).map(message));
    for record in tape {
        producer.push(record).expect("the tape ring has room");
    }
    TapeInput {
        records,
        boot: BootRecord::default(),
    }
}

fn records() -> Vec<Record> {
    let rows = |from: usize| {
        (from..from + ROWS).flat_map(|index| {
            [
                AuditRecord::Order(order_row(index)).into(),
                AuditRecord::Ledger(ledger_row(index)).into(),
                ResearchRecord::Trade(trade_row(index)).into(),
                ResearchRecord::LinkFrame(link_row(index)).into(),
            ]
        })
    };
    let mut records: Vec<Record> = rows(0).collect();
    records.push(AuditRecord::SealAll.into());
    records.push(ResearchRecord::SealAll.into());
    records.extend(rows(ROWS));
    records
}

fn ledger_row(index: usize) -> LedgerEntry {
    LedgerEntry {
        kind: LedgerKind::Fill,
        instrument: InstrumentId(0),
        position_base: Qty::from_mantissa(ONE),
        cash_quote: Notional::from_mantissa(-100 * ONE),
        basis_quote: Notional::from_mantissa(100 * ONE),
        realised_quote: Notional::from_mantissa(0),
        mark: Some(Price::from_mantissa(100 * ONE)),
        exposure_quote: Notional::from_mantissa(100 * ONE),
        pnl_quote: Notional::from_mantissa(0),
        client_id: Some(ClientOrderId(index as u64 + 1)),
        trade_id: None,
        received_ts_us: LocalTs::from_micros(BASE_TS_US + index as i64),
    }
}

fn link_row(index: usize) -> LinkFrameRow {
    LinkFrameRow {
        kind: LinkRowKind::Payload,
        sender_te_hash: 7,
        topic: 16,
        seq: index as u64,
        slot: 0,
        count: 1,
        value: index as f64,
        event_ts_us: VenueTs::from_micros(BASE_TS_US + index as i64),
        received_ts_us: LocalTs::from_micros(BASE_TS_US + index as i64),
    }
}

fn order_row(index: usize) -> OrderRow {
    OrderRow {
        instrument: InstrumentId(0),
        client_id: ClientOrderId(index as u64 + 1),
        quote_level: None,
        venue_order_id: None,
        transition: OrderTransition::Placed,
        state: OrderLifecycle::PendingNew,
        previous_state: OrderLifecycle::Free,
        provenance: Provenance::Mine,
        side: Side::Buy,
        style: None,
        price: Price::from_mantissa(100 * ONE),
        qty: Qty::from_mantissa(ONE),
        filled_qty: Qty::from_mantissa(0),
        filled_quote: 0,
        reject: None,
        reject_code: 0,
        exchange_ts_us: VenueTs::from_micros(0),
        received_ts_us: LocalTs::from_micros(BASE_TS_US + index as i64),
        depth_ahead: None,
    }
}

fn trade_row(index: usize) -> TradeRow {
    TradeRow {
        instrument: InstrumentId(0),
        price: Price::from_mantissa(100 * ONE),
        qty: Qty::from_mantissa(ONE),
        side: Side::Buy,
        exchange_ts_us: VenueTs::from_micros(BASE_TS_US + index as i64),
        received_ts_us: LocalTs::from_micros(BASE_TS_US + index as i64),
    }
}

fn rows_of_each_file(dir: &Path, table: &str) -> Vec<usize> {
    table_files(dir, table)
        .iter()
        .map(|path| read_parquet_file(path).rows.len())
        .collect()
}

fn wait_for_report(reports: &mut Consumer<InboundMessage>) -> InboundMessage {
    let deadline = Instant::now() + REPORT_DEADLINE;
    loop {
        if let Ok(report) = reports.pop() {
            return report;
        }
        assert!(
            Instant::now() < deadline,
            "the writer sent no report in {REPORT_DEADLINE:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn halt_of(ui_events: &mut Consumer<UiEvent>) -> Option<UiHalt> {
    let mut halt = None;
    while let Ok(event) = ui_events.pop() {
        if let UiEvent::Execution { halt: latest, .. } = event {
            halt = Some(latest);
        }
    }
    halt
}

/// The real file system, except that each write to a file of one table fails, as a full or broken
/// disk refuses it.
struct FailsWritesOf {
    table: &'static str,
}

impl FileSteps for FailsWritesOf {
    fn create_folder(&self, folder: &Path) -> io::Result<()> {
        DiskFiles.create_folder(folder)
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn StepFile>> {
        let file = DiskFiles.create(path)?;
        let is_of_table = path
            .components()
            .any(|part| part == Component::Normal(self.table.as_ref()));
        if is_of_table {
            return Ok(Box::new(RefusesWrites(file)));
        }
        Ok(file)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DiskFiles.rename(from, to)
    }

    fn sync_folder(&self, folder: &Path) -> io::Result<()> {
        DiskFiles.sync_folder(folder)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        DiskFiles.remove(path)
    }
}

struct RefusesWrites(Box<dyn StepFile>);

impl Write for RefusesWrites {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("the disk refused the write"))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl StepFile for RefusesWrites {
    fn sync(&mut self) -> io::Result<()> {
        self.0.sync()
    }
}
