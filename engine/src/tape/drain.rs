//! Writer thread: drain the record ring into one Parquet writer per table, sealing at each seal
//! record of the fold, row cap, hour cross, or shutdown. Dedicated thread (encode and fsync block).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rtrb::Consumer;
use tokio::sync::mpsc;

use crate::msg::persist::{
    BookEventRow, FeatureRow, FillRow, IncidentRow, KlineRow, LinkFrameRow, OrderRow,
    PersistRecord, RotationRow, TradeRow,
};
use crate::msg::time::wall_clock_now;
use crate::runtime::config::{RecordedTables, TableKind};
use crate::runtime::durable_file::{DiskFiles, FileSteps};
use crate::runtime::shutdown::WriterThread;

use super::fault::{Faults, StoppedTables, Table, TapeStatus};
use super::retention::Retention;
use super::schema::{self, TableRow};
use super::table::{TableFiles, TableWriter};
use super::{PersistConfig, PersistError, RunMeta};

const IDLE_SLEEP: Duration = Duration::from_millis(1);
const MAX_POP_BATCH: usize = 8_192;

/// The writer thread of the tape, before it starts. It takes each file step through its
/// [`FileSteps`], so a test can end a write at any step.
pub struct PersistWriter {
    steps: Arc<dyn FileSteps>,
    status: Option<TapeStatus>,
}

impl PersistWriter {
    pub fn on_disk() -> Self {
        Self::through(Arc::new(DiskFiles))
    }

    pub fn through(steps: Arc<dyn FileSteps>) -> Self {
        Self {
            steps,
            status: None,
        }
    }

    /// Without a status queue, a fault of an audit table still stops its table and fails the
    /// drain, but no message tells the fold, so quoting goes on.
    pub fn reporting_to(self, status: TapeStatus) -> Self {
        Self {
            status: Some(status),
            ..self
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the boot stamp keeps the file names of two runs apart; it decides no seal and no state"
    )]
    pub fn spawn(
        self,
        cfg: PersistConfig,
        meta: RunMeta,
        records: Consumer<PersistRecord>,
        rotations: mpsc::Receiver<RotationRow>,
    ) -> PersistHandle {
        let boot_ts_us = wall_clock_now().micros();
        let run_dir = meta.artifacts.tape_dir(&cfg.dir);
        let retention = Retention::new(run_dir.clone(), cfg.retention, boot_ts_us);
        let writers = TableWriterBuilder {
            tables: cfg.tables,
            files: TableFiles {
                run_dir,
                boot_ts_us,
                footer: schema::footer_metadata(&meta),
                steps: self.steps,
            },
        };
        let stopped = StoppedTables::default();
        let state = WriterState {
            consumer: records,
            features: writers.named(TableKind::Features),
            trades: writers.named(TableKind::Trades),
            book_events: writers.named(TableKind::BookEvents),
            klines: writers.named(TableKind::Klines),
            link_frames: writers.named(TableKind::LinkFrames),
            orders: writers.named(TableKind::Orders),
            fills: writers.named(TableKind::Fills),
            incidents: writers.always_written(),
            rotations: writers.always_written(),
            rotations_rx: rotations,
            unnamed_table_rows: 0,
            retention,
            faults: Faults::new(self.status, stopped.clone()),
        };
        let writer = WriterThread::spawn("persist", move |drain| state.run(&drain));
        PersistHandle { writer, stopped }
    }
}

struct TableWriterBuilder {
    tables: RecordedTables,
    files: TableFiles,
}

impl TableWriterBuilder {
    fn named<R: TableRow>(&self, table: TableKind) -> Table<R> {
        if self.tables.contains(table) {
            Table::Writing(Box::new(self.writer()))
        } else {
            Table::Unnamed
        }
    }

    /// For the tables that `TableKind` names nowhere, so `strategy.tables` has no way to switch them
    /// off: the venue lineage, without which no row says which market its `instrument_id` stood
    /// for, and the incidents, the record of money that the engine could not book.
    fn always_written<R: TableRow>(&self) -> Table<R> {
        Table::Writing(Box::new(self.writer()))
    }

    fn writer<R: TableRow>(&self) -> TableWriter<R> {
        TableWriter::new(self.files.clone())
    }
}

pub struct PersistHandle {
    writer: WriterThread<PersistError>,
    stopped: StoppedTables,
}

impl PersistHandle {
    /// The tables that the writer stopped after a fault. A drain reads the final count.
    pub fn stopped_tables(&self) -> StoppedTables {
        self.stopped.clone()
    }

    /// Call once the hot thread has stopped, so the records drained here are the run's last.
    ///
    /// # Errors
    /// [`PersistError`] of the first fault of an audit table, or if the writer thread panicked or
    /// was never joined. A fault of a research table stops that table only, and is counted.
    pub async fn drain(self) -> Result<(), PersistError> {
        self.writer
            .drain(
                |payload| PersistError::WriterPanicked { payload },
                PersistError::DrainInterrupted,
            )
            .await
    }
}

struct WriterState {
    consumer: Consumer<PersistRecord>,
    features: Table<FeatureRow>,
    trades: Table<TradeRow>,
    book_events: Table<BookEventRow>,
    klines: Table<KlineRow>,
    link_frames: Table<LinkFrameRow>,
    orders: Table<OrderRow>,
    fills: Table<FillRow>,
    incidents: Table<IncidentRow>,
    rotations: Table<RotationRow>,
    rotations_rx: mpsc::Receiver<RotationRow>,
    /// Rows of tables that the config does not name. The bank filters them first, so only a row
    /// sent past it counts. A warning, never fatal.
    unnamed_table_rows: u64,
    retention: Retention,
    faults: Faults,
}

impl WriterState {
    /// The root of the thread: each fault stops its own table as it happens, and the class of the
    /// table decides here whether the run ends clean.
    fn run(mut self, drain: &AtomicBool) -> Result<(), PersistError> {
        crate::log::register_thread("persist");
        self.retention.announce();
        self.pump(drain);
        self.seal_all();
        self.faults.into_result()
    }

    fn pump(&mut self, drain: &AtomicBool) {
        self.retention.prune();
        loop {
            let drained_any = self.consume_batch();
            if drain.load(Ordering::Acquire) {
                self.consume_all();
                return;
            }
            if !drained_any {
                std::thread::sleep(IDLE_SLEEP);
            }
        }
    }

    fn consume_batch(&mut self) -> bool {
        let mut count = 0;
        while count < MAX_POP_BATCH {
            let Ok(record) = self.consumer.pop() else { break };
            self.route(record);
            count += 1;
        }
        let mut rotations = 0;
        while rotations < MAX_POP_BATCH {
            let Ok(row) = self.rotations_rx.try_recv() else {
                break;
            };
            self.rotations.push(row, &mut self.faults);
            rotations += 1;
        }
        count > 0 || rotations > 0
    }

    fn consume_all(&mut self) {
        while let Ok(record) = self.consumer.pop() {
            self.route(record);
        }
        while let Ok(row) = self.rotations_rx.try_recv() {
            self.rotations.push(row, &mut self.faults);
        }
    }

    fn route(&mut self, record: PersistRecord) {
        let faults = &mut self.faults;
        let landed = match record {
            PersistRecord::Feature(row) => self.features.push(row, faults),
            PersistRecord::Trade(row) => self.trades.push(row, faults),
            PersistRecord::BookEvent(row) => self.book_events.push(row, faults),
            PersistRecord::Kline(row) => self.klines.push(row, faults),
            PersistRecord::LinkFrame(row) => self.link_frames.push(row, faults),
            PersistRecord::Order(row) => self.orders.push(row, faults),
            PersistRecord::Fill(row) => self.fills.push(row, faults),
            PersistRecord::Incident(row) => self.incidents.push(row, faults),
            // It comes through the ring, so each record banked before it is already here. The fold
            // sends one at each seal border of message time and at each park, and the prune of old
            // files follows each one.
            PersistRecord::SealAll => {
                self.seal_all();
                self.retention.prune();
                return;
            }
        };
        if let (false, Some(table)) = (landed, record.table()) {
            self.count_unnamed_table_row(table);
        }
    }

    /// Warns at each power of two, so a wrong config shows without a flood of lines.
    #[cold]
    fn count_unnamed_table_row(&mut self, table: TableKind) {
        self.unnamed_table_rows += 1;
        if self.unnamed_table_rows.is_power_of_two() {
            crate::warn!(
                "persistence dropped {} rows for tables strategy.tables does not name (latest {}) — name the table or stop emitting it",
                self.unnamed_table_rows,
                table.as_str()
            );
        }
    }

    fn seal_all(&mut self) {
        let faults = &mut self.faults;
        self.features.seal(faults);
        self.trades.seal(faults);
        self.book_events.seal(faults);
        self.klines.seal(faults);
        self.link_frames.seal(faults);
        self.orders.seal(faults);
        self.fills.seal(faults);
        self.incidents.seal(faults);
        self.rotations.seal(faults);
    }
}
