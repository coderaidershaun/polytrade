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
}

impl PersistWriter {
    pub fn on_disk() -> Self {
        Self::through(Arc::new(DiskFiles))
    }

    pub fn through(steps: Arc<dyn FileSteps>) -> Self {
        Self { steps }
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
        };
        let writer = WriterThread::spawn("persist", move |drain| state.run(&drain));
        PersistHandle(writer)
    }
}

struct TableWriterBuilder {
    tables: RecordedTables,
    files: TableFiles,
}

impl TableWriterBuilder {
    fn named<R: TableRow>(&self, table: TableKind) -> Option<TableWriter<R>> {
        self.tables.contains(table).then(|| self.writer())
    }

    /// For the tables that `TableKind` names nowhere, so `strategy.tables` has no way to switch them
    /// off: the venue lineage, without which no row says which market its `instrument_id` stood
    /// for, and the incidents, the record of money that the engine could not book.
    fn always_written<R: TableRow>(&self) -> TableWriter<R> {
        self.writer()
    }

    fn writer<R: TableRow>(&self) -> TableWriter<R> {
        TableWriter::new(self.files.clone())
    }
}

pub struct PersistHandle(WriterThread<PersistError>);

impl PersistHandle {
    /// Call once the hot thread has stopped, so the records drained here are the run's last.
    ///
    /// # Errors
    /// [`PersistError`] if a table failed to seal, or if the writer thread panicked or was never
    /// joined — an unsealed Parquet file has no footer and no reader can open it.
    pub async fn drain(self) -> Result<(), PersistError> {
        self.0
            .drain(
                |payload| PersistError::WriterPanicked { payload },
                PersistError::DrainInterrupted,
            )
            .await
    }
}

struct WriterState {
    consumer: Consumer<PersistRecord>,
    features: Option<TableWriter<FeatureRow>>,
    trades: Option<TableWriter<TradeRow>>,
    book_events: Option<TableWriter<BookEventRow>>,
    klines: Option<TableWriter<KlineRow>>,
    link_frames: Option<TableWriter<LinkFrameRow>>,
    orders: Option<TableWriter<OrderRow>>,
    fills: Option<TableWriter<FillRow>>,
    incidents: TableWriter<IncidentRow>,
    rotations: TableWriter<RotationRow>,
    rotations_rx: mpsc::Receiver<RotationRow>,
    /// Rows of tables that the config does not name. The bank filters them first, so only a row
    /// sent past it counts. A warning, never fatal.
    unnamed_table_rows: u64,
    retention: Retention,
}

impl WriterState {
    // SMELL: the first write error ends this thread, and nothing tells the hot thread, so the
    // engine trades on while no order or fill reaches disk. A writer fault needs to stop quoting.
    fn run(mut self, drain: &AtomicBool) -> Result<(), PersistError> {
        crate::log::register_thread("persist");
        self.retention.announce();
        let pump_result = self.pump(drain);
        // Seal on the failure path too: a stopped pump still owes its healthy tables a footer.
        let seal_result = self.seal_all();
        let outcome = pump_result.and(seal_result);
        if let Err(error) = &outcome {
            crate::error!("persistence writer stopped: {error}");
        }
        outcome
    }

    fn pump(&mut self, drain: &AtomicBool) -> Result<(), PersistError> {
        self.retention.prune();
        loop {
            let drained_any = self.consume_batch()?;
            if drain.load(Ordering::Acquire) {
                self.consume_all()?;
                return Ok(());
            }
            if !drained_any {
                std::thread::sleep(IDLE_SLEEP);
            }
        }
    }

    fn consume_batch(&mut self) -> Result<bool, PersistError> {
        let mut count = 0;
        while count < MAX_POP_BATCH {
            let Ok(record) = self.consumer.pop() else { break };
            self.route(record)?;
            count += 1;
        }
        let mut rotations = 0;
        while rotations < MAX_POP_BATCH {
            let Ok(row) = self.rotations_rx.try_recv() else {
                break;
            };
            self.rotations.push(row)?;
            rotations += 1;
        }
        Ok(count > 0 || rotations > 0)
    }

    fn consume_all(&mut self) -> Result<(), PersistError> {
        while let Ok(record) = self.consumer.pop() {
            self.route(record)?;
        }
        while let Ok(row) = self.rotations_rx.try_recv() {
            self.rotations.push(row)?;
        }
        Ok(())
    }

    fn route(&mut self, record: PersistRecord) -> Result<(), PersistError> {
        let landed = match record {
            PersistRecord::Feature(row) => push_row(&mut self.features, row)?,
            PersistRecord::Trade(row) => push_row(&mut self.trades, row)?,
            PersistRecord::BookEvent(row) => push_row(&mut self.book_events, row)?,
            PersistRecord::Kline(row) => push_row(&mut self.klines, row)?,
            PersistRecord::LinkFrame(row) => push_row(&mut self.link_frames, row)?,
            PersistRecord::Order(row) => push_row(&mut self.orders, row)?,
            PersistRecord::Fill(row) => push_row(&mut self.fills, row)?,
            PersistRecord::Incident(row) => self.incidents.push(row).map(|()| true)?,
            // It comes through the ring, so each record banked before it is already here. The fold
            // sends one at each seal border of message time and at each park, and the prune of old
            // files follows each one.
            PersistRecord::SealAll => {
                self.seal_all()?;
                self.retention.prune();
                return Ok(());
            }
        };
        if let (false, Some(table)) = (landed, record.table()) {
            self.count_unnamed_table_row(table);
        }
        Ok(())
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

    fn seal_all(&mut self) -> Result<(), PersistError> {
        let mut seal = SealOutcome::default();
        seal.observe_table(&mut self.features);
        seal.observe_table(&mut self.trades);
        seal.observe_table(&mut self.book_events);
        seal.observe_table(&mut self.klines);
        seal.observe_table(&mut self.link_frames);
        seal.observe_table(&mut self.orders);
        seal.observe_table(&mut self.fills);
        seal.observe(self.incidents.seal());
        seal.observe(self.rotations.seal());
        seal.into_result()
    }
}

/// `false` when `strategy.tables` does not name the table, so it has no writer.
fn push_row<R: TableRow>(
    writer: &mut Option<TableWriter<R>>,
    row: R,
) -> Result<bool, PersistError> {
    match writer {
        Some(writer) => writer.push(row).map(|()| true),
        None => Ok(false),
    }
}

/// Seals each table, so a fault of one still leaves a footer on each other.
#[derive(Default)]
struct SealOutcome {
    first_error: Option<PersistError>,
    failures: u32,
    tables: u32,
}

impl SealOutcome {
    /// A table that the config does not name has no file, so it counts as no table.
    fn observe_table<R: TableRow>(&mut self, writer: &mut Option<TableWriter<R>>) {
        let Some(writer) = writer else { return };
        self.observe(writer.seal());
    }

    fn observe(&mut self, result: Result<(), PersistError>) {
        self.tables += 1;
        let Err(error) = result else { return };
        crate::error!("persist table seal failed: {error}");
        self.failures += 1;
        if self.first_error.is_none() {
            self.first_error = Some(error);
        }
    }

    fn into_result(self) -> Result<(), PersistError> {
        let Some(error) = self.first_error else {
            return Ok(());
        };
        crate::error!(
            "persist seal: {} of {} tables failed; healthy tables kept their footers",
            self.failures,
            self.tables
        );
        Err(error)
    }
}
