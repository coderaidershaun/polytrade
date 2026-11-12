//! Writer thread: drain the record ring into one Parquet writer per table, sealing on interval, row
//! cap, hour cross, or shutdown. Dedicated thread (encode and fsync both block).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rtrb::Consumer;
use tokio::sync::mpsc;

use crate::config::{ExecutionMode, RecordedTables, TableKind};
use crate::msg::persist::{
    BookEventRow, FeatureRow, FillRow, KlineRow, LinkFrameRow, OrderRow, PersistRecord,
    RotationRow, TradeRow,
};
use crate::shutdown::WriterThread;
use crate::time::boot_stamp_us;

use super::retention::Retention;
use super::schema::{self, TableRow};
use super::table::TableWriter;
use super::{PersistConfig, PersistError, RunMeta};

const SEAL_INTERVAL: Duration = Duration::from_secs(300);
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const MAX_POP_BATCH: usize = 8_192;

pub struct PersistWriter;

impl PersistWriter {
    pub fn spawn(
        cfg: PersistConfig,
        meta: RunMeta,
        records: Consumer<PersistRecord>,
        rotations: mpsc::Receiver<RotationRow>,
    ) -> PersistHandle {
        let boot_ts_us = boot_stamp_us().micros();
        let footer = schema::footer_metadata(&meta);
        let run_dir = run_directory(&cfg.dir, &meta);
        let retention = Retention::new(run_dir.clone(), cfg.retention);
        let writers = TableWriterBuilder {
            tables: cfg.tables,
            run_dir,
            boot_ts_us,
            footer: &footer,
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
            rotations: writers.always_written(),
            rotations_rx: rotations,
            unnamed_table_rows: 0,
            retention,
        };
        let writer = WriterThread::spawn("persist", move |drain| state.run(&drain));
        PersistHandle(writer)
    }
}

/// Nests simulated runs under `sim/` to isolate their artifacts.
fn run_directory(dir: &Path, meta: &RunMeta) -> PathBuf {
    let mut run_dir = dir
        .join(meta.strategy_id.as_str())
        .join(meta.te_id.as_str());
    if let Some(segment) = ExecutionMode::artifact_segment(meta.execution_mode) {
        run_dir.push(segment);
    }
    run_dir
}

struct TableWriterBuilder<'a> {
    tables: RecordedTables,
    run_dir: PathBuf,
    boot_ts_us: i64,
    footer: &'a [(String, String)],
}

impl TableWriterBuilder<'_> {
    fn named<R: TableRow>(&self, table: TableKind) -> Option<TableWriter<R>> {
        self.tables.contains(table).then(|| self.writer())
    }

    /// Venue lineage: it says which market each `instrument_id` stood for over which window, so
    /// every other table's rows are unattributable without it. `TableKind` names it nowhere, and so
    /// `strategy.tables` has no way to switch it off.
    fn always_written(&self) -> TableWriter<RotationRow> {
        self.writer()
    }

    fn writer<R: TableRow>(&self) -> TableWriter<R> {
        TableWriter::new(&self.run_dir, self.boot_ts_us, self.footer)
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
    rotations: TableWriter<RotationRow>,
    rotations_rx: mpsc::Receiver<RotationRow>,
    /// Rows for unnamed tables (hot path gates, so counts direct calls only; loud not fatal).
    unnamed_table_rows: u64,
    retention: Retention,
}

impl WriterState {
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
        let mut last_seal = Instant::now();
        loop {
            let drained_any = self.consume_batch()?;
            if last_seal.elapsed() >= SEAL_INTERVAL {
                self.seal_all()?;
                self.retention.prune();
                last_seal = Instant::now();
            }
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
            // Ring-ordered (not latch) -> banked records reach writer before pop.
            PersistRecord::SealAll => return self.seal_all(),
        };
        if let (false, Some(table)) = (landed, record.table()) {
            self.count_unnamed_table_row(table);
        }
        Ok(())
    }

    /// Power-of-two WARNs (find misconfiguration, don't flood).
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
        seal.observe(self.rotations.seal());
        seal.into_result()
    }
}

/// false if table not in strategy.tables (no writer exists).
fn push_row<R: TableRow>(
    writer: &mut Option<TableWriter<R>>,
    row: R,
) -> Result<bool, PersistError> {
    match writer {
        Some(writer) => writer.push(row).map(|()| true),
        None => Ok(false),
    }
}

/// Seal all tables; one fault doesn't short-circuit siblings' footers.
#[derive(Default)]
struct SealOutcome {
    first_error: Option<PersistError>,
    failures: u32,
    tables: u32,
}

impl SealOutcome {
    /// Unnamed table = no writer/file (not counted in failure rate).
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
