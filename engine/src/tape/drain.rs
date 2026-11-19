//! Writer thread: drain the tape ring, the audit ring, the research ring, the rollup ring and the
//! rotation channel into one Parquet writer per table, sealing at each seal record of the fold, row
//! cap, hour cross, or shutdown. Dedicated thread (encode and fsync block).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rtrb::Consumer;
use tokio::sync::mpsc;

use crate::msg::persist::{AuditRecord, ResearchRecord, RotationRow, TapeRecord};
use crate::runtime::config::TableKind;
use crate::runtime::durable_file::{DiskFiles, FileSteps};
use crate::runtime::metrics::RollupRow;
use crate::runtime::shutdown::WriterThread;

use super::fault::{Faults, StoppedTables, TapeStatus};
use super::retention::Retention;
use super::schema;
use super::table::TableFiles;
use super::tables::{AuditTables, MetricTables, ResearchTables, Routed, TableWriterBuilder};
use super::tape_table::{TapeInput, TapeTable};
use super::{PersistConfig, PersistError, RunMeta, WriterInputs};

const IDLE_SLEEP: Duration = Duration::from_millis(1);
const MAX_POP_BATCH: usize = 8_192;

/// The writer thread of the tape, before it starts. It takes each file step through its
/// [`FileSteps`], so a test can end a write at any step.
pub struct PersistWriter {
    steps: Arc<dyn FileSteps>,
    status: Option<TapeStatus>,
    tape: Option<TapeInput>,
}

impl PersistWriter {
    pub fn on_disk() -> Self {
        Self::through(Arc::new(DiskFiles))
    }

    pub fn through(steps: Arc<dyn FileSteps>) -> Self {
        Self {
            steps,
            status: None,
            tape: None,
        }
    }

    /// Writes the tape table from the ring of the tee, its boot row first. Without it the run has
    /// no tape, as a replay has none.
    pub fn recording_tape(self, tape: TapeInput) -> Self {
        Self {
            tape: Some(tape),
            ..self
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

    /// The boot stamp comes from `meta`, read once by the start, so the writer reads no clock.
    pub fn spawn(self, cfg: PersistConfig, meta: RunMeta, inputs: WriterInputs) -> PersistHandle {
        let (state, stopped) = self.state(cfg, meta, inputs);
        let writer = WriterThread::spawn("persist", move |drain| state.run(&drain));
        PersistHandle { writer, stopped }
    }

    /// The writer of a fold on one thread, a re-fold: the loop body of the writer thread, run by the
    /// caller after each message, so no ring fills and no record waits on a thread.
    pub fn inline(self, cfg: PersistConfig, meta: RunMeta, inputs: WriterInputs) -> InlineWriter {
        let (state, stopped) = self.state(cfg, meta, inputs);
        state.retention.prune();
        InlineWriter { state, stopped }
    }

    fn state(
        self,
        cfg: PersistConfig,
        meta: RunMeta,
        inputs: WriterInputs,
    ) -> (WriterState, StoppedTables) {
        let boot_ts_us = meta.run_id.micros();
        let kind_dir = meta.artifacts.kind_dir(&cfg.dir, meta.run_kind);
        let retention = Retention::new(kind_dir, cfg.retention, boot_ts_us);
        let builder = TableWriterBuilder {
            tables: cfg.tables,
            files: TableFiles {
                run_dir: meta.run_dir(&cfg.dir),
                boot_ts_us,
                footer: schema::footer_metadata(&meta),
                steps: self.steps,
            },
        };
        let stopped = StoppedTables::default();
        let WriterInputs {
            audit,
            research,
            rollups,
            rotations,
        } = inputs;
        let mut faults = Faults::new(self.status, stopped.clone());
        let tape = self.tape.map(|TapeInput { records, boot }| {
            let table = TapeTable::open(builder.files.clone(), &boot, &mut faults);
            (records, table)
        });
        let state = WriterState {
            tape,
            audit_input: audit,
            research_input: research,
            rollup_input: rollups,
            rotation_input: rotations,
            audit: AuditTables::open(&builder),
            research: ResearchTables::open(&builder),
            metrics: MetricTables::open(&builder),
            unnamed_table_rows: 0,
            retention,
            faults,
        };
        (state, stopped)
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

/// The writer on the caller's thread. Each [`InlineWriter::pump`] drains every input once.
pub struct InlineWriter {
    state: WriterState,
    stopped: StoppedTables,
}

impl InlineWriter {
    pub fn pump(&mut self) {
        while self.state.consume_batch(usize::MAX) {}
    }

    /// Drains each input, then seals each table.
    ///
    /// # Errors
    /// [`PersistError`] of the first fault of an audit table. A fault of a research table stops
    /// that table only, and the tables it stopped come back on success.
    pub fn finish(mut self) -> Result<StoppedTables, PersistError> {
        self.pump();
        self.state.seal_all();
        self.state.faults.into_result()?;
        Ok(self.stopped)
    }
}

struct WriterState {
    tape: Option<(Consumer<TapeRecord>, TapeTable)>,
    audit_input: Consumer<AuditRecord>,
    research_input: Consumer<ResearchRecord>,
    rollup_input: Option<Consumer<RollupRow>>,
    rotation_input: mpsc::Receiver<RotationRow>,
    audit: AuditTables,
    research: ResearchTables,
    metrics: MetricTables,
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
            let drained_any = self.consume_batch(MAX_POP_BATCH);
            if drain.load(Ordering::Acquire) {
                while self.consume_batch(usize::MAX) {}
                return;
            }
            if !drained_any {
                std::thread::sleep(IDLE_SLEEP);
            }
        }
    }

    /// Pops up to `limit` records from each input in turn, and says whether any input had one.
    fn consume_batch(&mut self, limit: usize) -> bool {
        let mut taped = 0_usize;
        while let Some((input, table)) = self.tape.as_mut()
            && taped < limit
            && let Ok(record) = input.pop()
        {
            table.route(record, &mut self.faults);
            taped += 1;
        }
        let mut popped = 0_usize;
        while popped < limit {
            let Ok(record) = self.audit_input.pop() else {
                break;
            };
            let routed = self.audit.route(record, &mut self.faults);
            self.after_route(routed, Seal::Audit);
            popped += 1;
        }
        let mut research = 0_usize;
        while research < limit {
            let Ok(record) = self.research_input.pop() else {
                break;
            };
            let routed = self.research.route(record, &mut self.faults);
            self.after_route(routed, Seal::Research);
            research += 1;
        }
        let mut rollups = 0_usize;
        while let Some(input) = self.rollup_input.as_mut()
            && rollups < limit
            && let Ok(row) = input.pop()
        {
            self.metrics.push(row, &mut self.faults);
            rollups += 1;
        }
        let mut rotations = 0_usize;
        while rotations < limit {
            let Ok(row) = self.rotation_input.try_recv() else {
                break;
            };
            self.audit.push_rotation(row, &mut self.faults);
            rotations += 1;
        }
        taped + popped + research + rollups + rotations > 0
    }

    /// A seal comes through its ring, so each record banked before it is already here. The fold
    /// sends one on each ring at each seal border of message time and at each park, and the prune
    /// of old files follows each one of the research ring.
    fn after_route(&mut self, routed: Routed, ring: Seal) {
        match (routed, ring) {
            (Routed::Landed, _) => {}
            (Routed::Unnamed(table), _) => self.count_unnamed_table_row(table),
            (Routed::Seal, Seal::Audit) => self.audit.seal(&mut self.faults),
            (Routed::Seal, Seal::Research) => {
                self.research.seal(&mut self.faults);
                self.metrics.seal(&mut self.faults);
                self.retention.prune();
            }
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
        if let Some((_, table)) = self.tape.as_mut() {
            table.seal(&mut self.faults);
        }
        self.audit.seal(&mut self.faults);
        self.research.seal(&mut self.faults);
        self.metrics.seal(&mut self.faults);
    }
}

/// Which ring a seal record came on, and so which tables it closes.
#[derive(Clone, Copy)]
enum Seal {
    Audit,
    Research,
}
