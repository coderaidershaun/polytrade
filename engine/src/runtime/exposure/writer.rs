//! Writer thread: drain ring to newest snapshot, write to disk on change. Dedicated thread (blocks
//! on fsync). A writer that cannot write its limit of times in a row reports it once, and quoting
//! halts.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::hot::sink::{ExposureReceiver, ExposureSink};
use crate::hot::spawn::QueueProducer;
use crate::msg::failure::FailureClass;
use crate::msg::ids::{AssetId, FIXED_SCALE, InstrumentId};
use crate::msg::inbound::{FileWriter, InboundMessage, MessageBody, WriterFailure};
use crate::msg::time::{EngineClock, wall_clock_now};
use crate::runtime::config::{ExecutionMode, RunIdentity};
use crate::runtime::registry::Registry;
use crate::runtime::shutdown::WriterThread;
use crate::{error, info};

use super::file::{self, AssetEntry, ExposureDocument, FORMAT_VERSION, InstrumentEntry};
use super::{ExposureError, ExposureSnapshot, ExposureState, add_amount, asset_amounts, file_path};

/// Ring capacity: absorb burst between polls (absolute state, sink keeps newest).
const RING_CAPACITY: usize = 16;

/// Poll interval: 10x/sec bounds disk lag while keeping fsync rate low. A failed write is tried
/// again at the next poll, with no growing delay: a longer wait would only delay the halt.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// What a writer needs from the start. The registry gives the venue and symbol of each instrument
/// and the name of each asset, so the file names each one in words that outlive the run.
pub struct ExposureWriterSetup<'a> {
    pub dir: PathBuf,
    pub identity: RunIdentity,
    pub mode: Option<ExecutionMode>,
    pub registry: &'a Registry,
    /// `None` with execution off, where no quote can halt: the writer then only logs each failure.
    pub status: Option<WriterStatus>,
    /// The state on disk, which seeds the writer.
    pub restored: &'a ExposureState,
    pub lane: ExposureReceiver,
}

/// Where the writer reports that it gave up, and after how many failed writes in a row.
pub struct WriterStatus {
    pub queue: QueueProducer,
    /// The writer stamps its message with the clock of the engine, as each producer of an input
    /// queue does, so the message takes its place in the order of the input.
    pub clock: EngineClock,
    pub max_failed_writes: u32,
}

impl WriterStatus {
    #[expect(
        clippy::disallowed_methods,
        reason = "a writer that gives up stamps its one message as each producer of an input queue does"
    )]
    fn report(mut self, consecutive_failures: u32) {
        let failure = WriterFailure {
            writer: FileWriter::Exposure,
            consecutive_failures,
        };
        let message = InboundMessage::new(self.clock.now(), MessageBody::WriterFailed(failure));
        self.queue.push(message);
    }
}

/// Writer config (owned outright, thread borrows nothing).
struct ExposureWriterConfig {
    dir: PathBuf,
    identity: RunIdentity,
    mode: Option<ExecutionMode>,
    instruments: Vec<InstrumentFacts>,
    asset_names: Vec<Box<str>>,
    /// Taken when the writer reports, so it reports once in a run.
    status: Option<WriterStatus>,
}

impl ExposureWriterConfig {
    fn new(
        dir: PathBuf,
        identity: RunIdentity,
        mode: Option<ExecutionMode>,
        registry: &Registry,
        status: Option<WriterStatus>,
    ) -> Self {
        ExposureWriterConfig {
            dir,
            identity,
            mode,
            instruments: registry
                .instruments()
                .iter()
                .map(|row| InstrumentFacts {
                    venue: registry.venue_name(row.venue),
                    symbol: row.venue_symbol.clone(),
                    base_asset: row.base_asset,
                    quote_asset: row.quote_asset,
                })
                .collect(),
            asset_names: registry
                .assets()
                .entries()
                .iter()
                .map(|(_, name)| name.clone())
                .collect(),
            status,
        }
    }
}

struct InstrumentFacts {
    venue: &'static str,
    symbol: Box<str>,
    base_asset: AssetId,
    quote_asset: AssetId,
}

pub struct ExposureWriter;

impl ExposureWriter {
    /// The lane from the hot thread to the writer, with room for `instruments` instruments in each
    /// snapshot. The start makes it before the hot thread, and hands its read end to the writer
    /// once the writer-status queue exists.
    pub fn lane(instruments: usize) -> (ExposureSink, ExposureReceiver) {
        ExposureSink::lane(RING_CAPACITY, instruments)
    }

    pub fn spawn(setup: ExposureWriterSetup<'_>) -> ExposureHandle {
        let ExposureWriterSetup {
            dir,
            identity,
            mode,
            registry,
            status,
            restored,
            lane,
        } = setup;
        let mut latest = ExposureSnapshot::for_instruments(registry.instruments().len());
        latest.set_rows(restored.instruments().iter().copied());
        let state = WriterState {
            config: ExposureWriterConfig::new(dir, identity, mode, registry, status),
            lane,
            latest,
            // Sequence 0 is the state already on disk. Every snapshot the hot side emits starts at
            // 1, so the first genuine change is the first write.
            written_seq: Some(0),
            failed_writes: 0,
        };
        ExposureHandle(WriterThread::spawn("exposure", move |drain| {
            state.run(&drain)
        }))
    }
}

pub struct ExposureHandle(WriterThread<ExposureError>);

impl ExposureHandle {
    /// Call once the hot thread has stopped, so the snapshot drained here is the run's last.
    ///
    /// # Errors
    /// [`ExposureError`] if writing or joining the writer fails.
    pub async fn drain(self) -> Result<(), ExposureError> {
        self.0
            .drain(
                |payload| ExposureError::WriterPanicked { payload },
                ExposureError::DrainInterrupted,
            )
            .await
    }
}

struct WriterState {
    config: ExposureWriterConfig,
    lane: ExposureReceiver,
    latest: ExposureSnapshot,
    /// Sequence of last write (None until first, so static position still writes once).
    written_seq: Option<u64>,
    failed_writes: u32,
}

impl WriterState {
    fn run(mut self, drain: &AtomicBool) -> Result<(), ExposureError> {
        crate::log::register_thread("exposure");
        loop {
            self.take_latest();
            if drain.load(Ordering::Acquire) {
                self.take_latest();
                return self.write_if_changed();
            }
            if let Err(error) = self.write_if_changed() {
                self.on_failed_write(&error);
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// The root of the writer reads the class of each failure: a write is tried again at the next
    /// poll, and at the limit the writer reports once, so quoting halts.
    fn on_failed_write(&mut self, error: &ExposureError) {
        assert_eq!(
            error.failure_class(),
            FailureClass::Retry,
            "a write of the exposure file gave {error}, and only a failed write is tried again here"
        );
        self.failed_writes = self.failed_writes.saturating_add(1);
        let failed_writes = self.failed_writes;
        error!("exposure write failed ({failed_writes} in a row): {error}");
        let Some(status) = self
            .config
            .status
            .take_if(|status| failed_writes >= status.max_failed_writes)
        else {
            return;
        };
        error!(
            "exposure file not written {failed_writes} times in a row — it no longer follows the position, so quoting halts"
        );
        status.report(failed_writes);
    }

    /// Keep only newest snapshot (earlier = same state, old time; writing all = unnecessary fsyncs).
    fn take_latest(&mut self) {
        while self.lane.pop_into(&mut self.latest) {}
    }

    fn write_if_changed(&mut self) -> Result<(), ExposureError> {
        if self.written_seq == Some(self.latest.seq) {
            return Ok(());
        }
        let path = file_path(&self.config.dir, &self.config.identity, self.config.mode);
        file::write_atomically(&path, &self.document())?;
        self.failed_writes = 0;
        let is_first = self.written_seq.is_none();
        self.written_seq = Some(self.latest.seq);
        if is_first {
            info!("exposure: writing {}", path.display());
        }
        Ok(())
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "a writer stamps and paces its own files, which are never engine state"
    )]
    fn document(&self) -> ExposureDocument {
        ExposureDocument {
            version: FORMAT_VERSION,
            strategy_id: self.config.identity.strategy_id.as_str().to_owned(),
            te_id: self.config.identity.te_id.as_str().to_owned(),
            written_ts_us: wall_clock_now().micros(),
            seq: self.latest.seq,
            fixed_scale: FIXED_SCALE,
            instruments: self.instrument_entries(),
            assets: self.asset_entries(),
            last_exposure_quote: self.latest.exposure_quote,
        }
    }

    fn instrument_entries(&self) -> Vec<InstrumentEntry> {
        self.latest
            .active()
            .iter()
            .map(|exposure| {
                let facts = self.facts(exposure.instrument);
                InstrumentEntry {
                    venue: facts.venue.to_owned(),
                    symbol: facts.symbol.as_ref().to_owned(),
                    position_base: exposure.position_base.mantissa(),
                    cash_quote: exposure.cash_quote,
                    basis_quote: exposure.basis_quote,
                }
            })
            .collect()
    }

    fn asset_entries(&self) -> Vec<AssetEntry> {
        let mut amounts = vec![0i128; self.config.asset_names.len()];
        for exposure in self.latest.active() {
            let facts = self.facts(exposure.instrument);
            add_amount(
                &mut amounts,
                facts.base_asset,
                exposure.position_base.mantissa(),
            );
            add_amount(&mut amounts, facts.quote_asset, exposure.cash_quote);
        }
        // Same fold as load path; human section can never disagree with recomputed.
        asset_amounts(&amounts)
            .into_iter()
            .map(|entry| AssetEntry {
                asset: self.asset_name(entry.asset),
                amount: entry.amount,
            })
            .collect()
    }

    fn asset_name(&self, asset: AssetId) -> String {
        self.config
            .asset_names
            .get(usize::from(asset.0))
            .map_or_else(
                || format!("asset-{}", asset.0),
                |name| name.as_ref().to_owned(),
            )
    }

    /// # Panics
    /// A row of an instrument that this run's registry does not hold. The hot side numbers its rows
    /// by that registry, so such a row is a bug, and a file written without it would erase its
    /// position.
    fn facts(&self, instrument: InstrumentId) -> &InstrumentFacts {
        self.config
            .instruments
            .get(usize::from(instrument.0))
            .unwrap_or_else(|| {
                panic!(
                    "exposure row names instrument {}, and this run's registry holds {} instruments",
                    instrument.0,
                    self.config.instruments.len()
                )
            })
    }
}
