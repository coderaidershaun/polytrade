//! Writer thread: drain ring to newest snapshot, write to disk on change. Dedicated thread (blocks
//! on fsync).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rtrb::{Consumer, RingBuffer};

use crate::hot::sink::ExposureSink;
use crate::msg::ids::{AssetId, FIXED_SCALE, InstrumentId};
use crate::msg::time::wall_clock_now;
use crate::runtime::config::{ExecutionMode, RunIdentity};
use crate::runtime::registry::Registry;
use crate::runtime::shutdown::WriterThread;
use crate::{error, info};

use super::file::{self, AssetEntry, ExposureDocument, FORMAT_VERSION, InstrumentEntry};
use super::{
    ExposureError, ExposureSnapshot, ExposureState, MAX_EXPOSURE_INSTRUMENTS, add_amount,
    asset_amounts, file_path, venue_of,
};

/// Ring capacity: absorb burst between polls (absolute state, sink keeps newest).
const RING_CAPACITY: usize = 16;

/// Poll interval: 10x/sec bounds disk lag while keeping fsync rate low.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Writer config (owned outright, thread borrows nothing).
pub struct ExposureWriterConfig {
    dir: PathBuf,
    identity: RunIdentity,
    mode: Option<ExecutionMode>,
    instruments: Vec<InstrumentFacts>,
    asset_names: Vec<Box<str>>,
}

impl ExposureWriterConfig {
    pub fn new(
        dir: PathBuf,
        identity: RunIdentity,
        mode: Option<ExecutionMode>,
        registry: &Registry,
    ) -> Self {
        ExposureWriterConfig {
            dir,
            identity,
            mode,
            instruments: registry
                .instruments()
                .iter()
                .map(|row| InstrumentFacts {
                    venue: venue_of(row.market),
                    symbol: row.venue_symbol.clone(),
                    base_asset: row.base_asset,
                    quote_asset: row.quote_asset,
                })
                .collect(),
            asset_names: registry.assets().names().to_vec(),
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
    /// Spawn writer + ring; seed with restored state (disk match needed).
    pub fn spawn(
        config: ExposureWriterConfig,
        restored: &ExposureState,
    ) -> (ExposureHandle, ExposureSink) {
        let (producer, consumer) = RingBuffer::<ExposureSnapshot>::new(RING_CAPACITY);
        let state = WriterState {
            config,
            consumer,
            latest: seed_snapshot(restored),
            // Sequence 0 is the state already on disk. Every snapshot the hot side emits starts at
            // 1, so the first genuine change is the first write.
            written_seq: Some(0),
            failed_writes: 0,
        };
        let writer = WriterThread::spawn("exposure", move |drain| state.run(&drain));
        (ExposureHandle(writer), ExposureSink::new(producer))
    }
}

/// Restored rows as initial snapshot (excess dropped: writer wrote file, can't hold more).
fn seed_snapshot(restored: &ExposureState) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::EMPTY;
    for (slot, exposure) in snapshot
        .instruments
        .iter_mut()
        .zip(restored.instruments().iter())
    {
        *slot = *exposure;
    }
    snapshot.len = restored.instruments().len().min(MAX_EXPOSURE_INSTRUMENTS) as u8;
    snapshot
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
    consumer: Consumer<ExposureSnapshot>,
    latest: ExposureSnapshot,
    /// Sequence of last write (None until first, so static position still writes once).
    written_seq: Option<u64>,
    failed_writes: u64,
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
            // Failures retry on next change (full disk can't stop trading, position in memory).
            // SMELL: a failed write is only counted and logged, and the hot thread never learns of
            // it, so the engine trades on without end while the file on disk goes stale. A failure
            // that lasts past a limit should stop quoting.
            if let Err(error) = self.write_if_changed() {
                self.failed_writes += 1;
                error!(
                    "exposure write failed ({} so far): {error}",
                    self.failed_writes
                );
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// Keep only newest snapshot (earlier = same state, old time; writing all = unnecessary fsyncs).
    fn take_latest(&mut self) {
        while let Ok(snapshot) = self.consumer.pop() {
            self.latest = snapshot;
        }
    }

    fn write_if_changed(&mut self) -> Result<(), ExposureError> {
        if self.written_seq == Some(self.latest.seq) {
            return Ok(());
        }
        let path = file_path(&self.config.dir, &self.config.identity, self.config.mode);
        file::write_atomically(&path, &self.document())?;
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

    /// Drop rows for unknown instruments (unresolvable symbol on boot = refuse).
    fn instrument_entries(&self) -> Vec<InstrumentEntry> {
        self.latest
            .active()
            .iter()
            .filter_map(|exposure| {
                let facts = self.facts(exposure.instrument)?;
                Some(InstrumentEntry {
                    venue: facts.venue.to_owned(),
                    symbol: facts.symbol.as_ref().to_owned(),
                    position_base: exposure.position_base.mantissa(),
                    cash_quote: exposure.cash_quote,
                    basis_quote: exposure.basis_quote,
                })
            })
            .collect()
    }

    fn asset_entries(&self) -> Vec<AssetEntry> {
        let mut amounts = vec![0i128; self.config.asset_names.len()];
        for exposure in self.latest.active() {
            let Some(facts) = self.facts(exposure.instrument) else {
                continue;
            };
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

    fn facts(&self, instrument: InstrumentId) -> Option<&InstrumentFacts> {
        self.config.instruments.get(usize::from(instrument.0))
    }
}
