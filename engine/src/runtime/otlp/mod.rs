//! The OTLP export of a run: a copy of each log line and each latency row goes to a collector on
//! this host, from a thread of its own, so a slow or absent collector never holds up the engine.

mod lanes;
mod plan;
mod send;
mod wire;

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::error;

pub use lanes::{OtlpLanes, OtlpRows};
pub use plan::{OtlpError, OtlpPlan};
pub use send::{OtlpExporter, OtlpReport};

const SEND_INTERVAL: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT_MS: u64 = 500;
const REQUEST_TIMEOUT: Duration = Duration::from_millis(REQUEST_TIMEOUT_MS);
/// The pass in flight, then one last pass. Each pass ends within one request limit, because its
/// two requests run at the same time.
const STOP_LIMIT_MS: u64 = 2 * REQUEST_TIMEOUT_MS;
const ROWS_LANE_CAPACITY: usize = 1_024;

impl OtlpPlan {
    /// Opens the lanes into the exporter. The metrics thread writes its rows into the returned
    /// `OtlpRows`.
    pub fn open(&self) -> (OtlpRows, OtlpLanes) {
        let (rows, row_reader) = lanes::rows_lane(ROWS_LANE_CAPACITY);
        let lanes = OtlpLanes {
            plan: self.clone(),
            rows: row_reader,
        };
        (rows, lanes)
    }
}

impl OtlpLanes {
    /// Starts the exporter thread, which does not pin. `None` when the OS refuses the thread: the
    /// run goes on with no export, and one ERROR line says so.
    pub fn start(self) -> Option<OtlpExporter> {
        let (stop, stopped) = mpsc::channel();
        let spawned = thread::Builder::new()
            .name("polysim-otlp".to_owned())
            .spawn(move || send::run(self, &stopped));
        match spawned {
            Ok(join) => Some(OtlpExporter { stop, join }),
            Err(refused) => {
                error!("otlp export is off for this run: its thread could not start: {refused}");
                None
            }
        }
    }
}

impl OtlpExporter {
    /// Wakes the exporter at once: it ends the pass in flight, sends what waits in one last pass,
    /// and returns its counts, within `STOP_LIMIT_MS`. A thread that panicked sent nothing more.
    pub fn stop(self) -> OtlpReport {
        let OtlpExporter { stop, join } = self;
        drop(stop);
        join.join().unwrap_or_default()
    }
}
