//! Startup steps before threads spawn: build registry, validate core, build tokio runtime, run preflight.
//! Everything that can refuse a run happens here, so a failure returns with nothing to tear down.

use tokio::runtime::Runtime;

use crate::adapters::rest_quiet::SharedRestQuiet;
use crate::adapters::venue::{self, ExecutionVenue};
use crate::config::{Config, ExecutionMode, RunIdentity, SourceSpec};
use crate::exposure::{self, ExposureState};
use crate::info;
use crate::registry::{InstrumentRow, Registry};

use super::EngineError;

/// Startup results grouped as struct to avoid tuple-member mistakes.
pub(super) struct Prepared {
    pub(super) registry: Registry,
    pub(super) runtime: Runtime,
    /// Position from previous run; empty on cold start.
    pub(super) exposure: ExposureState,
    /// None unless execution.mode needs credentials.
    pub(super) execution: Option<ExecutionVenue>,
    /// A venue can charge market-data reads and signed order traffic against one shared allowance,
    /// so the window is built here — where the signed client is — and handed on to the adapters.
    pub(super) rest_quiet: SharedRestQuiet,
}

/// `mode` is the caller's, never re-derived here: it picks which position file this run owns, and a
/// second derivation could load one ledger while the writer keeps another.
pub(super) fn prepare<P>(
    config: &Config<P>,
    identity: &RunIdentity,
    mode: Option<ExecutionMode>,
) -> Result<Prepared, EngineError> {
    let mut registry = Registry::build(config)?;
    info!(
        "registry: {} instruments, {} input queues",
        registry.instruments().len(),
        registry.input_queue_count()
    );
    if let Some(core_id) = config.engine.hot_core_id {
        validate_core_id(core_id)?;
    }
    let runtime = build_runtime(config.engine.tokio_workers)?;
    // Every venue's grid is checked before any thread starts, against the deployment the registry
    // names rather than a constant, so a production run is checked against the production grid.
    runtime.block_on(venue::preflight_scales(&mut registry))?;
    if let Some(execution) = &config.execution {
        check_execution_order_capacity(&registry, execution.max_orders_per_side)?;
    }
    // Last gate before threads: unreadable position must stop here, not after engine begins.
    let exposure = exposure::load(&config.exposure.dir, identity, &registry, mode)?;
    report_restored(&exposure, &registry);
    let rest_quiet = SharedRestQuiet::new();
    // Last: one signed call that proves the key can trade, before strategy quotes with no permission.
    let execution = live_execution_source(config)
        .map(|source| {
            runtime.block_on(ExecutionVenue::probe(source, &registry, rest_quiet.clone()))
        })
        .transpose()?;
    Ok(Prepared {
        registry,
        runtime,
        exposure,
        execution,
        rest_quiet,
    })
}

/// The source whose venue this run will place orders on, or None when nothing is armed for live.
fn live_execution_source<P>(config: &Config<P>) -> Option<&SourceSpec> {
    config
        .execution
        .as_ref()
        .filter(|execution| execution.mode.needs_credentials())
        .map(|_| &config.source)
}

/// Confirms the configured ladder fits every instrument's venue order ceiling before any thread
/// starts.
///
/// # Errors
/// [`EngineError::ExecutionOrderCapacity`] on the first instrument whose ladder overruns.
fn check_execution_order_capacity(
    registry: &Registry,
    max_orders_per_side: u32,
) -> Result<(), EngineError> {
    for row in registry.instruments() {
        check_symbol_order_capacity(row, max_orders_per_side)?;
    }
    Ok(())
}

/// The per-instrument half of the startup order-capacity check. It does no I/O.
///
/// # Errors
/// [`EngineError::ExecutionOrderCapacity`] when both sides of the ladder exceed the venue ceiling.
pub fn check_symbol_order_capacity(
    row: &InstrumentRow,
    max_orders_per_side: u32,
) -> Result<(), EngineError> {
    let Some(venue_max) = row.max_num_orders else {
        return Ok(());
    };
    let required_total = max_orders_per_side.saturating_mul(2);
    if required_total <= venue_max {
        return Ok(());
    }
    Err(EngineError::ExecutionOrderCapacity {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        configured_per_side: max_orders_per_side,
        required_total,
        venue_max,
    })
}

/// Report what was carried in per asset. Cold start says so explicitly: silence = failed load.
fn report_restored(exposure: &ExposureState, registry: &Registry) {
    if exposure.is_empty() {
        info!("exposure: cold start — no position carried in from a previous run");
        return;
    }
    for asset in exposure.assets() {
        info!(
            "exposure restored: {} {}",
            registry.assets().name(asset.asset).unwrap_or("<unknown>"),
            crate::ids::Qty(asset.amount).to_f64()
        );
    }
}

/// Reject unavailable core so mis-set config fails at startup, not silently. Best-effort platforms skip.
fn validate_core_id(core_id: usize) -> Result<(), EngineError> {
    match core_affinity::get_core_ids() {
        Some(ids) if !ids.iter().any(|core| core.id == core_id) => Err(EngineError::CoreId {
            core_id,
            available: ids.len(),
        }),
        _ => Ok(()),
    }
}

fn build_runtime(workers: usize) -> Result<Runtime, EngineError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.max(1))
        .enable_all()
        .thread_name("polysim-tokio")
        .build()
        .map_err(EngineError::Runtime)
}
