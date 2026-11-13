//! The start checks of a run, each before any thread: a refused start has nothing to stop, spends no
//! run nonce, and writes no file. The lease is the last check.

use crate::hot::strategy::Strategy;
use crate::info;
use crate::runtime::config::{Config, ExecutionMode, RunIdentity, SourceSpec};
use crate::runtime::exposure::{self, ExposureState};
use crate::runtime::registry::{InstrumentRow, Registry};
use crate::venues::edge::rest_quiet::SharedRestQuiet;
use crate::venues::port::{NetworkPort, VenuePort};

use super::StartError;
use super::exec::{ExecCheck, ExecPlan, check_execution};
use super::link::{BoundLink, link_bring_up};

/// The checks of one start: the run, its parsed config, and the strategy, whose features and link
/// topics the link checks read.
pub struct Preflight<'a, P> {
    pub identity: &'a RunIdentity,
    pub config: &'a Config<P>,
    pub strategy: &'a dyn Strategy,
}

/// What preflight proved and built, handed whole to the start. Nothing in it runs a thread.
pub struct StartPlan {
    pub(super) identity: RunIdentity,
    /// The caller's mode, never derived again at the start: it picks which position file this run
    /// owns, and a second derivation could load one ledger while the writer keeps another.
    pub(super) mode: Option<ExecutionMode>,
    pub(super) registry: Registry,
    /// Position from previous run; empty on cold start.
    pub(super) exposure: ExposureState,
    /// A venue can charge market-data reads and signed order traffic against one shared allowance,
    /// so the window is built where the signed client is, and handed on to the adapters.
    pub(super) rest_quiet: SharedRestQuiet,
    /// `None` with no `link:` block.
    pub(super) link: Option<BoundLink>,
    /// `None` unless execution is armed.
    pub(super) execution: Option<ExecPlan>,
}

impl<P> Preflight<'_, P> {
    /// Runs each check against the venues themselves. Their reads run on one current-thread
    /// runtime, which ends before this returns.
    ///
    /// # Errors
    /// The [`StartError`] of the first check that refuses, or [`StartError::Runtime`] when that
    /// runtime cannot be built.
    pub fn run(self) -> Result<StartPlan, StartError> {
        let mut port = NetworkPort::new().map_err(StartError::Runtime)?;
        self.run_with(&mut port)
    }

    /// # Errors
    /// The [`StartError`] of the first check that refuses. The lease is the last check, so each
    /// other refusal leaves the run nonce unspent.
    pub fn run_with(self, port: &mut dyn VenuePort) -> Result<StartPlan, StartError> {
        let Preflight {
            identity,
            config,
            strategy,
        } = self;
        let mode = config.execution.as_ref().map(|execution| execution.mode);
        let mut registry = Registry::build(config)?;
        if let Some(core_id) = config.engine.hot_core_id {
            validate_core_id(core_id)?;
        }
        // Every venue's grid is checked against the deployment the registry names rather than a
        // constant, so a production run is checked against the production grid.
        port.read_scales(&registry)?.stamp(&mut registry);
        if let Some(execution) = &config.execution {
            check_execution_order_capacity(&registry, execution.max_orders_per_side)?;
        }
        let exposure = exposure::load(&config.exposure.dir, identity, &registry, mode)?;
        let rest_quiet = SharedRestQuiet::new();
        // One signed call that proves the key can trade, before a strategy quotes with no
        // permission.
        let probed = live_execution_source(config)
            .map(|source| port.probe(source, &registry, &rest_quiet))
            .transpose()?;
        let link = link_bring_up(config, identity, strategy, &registry)?;
        let execution = check_execution(ExecCheck {
            execution: config.execution.as_ref(),
            venue: probed.map(|probed| probed.0),
            registry: &registry,
            identity,
            spin_interval: config.engine.spin_interval(),
            input_capacity: config.queues.input_capacity,
            lease_dir: &config.exposure.dir,
        })?;
        Ok(StartPlan {
            identity: identity.clone(),
            mode,
            registry,
            exposure,
            rest_quiet,
            link,
            execution,
        })
    }
}

impl StartPlan {
    /// Preflight runs before the log exists, so the start writes what it found once the log does.
    pub(super) fn log_report(&self) {
        info!(
            "registry: {} instruments, {} input queues",
            self.registry.instruments().len(),
            self.registry.input_queue_count()
        );
        report_restored(&self.exposure, &self.registry);
        if let Some(link) = &self.link {
            let address = link
                .socket
                .local_addr()
                .map_or_else(|error| error.to_string(), |address| address.to_string());
            info!(
                "link bound on {address} — {} peer subscription(s)",
                link.peers.len()
            );
        }
    }
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
/// [`StartError::ExecutionOrderCapacity`] on the first instrument whose ladder overruns.
fn check_execution_order_capacity(
    registry: &Registry,
    max_orders_per_side: u32,
) -> Result<(), StartError> {
    for row in registry.instruments() {
        check_symbol_order_capacity(row, max_orders_per_side)?;
    }
    Ok(())
}

/// The per-instrument half of the startup order-capacity check. It does no I/O.
///
/// # Errors
/// [`StartError::ExecutionOrderCapacity`] when both sides of the ladder exceed the venue ceiling.
pub fn check_symbol_order_capacity(
    row: &InstrumentRow,
    max_orders_per_side: u32,
) -> Result<(), StartError> {
    let Some(venue_max) = row.max_num_orders else {
        return Ok(());
    };
    let required_total = max_orders_per_side.saturating_mul(2);
    if required_total <= venue_max {
        return Ok(());
    }
    Err(StartError::ExecutionOrderCapacity {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        configured_per_side: max_orders_per_side,
        required_total,
        venue_max,
    })
}

/// Reports the position carried in for each asset. A cold start says so, so that a silent log never
/// hides a failed load.
fn report_restored(exposure: &ExposureState, registry: &Registry) {
    if exposure.is_empty() {
        info!("exposure: cold start — no position carried in from a previous run");
        return;
    }
    for asset in exposure.assets() {
        info!(
            "exposure restored: {} {}",
            registry.assets().name(asset.asset).unwrap_or("<unknown>"),
            crate::msg::ids::Qty::from_mantissa(asset.amount).to_f64()
        );
    }
}

/// Reject unavailable core so mis-set config fails at startup, not silently. Best-effort platforms skip.
fn validate_core_id(core_id: usize) -> Result<(), StartError> {
    match core_affinity::get_core_ids() {
        Some(ids) if !ids.iter().any(|core| core.id == core_id) => Err(StartError::CoreId {
            core_id,
            available: ids.len(),
        }),
        _ => Ok(()),
    }
}
