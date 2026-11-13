//! The one place where a run's choices of venue are made: which scale check and probe run, which
//! execution edge starts, and which actor serves each producer group.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::hot::spawn::QueueProducer;
use crate::msg::persist::RotationRow;
use crate::msg::time::EngineClock;
use crate::runtime::config::{Config, SourceSpec, VenueMarket};
use crate::runtime::registry::Registry;
use crate::runtime::shutdown::{FatalSignal, RunStateCell, TaskHandle};
use crate::venues::binance::actor::{BinanceAdapter, BinanceAdapterContext};
use crate::venues::binance::exec::{
    self as binance_exec, ExecutionPreflight, ProbeError, SignError, preflight_execution,
};
use crate::venues::binance::preflight as binance_preflight;
use crate::venues::binance::rest::BinanceEnv;
use crate::venues::edge::backoff::BackoffCaps;
use crate::venues::edge::rest_quiet::SharedRestQuiet;
use crate::venues::edge::{EdgeCheck, EdgeInputs, VenueEdge};
use crate::venues::exec::{LeaseNamespace, VenueCapabilities};
use crate::venues::polymarket::actor::{PolymarketAdapter, PolymarketAdapterContext};
use crate::venues::polymarket::exec as polymarket_exec;
use crate::venues::polymarket::exec::handle::{
    self as polymarket_handle, PolymarketPreflight, PolymarketPreflightError, preflight_polymarket,
};
use crate::venues::polymarket::preflight as polymarket_preflight;
use crate::venues::port::VenueScales;
use crate::venues::sim::{self, SimPlan, SimVenueError};

pub(crate) use crate::venues::polymarket::rotation::WindowAssignment;

/// Every way a venue refuses a run before it starts.
#[derive(thiserror::Error, Debug)]
pub enum VenueError {
    #[error(transparent)]
    BinanceScale(#[from] binance_preflight::ScaleError),
    #[error(transparent)]
    PolymarketScale(#[from] polymarket_preflight::ScaleError),
    /// One variant per venue: their probes fail for different reasons and `#[from]` serves only one.
    #[error(
        "the binance execution startup gate failed — refusing to trade without proof the key can"
    )]
    BinanceGate(#[source] ProbeError),
    #[error(
        "the polymarket execution startup gate failed — refusing to trade without proof the wallet can"
    )]
    PolymarketGate(#[source] PolymarketPreflightError),
    #[error(
        "execution.recv_window_ms is outside what the venue signs inside — every request would be rejected, which reads on the wire like a dead key rather than a bad number"
    )]
    BinanceRecvWindow(#[source] SignError),
    #[error(transparent)]
    Simulated(#[from] SimVenueError),
}

/// Reads what every instrument's venue says of its grid and order limits, for preflight to check and
/// stamp onto the registry. A venue with no instrument in this run is skipped.
pub(crate) async fn read_scales(registry: &Registry) -> Result<VenueScales, VenueError> {
    let mut scales = match registry.binance_env() {
        Some(env) => binance_preflight::preflight_scales(registry, env).await?,
        None => VenueScales::default(),
    };
    scales.polymarket = polymarket_preflight::preflight_poly(registry).await?;
    Ok(scales)
}

/// The venue this run places orders on, carrying what that venue's edge needs. The variant IS the
/// venue: bring-up dispatches on it rather than re-deriving the venue from the source. The live gates
/// are boxed — a probed REST client and a wallet key are different sizes, and this is moved, never
/// hot.
pub(crate) enum ExecutionVenue {
    Simulated,
    Binance {
        gate: Box<ExecutionPreflight>,
        /// The deployment the gate was probed against, read once from the registry.
        env: BinanceEnv,
    },
    Polymarket(Box<PolymarketPreflight>),
}

impl ExecutionVenue {
    /// The startup gate of the venue the run's source names: one signed call that proves the
    /// credentials can trade, before a strategy quotes with no permission.
    pub(crate) async fn probe(
        source: &SourceSpec,
        registry: &Registry,
        rest_quiet: SharedRestQuiet,
    ) -> Result<Self, VenueError> {
        match source {
            SourceSpec::Binance { .. } => {
                // The deployment comes from the registry, not the config, so testnet market data can
                // never front production orders.
                let env = registry
                    .binance_env()
                    .expect("a binance source stamps its deployment on the registry");
                let gate = preflight_execution(env, rest_quiet)
                    .await
                    .map_err(VenueError::BinanceGate)?;
                Ok(Self::Binance {
                    gate: Box::new(gate),
                    env,
                })
            }
            SourceSpec::Polymarket { .. } => {
                let gate = preflight_polymarket()
                    .await
                    .map_err(VenueError::PolymarketGate)?;
                Ok(Self::Polymarket(Box::new(gate)))
            }
        }
    }

    pub(crate) fn capabilities(
        &self,
        registry: &Registry,
    ) -> Result<VenueCapabilities, VenueError> {
        Ok(match self {
            Self::Simulated => sim::capabilities(),
            Self::Binance { .. } => binance_exec::capabilities(binance_preflight::order_budget(
                registry.order_rate_limits(),
            )?),
            Self::Polymarket(_) => polymarket_exec::capabilities(),
        })
    }

    /// Checks everything the venue's bring-up needs from the config and the registry, so the build
    /// at the start cannot refuse once the lease has spent a run nonce.
    pub(crate) fn check(self, check: EdgeCheck<'_>) -> Result<CheckedVenue, VenueError> {
        Ok(match self {
            Self::Simulated => CheckedVenue::Simulated(Box::new(sim::plan(check)?)),
            Self::Binance { gate, env } => CheckedVenue::Binance {
                gate,
                env,
                recv_window: binance_exec::RecvWindow::from_millis(check.execution.recv_window_ms)
                    .map_err(VenueError::BinanceRecvWindow)?,
            },
            Self::Polymarket(gate) => CheckedVenue::Polymarket(gate),
        })
    }
}

/// A venue whose bring-up preflight has checked, with what each venue carries into its edge.
pub(crate) enum CheckedVenue {
    Simulated(Box<SimPlan>),
    Binance {
        gate: Box<ExecutionPreflight>,
        env: BinanceEnv,
        recv_window: binance_exec::RecvWindow,
    },
    Polymarket(Box<PolymarketPreflight>),
}

impl CheckedVenue {
    /// Lends the venue's lease namespace to `acquire` rather than returning it, because the
    /// namespace borrows account bytes the venue holds or derives, and a secret is never copied out.
    pub(crate) fn with_lease_namespace<R>(
        &self,
        acquire: impl FnOnce(&LeaseNamespace<'_>) -> R,
    ) -> R {
        match self {
            Self::Simulated(_) => acquire(&sim::lease_namespace()),
            Self::Binance { gate, env, .. } => acquire(&binance_exec::lease_namespace(
                *env,
                gate.credentials.api_key().expose_bytes(),
            )),
            Self::Polymarket(gate) => {
                let signer = gate.wallet.signer.to_checksum_hex();
                acquire(&polymarket_exec::lease_namespace(&signer))
            }
        }
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Simulated(_) => "the simulated binance spot venue".to_owned(),
            Self::Binance { env, .. } => format!("binance {}", env.as_str()),
            Self::Polymarket(_) => "polymarket".to_owned(),
        }
    }

    /// Only a venue that rotates markets reads `assignments`; every other venue drops it.
    pub(crate) fn bring_up(
        self,
        inputs: EdgeInputs<'_>,
        assignments: mpsc::Receiver<WindowAssignment>,
    ) -> VenueEdge {
        match self {
            Self::Simulated(plan) => sim::bring_up(*plan, inputs),
            Self::Binance {
                gate,
                env,
                recv_window,
            } => binance_exec::bring_up(*gate, env, recv_window, inputs),
            Self::Polymarket(gate) => polymarket_handle::bring_up(*gate, inputs, assignments),
        }
    }
}

/// The venue that `source` names, and how long its order client waits for an answer.
pub(crate) fn order_client_timeout(source: &SourceSpec) -> (&'static str, Duration) {
    match source {
        SourceSpec::Binance { .. } => ("binance", binance_exec::ORDER_REQUEST_TIMEOUT),
        SourceSpec::Polymarket { .. } => ("polymarket", polymarket_exec::ORDER_REQUEST_TIMEOUT),
    }
}

pub(crate) fn tap_heartbeat<P>(config: &Config<P>) -> Duration {
    let sim = config
        .sources
        .iter()
        .find_map(|source| source.execution.as_ref()?.sim)
        .unwrap_or_default();
    Duration::from_millis(sim.heartbeat_ms)
}

pub(crate) struct MarketDataWiring<'a> {
    pub(crate) registry: &'a Registry,
    pub(crate) producers: Vec<QueueProducer>,
    pub(crate) clock: &'a EngineClock,
    pub(crate) fatal: &'a FatalSignal,
    /// Adapters hold their sockets down according to this state. The execution edge reads
    /// it freely, with no restriction of its own.
    pub(crate) desired_run_state: &'a RunStateCell,
    pub(crate) rotations_tx: &'a mpsc::Sender<RotationRow>,
    /// Rotation bindings for the execution edge; absent when nothing is listening for them.
    pub(crate) window_assignments: Option<mpsc::Sender<WindowAssignment>>,
    pub(crate) tap_heartbeat: Duration,
    /// One window for the whole Binance deployment: its actors and the signed order client draw on
    /// a single per-IP allowance, so a rate limit any of them earns has to quiet all of them.
    pub(crate) rest_quiet: SharedRestQuiet,
    pub(crate) tokio_handle: &'a tokio::runtime::Handle,
}

/// One market-data actor per producer group. Spawned last, so every ring an actor writes into
/// already has its consumer.
pub(crate) fn spawn_market_data(wiring: MarketDataWiring<'_>) -> Vec<TaskHandle> {
    let MarketDataWiring {
        registry,
        producers,
        clock,
        fatal,
        desired_run_state,
        rotations_tx,
        window_assignments,
        tap_heartbeat,
        rest_quiet,
        tokio_handle,
    } = wiring;
    let mut handles = Vec::with_capacity(producers.len());
    for (producer, group) in producers.into_iter().zip(registry.producer_groups()) {
        match group.market {
            VenueMarket::Binance(market) => {
                let context = BinanceAdapterContext {
                    // A Binance producer group implies the source config chose a Binance
                    // deployment, so these stay aligned.
                    env: registry
                        .binance_env()
                        .expect("a binance producer group implies a binance source"),
                    clock: clock.clone(),
                    fatal: fatal.clone(),
                    run_state: desired_run_state.clone(),
                    backoff: BackoffCaps::default(),
                    tap_heartbeat,
                    rest_quiet: rest_quiet.clone(),
                };
                handles.push(BinanceAdapter::spawn(
                    group,
                    market,
                    registry.instruments(),
                    producer,
                    context,
                    tokio_handle,
                ));
            }
            VenueMarket::Polymarket(series) => {
                let context = PolymarketAdapterContext {
                    clock: clock.clone(),
                    fatal: fatal.clone(),
                    run_state: desired_run_state.clone(),
                    backoff: BackoffCaps::default(),
                    rotations_tx: rotations_tx.clone(),
                    window_assignments: window_assignments.clone(),
                };
                handles.push(PolymarketAdapter::spawn(
                    group,
                    series,
                    registry.instruments(),
                    producer,
                    context,
                    tokio_handle,
                ));
            }
        }
    }
    handles
}
