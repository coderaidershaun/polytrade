//! How the runtime starts this actor: the setup it is built from, and the edge handed back for the
//! runtime to spawn once the hot thread runs.

use tokio::runtime::Handle;

use crate::adapters::binance::rest::{BinanceEnv, SignedRestClient};
use crate::adapters::edge::{EdgeInputs, EdgeSettings, VenueEdge, run_edge};
use crate::adapters::exec::{EdgeHandle, EngineIdentity};
use crate::hot::spawn::QueueProducer;
use crate::msg::exec::ExecLaneItem;
use crate::registry::{AssetDictionary, InstrumentRow};
use crate::secrets::Credentials;
use crate::time::DurationUs;

use super::super::{ExecutionPreflight, RecvWindow, SignError};
use super::Actor;

pub(super) struct BinanceExecAdapterSetup {
    pub(super) instruments: Vec<InstrumentRow>,
    pub(super) assets: AssetDictionary,
    // The WS API signs api_key as a parameter, so it cannot borrow the REST client's copy.
    pub(super) credentials: Credentials,
    // Shares one weight budget and clock offset with the REST client, since the venue
    // charges all calls against the same IP.
    pub(super) rest: SignedRestClient,
    pub(super) commands: rtrb::Consumer<ExecLaneItem>,
    pub(super) producer: QueueProducer,
    pub(super) env: BinanceEnv,
    pub(super) identity: EngineIdentity,
    pub(super) recv_window: RecvWindow,
    // Clock skew beyond recvWindow makes the venue refuse every signed request.
    pub(super) loud_clock_skew: DurationUs,
    pub(super) edge: EdgeSettings,
}

/// # Errors
/// [`SignError`] when `execution.recv_window_ms` is outside what the venue signs inside.
pub(crate) fn bring_up(
    gate: ExecutionPreflight,
    env: BinanceEnv,
    inputs: EdgeInputs<'_>,
) -> Result<VenueEdge, SignError> {
    let EdgeInputs {
        registry,
        execution,
        identity,
        commands,
        producer,
        settings,
        ..
    } = inputs;
    let setup = BinanceExecAdapterSetup {
        instruments: registry.instruments().to_vec(),
        assets: registry.assets().clone(),
        credentials: gate.credentials,
        rest: gate.rest,
        commands,
        producer,
        env,
        identity,
        recv_window: RecvWindow::from_millis(execution.recv_window_ms)?,
        loud_clock_skew: DurationUs::from_millis(execution.max_clock_skew_ms as i64),
        edge: settings,
    };
    Ok(VenueEdge {
        spawn: Box::new(move |runtime: &Handle| spawn(setup, runtime)),
        tap_gate: None,
    })
}

fn spawn(setup: BinanceExecAdapterSetup, rt: &Handle) -> EdgeHandle {
    let sweep_deadline = setup.edge.sweep_deadline;
    EdgeHandle::spawn("binance execution adapter", sweep_deadline, |stop| {
        let actor = Actor::new(setup, stop, rt);
        rt.spawn(crate::log::tag_task("binance-exec", run_edge(actor)))
    })
}
