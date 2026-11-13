//! How the runtime starts this actor: the setup it is built from, and the edge handed back for the
//! runtime to spawn once the hot thread runs.

use tokio::runtime::Handle;

use crate::hot::spawn::QueueProducer;
use crate::msg::time::DurationUs;
use crate::runtime::registry::{AssetDictionary, InstrumentRow};
use crate::runtime::secrets::Credentials;
use crate::venues::binance::rest::{BinanceEnv, SignedRestClient};
use crate::venues::edge::{EdgeInputs, EdgeLane, EdgeSettings, VenueEdge, run_edge};
use crate::venues::exec::{EdgeHandle, EngineIdentity};

use super::super::{ExecutionPreflight, RecvWindow};
use super::Actor;

pub(super) struct BinanceExecAdapterSetup {
    pub(super) instruments: Vec<InstrumentRow>,
    pub(super) assets: AssetDictionary,
    // The WS API signs api_key as a parameter, so it cannot borrow the REST client's copy.
    pub(super) credentials: Credentials,
    // Shares one weight budget and clock offset with the REST client, since the venue
    // charges all calls against the same IP.
    pub(super) rest: SignedRestClient,
    pub(super) producer: QueueProducer,
    pub(super) env: BinanceEnv,
    pub(super) identity: EngineIdentity,
    pub(super) recv_window: RecvWindow,
    // Clock skew beyond recvWindow makes the venue refuse every signed request.
    pub(super) loud_clock_skew: DurationUs,
    pub(super) edge: EdgeSettings,
}

/// `recv_window` is the one that preflight checked against what the venue signs inside.
pub(crate) fn bring_up(
    gate: ExecutionPreflight,
    env: BinanceEnv,
    recv_window: RecvWindow,
    inputs: EdgeInputs<'_>,
) -> VenueEdge {
    let EdgeInputs {
        registry,
        execution,
        identity,
        commands,
        producer,
        settings,
        sender,
        ..
    } = inputs;
    let setup = BinanceExecAdapterSetup {
        instruments: registry.instruments().to_vec(),
        assets: registry.assets().clone(),
        credentials: gate.credentials,
        rest: gate.rest,
        producer,
        env,
        identity,
        recv_window,
        loud_clock_skew: DurationUs::from_millis(execution.max_clock_skew_ms as i64),
        edge: settings,
    };
    let lane = EdgeLane { commands, sender };
    VenueEdge {
        spawn: Box::new(move |runtime: &Handle| spawn(setup, lane, runtime)),
        tap_gate: None,
    }
}

fn spawn(setup: BinanceExecAdapterSetup, lane: EdgeLane, rt: &Handle) -> EdgeHandle {
    let sweep_deadline = setup.edge.sweep_deadline;
    EdgeHandle::spawn("binance execution adapter", sweep_deadline, |stop| {
        let actor = Actor::new(setup, stop, rt);
        rt.spawn(crate::log::tag_task("binance-exec", run_edge(actor, lane)))
    })
}
