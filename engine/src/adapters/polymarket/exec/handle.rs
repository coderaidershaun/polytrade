//! What starting the execution actor takes, and the startup gate that must pass first. The gate is
//! re-exported here so whatever starts this venue reaches both through one module.

use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::adapters::edge::{EdgeInputs, EdgeSettings, VenueEdge, run_edge};
use crate::adapters::exec::EdgeHandle;
use crate::adapters::polymarket::rotation::WindowAssignment;
use crate::hot::spawn::QueueProducer;
use crate::msg::exec::ExecLaneItem;
use crate::registry::InstrumentRow;
use crate::time::DurationUs;

use super::actor::Actor;
use super::sign::address::Address;
use super::sign::key::SigningKey;
use super::sign::l2::ApiCredentials;
use super::sign::order::SignatureType;

pub use super::preflight::{PolymarketPreflight, PolymarketPreflightError, preflight_polymarket};

// The maker is who the venue credits; the signer is whose key authenticates. They are the
// same address for an EOA wallet, but separate for a smart wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalletIdentity {
    pub maker: Address,
    pub signer: Address,
    pub signature_type: SignatureType,
}

pub(super) struct PolymarketExecAdapterSetup {
    pub(super) instruments: Vec<InstrumentRow>,
    // L2 API credentials, kept separate from the key that signs orders.
    pub(super) credentials: ApiCredentials,
    pub(super) key: SigningKey,
    pub(super) wallet: WalletIdentity,
    pub(super) commands: rtrb::Consumer<ExecLaneItem>,
    pub(super) producer: QueueProducer,
    /// Rotation bindings from the market-data actor, which is the single authority on how a
    /// token maps to a leg.
    pub(super) assignments: mpsc::Receiver<WindowAssignment>,
    pub(super) venue_clock_offset: DurationUs,
    pub(super) edge: EdgeSettings,
}

pub(crate) fn bring_up(
    gate: PolymarketPreflight,
    inputs: EdgeInputs<'_>,
    assignments: mpsc::Receiver<WindowAssignment>,
) -> VenueEdge {
    let EdgeInputs {
        registry,
        commands,
        producer,
        settings,
        ..
    } = inputs;
    let setup = PolymarketExecAdapterSetup {
        instruments: registry.instruments().to_vec(),
        credentials: gate.credentials,
        key: gate.key,
        wallet: gate.wallet,
        commands,
        producer,
        assignments,
        venue_clock_offset: gate.venue_clock_offset,
        edge: settings,
    };
    VenueEdge {
        spawn: Box::new(move |runtime: &Handle| spawn(setup, runtime)),
        tap_gate: None,
    }
}

fn spawn(setup: PolymarketExecAdapterSetup, runtime: &Handle) -> EdgeHandle {
    let sweep_deadline = setup.edge.sweep_deadline;
    EdgeHandle::spawn("polymarket execution adapter", sweep_deadline, |stop| {
        let actor = Actor::new(setup, stop, runtime);
        runtime.spawn(crate::log::tag_task("polymarket-exec", run_edge(actor)))
    })
}
