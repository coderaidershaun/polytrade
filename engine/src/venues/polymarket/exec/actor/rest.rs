//! Why the actor asked for each request, and what it needs to read the answer.

use std::sync::Arc;

use crate::msg::ids::{AssetId, ClientOrderId, InstrumentId};
use crate::venues::exec::{ExecRequest, RequestId};

use super::super::codec::PlaceRequestContext;

/// Who asked for an open-orders page, and so whether its answer settles a resync read.
///
/// The hot side's reconcile counter and the resync pass counter both start at zero and step on
/// unrelated triggers. Compared as bare numbers they cross, and a crossing retires a pass read that
/// never landed — the pass then finishes on two of its three reads and opens quoting having never
/// seen the balance sweep. Naming the asker is what makes that impossible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenOrdersRead {
    /// Account-wide, one of the reads a resync pass is waiting on.
    Pass { resync_seq: u64 },
    /// One instrument's page, asked for by the hot side's silence detector. `recon_seq` belongs to
    /// the hot side and is the only sequence this variant may carry.
    Instrument {
        instrument: InstrumentId,
        recon_seq: u64,
    },
    /// The check a freshly bound token gets. Part of no pass.
    FreshBinding { instrument: InstrumentId },
}

impl OpenOrdersRead {
    /// The instrument a follow-on page must be scoped to. `None` is the account-wide read.
    pub(super) fn instrument(self) -> Option<InstrumentId> {
        match self {
            OpenOrdersRead::Pass { .. } => None,
            OpenOrdersRead::Instrument { instrument, .. }
            | OpenOrdersRead::FreshBinding { instrument } => Some(instrument),
        }
    }
}

/// Why the call was made, and everything needed to read its answer.
#[derive(Debug, Clone)]
pub(super) enum RestPurpose {
    /// A request the execution core minted. `place` carries what was asked, because this venue
    /// echoes neither the client id nor the price back on the placement path.
    Core {
        request_id: RequestId,
        request: ExecRequest,
        recon_seq: u64,
        place: Option<PlaceRequestContext>,
    },
    /// One page of open orders, and how far into the page walk it is.
    OpenOrders {
        read: OpenOrdersRead,
        page: u32,
        /// Orders the earlier pages of THIS walk already named. Absence from the whole read is what
        /// makes an order missing, and a resync walk can be in flight beside a hot-side one, so the
        /// accumulation rides the walk rather than sitting on the driver where they would mix.
        seen: Vec<ClientOrderId>,
    },
    /// One asset's balance. The venue has no multi-asset read, so a sweep is N of these.
    Balance {
        asset: AssetId,
        /// `None` for the post-fill restatement, which answers the account table and settles no
        /// admission gate.
        resync_seq: Option<u64>,
    },
    Trades {
        resync_seq: Option<u64>,
        page: u32,
    },
    /// Rotation binding: tick and minimum size for the whole market.
    Market {
        condition_id: Arc<str>,
    },
    /// Rotation binding: which exchange contract this token's orders are signed against.
    NegRisk {
        condition_id: Arc<str>,
        instrument: InstrumentId,
    },
    /// The CLOB's allowance cache for one token, warmed before that token's first sell.
    Allowance {
        token_id: Box<str>,
    },
    Heartbeat,
    /// Every resting order on one token, in one call — the rotation sweep.
    MarketCancel {
        instrument: InstrumentId,
    },
}
