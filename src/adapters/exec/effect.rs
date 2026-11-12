//! What passes between the execution core and the driver that applies its decisions: the core
//! decides what to do, and the driver decides how to do it on its venue. These types live in their
//! own module because every driver, simulated or live, names them.

use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::{CancelReason, OrderStyle};

/// Correlates pipelined request/response (sends don't await; ID tethers answer to query).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(u64);

impl RequestId {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Core intent (not venue method). Codec picks endpoint. Core has no wire vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecRequest {
    Place {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        side: Side,
        price: Price,
        qty: Qty,
        style: OrderStyle,
    },
    Cancel {
        instrument: InstrumentId,
        client_id: ClientOrderId,
    },
    AmendQty {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        qty: Qty,
    },
    OrderStatus {
        instrument: InstrumentId,
        client_id: ClientOrderId,
    },
    OpenOrders {
        instrument: InstrumentId,
    },
    SubscribeUserStream,
}

/// Unanswered request fallout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutFallout {
    OrderInDoubt {
        instrument: InstrumentId,
        client_id: ClientOrderId,
    },
    ReadAbandoned,
    StreamUnusable,
}

impl ExecRequest {
    /// Which of the three timeout outcomes applies (on request, not driver, to pin without timer arms).
    pub fn timeout_fallout(self) -> TimeoutFallout {
        match self {
            ExecRequest::Place {
                instrument,
                client_id,
                ..
            }
            | ExecRequest::Cancel {
                instrument,
                client_id,
            }
            | ExecRequest::AmendQty {
                instrument,
                client_id,
                ..
            }
            | ExecRequest::OrderStatus {
                instrument,
                client_id,
            } => TimeoutFallout::OrderInDoubt {
                instrument,
                client_id,
            },
            ExecRequest::OpenOrders { .. } => TimeoutFallout::ReadAbandoned,
            ExecRequest::SubscribeUserStream => TimeoutFallout::StreamUnusable,
        }
    }
}

/// Skip reason logged so operator knows engine chose not to act.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkipReason {
    ForeignOrder,
    AlreadyCancelling,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlaceNotSentReason {
    PhaseClosed,
    SideCapacity,
    DuplicateClientId,
    MirrorStorage,
    Encoding,
    NoTransport,
}

impl PlaceNotSentReason {
    /// Whether the refusal exposes corrupt local execution state.
    pub const fn is_fatal(self) -> bool {
        matches!(
            self,
            PlaceNotSentReason::DuplicateClientId | PlaceNotSentReason::MirrorStorage
        )
    }
}

/// Driver actions. Everything core decides leaves here; driver's only job is applying effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecEffect {
    Send {
        request_id: RequestId,
        request: ExecRequest,
    },
    Skipped {
        client_id: Option<ClientOrderId>,
        reason: SkipReason,
    },
    PlaceNotSent {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        side: Side,
        reason: PlaceNotSentReason,
    },
    /// Carries no reason because none of them changes what anyone does with it: the order is still
    /// resting exactly as it was, so there is nothing to unwind and nothing that can be fatal.
    AmendNotSent {
        instrument: InstrumentId,
        client_id: ClientOrderId,
    },
    SweepComplete {
        reason: CancelReason,
    },
}

/// An effect paired with the hot reconcile sequence it answers. The core does not remember which
/// pass a read belongs to, so the driver carries the sequence until the request is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outgoing {
    pub effect: ExecEffect,
    pub recon_seq: u64,
}

impl Outgoing {
    pub fn sink(outgoing: &mut Vec<Outgoing>, recon_seq: u64) -> impl FnMut(ExecEffect) + '_ {
        move |effect| outgoing.push(Outgoing { effect, recon_seq })
    }
}
