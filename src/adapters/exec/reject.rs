//! What a venue's refusal meant, in two classes: a verdict on the order, or a statement about the
//! venue itself. The split is the point — an outage says nothing about the order that happened to be
//! in flight, so availability never feeds the reject streak that parks an engine for misbehaving.

use crate::msg::exec::RejectClass;

/// The venue's own state, distinct from any verdict on an order. Every variant means "come
/// back later," never "this order was bad."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueAvailability {
    /// Restarting and taking no traffic yet; may return in a reduced mode rather than fully open.
    Restarting,
    /// Posts and cancels still flow, so an engine that only quotes passively keeps working.
    PostOnlyMode { retry_after_secs: Option<i64> },
    /// Cancels are accepted; placements are refused.
    CancelOnly,
    /// Everything is refused, including cancels.
    TradingDisabled,
    /// Request budget spent. `retry_after_secs` is the venue's own wait hint, when it sends one.
    RateLimited { retry_after_secs: Option<i64> },
}

impl VenueAvailability {
    /// A parked engine that cannot cancel holds unintended risk, which is why this is
    /// distinguished from the plain "wait" states.
    pub const fn allows_cancel(self) -> bool {
        !matches!(self, VenueAvailability::TradingDisabled)
    }
}

/// The verdict on one failed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectVerdict {
    Order(RejectClass),
    Venue(VenueAvailability),
}
