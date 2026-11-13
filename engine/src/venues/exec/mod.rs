//! The venue-neutral execution machinery that each venue edge and the simulator build on, written
//! once so that no venue spells a money rule its own way.

mod capabilities;
mod core;
mod effect;
mod event;
mod exit;
mod identity;
mod inflight;
mod mirror;
mod reject;
mod resync;
mod session;

pub(crate) use capabilities::VenueCapabilities;
pub use core::{ExecCore, ObserveOrderError, Phase};
pub use effect::{
    ExecEffect, ExecRequest, Outgoing, PlaceNotSentReason, RequestId, SkipReason, TimeoutFallout,
};
pub use event::open_orders_snapshot_end;
pub(crate) use event::{
    ChunkStamps, account_chunks, amend_not_sent, blank, place_not_sent, request_timed_out,
    stream_ready, stream_reset,
};
pub(crate) use exit::{EdgeHandle, ExecStop};
pub use exit::{ExitPlan, SessionOutcome, SweepFailure};
pub use identity::{EngineIdentity, LeaseNamespace, OrderOwnership, TeTag};
pub use inflight::{InFlightRequest, InFlightTable};
pub use mirror::MirroredOrder;
pub use reject::{RejectVerdict, VenueAvailability};
pub use resync::{MAX_RESYNC_ATTEMPTS, ResyncPass, ResyncStep};
pub use session::{EventSink, ExecSession, Expired, ResyncPoll, SessionSetup};
