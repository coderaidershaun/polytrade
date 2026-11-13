//! The engine's own execution state: which orders exist, what they have filled, and what money
//! is committed to them. Only the hot thread writes it, and it never grows or allocates once built.
//!
//! The venue is not the authority here: this table is. The venue sends events that repeat, arrive
//! out of order, and arrive after their order was reaped, so each one folds through a transition
//! table and is never read as truth.

mod account;
mod audit;
mod booking;
mod budget;
mod capacity;
mod desired;
mod engine;
mod event;
mod flatten;
mod gates;
mod mint;
mod order;
mod reconcile;
mod silence;
mod spin;
mod view;
mod window;

pub use crate::runtime::config::MAX_QUOTE_LEVELS;

pub use account::{AccountTable, AccountWatermark, Balance, ReleaseOutcome};
pub use budget::{MAX_ORDER_BUDGET_WINDOWS, OrderBudget, OrderBudgetWindow};
pub use capacity::ExecCapacity;
pub use desired::{DesiredBook, DesiredQuote, QuoteLevel};
pub(crate) use engine::FoldStamps;
pub use engine::{ExecEngine, ExecEngineSetup, ExecSettings};
pub use flatten::{FeeModel, FlattenInput, FlattenOutcome, plan_flatten};
pub use gates::{
    ExecHalt, ExposureCheck, HaltReason, LossVerdict, QuotePermission, ReadinessGap, SessionPnl,
    assess_exposure, assess_loss,
};
pub use order::{
    Applied, ClaimRefusal, ClientIdLayout, CloseReason, FillDelta, MAX_ORDER_SLOTS, OrderClaim,
    OrderSlot, OrderState, OrderTable, ReconcilePass, level_of_slot, side_base,
};
pub use reconcile::{
    BookTop, ExecLimits, FundsView, PlaceIntent, ReconcileInput, ReconcileOutcome, RejectReason,
    RestingOrder, TickGrid, reconcile_side,
};
pub use spin::SpinInput;
pub use view::{
    ExecCallback, Fill, OrderReject, OrderUpdate, OrderView, RejectOrigin, WorkingOrderView,
};
