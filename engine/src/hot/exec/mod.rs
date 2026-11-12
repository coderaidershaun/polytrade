//! Engine-owned execution state: what orders exist, what they have filled, and what money is
//! committed to them. Single-writer on the hot thread, fixed capacity, no allocation after
//! construction.
//!
//! The venue is not the authority here — this table is. The venue speaks in events that duplicate,
//! reorder and arrive after the order they describe has been reaped, so every one of them folds
//! through a transition table rather than being read as truth.

mod account;
mod audit;
mod budget;
pub(crate) mod command;
mod desired;
mod engine;
mod event;
mod flatten;
mod gates;
mod mint;
mod order;
mod reconcile;
mod spin;
mod transition;
mod view;
mod window;

pub use crate::config::MAX_QUOTE_LEVELS;

pub use account::{AccountTable, AccountWatermark, Balance, ReleaseOutcome};
pub use budget::{MAX_ORDER_BUDGET_WINDOWS, OrderBudget, OrderBudgetWindow};
pub use command::{ExecLaneBudget, exec_lane_capacity};
pub use desired::{DesiredBook, DesiredQuote, QuoteLevel};
pub use engine::{ExecEngine, ExecEngineSetup, ExecSettings};
pub use flatten::{FeeModel, FlattenInput, FlattenOutcome, plan_flatten};
pub use gates::{
    ExecHalt, ExposureCheck, HaltReason, LossVerdict, QuotePermission, ReadinessGap, SessionPnl,
    assess_exposure, assess_loss,
};
pub use order::{
    ClientIdLayout, CloseReason, FillDelta, MAX_ORDER_INSTRUMENTS, MAX_ORDER_SLOTS, OrderClaim,
    OrderSlot, OrderState, OrderTable, ReconcilePass, level_of_slot, side_base,
};
pub use reconcile::{
    BookTop, ExecLimits, FundsView, PlaceIntent, ReconcileInput, ReconcileOutcome, RejectReason,
    RestingOrder, TickGrid, reconcile_side,
};
pub use spin::SpinInput;
pub use transition::{Applied, apply_exec_event};
pub use view::{
    ExecCallback, Fill, OrderReject, OrderUpdate, OrderView, RejectOrigin, WorkingOrderView,
};
