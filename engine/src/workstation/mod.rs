//! The workstation: it attaches to one running trading engine over the link, folds the feed into
//! models, projects each model to geometry, and holds every word and colour role the panels show.
//!
//! Only `browser/` names egui or the browser, and it compiles only for wasm32. Everything else
//! compiles for every target and is tested natively. A model folds the feed into state, and a view
//! projects that state to geometry.
//!
//! The link session is `pub` on its own merit. The models, views and painters are `pub` because the
//! `dom-fixture` page builds its scenes from them and paints with them. The shell is `pub` because
//! the same page arranges its panes with it. The page itself is private.

// SMELL: many model, view and `format` items are `pub` only because the fitness suite names them,
// which the project's rules do not accept as a reason. Narrowing them means re-expressing those
// tests through what stays public.
pub(crate) mod attachment;
#[cfg(target_arch = "wasm32")]
pub mod browser;
pub mod chart;
pub mod dom_view;
pub mod exec_model;
pub mod format;
mod history;
pub mod link_model;
pub mod link_session;
pub mod link_view;
pub mod model;
pub mod monitor;
pub mod shell;
pub(crate) mod text;
