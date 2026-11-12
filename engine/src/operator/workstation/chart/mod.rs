//! The mid and exposure charts, each a model that folds the feed into buckets and a view that
//! projects them to plot geometry, plus how the chart pane shares its height between its bands.

pub mod model;
pub mod pane;
pub mod position_model;
pub mod position_view;
pub mod view;
