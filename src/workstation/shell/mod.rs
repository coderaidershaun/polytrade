//! The shell decides what the page shows and where, as plain numbers that name no GUI type, so the
//! browser page and the dom-fixture arrange their panes from one model and cannot disagree.

mod arrangement;
mod navigation;

pub(crate) use arrangement::TOUCH_HEIGHT;
pub use arrangement::{Area, Arrangement, Pane, TopBar, zoom_to_fit};
pub use navigation::{Navigation, Selection, SizeClass, View};
