//! The depth ladder's painter and the overlay it draws on top, and the frame of book, quote and
//! feed state that both paint from.

mod geometry;
pub mod ladder;
mod overlay;
mod pin;

use crate::ids::Price;
use crate::msg::ui::UiBookSnapshot;
use crate::time::DurationUs;
use crate::workstation::dom_view::{DomGrouping, DomOverlay, FeedStatus};
use crate::workstation::text;
pub(crate) use geometry::{ladder_rect, rows_that_fit};

pub struct DomFrame<'a> {
    pub snapshot: Option<&'a UiBookSnapshot>,
    pub overlay: DomOverlay<'a>,
    pub tick: Price,
    pub grouping: DomGrouping,
    /// Rows per side asked for; the panel clamps it to what fits above the legibility floor.
    pub levels: usize,
    pub price_decimals: usize,
    pub qty_scale: i64,
    pub qty_decimals: usize,
    pub feed: FeedStatus,
    /// How far the book trails the engine's own newest stamp, when that is far enough to say so.
    pub stale_age: Option<DurationUs>,
}

impl DomFrame<'_> {
    pub(crate) fn write_row_price(&self, buf: &mut String, tick_index: i64) {
        text::row_price(buf, self.tick, self.price_decimals, tick_index);
    }
}
