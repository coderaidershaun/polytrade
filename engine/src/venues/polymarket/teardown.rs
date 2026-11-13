//! Spots the burst of removals at one venue time with which the venue empties a resolved market,
//! the fast sign of a teardown. The silence after it is for the rotation to weigh.

use crate::msg::ids::{Price, Qty, Side};
use crate::msg::time::VenueTs;

use super::shadow::ShadowBook;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LevelUpdate {
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub exchange_ts_us: VenueTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CollapseSignal {
    Quiet,
    Collapsed,
}

/// Separates mass removal from incidental one.
const MIN_BURST_REMOVALS: u32 = 2;

/// One detector per token book; OR the two books' collapsed state before signalling slot FSM.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollapseDetector {
    book: ShadowBook,
    burst_ts: Option<VenueTs>,
    burst_removals: u32,
    two_sided_at_burst_start: bool,
    collapsed: bool,
}

impl CollapseDetector {
    pub fn observe(&mut self, update: LevelUpdate) -> CollapseSignal {
        let is_removal = update.qty == Qty::from_mantissa(0);
        if is_removal {
            if self.burst_ts != Some(update.exchange_ts_us) {
                self.burst_ts = Some(update.exchange_ts_us);
                self.burst_removals = 0;
                self.two_sided_at_burst_start = self.book.is_two_sided();
            }
            self.burst_removals += 1;
        } else {
            self.burst_ts = None;
            self.burst_removals = 0;
        }

        self.book.apply(update.side, update.price, update.qty);

        let collapse_now = self.burst_ts == Some(update.exchange_ts_us)
            && self.burst_removals >= MIN_BURST_REMOVALS
            && self.two_sided_at_burst_start
            && self.book.has_empty_side();
        if collapse_now {
            self.collapsed = true;
        } else if self.collapsed && self.book.is_two_sided() {
            self.collapsed = false;
        }

        if self.collapsed { CollapseSignal::Collapsed } else { CollapseSignal::Quiet }
    }

    pub fn has_collapsed(&self) -> bool {
        self.collapsed
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}
