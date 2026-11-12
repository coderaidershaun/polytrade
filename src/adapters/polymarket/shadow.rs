//! Shadow book + validator: the venue re-emits a full `book` frame ~every 150ms. Only
//! the first after a (re)subscribe forwards as a Snapshot; the rest are consumed here as validation.
//! A shadow book, built from the messages actually forwarded to the hot path, is compared level-for-
//! level against each venue book; because the cut carries no sequence number, a lone mismatch is a
//! timing artefact — re-baseline waits for several consecutive disagreements (a real desync).
//!
//! Neutral types only (Price/Qty/`Level`) — no venue wire structs cross into this module.

use std::collections::BTreeMap;

use crate::ids::{Price, Qty, Side};
use crate::msg::inbound::Level;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShadowBook {
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
}

impl ShadowBook {
    pub fn apply(&mut self, side: Side, price: Price, qty: Qty) {
        let book = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if qty == Qty(0) {
            book.remove(&price);
        } else {
            book.insert(price, qty);
        }
    }

    pub fn rebuild(&mut self, bids: &[Level], asks: &[Level]) {
        Self::fill(&mut self.bids, bids);
        Self::fill(&mut self.asks, asks);
    }

    fn fill(book: &mut BTreeMap<Price, Qty>, levels: &[Level]) {
        book.clear();
        for level in levels {
            if level.qty != Qty(0) {
                book.insert(level.price, level.qty);
            }
        }
    }

    pub fn is_two_sided(&self) -> bool {
        !self.bids.is_empty() && !self.asks.is_empty()
    }

    pub fn has_empty_side(&self) -> bool {
        self.bids.is_empty() || self.asks.is_empty()
    }

    /// Both sides MUST arrive ascending by price.
    pub fn first_mismatch(&self, bids: &[Level], asks: &[Level]) -> Option<BookMismatch> {
        Self::side_mismatch(&self.bids, bids, Side::Buy)
            .or_else(|| Self::side_mismatch(&self.asks, asks, Side::Sell))
    }

    /// Skip zero-size like `apply` does; zip catches dupes.
    fn side_mismatch(
        book: &BTreeMap<Price, Qty>,
        ascending_levels: &[Level],
        side: Side,
    ) -> Option<BookMismatch> {
        let venue_level = |level: &&Level| level.qty != Qty(0);
        let mut shadow = book.iter().map(|(&price, &qty)| Level { price, qty });
        let mut venue = ascending_levels.iter().filter(venue_level).copied();
        loop {
            let (mine, theirs) = (shadow.next(), venue.next());
            if mine == theirs {
                theirs?;
                continue;
            }
            return Some(BookMismatch {
                side,
                shadow: mine,
                venue: theirs,
                shadow_levels: book.len(),
                venue_levels: ascending_levels.iter().filter(venue_level).count(),
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BookMismatch {
    pub side: Side,
    pub shadow: Option<Level>,
    pub venue: Option<Level>,
    pub shadow_levels: usize,
    pub venue_levels: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookFrameOutcome {
    ForwardSnapshot,
    Validated,
    Diverged(BookMismatch),
}

/// 3 consecutive mismatches (~450ms) vs 1 timing artefact; delta reconstruction is exact.
const DIVERGENCE_CONFIRM: u32 = 3;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShadowValidator {
    shadow: ShadowBook,
    has_first_book: bool,
    suspect_streak: u32,
}

impl ShadowValidator {
    pub fn on_forwarded_delta(&mut self, side: Side, price: Price, qty: Qty) {
        self.shadow.apply(side, price, qty);
    }

    pub fn on_venue_book(&mut self, bids: &[Level], asks: &[Level]) -> BookFrameOutcome {
        if !self.has_first_book {
            self.has_first_book = true;
            self.suspect_streak = 0;
            self.shadow.rebuild(bids, asks);
            return BookFrameOutcome::ForwardSnapshot;
        }
        let Some(mismatch) = self.shadow.first_mismatch(bids, asks) else {
            self.suspect_streak = 0;
            return BookFrameOutcome::Validated;
        };
        self.suspect_streak += 1;
        if self.suspect_streak < DIVERGENCE_CONFIRM {
            return BookFrameOutcome::Validated;
        }
        self.suspect_streak = 0;
        self.shadow.rebuild(bids, asks);
        BookFrameOutcome::Diverged(mismatch)
    }

    pub fn resubscribe(&mut self) {
        *self = Self::default();
    }
}
