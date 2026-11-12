//! Simulated order state.

use crate::ids::{ClientOrderId, Price, Qty, Side, VenueOrderId};

pub const CORPUS_VENUE_ORDER_ID: VenueOrderId = VenueOrderId(12_510_053_279);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimOrder {
    pub client_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub filled_quote: i64,
}

impl SimOrder {
    pub fn is_complete(&self) -> bool {
        self.filled == self.qty
    }

    pub fn take(&mut self, qty: Qty) -> Qty {
        assert!(
            qty.0 >= 0,
            "cannot take a negative quantity {} from a simulated order",
            qty.0
        );
        let remaining = self
            .qty
            .0
            .checked_sub(self.filled.0)
            .expect("simulated order filled beyond its total");
        let taken = Qty(qty.0.min(remaining));
        self.filled = Qty(self
            .filled
            .0
            .checked_add(taken.0)
            .expect("simulated cumulative fill quantity overflowed"));
        self.filled_quote = self.price.notional(self.filled);
        taken
    }
}
