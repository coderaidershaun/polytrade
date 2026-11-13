//! Simulated order state.

use crate::msg::ids::{ClientOrderId, Notional, Price, Qty, Side, VenueOrderId};

pub const CORPUS_VENUE_ORDER_ID: VenueOrderId = VenueOrderId(12_510_053_279);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimOrder {
    pub client_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub filled_quote: Notional,
}

impl SimOrder {
    pub fn is_complete(&self) -> bool {
        self.filled == self.qty
    }

    pub fn take(&mut self, qty: Qty) -> Qty {
        assert!(
            qty.mantissa() >= 0,
            "cannot take a negative quantity {} from a simulated order",
            qty.mantissa()
        );
        let remaining = self
            .qty
            .mantissa()
            .checked_sub(self.filled.mantissa())
            .expect("simulated order filled beyond its total");
        let taken = Qty::from_mantissa(qty.mantissa().min(remaining));
        self.filled = Qty::from_mantissa(
            self.filled
                .mantissa()
                .checked_add(taken.mantissa())
                .expect("simulated cumulative fill quantity overflowed"),
        );
        self.filled_quote = self.price.notional(self.filled);
        taken
    }
}
