//! Schemas for real-money outcomes: order-lifecycle transitions + fills (audit trail, statement reconciliation).
//! Venue IDs are nullable (sent order never acked = no venue id, sentinel would misread).

use crate::msg::exec::{Liquidity, OrderStyle, RejectClass};
use crate::msg::persist::{FillRow, OrderRow};
use crate::time::TsUs;

use super::TableRow;
use super::column::Column;

fn reject_str(reject: Option<RejectClass>) -> &'static str {
    reject.map_or("none", RejectClass::as_str)
}

/// `None` means the engine did not place this order, so it never knew how it was sent — the same
/// idiom `liquidity_str` uses for an event that did not say.
fn style_str(style: Option<OrderStyle>) -> &'static str {
    style.map_or("unknown", OrderStyle::as_str)
}

fn liquidity_str(liquidity: Option<Liquidity>) -> &'static str {
    liquidity.map_or("none", Liquidity::as_str)
}

impl TableRow for OrderRow {
    const NAME: &'static str = "orders";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::U64("client_order_id", |row| row.client_id.0),
        Column::QuoteLevel("quote_level", |row| {
            row.quote_level.map(|level| level.get())
        }),
        Column::VenueId("venue_order_id", |row| row.venue_order_id.map(|id| id.0)),
        Column::Text("transition", |row| row.transition.as_str()),
        Column::Text("state", |row| row.state.as_str()),
        Column::Text("previous_state", |row| row.previous_state.as_str()),
        Column::Text("provenance", |row| row.provenance.as_str()),
        Column::Text("side", |row| row.side.as_str()),
        Column::Text("style", |row| style_str(row.style)),
        Column::Mantissa("price", |row| row.price.0),
        Column::Mantissa("qty", |row| row.qty.0),
        Column::Mantissa("filled_qty", |row| row.filled_qty.0),
        Column::Mantissa("filled_quote", |row| row.filled_quote),
        Column::Text("reject_class", |row| reject_str(row.reject)),
        Column::I32("reject_code", |row| row.reject_code),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}

impl TableRow for FillRow {
    const NAME: &'static str = "fills";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::VenueId("trade_id", |row| row.trade_id.map(|id| id.0)),
        Column::VenueId("venue_order_id", |row| row.venue_order_id.map(|id| id.0)),
        Column::U64("client_order_id", |row| row.client_id.0),
        Column::QuoteLevel("quote_level", |row| {
            row.quote_level.map(|level| level.get())
        }),
        Column::Text("provenance", |row| row.provenance.as_str()),
        Column::Text("side", |row| row.side.as_str()),
        Column::Text("liquidity", |row| liquidity_str(row.liquidity)),
        Column::Mantissa("last_price", |row| row.last_price.0),
        Column::Mantissa("last_qty", |row| row.last_qty.0),
        Column::Mantissa("booked_qty", |row| row.booked_qty.0),
        Column::Mantissa("booked_quote", |row| row.booked_quote),
        Column::Mantissa("commission", |row| row.commission),
        Column::U16("commission_asset_id", |row| row.commission_asset.0),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}
