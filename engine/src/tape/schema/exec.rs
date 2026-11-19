//! The schemas of the audit trail of real money: order transitions, fills, the incidents that the
//! engine could not book, and the ledger. A venue id is nullable, because an unacknowledged order
//! has none.

use crate::msg::exec::{Liquidity, OrderStyle, RejectClass};
use crate::msg::persist::{FillRow, IncidentRow, LedgerEntry, OrderRow};
use crate::msg::time::LocalTs;
use crate::tape::TapeTable;

use super::TableRow;
use super::column::Column;
use super::labels::{
    INCIDENT_KIND, LEDGER_KIND, LIQUIDITY, ORDER_LIFECYCLE, ORDER_STYLE, ORDER_TRANSITION,
    PROVENANCE, REJECT_CLASS, SIDE,
};

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
    const TABLE: TapeTable = TapeTable::Orders;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::VenueTs("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::U64("client_order_id", |row| row.client_id.0),
        Column::QuoteLevel("quote_level", |row| {
            row.quote_level.map(|level| level.get())
        }),
        Column::VenueId("venue_order_id", |row| row.venue_order_id.map(|id| id.0)),
        Column::Label(
            "transition",
            |row| row.transition.as_str(),
            &ORDER_TRANSITION,
        ),
        Column::Label("state", |row| row.state.as_str(), &ORDER_LIFECYCLE),
        Column::Label(
            "previous_state",
            |row| row.previous_state.as_str(),
            &ORDER_LIFECYCLE,
        ),
        Column::Label("provenance", |row| row.provenance.as_str(), &PROVENANCE),
        Column::Label("side", |row| row.side.as_str(), &SIDE),
        Column::Label("style", |row| style_str(row.style), &ORDER_STYLE),
        Column::Mantissa("price", |row| row.price.mantissa()),
        Column::Mantissa("qty", |row| row.qty.mantissa()),
        Column::Mantissa("filled_qty", |row| row.filled_qty.mantissa()),
        Column::Mantissa("filled_quote", |row| row.filled_quote),
        Column::Label("reject_class", |row| reject_str(row.reject), &REJECT_CLASS),
        Column::I32("reject_code", |row| row.reject_code),
        Column::NullableMantissa("depth_ahead", |row| {
            row.depth_ahead.map(|qty| qty.mantissa())
        }),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for FillRow {
    const TABLE: TapeTable = TapeTable::Fills;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::VenueTs("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::VenueId("trade_id", |row| row.trade_id.map(|id| id.0)),
        Column::VenueId("venue_order_id", |row| row.venue_order_id.map(|id| id.0)),
        Column::U64("client_order_id", |row| row.client_id.0),
        Column::QuoteLevel("quote_level", |row| {
            row.quote_level.map(|level| level.get())
        }),
        Column::Label("provenance", |row| row.provenance.as_str(), &PROVENANCE),
        Column::Label("side", |row| row.side.as_str(), &SIDE),
        Column::Label("liquidity", |row| liquidity_str(row.liquidity), &LIQUIDITY),
        Column::Mantissa("last_price", |row| row.last_price.mantissa()),
        Column::Mantissa("last_qty", |row| row.last_qty.mantissa()),
        Column::Mantissa("booked_qty", |row| row.booked_qty.mantissa()),
        Column::Mantissa("booked_quote", |row| row.booked_quote),
        Column::Mantissa("commission", |row| row.commission),
        Column::U16("commission_asset_id", |row| row.commission_asset.0),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for IncidentRow {
    const TABLE: TapeTable = TapeTable::Incidents;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::VenueTs("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::Label("kind", |row| row.kind.as_str(), &INCIDENT_KIND),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::NullableU64("client_order_id", |row| row.client_id.map(|id| id.0)),
        Column::VenueId("trade_id", |row| row.trade_id.map(|id| id.0)),
        Column::Mantissa("qty", |row| row.qty.mantissa()),
        Column::Mantissa("amount", |row| row.amount),
        Column::NullableVenueTs("window_close_ts_us", |row| row.window_close_ts_us),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for LedgerEntry {
    const TABLE: TapeTable = TapeTable::Ledger;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::Label("kind", |row| row.kind.as_str(), &LEDGER_KIND),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Mantissa("position_base", |row| row.position_base.mantissa()),
        Column::Mantissa("cash_quote", |row| row.cash_quote.mantissa()),
        Column::Mantissa("basis_quote", |row| row.basis_quote.mantissa()),
        Column::Mantissa("realised_quote", |row| row.realised_quote.mantissa()),
        Column::NullableMantissa("mark", |row| row.mark.map(|price| price.mantissa())),
        Column::Mantissa("exposure_quote", |row| row.exposure_quote.mantissa()),
        Column::Mantissa("pnl_quote", |row| row.pnl_quote.mantissa()),
        Column::NullableU64("client_order_id", |row| row.client_id.map(|id| id.0)),
        Column::VenueId("trade_id", |row| row.trade_id.map(|id| id.0)),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}
