//! Schemas for what the engine saw and derived: features, trades, book events, klines, link tape,
//! mids, the sides that did not quote, and the rotation lineage.

use crate::msg::ids::Side;
use crate::msg::persist::{
    BookEventRow, FeatureRow, KlineRow, LinkFrameRow, MidRow, NoQuoteRow, RotationRow, TradeRow,
};
use crate::msg::time::LocalTs;
use crate::tape::TapeTable;

use super::TableRow;
use super::column::Column;
use super::labels::{
    BOOK_EVENT_KIND, BOOK_SIDE, KLINE_INTERVAL, LINK_ROW_KIND, NO_QUOTE_REASON, SIDE,
};

impl TableRow for FeatureRow {
    const TABLE: TapeTable = TapeTable::Features;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("event_ts_us", |row| row.event_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::U16("feature_id", |row| row.feature.0),
        Column::F64("value", |row| row.value),
    ];
    const PARTITION_COLUMN: &'static str = "event_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.event_ts_us
    }
}

impl TableRow for TradeRow {
    const TABLE: TapeTable = TapeTable::Trades;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::VenueTs("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Mantissa("price", |row| row.price.mantissa()),
        Column::Mantissa("qty", |row| row.qty.mantissa()),
        Column::Label("side", |row| row.side.as_str(), &SIDE),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for BookEventRow {
    const TABLE: TapeTable = TapeTable::BookEvents;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Label("kind", |row| row.kind.as_str(), &BOOK_EVENT_KIND),
        Column::Label(
            "side",
            |row| row.side.map_or("none", Side::as_str),
            &BOOK_SIDE,
        ),
        Column::Mantissa("price", |row| row.price.mantissa()),
        Column::Mantissa("qty", |row| row.qty.mantissa()),
        Column::U64("update_id", |row| row.update_id),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for KlineRow {
    const TABLE: TapeTable = TapeTable::Klines;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::VenueTs("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Label("interval", |row| row.interval.as_str(), &KLINE_INTERVAL),
        Column::VenueTs("open_ts_us", |row| row.open_ts_us),
        Column::Mantissa("open", |row| row.open.mantissa()),
        Column::Mantissa("high", |row| row.high.mantissa()),
        Column::Mantissa("low", |row| row.low.mantissa()),
        Column::Mantissa("close", |row| row.close.mantissa()),
        Column::Mantissa("base_volume", |row| row.base_volume.mantissa()),
        Column::Mantissa("quote_volume", |row| row.quote_volume),
        Column::U32("trade_count", |row| row.trade_count),
        Column::Bool("is_closed", |row| row.is_closed),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for LinkFrameRow {
    const TABLE: TapeTable = TapeTable::LinkFrames;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::VenueTs("event_ts_us", |row| row.event_ts_us),
        Column::Label("kind", |row| row.kind.as_str(), &LINK_ROW_KIND),
        Column::U64("sender_te_hash", |row| row.sender_te_hash),
        Column::U16("topic", |row| row.topic),
        Column::U64("seq", |row| row.seq),
        Column::U16("slot", |row| row.slot),
        Column::U16("count", |row| row.count),
        Column::F64("value", |row| row.value),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for RotationRow {
    const TABLE: TapeTable = TapeTable::Rotations;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::VenueTs("window_open_ts_us", |row| row.window_open_ts_us),
        Column::VenueTs("window_close_ts_us", |row| row.window_close_ts_us),
        Column::Text("token_id_up", |row| row.token_id_up.as_ref()),
        Column::Text("token_id_down", |row| row.token_id_down.as_ref()),
        Column::Text("condition_id", |row| row.condition_id.as_ref()),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}

impl TableRow for MidRow {
    const TABLE: TapeTable = TapeTable::Mids;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("event_ts_us", |row| row.event_ts_us),
        Column::U64("spin_seq", |row| row.spin_seq),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::NullableMantissa("bid", |row| row.bid.map(|price| price.mantissa())),
        Column::NullableMantissa("ask", |row| row.ask.map(|price| price.mantissa())),
        Column::NullableMantissa("mid", |row| row.mid.map(|price| price.mantissa())),
    ];
    const PARTITION_COLUMN: &'static str = "event_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.event_ts_us
    }
}

impl TableRow for NoQuoteRow {
    const TABLE: TapeTable = TapeTable::NoQuotes;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("event_ts_us", |row| row.event_ts_us),
        Column::U64("spin_seq", |row| row.spin_seq),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Label("side", |row| row.side.as_str(), &SIDE),
        Column::Label("reason", |row| row.reason.as_str(), &NO_QUOTE_REASON),
    ];
    const PARTITION_COLUMN: &'static str = "event_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.event_ts_us
    }
}
