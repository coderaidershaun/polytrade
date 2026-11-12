//! Schemas for venue public data: features, trades, book events, klines, link tape, rotation lineage.

use crate::ids::Side;
use crate::msg::persist::{
    BookEventKind, BookEventRow, FeatureRow, KlineRow, LinkFrameRow, RotationRow, TradeRow,
};
use crate::time::TsUs;

use super::TableRow;
use super::column::Column;

fn kind_str(kind: BookEventKind) -> &'static str {
    match kind {
        BookEventKind::Delta => "delta",
        BookEventKind::Snapshot => "snapshot",
        BookEventKind::Reset => "reset",
    }
}

impl TableRow for FeatureRow {
    const NAME: &'static str = "features";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("event_ts_us", |row| row.event_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::U16("feature_id", |row| row.feature.0),
        Column::F64("value", |row| row.value),
    ];

    fn partition_ts(&self) -> TsUs {
        self.event_ts_us
    }
}

impl TableRow for TradeRow {
    const NAME: &'static str = "trades";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Mantissa("price", |row| row.price.0),
        Column::Mantissa("qty", |row| row.qty.0),
        Column::Text("side", |row| row.side.as_str()),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}

impl TableRow for BookEventRow {
    const NAME: &'static str = "book_events";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Text("kind", |row| kind_str(row.kind)),
        Column::Text("side", |row| row.side.map_or("none", Side::as_str)),
        Column::Mantissa("price", |row| row.price.0),
        Column::Mantissa("qty", |row| row.qty.0),
        Column::U64("update_id", |row| row.update_id),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}

impl TableRow for KlineRow {
    const NAME: &'static str = "klines";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("exchange_ts_us", |row| row.exchange_ts_us),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Text("interval", |row| row.interval.as_str()),
        Column::Ts("open_ts_us", |row| row.open_ts_us),
        Column::Mantissa("open", |row| row.open.0),
        Column::Mantissa("high", |row| row.high.0),
        Column::Mantissa("low", |row| row.low.0),
        Column::Mantissa("close", |row| row.close.0),
        Column::Mantissa("base_volume", |row| row.base_volume.0),
        Column::Mantissa("quote_volume", |row| row.quote_volume),
        Column::U32("trade_count", |row| row.trade_count),
        Column::Bool("is_closed", |row| row.is_closed),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}

impl TableRow for LinkFrameRow {
    const NAME: &'static str = "link_frames";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::Ts("event_ts_us", |row| row.event_ts_us),
        Column::Text("kind", |row| row.kind.as_str()),
        Column::U64("sender_te_hash", |row| row.sender_te_hash),
        Column::U16("topic", |row| row.topic),
        Column::U64("seq", |row| row.seq),
        Column::U16("slot", |row| row.slot),
        Column::U16("count", |row| row.count),
        Column::F64("value", |row| row.value),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}

impl TableRow for RotationRow {
    const NAME: &'static str = "rotations";
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::U16("instrument_id", |row| row.instrument.0),
        Column::Ts("window_open_ts_us", |row| row.window_open_ts_us),
        Column::Ts("window_close_ts_us", |row| row.window_close_ts_us),
        Column::Text("token_id_up", |row| row.token_id_up.as_ref()),
        Column::Text("token_id_down", |row| row.token_id_down.as_ref()),
        Column::Text("condition_id", |row| row.condition_id.as_ref()),
    ];

    fn partition_ts(&self) -> TsUs {
        self.received_ts_us
    }
}
