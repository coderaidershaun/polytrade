//! Parquet table kinds; bitmask gates hot-path emissions.

use serde::Deserialize;

use crate::labelled_enum::labelled_enum;

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum TableKind {
        Trades = "trades",
        BookEvents = "book_events",
        Klines = "klines",
        Features = "features",
        LinkFrames = "link_frames",
        Orders = "orders",
        Fills = "fills",
    }
    pub fn as_str;
}

impl TableKind {
    #[inline]
    const fn bit(self) -> u16 {
        match self {
            TableKind::Trades => 1,
            TableKind::BookEvents => 1 << 1,
            TableKind::Klines => 1 << 2,
            TableKind::Features => 1 << 3,
            TableKind::LinkFrames => 1 << 4,
            TableKind::Orders => 1 << 5,
            TableKind::Fills => 1 << 6,
        }
    }
}

/// Copy bitmask for hot-path AND gate; u16 for 7 tables.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RecordedTables(u16);

impl RecordedTables {
    pub fn new(tables: &[TableKind]) -> Self {
        RecordedTables(tables.iter().fold(0, |bits, table| bits | table.bit()))
    }

    #[inline]
    pub fn contains(self, table: TableKind) -> bool {
        self.0 & table.bit() != 0
    }
}
