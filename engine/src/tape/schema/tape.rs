//! The schema of the tape table: one row for each record of the tee, its payload as the bytes of
//! the field list of its kind.

use crate::msg::time::LocalTs;
use crate::tape::TapeTable;

use super::column::Column;
use super::{Seals, TableRow};

/// The rows of one tape file hold the messages of one interval between two seal borders, so a
/// reader decodes one row group at a time and holds two at most.
const TAPE_ROW_GROUP_ROWS: usize = 65_536;

/// One row of the tape, as the writer thread builds it from a record of the tee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::tape) struct TapeRow {
    pub(in crate::tape) seq: u64,
    pub(in crate::tape) kind: u16,
    /// The queue of the pop, and `None` for a row that no queue gave: boot, snapshot and gap.
    pub(in crate::tape) queue_id: Option<u8>,
    pub(in crate::tape) received_ts_us: LocalTs,
    pub(in crate::tape) queued_ts_us: LocalTs,
    pub(in crate::tape) payload: Vec<u8>,
}

impl TableRow for TapeRow {
    const TABLE: TapeTable = TapeTable::Tape;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::U64("seq", |row| row.seq),
        Column::U16("kind", |row| row.kind),
        Column::NullableU8("queue_id", |row| row.queue_id),
        Column::Ts("received_ts_us", |row| row.received_ts_us),
        Column::Ts("queued_ts_us", |row| row.queued_ts_us),
        Column::Bytes("payload", |row| &row.payload),
    ];
    const PARTITION_COLUMN: &'static str = "received_ts_us";
    const SEALS: Seals = Seals::Border;
    const ROW_GROUP_ROWS: Option<usize> = Some(TAPE_ROW_GROUP_ROWS);

    fn partition_ts(&self) -> LocalTs {
        self.received_ts_us
    }
}
