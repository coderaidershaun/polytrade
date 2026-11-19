//! The tape table on the writer thread: the boot row first, then one row for each record of the
//! tee in the order of the ring, with a new file at each seal record.

use crate::msg::persist::{BootRecord, TapeRecord};
use crate::msg::time::LocalTs;

use super::fault::{Faults, Table};
use super::payload::{encode_boot, encode_gap, encode_message, encode_snapshot};
use super::schema::TapeRow;
use super::table::{TableFiles, TableWriter};

/// The tape's own input of the writer: the ring of the tee and the start state of the fold.
pub struct TapeInput {
    pub records: rtrb::Consumer<TapeRecord>,
    pub boot: BootRecord,
}

pub(super) struct TapeTable {
    table: Table<TapeRow>,
}

impl TapeTable {
    /// Writes the boot row before any record, so each tape opens with the start state of its fold.
    pub(super) fn open(files: TableFiles, boot: &BootRecord, faults: &mut Faults) -> Self {
        let boot_ts_us = LocalTs::from_micros(files.boot_ts_us);
        let mut table = Table::Writing(Box::new(TableWriter::new(files)));
        let mut payload = Vec::new();
        let kind = encode_boot(boot, &mut payload);
        table.push(
            TapeRow {
                seq: 0,
                kind,
                queue_id: None,
                received_ts_us: boot_ts_us,
                queued_ts_us: boot_ts_us,
                payload,
            },
            faults,
        );
        Self { table }
    }

    pub(super) fn route(&mut self, record: TapeRecord, faults: &mut Faults) {
        let mut payload = Vec::new();
        let row = match record {
            TapeRecord::Message {
                seq,
                queue,
                message,
            } => TapeRow {
                seq,
                kind: encode_message(&message.body, &mut payload),
                queue_id: Some(queue.index() as u8),
                received_ts_us: message.header.received_ts_us,
                queued_ts_us: message.header.queued_ts_us,
                payload,
            },
            TapeRecord::BookSnapshot {
                seq,
                received_ts_us,
                chunk,
            } => TapeRow {
                seq,
                kind: encode_snapshot(&chunk, &mut payload),
                queue_id: None,
                received_ts_us,
                queued_ts_us: received_ts_us,
                payload,
            },
            TapeRecord::Gap(gap) => TapeRow {
                seq: gap.first_lost_seq,
                kind: encode_gap(&gap, &mut payload),
                queue_id: None,
                received_ts_us: gap.received_ts_us,
                queued_ts_us: gap.received_ts_us,
                payload,
            },
            TapeRecord::Seal => return self.seal(faults),
        };
        self.table.push(row, faults);
    }

    pub(super) fn seal(&mut self, faults: &mut Faults) {
        self.table.seal(faults);
    }
}
