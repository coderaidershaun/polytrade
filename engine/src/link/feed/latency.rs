//! Self-timing half of the UI event lane: three stage rows plus the input-ring backlog.

use crate::msg::ui::{UiLatencyCell, UiLatencyRow, UiLatencySummary};

use super::super::envelope::{ByteReader, ByteWriter, LinkDecodeError};
use super::super::wire::{OPTIONAL_F64_LEN, WireField, wire_struct};

const CELL_LEN: usize = 4 + 8;
const ROW_LEN: usize = 6 * CELL_LEN;
pub(super) const LATENCY_LEN: usize = 3 * ROW_LEN + OPTIONAL_F64_LEN;

/// Counts ride as u32 and saturate rather than widen the tail. The ten-minute window would need
/// seven million messages a second to reach the ceiling, and a displayed mean cannot tell by then.
impl WireField for UiLatencyCell {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        u32::try_from(self.count).unwrap_or(u32::MAX).write(writer);
        self.sum_us.write(writer);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        Ok(Self {
            count: u64::from(u32::read(reader)?),
            sum_us: WireField::read(reader)?,
        })
    }
}

wire_struct! {
    UiLatencyRow {
        exchange_to_received,
        received_to_queued,
        queue_wait,
        processing,
        end_to_end,
        order_round_trip,
    }
}

wire_struct! {
    UiLatencySummary {
        market_data,
        execution,
        hot_path,
        backlog_ema,
    }
}
