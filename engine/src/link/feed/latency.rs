//! The latency feed: the report of the metrics thread on its own topic, and the older summary that
//! still rides the event lane until the hot thread stops timing itself.

use crate::msg::time::DurationUs;
use crate::msg::ui::{
    LATENCY_REPORT_CELLS, LatencyCell, LatencyFlow, LatencyReport, UiLatencyCell, UiLatencyRow,
    UiLatencySummary,
};

use super::super::envelope::{ByteReader, ByteWriter, LinkDecodeError};
use super::super::wire::{OPTIONAL_F64_LEN, WireField, wire_array, wire_struct};

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

const REPORT_CELL_LEN: usize = 5 * 8;
const OPTIONAL_DURATION_LEN: usize = 1 + 8;
pub(in crate::link) const REPORT_BODY_LEN: usize = 8
    + LatencyFlow::ALL.len() * OPTIONAL_DURATION_LEN
    + LATENCY_REPORT_CELLS * REPORT_CELL_LEN
    + OPTIONAL_F64_LEN;

wire_struct! {
    LatencyCell {
        count,
        p50_ns,
        p99_ns,
        p999_ns,
        max_ns,
    }
}

/// Rides as an optional count of microseconds, so its bytes match every other optional on the wire.
impl WireField for Option<DurationUs> {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        self.map(DurationUs::micros).write(writer);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        Ok(Option::<i64>::read(reader)?.map(DurationUs::from_micros))
    }
}

wire_array! {
    LatencyCell; LATENCY_REPORT_CELLS; LatencyCell::EMPTY,
    Option<DurationUs>; LatencyFlow::ALL.len(); None,
}

wire_struct! {
    LatencyReport {
        built_ts_us,
        ages,
        cells,
        backlog_ema,
    }
}
