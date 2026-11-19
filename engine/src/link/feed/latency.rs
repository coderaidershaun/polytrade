//! The latency feed: the report of the metrics thread, on its own topic.

use crate::msg::time::DurationUs;
use crate::msg::ui::{LATENCY_REPORT_CELLS, LatencyCell, LatencyFlow, LatencyReport};

use super::super::envelope::{ByteReader, ByteWriter, LinkDecodeError};
use super::super::wire::{WireField, wire_array, wire_struct};

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
    const WIDTH: usize = Option::<i64>::WIDTH;
    const DIGEST: u64 = Option::<i64>::DIGEST;

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
