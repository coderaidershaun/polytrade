//! Schemas for how the engine ran: the latency, drop and backlog rows of the metrics thread. They
//! carry the wall clock of that thread, so no re-fold compares them.

use crate::msg::time::LocalTs;
use crate::runtime::metrics::{BacklogRow, DropRow, LatencyRow, SenderDropRow};
use crate::tape::TapeTable;

use super::TableRow;
use super::column::Column;
use super::labels::{CLOCK_SOURCE, LATENCY_CATEGORY, LATENCY_STAGE, SINK_KIND};

/// The row of the senders that came after the table of senders was full.
const OTHER_SENDERS: &str = "others";

impl TableRow for LatencyRow {
    const TABLE: TapeTable = TapeTable::MetricsLatency;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("interval_start_ts_us", |row| row.interval_start_ts_us),
        Column::Ts("interval_end_ts_us", |row| row.interval_end_ts_us),
        Column::Label("category", |row| row.category.label(), &LATENCY_CATEGORY),
        Column::Label("stage", |row| row.stage.label(), &LATENCY_STAGE),
        Column::U64("count", |row| row.count),
        Column::U64("dropped", |row| row.dropped),
        Column::U64("backward", |row| row.backward),
        Column::U64("negative", |row| row.negative),
        Column::U64("p50_ns", |row| row.p50_ns),
        Column::U64("p99_ns", |row| row.p99_ns),
        Column::U64("p999_ns", |row| row.p999_ns),
        Column::U64("max_ns", |row| row.max_ns),
        Column::NullableU64("budget_p99_ns", |row| row.budget_p99_ns),
        Column::NullableU64("budget_p999_ns", |row| row.budget_p999_ns),
        Column::Bool("is_over_budget", |row| row.is_over_budget),
        Column::Label(
            "clock_source",
            |row| row.clock_source.label(),
            &CLOCK_SOURCE,
        ),
    ];
    const PARTITION_COLUMN: &'static str = "interval_end_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.interval_end_ts_us
    }
}

impl TableRow for DropRow {
    const TABLE: TapeTable = TapeTable::MetricsDrops;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("interval_end_ts_us", |row| row.interval_end_ts_us),
        Column::Label("sink", |row| row.sink.label(), &SINK_KIND),
        Column::U64("dropped_total", |row| row.dropped_total),
    ];
    const PARTITION_COLUMN: &'static str = "interval_end_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.interval_end_ts_us
    }
}

impl TableRow for SenderDropRow {
    const TABLE: TapeTable = TapeTable::MetricsSenderDrops;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("interval_end_ts_us", |row| row.interval_end_ts_us),
        Column::PrintedText("sender", |row| {
            row.sender
                .map_or_else(|| OTHER_SENDERS.to_owned(), |sender| sender.to_string())
        }),
        Column::U64("queue_full_total", |row| row.queue_full_total),
        Column::U64("over_rate_total", |row| row.over_rate_total),
    ];
    const PARTITION_COLUMN: &'static str = "interval_end_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.interval_end_ts_us
    }
}

impl TableRow for BacklogRow {
    const TABLE: TapeTable = TapeTable::MetricsBacklog;
    const COLUMNS: &'static [Column<Self>] = &[
        Column::Ts("interval_end_ts_us", |row| row.interval_end_ts_us),
        Column::U8("queue_id", |row| row.queue.index() as u8),
        Column::U32("max", |row| row.max),
        Column::F64("ema", |row| row.ema),
    ];
    const PARTITION_COLUMN: &'static str = "interval_end_ts_us";

    fn partition_ts(&self) -> LocalTs {
        self.interval_end_ts_us
    }
}
