//! The rows of the metrics thread in the golden run: latency rows that give each category, each
//! stage and each clock source, and a row of each sink and each kind of sender.

use std::net::SocketAddr;

use engine::msg::ids::QueueId;
use engine::msg::latency::{Category, Stage};
use engine::msg::time::{ClockSource, LocalTs};
use engine::runtime::metrics::{BacklogRow, DropRow, LatencyRow, SenderDropRow, SinkKind};
use serde_json::{Value, json};

use crate::cells::Run;

/// Ten latency rows give each category and each stage at least once, each other row of the
/// metrics thread each sink and both kinds of sender.
pub fn coverage(run: &mut Run, start: LocalTs, end: LocalTs) -> Value {
    let sources = [
        ClockSource::Tsc,
        ClockSource::Cntvct,
        ClockSource::Monotonic,
    ];
    let mut latency = serde_json::Map::new();
    for (index, stage) in Stage::ALL.into_iter().enumerate() {
        let step = index as u64 + 1;
        let category = Category::ALL[index % Category::ALL.len()];
        let row = LatencyRow {
            interval_start_ts_us: start,
            interval_end_ts_us: end,
            category,
            stage,
            count: 10 * step,
            dropped: step % 2,
            backward: 0,
            negative: 0,
            p50_ns: 100 * step,
            p99_ns: 200 * step,
            p999_ns: 300 * step,
            max_ns: 400 * step,
            budget_p99_ns: (index % 2 == 0).then_some(1_000 * step),
            budget_p999_ns: (index % 2 == 1).then_some(2_000 * step),
            is_over_budget: index % 3 == 0,
            clock_source: sources[index % sources.len()],
        };
        run.latency(row);
        let budget = row
            .budget_p99_ns
            .map_or_else(|| Value::from("no budget"), Value::from);
        latency.insert(
            format!("{}/{}", category.label(), stage.label()),
            json!({
                "p50_ns": row.p50_ns, "p99_ns": row.p99_ns, "p999_ns": row.p999_ns,
                "max_ns": row.max_ns, "budget_p99_ns": budget
            }),
        );
    }
    // The golden run is a correct run, and a run whose engine lost audit rows is no correct run:
    // the order audit refuses it. So `audit_lost` counts 0; each other sink keeps its count.
    for (index, sink) in SinkKind::ALL.into_iter().enumerate() {
        let dropped_total = if sink == SinkKind::AuditLost { 0 } else { index as u64 };
        run.drops(DropRow {
            interval_end_ts_us: end,
            sink,
            dropped_total,
        });
    }
    let peer: SocketAddr = ([127, 0, 0, 1], 9_311).into();
    for (sender, text) in [(Some(peer), "127.0.0.1:9311"), (None, "others")] {
        let row = SenderDropRow {
            interval_end_ts_us: end,
            sender,
            queue_full_total: 2,
            over_rate_total: 1,
        };
        run.sender_drops(row, text);
    }
    for (queue, max, ema) in [(0, 12, 1.5), (5, 0, 0.0)] {
        run.backlog(BacklogRow {
            interval_end_ts_us: end,
            queue: QueueId::new(queue).expect("a queue id below the cap"),
            max,
            ema,
        });
    }
    Value::Object(latency)
}

/// One stage of one category over the minute of the first boot, with a budget for each tail.
pub fn one_stage(run: &mut Run, start: LocalTs, end: LocalTs) {
    run.latency(LatencyRow {
        interval_start_ts_us: start,
        interval_end_ts_us: end,
        category: Category::Trade,
        stage: Stage::VenueToEngine,
        count: 188,
        dropped: 0,
        backward: 0,
        negative: 0,
        p50_ns: 1_000,
        p99_ns: 5_000,
        p999_ns: 9_000,
        max_ns: 12_000,
        budget_p99_ns: Some(50_000),
        budget_p999_ns: Some(80_000),
        is_over_budget: false,
        clock_source: ClockSource::Monotonic,
    });
}
