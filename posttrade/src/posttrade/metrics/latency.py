"""The latency of each stage of one run, from the metrics table: p50, p99, p99.9 and max, never
a mean, each beside the stage's p99 budget when the config held one."""

from collections.abc import Mapping
from typing import Final

from posttrade import tables
from posttrade.runs import Run

__all__ = ["NO_BUDGET", "measure"]

NO_BUDGET: Final = "no budget"
PERCENTILES: Final = ("p50_ns", "p99_ns", "p999_ns", "max_ns")


def measure(run: Run) -> Mapping[str, object]:
    """Each value is the largest of the run's intervals: the worst interval stands for the run."""
    rows = tables.read_table(run, "metrics_latency")
    stages: dict[str, dict[str, object]] = {}
    columns = ("category", "stage", *PERCENTILES, "budget_p99_ns")
    for category, stage, *values, budget in rows.select(columns).iter_rows():
        name = f"{category}/{stage}"
        seen = stages.setdefault(
            name, {key: 0 for key in PERCENTILES} | {"budget_p99_ns": NO_BUDGET}
        )
        for key, value in zip(PERCENTILES, values, strict=True):
            seen[key] = max(int(str(seen[key])), int(value))
        if budget is not None:
            seen["budget_p99_ns"] = int(budget)
    return {"latency": stages}
