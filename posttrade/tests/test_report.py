"""`posttrade report` checks the run first and writes one HTML page and one JSON summary."""

import io
import json
from pathlib import Path

import polars as pl

from posttrade import cli

from conftest import FIRST_BOOT, golden_dir, rewrite, table_file


def report(run_dir: Path, out: Path) -> tuple[int, str]:
    stream = io.StringIO()
    code = cli.main(["report", str(run_dir), "--out", str(out)], cli.CommandEnv(stream, stream))
    return code, stream.getvalue()


def written(out: Path) -> tuple[dict[str, object], str]:
    (summary,) = out.rglob("summary.json")
    page = (summary.parent / "report.html").read_text()
    loaded: dict[str, object] = json.loads(summary.read_text())
    return loaded, page


def test_report_of_golden_run_holds_its_fill_measures(tmp_path: Path) -> None:
    code, output = report(golden_dir() / FIRST_BOOT, tmp_path)

    summary, page = written(tmp_path)
    assert code == 0, output
    assert summary["status"] == "PASSED"
    metrics = summary["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["markout.1s.mean_bps"] == 243.75294672324375
    assert metrics["fills.fill_rate"] == 0.4
    assert "<h2>Fills</h2>" in page
    assert "http" not in page and "<script" not in page and "<link" not in page


def test_report_of_broken_run_is_failed_and_holds_no_measure(
    first_boot_copy: Path, tmp_path: Path
) -> None:
    lost_ack = ~((pl.col("client_order_id") == 3) & (pl.col("transition") == "ack_placed"))
    rewrite(table_file(first_boot_copy, "orders"), lambda frame: frame.filter(lost_ack))
    out = tmp_path / "reports"

    code, output = report(first_boot_copy, out)

    summary, page = written(out)
    assert code == 1, output
    assert (summary["status"], summary["metrics"]) == ("FAILED", {})
    assert "client order 3" in page and "<h2>Fills</h2>" not in page


def test_report_refuses_output_into_the_run(tmp_path: Path) -> None:
    out = golden_dir() / FIRST_BOOT / "reports"

    code, output = report(golden_dir() / FIRST_BOOT, out)

    assert code == 2 and "refused to write" in output
    assert not out.exists()
