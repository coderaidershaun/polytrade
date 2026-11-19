"""The order audit counts every fill and every lost audit row, and audits only recorded tables."""

import io
import json
import shutil
from pathlib import Path

import polars as pl

from posttrade import cli

from conftest import rewrite, table_file


def check(run_dir: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(["check", str(run_dir)], cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def findings(output: str) -> list[str]:
    return [line.split(":")[0] for line in output.splitlines() if line.startswith("finding")]


def test_fill_whose_last_order_row_was_lost_is_a_finding(first_boot_copy: Path) -> None:
    """F8. A full audit bank loses order 1's `report_trade` row; its fill and ledger rows stay."""
    lost = ~((pl.col("client_order_id") == 1) & (pl.col("transition") == "report_trade"))
    rewrite(table_file(first_boot_copy, "orders"), lambda frame: frame.filter(lost))

    code, output = check(first_boot_copy)

    assert (code, findings(output)) == (1, ["finding order_totals"]), output
    assert "taken by no order row" in output


def test_doubled_fill_row_is_a_finding(first_boot_copy: Path) -> None:
    """F8. The same fill written twice: one order row and one ledger row take one of them."""
    rewrite(table_file(first_boot_copy, "fills"), lambda frame: pl.concat([frame, frame.head(1)]))

    code, output = check(first_boot_copy)

    assert (code, findings(output)) == (1, ["finding order_totals", "finding ledger"]), output


def test_lost_audit_rows_are_a_finding_alone(golden_copy: Path) -> None:
    """F8. The engine counted audit rows lost at a full bank; no other record shows the loss."""
    lost = (
        pl.when(pl.col("sink") == "audit_lost")
        .then(pl.lit(3, dtype=pl.UInt64))
        .otherwise(pl.col("dropped_total"))
        .alias("dropped_total")
    )
    rewrite(table_file(golden_copy, "metrics_drops"), lambda frame: frame.with_columns(lost))

    code, output = check(golden_copy)

    assert (code, findings(output)) == (1, ["finding records_lost"]), output
    assert "records dropped: 3" in output


def test_run_that_recorded_no_orders_or_fills_passes(first_boot_copy: Path) -> None:
    """F9. `strategy.tables` left out `orders` and `fills`; ledger rows are written always."""
    manifest_path = first_boot_copy / "run.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"] = [t for t in manifest["tables"] if t not in ("orders", "fills")]
    manifest_path.write_text(json.dumps(manifest))
    shutil.rmtree(first_boot_copy / "orders")
    shutil.rmtree(first_boot_copy / "fills")

    code, output = check(first_boot_copy)

    assert code == 0, output
