"""`posttrade check` finds each planted break once, and passes each golden run as written."""

import io
from pathlib import Path

import polars as pl
import pytest

from posttrade import cli

from conftest import FIRST_BOOT, SECOND_BOOT, golden_dir, rewrite, table_file


def check(run_dir: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(["check", str(run_dir)], cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


@pytest.mark.parametrize("boot", [FIRST_BOOT, SECOND_BOOT])
def test_golden_run_passes_the_check(boot: Path) -> None:
    code, output = check(golden_dir() / boot)
    assert (code, output.splitlines()[-1]) == (0, f"PASSED: {golden_dir() / boot}"), output


def test_four_planted_breaks_give_four_findings(first_boot_copy: Path) -> None:
    lost_ack = ~((pl.col("client_order_id") == 3) & (pl.col("transition") == "ack_placed"))
    jumped_total = (
        pl.when((pl.col("client_order_id") == 1) & (pl.col("transition") == "report_trade"))
        .then(pl.lit(200_000_000, dtype=pl.Int64))
        .otherwise(pl.col("filled_qty"))
        .alias("filled_qty")
    )
    wrong_cash = (
        pl.when(pl.col("kind") == "interval")
        .then(pl.col("cash_quote") + 1)
        .otherwise(pl.col("cash_quote"))
        .alias("cash_quote")
    )
    rewrite(
        table_file(first_boot_copy, "orders"),
        change_frame=lambda frame: frame.filter(lost_ack).with_columns(jumped_total),
    )
    rewrite(table_file(first_boot_copy, "ledger"), lambda frame: frame.with_columns(wrong_cash))
    rewrite(
        table_file(first_boot_copy, "features"), lambda frame: pl.concat([frame, frame.head(1)])
    )

    code, output = check(first_boot_copy)

    findings = [line for line in output.splitlines() if line.startswith("finding")]
    assert code == 1, output
    assert [line.split(":")[0] for line in findings] == [
        "finding order_chain",
        "finding order_totals",
        "finding ledger",
        "finding duplicate_feature",
    ], output
