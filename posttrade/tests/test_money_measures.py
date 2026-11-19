"""Money, risk, order and latency measures of the golden run equal its hand values, and an
attribution whose parts do not sum to the ledger's P&L is withheld."""

import io
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from posttrade import cli, contract, runs
from posttrade.metrics import pnl

from conftest import FIRST_BOOT, SECOND_BOOT, golden_dir, rewrite, table_file

ATTRIBUTION = ("pnl.spread_quote", "pnl.inventory_quote", "pnl.fees_quote", "pnl.remainder_quote")


def report(run_dir: Path, out: Path) -> tuple[int, str, Any, str]:
    stream = io.StringIO()
    code = cli.main(["report", str(run_dir), "--out", str(out)], cli.CommandEnv(stream, stream))
    (summary,) = out.rglob("summary.json")
    page = (summary.parent / "report.html").read_text()
    return code, stream.getvalue(), json.loads(summary.read_text()), page


@pytest.mark.parametrize("boot", [FIRST_BOOT, SECOND_BOOT])
def test_golden_parts_sum_to_ledger_pnl(boot: Path) -> None:
    run = runs.open_run(golden_dir() / boot, contract.load_repo_contract())

    money = pnl.measure(run).metrics

    parts = sum(int(str(money[name])) for name in ATTRIBUTION[:3])
    assert parts == money["pnl.session_quote"]
    assert money["pnl.remainder_quote"] == 0


@pytest.mark.parametrize("index, boot", [(0, FIRST_BOOT), (1, SECOND_BOOT)])
def test_each_metric_of_golden_run_equals_expected(index: int, boot: Path, tmp_path: Path) -> None:
    expected: Any = json.loads((golden_dir() / "expected.json").read_text())
    want = expected["runs"][index]["metrics"]

    code, output, summary, _ = report(golden_dir() / boot, tmp_path)

    assert code == 0, output
    assert sorted(summary["metrics"]) == sorted(contract.load_repo_contract().metric_names)
    assert len(summary["metrics"]) == 27
    assert summary["metrics"] == want


def with_fee_raised(delta: int) -> tuple[pl.Expr, pl.Expr]:
    """Client 1's fee, in the quote asset, raised; the ledger's cash after it lowered to match,
    so that the order audit still passes and only the attribution can tell."""
    fee = (
        pl.when(pl.col("client_order_id") == 1)
        .then(pl.col("commission") + delta)
        .otherwise(pl.col("commission"))
        .alias("commission")
    )
    cash = (
        pl.when(pl.col("received_ts_us") >= 1_784_000_025_000_000)
        .then(pl.col("cash_quote") - delta)
        .otherwise(pl.col("cash_quote"))
        .alias("cash_quote")
    )
    return fee, cash


@pytest.mark.parametrize("delta", [1, -1, 12_345])
def test_remainder_withholds_attribution(first_boot_copy: Path, tmp_path: Path, delta: int) -> None:
    fee, cash = with_fee_raised(delta)
    rewrite(table_file(first_boot_copy, "fills"), lambda frame: frame.with_columns(fee))
    rewrite(table_file(first_boot_copy, "ledger"), lambda frame: frame.with_columns(cash))

    code, output, summary, page = report(first_boot_copy, tmp_path / "reports")

    assert code == 1, output
    assert summary["status"] == "FAILED"
    assert f"remainder {delta}" in output
    assert not set(ATTRIBUTION) & set(summary["metrics"])
    assert not any(name in page for name in ATTRIBUTION)
