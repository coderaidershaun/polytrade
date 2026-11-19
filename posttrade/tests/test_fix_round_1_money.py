"""The P&L walks the ledger in the fold's order and counts the ledger's own truncation."""

from pathlib import Path

import polars as pl

from posttrade import contract, runs
from posttrade.metrics import pnl

from conftest import rewrite, table_file

MID = 150_000_001  # 1.50000001: a price whose product with 1 mantissa is no whole mantissa


def two_buys_of_one_mantissa(fills: pl.DataFrame) -> pl.DataFrame:
    template = fills.head(1)
    rows = [
        template.with_columns(
            pl.lit(at, dtype=pl.Int64).alias("received_ts_us"),
            pl.lit(MID, dtype=pl.Int64).alias("last_price"),
            pl.lit(1, dtype=pl.Int64).alias("last_qty"),
            pl.lit(1, dtype=pl.Int64).alias("booked_qty"),
            pl.lit(1, dtype=pl.Int64).alias("booked_quote"),
            pl.lit(0, dtype=pl.Int64).alias("commission"),
        )
        for at in (1_784_000_020_000_000, 1_784_000_040_000_000)
    ]
    return pl.concat(rows)


def ledger_of_the_two_buys(ledger: pl.DataFrame) -> pl.DataFrame:
    """The engine's rows: cash is the booked quote; exposure is its truncation of mark x position;
    P&L is cash plus exposure."""
    template = ledger.filter(pl.col("kind") == "fill").head(1)
    rows = [
        template.with_columns(
            pl.lit(at, dtype=pl.Int64).alias("received_ts_us"),
            pl.lit(position, dtype=pl.Int64).alias("position_base"),
            pl.lit(cash, dtype=pl.Int64).alias("cash_quote"),
            pl.lit(MID, dtype=pl.Int64).alias("mark"),
            pl.lit(exposure, dtype=pl.Int64).alias("exposure_quote"),
            pl.lit(cash + exposure, dtype=pl.Int64).alias("pnl_quote"),
        )
        for at, position, cash, exposure in (
            (1_784_000_020_000_000, 1, -1, 1),
            (1_784_000_040_000_000, 2, -2, 3),
        )
    ]
    return pl.concat(rows)


def test_attribution_counts_the_ledgers_own_truncation(first_boot_copy: Path) -> None:
    """F13. Mids and marks of 1.50000001, two buys of 1 mantissa: P&L 0 then 1."""
    rewrite(table_file(first_boot_copy, "fills"), two_buys_of_one_mantissa)
    rewrite(table_file(first_boot_copy, "ledger"), ledger_of_the_two_buys)

    money = pnl.measure(runs.open_run(first_boot_copy, contract.load_repo_contract())).metrics

    assert money["pnl.session_quote"] == 1
    assert (money["pnl.spread_quote"], money["pnl.inventory_quote"]) == (1, 0)
    assert money["pnl.remainder_quote"] == 0


def test_session_ends_at_the_last_row_in_file_order() -> None:
    """F15. The spin's interval row at 41 s comes before the fill row stamped 40.9 s that the fold
    took after it; the segment ends at the fill row's P&L."""
    ledger = pl.DataFrame(
        {
            "instrument_id": [0, 0, 0],
            "kind": ["boot", "interval", "fill"],
            "received_ts_us": [5_000_000, 41_000_000, 40_900_000],
            "pnl_quote": [0, 10, 12],
        },
        schema={
            "instrument_id": pl.UInt16,
            "kind": pl.String,
            "received_ts_us": pl.Int64,
            "pnl_quote": pl.Int64,
        },
    )

    assert pnl.session_pnl_after_each_row(ledger) == [0, 10, 12]
