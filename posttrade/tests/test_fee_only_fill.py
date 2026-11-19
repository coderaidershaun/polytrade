"""A late trade report that books only its fee: a fill row of no quantity carries the fee, and
its ledger `fill` row holds the cash it left. It moves no order totals."""

import io
from pathlib import Path

import polars as pl

from posttrade import cli, contract, runs
from posttrade.metrics import pnl

from conftest import rewrite, table_file

UNIT = 100_000_000
AT_25S = 1_784_000_025_000_000
HALF = UNIT // 2


def order_1_fills_in_two_halves(orders: pl.DataFrame) -> pl.DataFrame:
    """Order 1's report_trade becomes two: a half that leaves it live, then the rest."""
    trade = (pl.col("client_order_id") == 1) & (pl.col("transition") == "report_trade")
    first_half = orders.filter(trade).with_columns(
        pl.lit("live").alias("state"),
        pl.lit(HALF, dtype=pl.Int64).alias("filled_qty"),
        pl.lit(101 * HALF, dtype=pl.Int64).alias("filled_quote"),
    )
    rest = orders.filter(trade).with_columns(pl.lit("live").alias("previous_state"))
    index = orders.with_row_index("i").filter(trade).get_column("i")[0]
    return pl.concat([orders.head(index), first_half, rest, orders.slice(index + 1)])


def fills_with_a_fee_only_row_between(fills: pl.DataFrame) -> pl.DataFrame:
    one = fills.filter(pl.col("client_order_id") == 1)
    half = one.with_columns(
        pl.lit(HALF, dtype=pl.Int64).alias("last_qty"),
        pl.lit(HALF, dtype=pl.Int64).alias("booked_qty"),
        pl.lit(101 * HALF, dtype=pl.Int64).alias("booked_quote"),
        pl.lit(HALF, dtype=pl.Int64).alias("commission"),
    )
    fee_only = one.with_columns(
        pl.lit(0, dtype=pl.Int64).alias("booked_qty"),
        pl.lit(0, dtype=pl.Int64).alias("booked_quote"),
        pl.lit(1, dtype=pl.Int64).alias("commission"),
    )
    return pl.concat([half, fee_only, half, fills.filter(pl.col("client_order_id") != 1)])


def ledger_of_the_two_halves_and_the_fee(ledger: pl.DataFrame) -> pl.DataFrame:
    """The engine's rows: P&L = cash + exposure; the fee lowers cash and P&L by 1 from then on."""
    fill_1 = ledger.filter(pl.col("received_ts_us") == AT_25S)
    mark = fill_1.get_column("mark")[0]

    def row(position: int, cash: int) -> pl.DataFrame:
        exposure = mark * position // UNIT
        return fill_1.with_columns(
            pl.lit(position, dtype=pl.Int64).alias("position_base"),
            pl.lit(cash, dtype=pl.Int64).alias("cash_quote"),
            pl.lit(exposure, dtype=pl.Int64).alias("exposure_quote"),
            pl.lit(cash + exposure, dtype=pl.Int64).alias("pnl_quote"),
        )

    after_half = -100 * UNIT - 101 * HALF - HALF
    later = ledger.filter(pl.col("received_ts_us") > AT_25S).with_columns(
        (pl.col("cash_quote") - 1).alias("cash_quote"),
        (pl.col("pnl_quote") - 1).alias("pnl_quote"),
    )
    return pl.concat(
        [
            ledger.filter(pl.col("received_ts_us") < AT_25S),
            row(UNIT + HALF, after_half),
            row(UNIT + HALF, after_half - 1),
            row(2 * UNIT, after_half - 1 - 101 * HALF - HALF),
            later,
        ]
    )


def test_fee_only_fill_between_two_moving_fills_is_no_break(first_boot_copy: Path) -> None:
    rewrite(table_file(first_boot_copy, "orders"), order_1_fills_in_two_halves)
    rewrite(table_file(first_boot_copy, "fills"), fills_with_a_fee_only_row_between)
    rewrite(table_file(first_boot_copy, "ledger"), ledger_of_the_two_halves_and_the_fee)
    out = io.StringIO()

    code = cli.main(["check", str(first_boot_copy)], cli.CommandEnv(out=out, err=out))
    money = pnl.measure(runs.open_run(first_boot_copy, contract.load_repo_contract())).metrics

    assert code == 0, out.getvalue()
    assert money["pnl.fees_quote"] == -UNIT - 1
    assert money["pnl.unpriced_fills"] == 0
    assert money["pnl.session_quote"] == 6 * UNIT - 1
    assert money["pnl.remainder_quote"] == 0
