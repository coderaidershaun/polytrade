"""A fill names the market of the window its order was placed in, not the window it arrived in."""

from pathlib import Path

import polars as pl

from posttrade import contract, runs, tables

from conftest import rewrite, table_file

NOON_US = 1_784_030_400_000_000  # 2026-07-14 12:00:00 UTC
SECOND_US = 1_000_000


def at(minutes: int, seconds: int) -> int:
    return NOON_US + (minutes * 60 + seconds) * SECOND_US


def two_windows(frame: pl.DataFrame) -> pl.DataFrame:
    window = frame.head(1)
    first = window.with_columns(
        pl.lit(at(0, 0)).alias("window_open_ts_us"),
        pl.lit(at(5, 0)).alias("window_close_ts_us"),
        pl.lit("111").alias("token_id_up"),
    )
    second = window.with_columns(
        pl.lit(at(5, 0)).alias("window_open_ts_us"),
        pl.lit(at(10, 0)).alias("window_close_ts_us"),
        pl.lit("222").alias("token_id_up"),
    )
    return pl.concat([first, second])


def one_placed_order(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.filter(pl.col("transition") == "placed")
        .head(1)
        .with_columns(
            pl.lit(7, dtype=pl.UInt64).alias("client_order_id"),
            pl.lit(at(4, 58)).alias("received_ts_us"),
        )
    )


def one_late_fill(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.head(1).with_columns(
        pl.lit(7, dtype=pl.UInt64).alias("client_order_id"),
        pl.lit(at(5, 2)).alias("received_ts_us"),
    )


def test_late_fill_maps_by_order_placement(golden_copy: Path) -> None:
    rewrite(table_file(golden_copy, "rotations"), change_frame=two_windows)
    rewrite(table_file(golden_copy, "orders"), change_frame=one_placed_order)
    rewrite(table_file(golden_copy, "fills"), change_frame=one_late_fill)
    run = runs.open_run(golden_copy, contract.load_repo_contract())

    markets = tables.fill_markets(run)

    assert markets.select("client_order_id", "placed_ts_us", "token_id_up").rows() == [
        (7, at(4, 58), "111")
    ]
