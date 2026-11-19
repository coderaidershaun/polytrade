"""The orders of one run: placed, canceled, rejected and filled, the round trip of each place,
and the spins that a side did not quote, by cause."""

import math
from collections.abc import Mapping, Sequence

import polars as pl

from posttrade import tables
from posttrade.runs import Run

__all__ = ["measure", "nearest_rank"]


def measure(run: Run) -> Mapping[str, object]:
    orders = tables.read_table(run, "orders")
    no_quotes = tables.read_table(run, "no_quotes")
    reasons = run.contract.labels("no_quote_reason")
    counted = dict(no_quotes.get_column("reason").value_counts().iter_rows())
    round_trips = _ack_round_trips_us(orders)
    return {
        "orders.placed": orders.filter(pl.col("transition") == "placed").height,
        "orders.canceled": _orders_reaching(orders, "closed_canceled"),
        "orders.rejected": orders.filter(pl.col("reject_class") != "none").height,
        "orders.filled": _orders_reaching(orders, "closed_filled"),
        "orders.ack_round_trip_us": {
            "p50": nearest_rank(round_trips, 0.50),
            "p99": nearest_rank(round_trips, 0.99),
            "max": max(round_trips, default=None),
        },
        "quoting.not_quoting_spins": {reason: int(counted.get(reason, 0)) for reason in reasons},
    }


def nearest_rank(values: Sequence[int], share: float) -> int | None:
    """The smallest value with at least `share` of the values at or below it."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(share * len(ordered)) - 1]


def _orders_reaching(orders: pl.DataFrame, state: str) -> int:
    reached = orders.filter(pl.col("state") == state)
    return reached.select("instrument_id", "client_order_id").unique().height


def _ack_round_trips_us(orders: pl.DataFrame) -> list[int]:
    """From the received stamp of each `placed` row to that of the order's `ack_placed` row."""
    key = ["instrument_id", "client_order_id"]
    placed = (
        orders.filter(pl.col("transition") == "placed")
        .group_by(key)
        .agg(pl.col("received_ts_us").min().alias("placed_us"))
    )
    acked = (
        orders.filter(pl.col("transition") == "ack_placed")
        .group_by(key)
        .agg(pl.col("received_ts_us").min().alias("acked_us"))
    )
    both = placed.join(acked, on=key, how="inner")
    return [int(a) - int(p) for p, a in both.select("placed_us", "acked_us").iter_rows()]
