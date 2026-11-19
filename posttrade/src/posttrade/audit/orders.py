"""Checks the order records of one run: each chain of transitions, each order's filled totals
against its fills, the ledger against the fills added from the boot position, that each fill of
this run is taken, and that the engine lost no audit row.

Only the tables that `run.json` says the run recorded are audited. Sums use Python integers, so no
total of mantissas can wrap.
"""

from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import polars as pl

from posttrade import tables
from posttrade.errors import ContractMismatch, Finding, FindingKind
from posttrade.runs import Run

__all__ = ["check_orders"]

FIRST_PREVIOUS_STATE = "free"


@dataclass(frozen=True, slots=True)
class _Fill:
    row: int
    provenance: str
    side: str
    booked_qty: int
    booked_quote: int
    commission: int
    commission_asset_id: int

    @property
    def moves_totals(self) -> bool:
        """A late report that books only its fee writes a fill row of no quantity and no quote:
        it moves the cash and the ledger, but no order row's totals."""
        return (self.booked_qty, self.booked_quote) != (0, 0)


Fills = dict[tuple[int, int], list[_Fill]]
Taken = dict[tuple[int, int], int]


def check_orders(run: Run) -> tuple[Finding, ...]:
    """The findings of each rule whose tables the run recorded.

    A strategy can leave `orders` and `fills` out of `strategy.tables`, while the engine writes
    `ledger` always; a rule that needs a table the run did not record would find false breaks.
    """
    recorded = set(run.manifest.tables)
    orders = tables.read_table(run, "orders")
    fills = tables.read_table(run, "fills")
    ledger = tables.read_table(run, "ledger")
    found: list[Finding] = []
    if "orders" in recorded:
        found += _chain_breaks(orders)
    every_fill = _fills_by_order(fills)
    if {"orders", "fills"} <= recorded:
        moving = {key: [f for f in group if f.moves_totals] for key, group in every_fill.items()}
        breaks, taken = _total_breaks(orders, moving)
        found += breaks
        found += _untaken(moving, taken, FindingKind.ORDER_TOTALS, "order row")
    if {"ledger", "fills"} <= recorded:
        breaks, taken = _ledger_breaks(run, ledger, every_fill, run.manifest.quote_asset_ids)
        found += breaks
        found += _untaken(every_fill, taken, FindingKind.LEDGER, "ledger row")
    found += _lost_audit_rows(run)
    return tuple(found)


def _untaken(fills_of: Fills, taken: Taken, kind: FindingKind, taker: str) -> list[Finding]:
    """Each fill of this run's orders is taken by one row: a fill left over is a lost row, or a
    fill row written twice."""
    left: list[Finding] = []
    for key, pending in fills_of.items():
        for fill in pending[taken.get(key, 0) :]:
            if fill.provenance != "mine":
                continue
            left.append(
                Finding(
                    kind=kind,
                    table="fills",
                    row=fill.row,
                    client_order_id=key[1],
                    detail=f"fill of {fill.booked_qty} at {fill.booked_quote} is taken by no "
                    f"{taker}",
                )
            )
    return left


def _lost_audit_rows(run: Run) -> list[Finding]:
    """The engine counts each audit row that a full bank lost; no other record shows the loss."""
    drops = tables.read_table(run, "metrics_drops").filter(pl.col("sink") == "audit_lost")
    largest = drops.get_column("dropped_total").max()
    lost = largest if isinstance(largest, int) else 0
    if lost == 0:
        return []
    return [
        Finding(
            kind=FindingKind.RECORDS_LOST,
            table="metrics_drops",
            row=0,
            client_order_id=None,
            detail=f"the engine lost {lost} audit rows at a full bank",
        )
    ]


def _chain_breaks(orders: pl.DataFrame) -> Iterator[Finding]:
    """A row whose previous state is not the state of the row before it shows a lost row."""
    state_of: dict[tuple[int, int], str] = {}
    columns = ("instrument_id", "client_order_id", "transition", "previous_state", "state")
    for row, (instrument, client, transition, previous, state) in enumerate(
        orders.select(columns).iter_rows(), start=1
    ):
        expected = state_of.get((instrument, client), FIRST_PREVIOUS_STATE)
        if previous != expected:
            yield Finding(
                kind=FindingKind.ORDER_CHAIN,
                table="orders",
                row=row,
                client_order_id=client,
                detail=f"{transition} has previous_state {previous!r}, the row before left "
                f"{expected!r}",
            )
        state_of[(instrument, client)] = state


def _total_breaks(orders: pl.DataFrame, fills_of: Fills) -> tuple[list[Finding], Taken]:
    """Each row of an order of this run holds the totals of the fills that the fold took before it.

    Rows and fills are compared in file order, which is the order of the fold, and never by
    stamp: a cancel stamped by a spin can come before a fill that the venue stamped earlier but
    the fold took later, and that cancel rightly holds no fill. The engine writes each fill row
    beside the order row that it moves, so each row whose totals moved takes the order's next fill.
    A prior run's order is left out: its fills come after the order was reconciled away, so its
    rows hold no totals. `fills_of` holds only the fills that move totals: a fee-only fill has no
    order row, and the ledger rule takes it.
    """
    taken: Taken = defaultdict(int)
    breaks: list[Finding] = []
    shown: dict[tuple[int, int], tuple[int, int]] = defaultdict(lambda: (0, 0))
    booked: dict[tuple[int, int], tuple[int, int]] = defaultdict(lambda: (0, 0))
    columns = (
        "instrument_id",
        "client_order_id",
        "provenance",
        "transition",
        "filled_qty",
        "filled_quote",
    )
    for row, (instrument, client, provenance, transition, qty, quote) in enumerate(
        orders.select(columns).iter_rows(), start=1
    ):
        if provenance != "mine":
            continue
        key = (instrument, client)
        pending = fills_of.get(key, [])
        if (qty, quote) != shown[key] and taken[key] < len(pending):
            fill = pending[taken[key]]
            booked_qty, booked_quote = booked[key]
            booked[key] = (booked_qty + fill.booked_qty, booked_quote + fill.booked_quote)
            taken[key] += 1
        shown[key] = (qty, quote)
        if (qty, quote) != booked[key]:
            booked_qty, booked_quote = booked[key]
            breaks.append(
                Finding(
                    kind=FindingKind.ORDER_TOTALS,
                    table="orders",
                    row=row,
                    client_order_id=client,
                    detail=f"{transition} holds filled_qty {qty} and filled_quote {quote}, its "
                    f"fills booked {booked_qty} and {booked_quote}",
                )
            )
    return breaks, taken


def _ledger_breaks(
    run: Run, ledger: pl.DataFrame, fills_of: Fills, quote_asset_ids: Mapping[int, int]
) -> tuple[list[Finding], Taken]:
    """Each ledger row holds the boot position with the fills of the ledger's fill rows before it
    added; a rotation row is the last row of its window, and the next window starts from nothing.

    A fill row of the ledger takes the next fill of its order, in file order: an interval row
    stamped by a spin can come before a fill that the venue stamped earlier. A fee-only fill has
    its own ledger fill row, so it is taken here like each other fill.
    """
    taken: Taken = defaultdict(int)
    breaks: list[Finding] = []
    held: dict[int, tuple[int, int]] = defaultdict(lambda: (0, 0))
    columns = ("instrument_id", "kind", "client_order_id", "position_base", "cash_quote")
    for row, (instrument, kind, client, position, cash) in enumerate(
        ledger.select(columns).iter_rows(), start=1
    ):
        if kind == "boot":
            held[instrument] = (position, cash)
            continue
        pending = fills_of.get((instrument, client), [])
        if kind == "fill" and taken[(instrument, client)] < len(pending):
            fill = pending[taken[(instrument, client)]]
            if instrument not in quote_asset_ids:
                raise ContractMismatch(
                    run.dir / run.contract.manifest_file,
                    "instruments",
                    f"no instrument {instrument}",
                    "each instrument of the ledger",
                )
            held[instrument] = _after_fill(held[instrument], fill, quote_asset_ids[instrument])
            taken[(instrument, client)] += 1
        if (position, cash) != held[instrument]:
            want_position, want_cash = held[instrument]
            breaks.append(
                Finding(
                    kind=FindingKind.LEDGER,
                    table="ledger",
                    row=row,
                    client_order_id=None,
                    detail=f"{kind} row of instrument {instrument} holds position {position} and "
                    f"cash {cash}, its fills give {want_position} and {want_cash}",
                )
            )
        if kind == "rotation":
            held[instrument] = (0, 0)
    return breaks, taken


def _after_fill(held: tuple[int, int], fill: _Fill, quote_asset_id: int) -> tuple[int, int]:
    position, cash = held
    sign = 1 if fill.side == "buy" else -1
    fee = fill.commission if fill.commission_asset_id == quote_asset_id else 0
    return position + sign * fill.booked_qty, cash - sign * fill.booked_quote - fee


def _fills_by_order(fills: pl.DataFrame) -> Fills:
    """The fills of each order, in file order, which is the order of the fold."""
    grouped: dict[tuple[int, int], list[_Fill]] = defaultdict(list)
    for instrument, client, fill in _each_fill(fills):
        grouped[(instrument, client)].append(fill)
    return dict(grouped)


def _each_fill(fills: pl.DataFrame) -> Iterator[tuple[int, int, _Fill]]:
    columns = (
        "instrument_id",
        "client_order_id",
        "provenance",
        "side",
        "booked_qty",
        "booked_quote",
        "commission",
        "commission_asset_id",
    )
    for row, (instrument, client, provenance, side, qty, quote, fee, fee_asset) in enumerate(
        fills.select(columns).iter_rows(), start=1
    ):
        yield instrument, client, _Fill(row, provenance, side, qty, quote, fee, fee_asset)
