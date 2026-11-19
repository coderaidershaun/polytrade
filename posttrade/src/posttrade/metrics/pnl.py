"""The P&L of one run from its ledger, and its attribution to spread, inventory and fees.

The ledger rows are walked in file order, which is the order of the fold, and never by stamp.
For each row j after the start of a segment, with x the position of the row before it and M the
mark of row j: inventory is notional(M, x) less the exposure of the row before; a fill row books
the next fill of its own order in file order, so its spread is its exposure less notional(M, x)
less s x the fill's booked quote, and its fee is minus the commission paid in the quote asset;
on any other row the spread is its exposure less notional(M, x). Each exposure is the ledger's
own truncation of mark x position, so the parts of a row sum to the change of its P&L exactly:
the remainder is 0 on correct records at any tick, and is not 0 only where a booking differs
from its fill row or a P&L is not its cash plus its exposure. The ledger's truncation is thus
counted, not a source of remainder. `notional` truncates toward zero, as the engine does;
Python's `//` floors, so it only ever divides a value that is not negative.
"""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

import polars as pl

from posttrade import tables
from posttrade.errors import ContractMismatch
from posttrade.runs import Run

__all__ = ["ATTRIBUTION_PARTS", "PnlMeasures", "measure", "notional", "session_pnl_after_each_row"]

ATTRIBUTION_PARTS: Final = (
    "pnl.spread_quote",
    "pnl.inventory_quote",
    "pnl.fees_quote",
    "pnl.remainder_quote",
)


# A run whose `strategy.tables` leaves out `fills` still writes its ledger.
WITHHELD_WITHOUT_FILLS: Final = (*ATTRIBUTION_PARTS, "pnl.unpriced_fills")
NO_FILLS_RECORDED: Final = "the run records no fills, so its P&L has no attribution"


@dataclass(frozen=True, slots=True)
class PnlMeasures:
    """The P&L measures; `withheld` says why the attribution is left out, when it is."""

    metrics: Mapping[str, object]
    remainder: int
    withheld: str | None = None


@dataclass(frozen=True, slots=True)
class _Fill:
    side: str
    booked_quote: int
    commission: int
    commission_asset_id: int


@dataclass(slots=True)
class _Segment:
    """One instrument's walk: the windows it closed, the window it is in, and the row before."""

    closed: int = 0
    start: int = 0
    current: int = 0
    is_open: bool = False
    position: int = 0
    exposure: int = 0

    @property
    def so_far(self) -> int:
        return self.closed + (self.current - self.start if self.is_open else 0)

    def begin(self, start: int, position: int, exposure: int) -> None:
        self.start = self.current = start
        self.position, self.exposure = position, exposure
        self.is_open = True

    def close(self) -> None:
        self.closed += self.current - self.start
        self.is_open = False


def notional(price: int, qty: int, fixed_scale: int) -> int:
    """`price x qty / fixed_scale`, truncated toward zero."""
    product = price * qty
    sign = -1 if product < 0 else 1
    return sign * (abs(product) // fixed_scale)


def measure(run: Run) -> PnlMeasures:
    scale = run.contract.fixed_scale
    fills = tables.read_table(run, "fills")
    ledger = tables.read_table(run, "ledger")
    quote_assets = run.manifest.quote_asset_ids
    pending = _fills_by_order(fills)
    booked = 0
    spread = inventory = fees = 0
    segments: dict[int, _Segment] = defaultdict(_Segment)
    columns = (
        "instrument_id",
        "kind",
        "client_order_id",
        "position_base",
        "mark",
        "exposure_quote",
        "pnl_quote",
    )
    for instrument, kind, client, position, mark, exposure, pnl in ledger.select(
        columns
    ).iter_rows():
        segment = segments[instrument]
        if kind == "boot":
            segment.begin(pnl, position, exposure)
            continue
        if not segment.is_open:
            segment.begin(0, 0, 0)
        marked = 0 if mark is None else notional(mark, segment.position, scale)
        inventory += marked - segment.exposure
        row_spread = exposure - marked
        queue = pending.get((instrument, client), []) if kind == "fill" else []
        if queue:
            fill = queue.pop(0)
            booked += 1
            sign = 1 if fill.side == "buy" else -1
            row_spread -= sign * fill.booked_quote
            if fill.commission_asset_id == _quote_asset(run, quote_assets, instrument):
                fees -= fill.commission
        spread += row_spread
        segment.position, segment.exposure, segment.current = position, exposure, pnl
        if kind == "rotation":
            segment.close()
    session = sum(segment.so_far for segment in segments.values())
    fees_quote, other_asset_fills = _fees(run, fills, quote_assets)
    remainder = session - (spread + inventory + fees)
    metrics = {
        "fees.quote": fees_quote,
        "fees.other_asset_fills": other_asset_fills,
        "pnl.session_quote": session,
        "pnl.spread_quote": spread,
        "pnl.unpriced_fills": fills.height - booked,
        "pnl.inventory_quote": inventory,
        "pnl.fees_quote": fees,
        "pnl.remainder_quote": remainder,
    }
    if "fills" not in run.manifest.tables:
        # The attribution books each ledger fill row against its fill; with no fills table each
        # fill row books nothing, and a remainder would only name the missing table.
        kept = {k: v for k, v in metrics.items() if k not in WITHHELD_WITHOUT_FILLS}
        return PnlMeasures(metrics=kept, remainder=0, withheld=NO_FILLS_RECORDED)
    return PnlMeasures(metrics=metrics, remainder=remainder)


def session_pnl_after_each_row(ledger: pl.DataFrame) -> list[int]:
    """The session P&L of all instruments after each ledger row, in file order.

    A segment of an instrument starts at its boot row, at that row's P&L, before its first row
    when it has no boot row, at 0, or after a rotation row, at 0; it ends at a rotation row or at
    the instrument's last row.
    """
    segments: dict[int, _Segment] = defaultdict(_Segment)
    totals = []
    for instrument, kind, pnl in ledger.select("instrument_id", "kind", "pnl_quote").iter_rows():
        segment = segments[instrument]
        if kind == "boot":
            segment.begin(pnl, 0, 0)
        elif not segment.is_open:
            segment.begin(0, 0, 0)
        segment.current = pnl
        if kind == "rotation":
            segment.close()
        totals.append(sum(s.so_far for s in segments.values()))
    return totals


def _quote_asset(run: Run, quote_assets: Mapping[int, int], instrument: int) -> int:
    if instrument not in quote_assets:
        raise ContractMismatch(
            run.dir / run.contract.manifest_file,
            "instruments",
            f"no instrument {instrument}",
            "each instrument of the ledger and the fills",
        )
    return quote_assets[instrument]


def _fees(run: Run, fills: pl.DataFrame, quote_assets: Mapping[int, int]) -> tuple[int, int]:
    """Fees paid in the quote asset, and the count of fills that paid a fee in another asset."""
    in_quote = 0
    other = 0
    columns = ("instrument_id", "commission", "commission_asset_id")
    for instrument, commission, asset in fills.select(columns).iter_rows():
        if asset == _quote_asset(run, quote_assets, instrument):
            in_quote += commission
        elif commission != 0:
            other += 1
    return in_quote, other


def _fills_by_order(fills: pl.DataFrame) -> dict[tuple[int, int], list[_Fill]]:
    """The fills of each order in file order: each ledger fill row books the next one."""
    grouped: dict[tuple[int, int], list[_Fill]] = defaultdict(list)
    columns = (
        "instrument_id",
        "client_order_id",
        "side",
        "booked_quote",
        "commission",
        "commission_asset_id",
    )
    for instrument, client, side, quote, fee, asset in fills.select(columns).iter_rows():
        grouped[(instrument, client)].append(_Fill(side, quote, fee, asset))
    return dict(grouped)
