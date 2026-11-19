"""The risk of one run from its ledger: the largest position and the largest drawdown."""

from collections.abc import Mapping

from posttrade import tables
from posttrade.metrics.pnl import session_pnl_after_each_row
from posttrade.runs import Run

__all__ = ["measure"]


def measure(run: Run) -> Mapping[str, object]:
    ledger = tables.read_table(run, "ledger")
    positions = [abs(int(p)) for p in ledger.get_column("position_base").to_list()]
    return {
        "risk.max_abs_position_base": max(positions, default=0),
        "risk.max_drawdown_quote": _max_drawdown(session_pnl_after_each_row(ledger)),
    }


def _max_drawdown(session: list[int]) -> int:
    """The largest fall of the session P&L from its running peak."""
    peak = 0
    largest = 0
    for value in session:
        peak = max(peak, value)
        largest = max(largest, peak - value)
    return largest
