"""Measures the difference of two runs. Exact mode compares a run with its re-fold, cell for cell:
any difference is a determinism breach, or look-ahead in a feature.

Two kinds of row are in the re-fold and not in the run, and neither is a difference: a research row
that the run dropped on a full ring, while a table holds no more of them than the run counted
dropped in `metrics_drops`; and each row past the last row of a table that a writer fault stopped,
which `stopped_tables` of the run's `run.json` names.
"""

import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Final

import polars as pl

from posttrade import tables
from posttrade.audit import tape_health
from posttrade.errors import ContractMismatch, RefoldDiff, RunsUnpaired, TapeGapped
from posttrade.runs import Run

__all__ = ["CompareMode", "RefoldMatch", "TableMatch", "compare_exact"]

#: The record of money: each row equal, none extra.
AUDIT_TABLES: Final = ("orders", "fills", "incidents", "ledger")

#: Research tables, each with the sink that counts its drops in `metrics_drops`.
RESEARCH_TABLES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "features": "research_features",
        "trades": "research_trades",
        "book_events": "research_book_events",
        "klines": "research_klines",
        "link_frames": "research_link_frames",
        "mids": "research_mids",
        "no_quotes": "research_no_quotes",
    }
)

REPLAY_KIND: Final = "replay"


class CompareMode(Enum):
    EXACT = "exact"


@dataclass(frozen=True, slots=True)
class TableMatch:
    """One table that the run and its re-fold hold alike: its rows in the run, the rows that only
    the re-fold holds taken by the run's drop count, and those past the stop of a stopped table."""

    table: str
    rows: int
    taken_by_drops: int
    past_stop: int


@dataclass(frozen=True, slots=True)
class RefoldMatch:
    tables: tuple[TableMatch, ...]

    def summary(self) -> str:
        held = ", ".join(f"{match.table} {match.rows}" for match in self.tables)
        taken = [m for m in self.tables if m.taken_by_drops > 0]
        dropped = "".join(
            f"; {match.table}: {match.taken_by_drops} rows only in the re-fold, dropped by the run"
            for match in taken
        )
        stops = "".join(
            f"; {match.table}: {match.past_stop} rows past the stop of the table in the run"
            for match in self.tables
            if match.past_stop > 0
        )
        return f"0 differences in exact mode (rows: {held}){dropped}{stops}"


def compare_exact(run: Run, replay: Run) -> RefoldMatch:
    """Compares each table that the fold writes, in contract order.

    Raises:
        RunsUnpaired: `replay` is no re-fold of `run` by its manifest.
        TapeGapped: the tape of `run` lost records, so no re-fold of it can equal it.
        RefoldDiff: the first cell, or the first row, in which the two runs part.
        FileVanished: a file was removed after it was listed.
        ContractMismatch: a file differs from the contract.
    """
    _check_pair(run, replay)
    gaps = tape_health(run).gaps
    if gaps:
        raise TapeGapped(run.dir, gaps[0].first_lost_seq, gaps[0].last_lost_seq)
    drops = _research_drops(run)
    stopped = _stopped_tables(run)
    matches = [_exact_table(name, run, replay, name in stopped) for name in AUDIT_TABLES]
    for name, sink in RESEARCH_TABLES.items():
        matches.append(_research_table(name, run, replay, drops.get(sink, 0), name in stopped))
    return RefoldMatch(tuple(matches))


def _check_pair(run: Run, replay: Run) -> None:
    source = replay.manifest.raw.get("source")
    if replay.manifest.kind.value != REPLAY_KIND:
        raise RunsUnpaired(run.dir, replay.dir, f"its kind is {replay.manifest.kind.value}")
    if not isinstance(source, dict):
        raise RunsUnpaired(run.dir, replay.dir, "its run.json names no source run")
    if source.get("run_id") != run.manifest.run_id:
        raise RunsUnpaired(
            run.dir,
            replay.dir,
            f"its source is run {source.get('run_id')!r}, not {run.manifest.run_id}",
        )
    if source.get("config_digest") != run.manifest.raw.get("config_digest"):
        raise RunsUnpaired(run.dir, replay.dir, "it was folded with another config")


def _stopped_tables(run: Run) -> frozenset[str]:
    stopped = run.manifest.raw["stopped_tables"]
    if not isinstance(stopped, list) or not all(isinstance(name, str) for name in stopped):
        raise ContractMismatch(
            run.dir / run.contract.manifest_file, "stopped_tables", stopped, "a list of names"
        )
    return frozenset(stopped)


def _exact_table(name: str, run: Run, replay: Run, is_stopped: bool) -> TableMatch:
    ours, theirs = _rows(run, name), _rows(replay, name)
    columns = run.contract.table(name).column_names
    for index, (mine, other) in enumerate(zip(ours, theirs, strict=False)):
        _same_row(name, index, columns, mine, other)
    if len(ours) > len(theirs):
        raise RefoldDiff(name, len(theirs) + 1, None, ours[len(theirs)], None)
    if len(theirs) > len(ours) and not is_stopped:
        raise RefoldDiff(name, len(ours) + 1, None, None, theirs[len(ours)])
    return TableMatch(name, len(ours), 0, len(theirs) - min(len(theirs), len(ours)))


def _research_table(name: str, run: Run, replay: Run, dropped: int, is_stopped: bool) -> TableMatch:
    """The run's rows come in the re-fold in order; a row of the re-fold alone is taken as one
    that the run dropped, until the drops are spent, and, for a stopped table, as one past the
    stop once each row of the run is matched."""
    ours, theirs = _rows(run, name), _rows(replay, name)
    columns = run.contract.table(name).column_names
    matched = 0
    extra = 0
    past_stop = 0
    for other in theirs:
        if matched < len(ours) and _keys(ours[matched]) == _keys(other):
            matched += 1
            continue
        if matched == len(ours) and is_stopped:
            past_stop += 1
            continue
        if extra < dropped:
            extra += 1
            continue
        if matched < len(ours):
            _same_row(name, matched, columns, ours[matched], other)
        raise RefoldDiff(name, matched + 1, None, None, other)
    if matched < len(ours):
        raise RefoldDiff(name, matched + 1, None, ours[matched], None)
    return TableMatch(name, len(ours), extra, past_stop)


def _same_row(
    table: str,
    index: int,
    columns: Sequence[str],
    mine: tuple[object, ...],
    other: tuple[object, ...],
) -> None:
    for column, a, b in zip(columns, mine, other, strict=True):
        if _key(a) != _key(b):
            raise RefoldDiff(table, index + 1, column, a, b)


def _rows(run: Run, name: str) -> list[tuple[object, ...]]:
    """The contract's columns of each row; the decoded name columns follow from them."""
    columns = run.contract.table(name).column_names
    return list(tables.read_table(run, name).select(columns).iter_rows())


def _keys(row: tuple[object, ...]) -> tuple[object, ...]:
    return tuple(_key(value) for value in row)


def _key(value: object) -> object:
    """A float compares by its bits, so NaN equals NaN and 0.0 differs from -0.0, as in the file."""
    if isinstance(value, float):
        return struct.pack("<d", value)
    return value


def _research_drops(run: Run) -> dict[str, int]:
    """The last count of each sink in the run's `metrics_drops`: each row holds the total since the
    start of the run."""
    frame = tables.read_table(run, "metrics_drops")
    totals = frame.group_by("sink").agg(pl.col("dropped_total").max())
    return {sink: int(total) for sink, total in totals.iter_rows()}
