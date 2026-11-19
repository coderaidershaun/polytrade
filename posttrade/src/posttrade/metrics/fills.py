"""Measures of the fills of one run: markouts, effective spread, slices, fill rate, quoting time.

A markout of a fill at horizon h is s x (mid(t + h) - p) / p in basis points, where s is +1 for a
buy and -1 for a sell, p the fill price, t its received stamp, and mid(t + h) the first mid of the
instrument at or after t + h. Negative is against us. The effective spread is the same with the
last mid at or before t. Prices stay integer mantissas; only these ratios are floats.
"""

import random
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final

import polars as pl

from posttrade import tables
from posttrade.runs import Run

__all__ = [
    "HORIZONS_S",
    "Band",
    "DropCause",
    "FillMeasures",
    "FillParams",
    "SampleDropped",
    "Slice",
    "measure",
]

HORIZONS_S: Final = (1, 5, 30, 60)
SECOND_US: Final = 1_000_000
BASIS_POINTS: Final = 10_000


class DropCause(Enum):
    TAPE_GAP = "tape_gap"
    END_OF_RUN = "end_of_run"


@dataclass(frozen=True, slots=True)
class SampleDropped:
    """A (fill, horizon) pair with no honest mid: past the last mid, or behind a tape gap."""

    horizon_s: int
    cause: DropCause


@dataclass(frozen=True, slots=True)
class FillParams:
    seed: int = 0
    resamples: int = 1_000


@dataclass(frozen=True, slots=True)
class Band:
    """The 2.5 % and 97.5 % points of the mean over resamples of whole client orders."""

    low: float
    high: float


@dataclass(frozen=True, slots=True)
class Slice:
    """The fills of one side and one liquidity, with the mean markout of each horizon."""

    side: str
    liquidity: str
    count: int
    mean_bps: Mapping[int, float | None]


@dataclass(frozen=True, slots=True)
class FillMeasures:
    metrics: Mapping[str, object]
    per_fill: pl.DataFrame
    samples: Mapping[int, tuple[float | SampleDropped, ...]]
    dropped_by_cause: Mapping[DropCause, int]
    bands: Mapping[int, Band | None]
    slices: tuple[Slice, ...]
    quoting_seconds: float

    def markouts_at(self, horizon_s: int) -> list[float | SampleDropped]:
        return list(self.samples[horizon_s])


@dataclass(frozen=True, slots=True)
class _Mids:
    """The mids of one instrument that hold a value, in stamp order."""

    stamps: list[int]
    values: list[int]

    def first_at_or_after(self, at_us: int) -> tuple[int, int] | None:
        i = bisect_left(self.stamps, at_us)
        return (self.stamps[i], self.values[i]) if i < len(self.stamps) else None

    def last_at_or_before(self, at_us: int) -> int | None:
        i = bisect_right(self.stamps, at_us)
        return self.values[i - 1] if i > 0 else None


def measure(run: Run, params: FillParams) -> FillMeasures:
    fills = tables.read_table(run, "fills")
    mids = _mids_by_instrument(tables.read_table(run, "mids"))
    gaps = _gap_stamps(run)
    rows = list(
        fills.select(
            "instrument_id", "client_order_id", "received_ts_us", "side", "liquidity", "last_price"
        ).iter_rows()
    )
    samples: dict[int, tuple[float | SampleDropped, ...]] = {}
    for horizon in HORIZONS_S:
        samples[horizon] = tuple(
            _markout(mids.get(instrument), gaps, received, side, price, horizon)
            for instrument, _, received, side, _, price in rows
        )
    effective = [
        _effective_spread(mids.get(instrument), received, side, price)
        for instrument, _, received, side, _, price in rows
    ]
    clients = [client for _, client, _, _, _, _ in rows]
    dropped = Counter(
        sample.cause
        for horizon in HORIZONS_S
        for sample in samples[horizon]
        if isinstance(sample, SampleDropped)
    )
    quoting_side_spins = _quoting_side_spins(run)
    metrics: dict[str, object] = {
        "fills.count": fills.height,
        "fills.base_bought": _sum_where(fills, "last_qty", "buy"),
        "fills.base_sold": _sum_where(fills, "last_qty", "sell"),
        "fills.fill_rate": _fill_rate(run, clients),
        **{f"markout.{h}s.mean_bps": _mean(_values(samples[h])) for h in HORIZONS_S},
        "markout.dropped": sum(dropped.values()),
        "quoting.quoting_side_spins": quoting_side_spins,
    }
    per_fill = fills.with_columns(
        pl.Series("effective_spread_bps", effective, dtype=pl.Float64),
        *(
            pl.Series(f"markout_{h}s_bps", _floats_or_none(samples[h]), dtype=pl.Float64)
            for h in HORIZONS_S
        ),
    )
    rng = random.Random(params.seed)
    return FillMeasures(
        metrics=metrics,
        per_fill=per_fill,
        samples=samples,
        dropped_by_cause={cause: dropped[cause] for cause in DropCause},
        bands={h: _band(samples[h], clients, rng, params.resamples) for h in HORIZONS_S},
        slices=_slices(rows, samples),
        quoting_seconds=quoting_side_spins * run.manifest.spin_interval_us / SECOND_US,
    )


def _markout(
    mids: _Mids | None, gaps: Sequence[int], received: int, side: str, price: int, horizon: int
) -> float | SampleDropped:
    later = None if mids is None else mids.first_at_or_after(received + horizon * SECOND_US)
    if later is None:
        return SampleDropped(horizon, DropCause.END_OF_RUN)
    mid_stamp, mid = later
    if bisect_right(gaps, mid_stamp) > bisect_right(gaps, received):
        return SampleDropped(horizon, DropCause.TAPE_GAP)
    return _signed_bps(side, mid, price)


def _effective_spread(mids: _Mids | None, received: int, side: str, price: int) -> float | None:
    mid = None if mids is None else mids.last_at_or_before(received)
    return None if mid is None else _signed_bps(side, mid, price)


def _signed_bps(side: str, mid: int, price: int) -> float:
    sign = 1 if side == "buy" else -1
    return sign * (mid - price) / price * BASIS_POINTS


def _mids_by_instrument(frame: pl.DataFrame) -> dict[int, _Mids]:
    grouped: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for instrument, stamp, mid in frame.select("instrument_id", "event_ts_us", "mid").iter_rows():
        if mid is not None:
            grouped[instrument].append((stamp, mid))
    by_instrument = {}
    for instrument, pairs in grouped.items():
        pairs.sort()
        by_instrument[instrument] = _Mids([s for s, _ in pairs], [m for _, m in pairs])
    return by_instrument


def _gap_stamps(run: Run) -> list[int]:
    """The received stamp of each gap row of the tape: records from there on were lost."""
    if "tape" not in run.contract.table_names:
        return []
    gap = run.contract.tape_kind("gap")
    tape = tables.read_table(run, "tape").filter(pl.col("kind") == gap)
    return sorted(tape.get_column("received_ts_us").to_list())


def _quoting_side_spins(run: Run) -> int:
    """Each spin gives each trading instrument two sides; a no-quote row takes one side out."""
    trading = run.manifest.trading_instrument_ids
    mids = tables.read_table(run, "mids").filter(pl.col("instrument_id").is_in(list(trading)))
    no_quotes = tables.read_table(run, "no_quotes")
    return 2 * mids.height - no_quotes.height


def _fill_rate(run: Run, clients: Sequence[int]) -> float | None:
    orders = tables.read_table(run, "orders")
    placed = orders.filter(pl.col("transition") == "placed").height
    return None if placed == 0 else len(set(clients)) / placed


def _sum_where(fills: pl.DataFrame, column: str, side: str) -> int:
    values = fills.filter(pl.col("side") == side).get_column(column).to_list()
    return sum(int(value) for value in values)


def _values(samples: Sequence[float | SampleDropped]) -> list[float]:
    return [sample for sample in samples if not isinstance(sample, SampleDropped)]


def _floats_or_none(samples: Sequence[float | SampleDropped]) -> list[float | None]:
    return [None if isinstance(sample, SampleDropped) else sample for sample in samples]


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _band(
    samples: Sequence[float | SampleDropped],
    clients: Sequence[int],
    rng: random.Random,
    resamples: int,
) -> Band | None:
    """Resamples whole client orders, so that the fills of one order count as one draw."""
    by_client: dict[int, list[float]] = defaultdict(list)
    for client, sample in zip(clients, samples, strict=True):
        if not isinstance(sample, SampleDropped):
            by_client[client].append(sample)
    clusters = list(by_client.values())
    if len(clusters) < 2:
        return None
    means = []
    for _ in range(resamples):
        drawn = [value for _ in clusters for value in rng.choice(clusters)]
        means.append(sum(drawn) / len(drawn))
    means.sort()
    return Band(low=means[int(0.025 * resamples)], high=means[int(0.975 * resamples) - 1])


def _slices(
    rows: Sequence[tuple[int, int, int, str, str, int]],
    samples: Mapping[int, Sequence[float | SampleDropped]],
) -> tuple[Slice, ...]:
    keys = sorted({(side, liquidity) for _, _, _, side, liquidity, _ in rows})
    slices = []
    for side, liquidity in keys:
        members = [i for i, row in enumerate(rows) if (row[3], row[4]) == (side, liquidity)]
        slices.append(
            Slice(
                side=side,
                liquidity=liquidity,
                count=len(members),
                mean_bps={h: _mean(_values([samples[h][i] for i in members])) for h in HORIZONS_S},
            )
        )
    return tuple(slices)
