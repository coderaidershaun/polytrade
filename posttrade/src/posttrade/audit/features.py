"""Checks the feature records of one run: a duplicate emission is a break, and each declared
feature that the run never emitted is listed."""

from collections.abc import Iterator
from dataclasses import dataclass

import polars as pl

from posttrade import tables
from posttrade.errors import Finding, FindingKind
from posttrade.runs import Run

__all__ = ["FeatureHealth", "feature_health"]

FEATURE_KEY = ("event_ts_us", "instrument_id", "feature_id")


@dataclass(frozen=True, slots=True)
class FeatureHealth:
    never_emitted: tuple[str, ...]
    findings: tuple[Finding, ...]


def feature_health(run: Run) -> FeatureHealth:
    features = tables.read_table(run, "features")
    return FeatureHealth(
        never_emitted=_never_emitted(run, features),
        findings=tuple(_duplicates(features)),
    )


def _never_emitted(run: Run, features: pl.DataFrame) -> tuple[str, ...]:
    files = run.all_sealed_files()
    if not files:
        return ()
    declared = tables.read_footer(files[0], run.contract).names("feature_dictionary")
    emitted = set(features.get_column("feature").unique().to_list())
    return tuple(name for name in declared if name not in emitted)


def _duplicates(features: pl.DataFrame) -> Iterator[Finding]:
    """The engine emits each feature of an instrument once for each event stamp."""
    seen: set[tuple[int, int, int]] = set()
    for row, (stamp, instrument, feature_id, name) in enumerate(
        features.select(*FEATURE_KEY, "feature").iter_rows(), start=1
    ):
        key = (stamp, instrument, feature_id)
        if key in seen:
            yield Finding(
                kind=FindingKind.DUPLICATE_FEATURE,
                table="features",
                row=row,
                client_order_id=None,
                detail=f"{name} of instrument {instrument} at {stamp} was emitted twice",
            )
        seen.add(key)
