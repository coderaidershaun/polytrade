"""Markouts of each fill at each horizon of the contract, against values a person works out."""

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from posttrade import contract, runs
from posttrade.metrics import fills
from posttrade.metrics.fills import DropCause, FillParams, SampleDropped

from conftest import FIRST_BOOT, SECOND_BOOT, golden_dir, rewrite, table_file

T0_US = 1_784_000_100_000_000
SECOND_US = 1_000_000
UNIT = 100_000_000
PRICE = 100 * UNIT  # each fill of the fixture trades at 100
BASIS_POINT = PRICE // 10_000


def at(seconds: float) -> int:
    return T0_US + int(seconds * SECOND_US)


def mids_rising_one_basis_point_a_second(
    last_second: int, without: Sequence[int] = ()
) -> pl.DataFrame:
    """A mid each second; the mid at second i is 100 plus i basis points of 100."""
    seconds = [i for i in range(last_second + 1)]
    mids = [None if i in without else PRICE + i * BASIS_POINT for i in seconds]
    return pl.DataFrame(
        {
            "event_ts_us": [at(i) for i in seconds],
            "spin_seq": [i + 1 for i in seconds],
            "instrument_id": [0] * len(seconds),
            "bid": mids,
            "ask": mids,
            "mid": mids,
        },
        schema={
            "event_ts_us": pl.Int64,
            "spin_seq": pl.UInt64,
            "instrument_id": pl.UInt16,
            "bid": pl.Int64,
            "ask": pl.Int64,
            "mid": pl.Int64,
        },
    )


def fills_at(template: pl.DataFrame, trades: Sequence[tuple[float, str]]) -> pl.DataFrame:
    """One fill at 100 for each (second, side), each of its own client order."""
    rows = [
        template.head(1).with_columns(
            pl.lit(at(second)).alias("received_ts_us"),
            pl.lit(at(second) - 10_000).alias("exchange_ts_us"),
            pl.lit(100 + i, dtype=pl.UInt64).alias("client_order_id"),
            pl.lit(side).alias("side"),
            pl.lit(PRICE, dtype=pl.Int64).alias("last_price"),
            pl.lit(PRICE, dtype=pl.Int64).alias("booked_quote"),
        )
        for i, (second, side) in enumerate(trades)
    ]
    return pl.concat(rows)


def measured(run_dir: Path) -> fills.FillMeasures:
    return fills.measure(runs.open_run(run_dir, contract.load_repo_contract()), FillParams())


def test_six_fills_give_hand_markouts(first_boot_copy: Path) -> None:
    rewrite(
        table_file(first_boot_copy, "mids"),
        lambda _: mids_rising_one_basis_point_a_second(100, without=[31]),
    )
    trades = [(10, "buy"), (20, "sell"), (30, "buy"), (40, "sell"), (50, "buy"), (60.5, "buy")]
    rewrite(table_file(first_boot_copy, "fills"), lambda frame: fills_at(frame, trades))

    metrics = measured(first_boot_copy).metrics

    # A buy at second t, horizon h: the first mid at or after t + h, in basis points over 100.
    # A sell is the same with its sign turned. The mid of second 31 is absent, so the buy at 30
    # meets the mid of 32 at 1 s. Second 60.5 + h meets the mid of the next whole second. A
    # horizon past second 100 has no mid: the buys at 50 and 60.5 drop their 60 s pair.
    assert metrics["markout.1s.mean_bps"] == pytest.approx((11 - 21 + 32 - 41 + 51 + 62) / 6)
    assert metrics["markout.5s.mean_bps"] == pytest.approx((15 - 25 + 35 - 45 + 55 + 66) / 6)
    assert metrics["markout.30s.mean_bps"] == pytest.approx((40 - 50 + 60 - 70 + 80 + 91) / 6)
    assert metrics["markout.60s.mean_bps"] == pytest.approx((70 - 80 + 90 - 100) / 4)
    assert metrics["markout.dropped"] == 2
    assert metrics["fills.count"] == 6


def tape_with_gap_at(received_us: int, gap_kind: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "seq": [55],
            "kind": [gap_kind],
            "queue_id": [None],
            "received_ts_us": [received_us],
            "queued_ts_us": [received_us],
            "payload": [bytes(16)],
        },
        schema={
            "seq": pl.UInt64,
            "kind": pl.UInt16,
            "queue_id": pl.UInt8,
            "received_ts_us": pl.Int64,
            "queued_ts_us": pl.Int64,
            "payload": pl.Binary,
        },
    )


def add_tape(run_dir: Path, tape: pl.DataFrame) -> None:
    manifest_path = run_dir / "run.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"].append("tape")
    manifest_path.write_text(json.dumps(manifest))
    mids_file = table_file(run_dir, "mids")
    footer = {k: v for k, v in pl.read_parquet_metadata(mids_file).items() if k != "ARROW:schema"}
    target = run_dir / "tape" / mids_file.parent.name / mids_file.name
    target.parent.mkdir(parents=True)
    tape.write_parquet(target, metadata=footer)


def test_markout_across_gap_is_dropped(first_boot_copy: Path) -> None:
    rewrite(table_file(first_boot_copy, "mids"), lambda _: mids_rising_one_basis_point_a_second(60))
    rewrite(
        table_file(first_boot_copy, "fills"),
        lambda frame: fills_at(frame, [(10, "buy"), (50, "buy")]),
    )
    gap_kind = contract.load_repo_contract().tape_kind("gap")
    add_tape(first_boot_copy, tape_with_gap_at(at(54.5), gap_kind))

    result = measured(first_boot_copy)

    before_gap, across_gap = result.markouts_at(5)
    assert before_gap == pytest.approx(15)
    assert across_gap == SampleDropped(5, DropCause.TAPE_GAP)
    assert result.metrics["markout.5s.mean_bps"] == pytest.approx(15)
    assert result.dropped_by_cause[DropCause.TAPE_GAP] == 1


T13_METRICS = (
    "fills.count",
    "fills.base_bought",
    "fills.base_sold",
    "fills.fill_rate",
    "markout.1s.mean_bps",
    "markout.5s.mean_bps",
    "markout.30s.mean_bps",
    "markout.60s.mean_bps",
    "markout.dropped",
    "quoting.quoting_side_spins",
)


@pytest.mark.parametrize("index, boot", [(0, FIRST_BOOT), (1, SECOND_BOOT)])
def test_fill_metrics_of_golden_run_equal_expected(index: int, boot: Path) -> None:
    expected: Any = json.loads((golden_dir() / "expected.json").read_text())
    want = expected["runs"][index]["metrics"]

    got = measured(golden_dir() / boot).metrics

    assert {name: got[name] for name in T13_METRICS} == {name: want[name] for name in T13_METRICS}
