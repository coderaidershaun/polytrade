"""Checks the tape of one run before any measure of it is shown: the sequence is continuous but at a
recorded gap, the stamps never run back within one queue, and each gap's cost is counted.

The tape is read one file at a time, its checked columns only, and the check carries the last
sequence number and the last stamps of each queue over each file border.
"""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Final

import polars as pl

from posttrade import tables
from posttrade.errors import ContractMismatch, FileVanished, Finding, FindingKind
from posttrade.runs import Run

__all__ = ["GapCost", "TapeHealth", "tape_health"]

TAPE: Final = "tape"
CHECKED: Final = ("seq", "kind", "queue_id", "received_ts_us", "queued_ts_us")
STAMPS: Final = ("received_ts_us", "queued_ts_us")

#: A gap row holds the first and the last lost sequence number, each a little-endian u64.
GAP_PAYLOAD_BYTES: Final = 16


@dataclass(frozen=True, slots=True)
class GapCost:
    """One recorded gap: the records it lost, and the time between the rows on either side of it."""

    first_lost_seq: int
    last_lost_seq: int
    before_ts_us: int
    after_ts_us: int | None

    @property
    def records_lost(self) -> int:
        return self.last_lost_seq - self.first_lost_seq + 1

    @property
    def time_lost_us(self) -> int:
        return 0 if self.after_ts_us is None else self.after_ts_us - self.before_ts_us


@dataclass(frozen=True, slots=True)
class TapeHealth:
    """What the tape of a run shows; a run that records no tape has 0 rows."""

    files: int
    rows: int
    first_ts_us: int | None
    last_ts_us: int | None
    gaps: tuple[GapCost, ...]
    findings: tuple[Finding, ...]

    @property
    def records_lost(self) -> int:
        return sum(gap.records_lost for gap in self.gaps)

    @property
    def time_lost_us(self) -> int:
        return sum(gap.time_lost_us for gap in self.gaps)

    @property
    def span_us(self) -> int:
        if self.first_ts_us is None or self.last_ts_us is None:
            return 0
        return self.last_ts_us - self.first_ts_us

    def summary(self) -> str:
        if self.rows == 0:
            return "tape: none recorded"
        share = 0.0 if self.span_us == 0 else 100 * self.time_lost_us / self.span_us
        return (
            f"tape: {self.rows} rows in {self.files} files; {len(self.gaps)} gaps lost "
            f"{self.records_lost} records over {self.time_lost_us} us ({share:.3f} % of the run)"
        )


@dataclass(slots=True)
class _Carry:
    """What the check of one file hands the next: the last row, the gaps so far by their first
    lost seq, and the last stamps of each queue."""

    rows: int = 0
    last_seq: int | None = None
    last_kind: int | None = None
    last_ts_us: int | None = None
    first_ts_us: int | None = None
    gaps: dict[int, GapCost] = field(default_factory=dict)
    queues: pl.DataFrame | None = None


def tape_health(run: Run) -> TapeHealth:
    """Checks each sealed tape file of the run in seal order.

    Raises:
        FileVanished: a file was removed after it was listed.
        ContractMismatch: a file differs from the contract or belongs to another run.
    """
    files = _seal_order(run.sealed_files.get(TAPE, ()))
    boot_kind = run.contract.tape_kind("boot")
    gap_kind = run.contract.tape_kind("gap")
    carry = _Carry()
    findings: list[Finding] = []
    for path in files:
        frame = _read(path, run)
        findings += _sequence(path, frame, carry, boot_kind, gap_kind)
        findings += _stamps(path, frame, carry)
        carry.rows += frame.height
    return TapeHealth(
        files=len(files),
        rows=carry.rows,
        first_ts_us=carry.first_ts_us,
        last_ts_us=carry.last_ts_us,
        gaps=tuple(carry.gaps.values()),
        findings=tuple(findings),
    )


def _seal_order(files: Sequence[Path]) -> tuple[Path, ...]:
    """The number at the end of each file name is its place in seal order. The boot row takes the
    boot stamp, so the date folders alone do not sort a tape whose messages came long before."""
    return tuple(sorted(files, key=lambda path: int(path.stem.rsplit("-", 1)[-1])))


def _read(path: Path, run: Run) -> pl.DataFrame:
    footer = tables.read_footer(path, run.contract)
    for key, expected in (
        ("boot_ts_us", run.manifest.run_id),
        ("run_kind", run.manifest.kind.value),
    ):
        if footer.values[key] != expected:
            raise ContractMismatch(path, f"footer {key}", footer.values[key], expected)
    try:
        frame = pl.scan_parquet(path).select(*CHECKED, "payload").collect()
    except FileNotFoundError:
        raise FileVanished(path) from None
    for column in run.contract.table(TAPE).columns:
        series = frame.get_column(column.name)
        if series.dtype != column.dtype:
            raise ContractMismatch(path, f"type of {column.name}", series.dtype, column.dtype)
        if not column.nullable and series.null_count() > 0:
            raise ContractMismatch(path, f"nulls of {column.name}", series.null_count(), 0)
    return frame


def _sequence(
    path: Path, frame: pl.DataFrame, carry: _Carry, boot_kind: int, gap_kind: int
) -> Iterator[Finding]:
    """Each row is one past the row before it, or, after a gap row, one past the last seq that the
    gap lost. Only the gap rows have their payload read."""
    if frame.height == 0:
        return
    rows = frame.select("seq", "kind", "received_ts_us").with_row_index(
        "row", offset=carry.rows + 1
    )
    rows = rows.with_columns(
        pl.col("seq").shift(1, fill_value=carry.last_seq).alias("previous_seq"),
        pl.col("kind").shift(1, fill_value=carry.last_kind).alias("previous_kind"),
        pl.col("received_ts_us").shift(1, fill_value=carry.last_ts_us).alias("previous_ts"),
    )
    if carry.last_seq is None:
        carry.first_ts_us = rows.item(0, "received_ts_us")
        seq, kind = rows.item(0, "seq"), rows.item(0, "kind")
        if (seq, kind) != (0, boot_kind):
            yield _finding(
                FindingKind.TAPE_BOOT,
                carry.rows + 1,
                f"{path.name}: the tape opens with seq {seq} of kind {kind}, not the boot row",
            )
    gap_rows = frame.with_row_index("index").filter(pl.col("kind") == gap_kind)
    for index, seq, payload in gap_rows.select("index", "seq", "payload").iter_rows():
        before = rows.item(index, "previous_ts")
        carry.gaps[seq] = _gap(path, seq, payload, before if before is not None else 0)
    last_lost = {first: gap.last_lost_seq for first, gap in carry.gaps.items()}
    after_gap = pl.col("previous_kind") == gap_kind
    checked = rows.filter(pl.col("previous_seq").is_not_null()).with_columns(
        pl.when(after_gap)
        .then(pl.col("previous_seq").replace(last_lost) + 1)
        .otherwise(pl.col("previous_seq") + 1)
        .alias("expected")
    )
    for first, after_ts in (
        checked.filter(after_gap).select("previous_seq", "received_ts_us").iter_rows()
    ):
        carry.gaps[first] = replace(carry.gaps[first], after_ts_us=after_ts)
    broken = checked.filter(pl.col("seq") != pl.col("expected"))
    for row, seq, previous, expected in broken.select(
        "row", "seq", "previous_seq", "expected"
    ).iter_rows():
        detail = (
            f"a hole of {seq - expected} records that no gap row covers"
            if seq > expected
            else "which does not ascend"
        )
        yield _finding(
            FindingKind.TAPE_SEQUENCE,
            row,
            f"{path.name}: seq {previous} is followed by seq {seq}, {detail}",
        )
    carry.last_seq = rows.item(-1, "seq")
    carry.last_kind = rows.item(-1, "kind")
    carry.last_ts_us = rows.item(-1, "received_ts_us")


def _gap(path: Path, seq: int, payload: bytes, before_ts_us: int) -> GapCost:
    if len(payload) != GAP_PAYLOAD_BYTES:
        raise ContractMismatch(path, f"payload of gap row seq {seq}", len(payload), "16 bytes")
    first = int.from_bytes(payload[:8], "little")
    last = int.from_bytes(payload[8:], "little")
    if first != seq or last < first:
        raise ContractMismatch(
            path, f"span of gap row seq {seq}", (first, last), f"({seq}, {seq} or more)"
        )
    return GapCost(first, last, before_ts_us, None)


def _stamps(path: Path, frame: pl.DataFrame, carry: _Carry) -> Iterator[Finding]:
    """Within one queue, neither stamp goes below the stamp of the queue's row before it. A row of
    no queue (boot, snapshot, gap) is not a pop and is left out."""
    popped = (
        frame.with_row_index("row", offset=carry.rows + 1)
        .filter(pl.col("queue_id").is_not_null())
        .select("row", "seq", "queue_id", *STAMPS)
    )
    carried = carry.queues
    if carried is not None:
        popped = pl.concat(
            [carried.with_columns(pl.lit(0, pl.UInt32).alias("row")).select(popped.columns), popped]
        )
    previous = popped.with_columns(
        pl.col(stamp).shift(1).over("queue_id").alias(f"previous_{stamp}") for stamp in STAMPS
    ).filter(pl.col("row") > 0)
    went_back = pl.any_horizontal(pl.col(stamp) < pl.col(f"previous_{stamp}") for stamp in STAMPS)
    for row in previous.filter(went_back).iter_rows(named=True):
        backs = [
            f"{stamp} {row[stamp]}, below {row[f'previous_{stamp}']}"
            for stamp in STAMPS
            if row[f"previous_{stamp}"] is not None and row[stamp] < row[f"previous_{stamp}"]
        ]
        yield _finding(
            FindingKind.TAPE_STAMP,
            row["row"],
            f"{path.name}: seq {row['seq']} of queue {row['queue_id']} has "
            f"{' and '.join(backs)} of the queue's row before it",
        )
    if popped.height > 0:
        carry.queues = popped.group_by("queue_id", maintain_order=True).last().drop("row")


def _finding(kind: FindingKind, row: int, detail: str) -> Finding:
    return Finding(kind=kind, table=TAPE, row=row, client_order_id=None, detail=detail)
