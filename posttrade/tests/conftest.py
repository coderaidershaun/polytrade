"""Copies of the golden run that the Rust writer made, for tests that change one thing in them."""

import json
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path

import polars as pl
import pytest

REPO = Path(__file__).resolve().parents[2]
GOLDEN = REPO / "target" / "golden-run"
FIRST_BOOT = Path("golden/te-golden/sim/1784000000000000")
SECOND_BOOT = Path("golden/te-golden/sim/1784000600000000")


def golden_dir() -> Path:
    if not (GOLDEN / "expected.json").is_file():
        pytest.fail(
            f"no golden run at {GOLDEN}: run "
            "`cargo run -p engine --bin golden-run -- target/golden-run` in the repo root first"
        )
    return GOLDEN


@pytest.fixture
def golden_copy(tmp_path: Path) -> Path:
    """The second golden boot, copied below the test folder: the copy's run folder."""
    target = tmp_path / SECOND_BOOT
    shutil.copytree(golden_dir() / SECOND_BOOT, target)
    return target


@pytest.fixture
def first_boot_copy(tmp_path: Path) -> Path:
    """The first golden boot, copied below the test folder: the copy's run folder."""
    target = tmp_path / FIRST_BOOT
    shutil.copytree(golden_dir() / FIRST_BOOT, target)
    return target


def table_file(run_dir: Path, table: str) -> Path:
    (path,) = sorted((run_dir / table).glob("date=*/*.parquet"))
    return path


def rewrite(
    path: Path,
    change_frame: Callable[[pl.DataFrame], pl.DataFrame] = lambda frame: frame,
    change_footer: Callable[[dict[str, str]], dict[str, str]] = lambda footer: footer,
) -> None:
    """Writes the file again with a changed frame or footer, as a writer of another shape would."""
    footer = {k: v for k, v in pl.read_parquet_metadata(path).items() if k != "ARROW:schema"}
    frame = pl.read_parquet(path)
    change_frame(frame).write_parquet(path, metadata=change_footer(footer))


# The tape of a run, written by hand for the tests of the tape check.
BOOT = 256
SNAPSHOT = 257
GAP = 258
TRADE = 1
SPIN = 6
START_US = 1_784_000_600_000_000

#: (seq, kind, queue id, received stamp); a gap row carries its lost span as its payload.
TapeRow = tuple[int, int, int | None, int]


def payload(row: TapeRow, spans: dict[int, tuple[int, int]]) -> bytes:
    seq, kind, _, _ = row
    if kind != GAP:
        return b""
    first, last = spans[seq]
    return first.to_bytes(8, "little") + last.to_bytes(8, "little")


def write_tape(
    run_dir: Path,
    files: Sequence[Sequence[TapeRow]],
    spans: dict[int, tuple[int, int]] | None = None,
) -> None:
    """Writes each file of rows as one sealed tape file, in seal order, and names the table in
    `run.json`."""
    manifest_path = run_dir / "run.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"] = [*manifest["tables"], "tape"]
    manifest_path.write_text(json.dumps(manifest))
    source = table_file(run_dir, "orders")
    footer = {k: v for k, v in pl.read_parquet_metadata(source).items() if k != "ARROW:schema"}
    folder = run_dir / "tape" / "date=2026-07-14"
    folder.mkdir(parents=True)
    for number, rows in enumerate(files):
        frame = pl.DataFrame(
            {
                "seq": [row[0] for row in rows],
                "kind": [row[1] for row in rows],
                "queue_id": [row[2] for row in rows],
                "received_ts_us": [row[3] for row in rows],
                "queued_ts_us": [row[3] for row in rows],
                "payload": [payload(row, spans or {}) for row in rows],
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
        frame.write_parquet(folder / f"03-{START_US}-{number:06d}.parquet", metadata=footer)


def messages(first: int, last: int) -> list[TapeRow]:
    """Messages `first` to `last`: a trade from queue 0, then a spin from queue 1, in turn, each
    1 ms after the one before it."""
    return [
        (seq, TRADE if seq % 2 else SPIN, 0 if seq % 2 else 1, START_US + seq * 1_000)
        for seq in range(first, last + 1)
    ]


def boot() -> TapeRow:
    return (0, BOOT, None, START_US)
