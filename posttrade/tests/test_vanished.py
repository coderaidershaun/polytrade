"""A file that vanishes between its listing and its read: the check lists again, at most 3 times."""

import io
import json
import shutil
from pathlib import Path

import polars as pl

from posttrade import cli, runs
from posttrade.contract import Contract

from conftest import table_file


def fills_in_two_files(run_dir: Path) -> tuple[Path, Path]:
    """The run keeps only `fills`, one fill in each of two sealed files."""
    for entry in run_dir.iterdir():
        if entry.is_dir() and entry.name != "fills":
            shutil.rmtree(entry)
    manifest = json.loads((run_dir / "run.json").read_text())
    manifest["tables"] = ["fills"]
    (run_dir / "run.json").write_text(json.dumps(manifest))
    first = table_file(run_dir, "fills")
    footer = {k: v for k, v in pl.read_parquet_metadata(first).items() if k != "ARROW:schema"}
    frame = pl.read_parquet(first)
    second = first.with_name(first.name.replace("-000000.", "-000001."))
    frame.head(1).write_parquet(first, metadata=footer)
    frame.tail(1).write_parquet(second, metadata=footer)
    return first, second


class Deleting:
    """Lists the run as `runs` does, then deletes a file before the check reads it."""

    def __init__(self, path: Path, times: int) -> None:
        self.path = path
        self.saved = path.read_bytes()
        self.times = times
        self.listings = 0

    def open_run(self, run_dir: Path, terms: Contract) -> runs.Run:
        self.listings += 1
        if self.listings <= self.times:
            self.path.write_bytes(self.saved)
        listed = runs.open_run(run_dir, terms)
        if self.listings <= self.times:
            self.path.unlink()
        return listed


def check(run_dir: Path, deleting: Deleting) -> tuple[int, str, list[float]]:
    out = io.StringIO()
    waits: list[float] = []
    env = cli.CommandEnv(out=out, err=out, open_run=deleting.open_run, sleep=waits.append)
    code = cli.main(["check", str(run_dir), "--seed", "7"], env)
    return code, out.getvalue(), waits


def test_vanished_file_is_listed_again(first_boot_copy: Path) -> None:
    first, _ = fills_in_two_files(first_boot_copy)
    deleting = Deleting(first, times=1)

    code, output, waits = check(first_boot_copy, deleting)

    assert code == 0, output
    assert deleting.listings == 2
    assert len(waits) == 1 and 0.2 <= waits[0] <= 0.22
    (retry,) = [line for line in output.splitlines() if "listing again" in line]
    assert str(first) in retry


def test_vanished_file_limit_halts_command(first_boot_copy: Path) -> None:
    first, _ = fills_in_two_files(first_boot_copy)
    deleting = Deleting(first, times=3)
    before = sorted(first_boot_copy.parent.rglob("*"))

    code, output, waits = check(first_boot_copy, deleting)

    assert code == 1, output
    assert deleting.listings == 3
    assert len(waits) == 2 and 0.2 <= waits[0] <= 0.22 and 2.0 <= waits[1] <= 2.2
    (halt,) = [line for line in output.splitlines() if "tries" in line and "vanished" in line]
    assert str(first) in halt and "3 tries" in halt
    assert sorted(first_boot_copy.parent.rglob("*")) == [p for p in before if p != first]
