"""A run as the engine leaves it on disk: files in seal order, and `run.json` mid-rewrite."""

import io
from pathlib import Path

import polars as pl

from posttrade import cli, contract, runs

from conftest import table_file


def check(run_dir: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(["check", str(run_dir)], cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def test_files_are_read_in_seal_order_across_an_hour_border(first_boot_copy: Path) -> None:
    """F7. A seal at 12:00:00.100 opens file 4 in hour 12; the next file, 5, opens in hour 11,
    the hour of its first row, an answer stamped 11:59:59.990 that the fold took next."""
    whole = table_file(first_boot_copy, "orders")
    footer = {k: v for k, v in pl.read_parquet_metadata(whole).items() if k != "ARROW:schema"}
    rows = pl.read_parquet(whole)
    run_id = first_boot_copy.name
    first = whole.with_name(f"12-{run_id}-000004.parquet")
    second = whole.with_name(f"11-{run_id}-000005.parquet")
    whole.unlink()
    rows.head(9).write_parquet(first, metadata=footer)
    rows.tail(rows.height - 9).write_parquet(second, metadata=footer)
    opened = runs.open_run(first_boot_copy, contract.load_repo_contract())

    assert [path.name for path in opened.sealed_files["orders"]] == [first.name, second.name]
    assert check(first_boot_copy)[0] == 0


def test_run_json_being_rewritten_is_no_entry_of_the_run(first_boot_copy: Path) -> None:
    """F11. The engine writes `run.json` through `run.json.<pid>.tmp` beside it, at the start and
    at the end of the drain; a kill inside that write leaves the temporary file for good."""
    (first_boot_copy / "run.json.4242.tmp").write_text('{"half": ')

    opened = runs.open_run(first_boot_copy, contract.load_repo_contract())
    found = runs.find_runs(first_boot_copy.parent, contract.load_repo_contract())

    assert opened.manifest.run_id == first_boot_copy.name
    assert [run.dir for run in found] == [first_boot_copy]
