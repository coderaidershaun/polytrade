"""`timeline` prints one window of a run as one list; `export` writes CSV outside each run."""

import io
from pathlib import Path

import pytest

from posttrade import cli

from conftest import FIRST_BOOT, REPO, golden_dir


def run_command(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(list(argv), cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def test_timeline_lists_each_table_in_stamp_order() -> None:
    run_dir = golden_dir() / FIRST_BOOT
    code, output = run_command(
        "timeline", str(run_dir), "--from", "1784000025000000", "--to", "1784000025000000"
    )

    assert code == 0, output
    assert [line.split(" ")[:2] for line in output.splitlines()] == [
        ["1784000025000000", "orders"],
        ["1784000025000000", "fills"],
        ["1784000025000000", "ledger"],
        ["1784000025000000", "mids"],
        ["1784000025000000", "no_quotes"],
        ["1784000025000000", "no_quotes"],
    ]
    assert "transition=report_trade" in output and "booked_quote=10100000000" in output
    assert "ask=null" in output


def test_export_writes_exact_mantissas(tmp_path: Path) -> None:
    code, output = run_command(
        "export", str(golden_dir() / FIRST_BOOT), "fills", "--out", str(tmp_path)
    )

    assert code == 0, output
    lines = (tmp_path / "1784000000000000-fills.csv").read_text().splitlines()
    assert len(lines) == 3
    assert ",10100000000,100000000,100000000,10100000000," in lines[1]


@pytest.mark.parametrize(
    "out",
    [
        golden_dir() / FIRST_BOOT / "exports",
        REPO / "engine" / "exports",
        REPO / "lab" / "exports",
        REPO / "strategies" / "exports",
    ],
    ids=["run folder", "engine", "lab", "strategies"],
)
def test_export_refuses_output_into_a_run_or_the_code(out: Path) -> None:
    code, output = run_command("export", str(golden_dir() / FIRST_BOOT), "fills", "--out", str(out))

    assert code == 2
    assert "refused to write" in output
    assert not out.exists()
