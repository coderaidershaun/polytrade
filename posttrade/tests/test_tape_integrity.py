"""`posttrade check` reads the tape: a hole that no gap row covers is a finding, a stamp that runs
back within one queue is a finding, and a recorded gap is no finding but is counted with its cost.

Each tape is written into a copy of the second golden boot, which records no tape of its own, with
the footer of the boot's other files, so the reader takes it as the boot's own.
"""

import io
import shutil
from pathlib import Path

from posttrade import cli

from conftest import (
    GAP,
    SECOND_BOOT,
    SNAPSHOT,
    SPIN,
    START_US,
    TRADE,
    TapeRow,
    boot,
    golden_dir,
    messages,
    write_tape,
)


def boot_copy(tmp_path: Path, name: str) -> Path:
    """A fresh copy of the second golden boot for one case."""
    target = tmp_path / name / SECOND_BOOT
    shutil.copytree(golden_dir() / SECOND_BOOT, target)
    return target


def check(run_dir: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(["check", str(run_dir)], cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def findings(output: str) -> list[str]:
    return [line for line in output.splitlines() if line.startswith("finding")]


def test_planted_sequence_hole_is_a_finding(tmp_path: Path) -> None:
    """FITNESS: the tape holds each popped message once, in order, so a hole in its sequence is a
    message that a replay would never fold, unless a gap row records it. The trace of X2: rows 0 to
    8, a gap row 9 that lost 9 to 12 and ends file 1, then the book snapshots 13 and 14 and the
    message 15. With the gap recorded, `check` passes and counts its cost. A hole planted in a file,
    and one at the first row of the second file, are each one finding, and `check` fails."""
    gap_row: TapeRow = (9, GAP, None, START_US + 12_500)
    after_gap: list[TapeRow] = [
        (13, SNAPSHOT, None, START_US + 13_000),
        (14, SNAPSHOT, None, START_US + 13_000),
        *messages(15, 20),
    ]
    spans = {9: (9, 12)}
    recorded = boot_copy(tmp_path, "recorded")
    write_tape(recorded, [[boot(), *messages(1, 8), gap_row], after_gap], spans)

    code, output = check(recorded)

    assert (code, findings(output)) == (0, []), output
    assert "tape: 18 rows in 2 files; 1 gaps lost 4 records over 5000 us" in output, output

    for name, files in [
        ("in-a-file", [[boot(), *messages(1, 4), *messages(6, 8), gap_row], after_gap]),
        ("at-a-file-border", [[boot(), *messages(1, 8), gap_row], after_gap[1:]]),
    ]:
        planted = boot_copy(tmp_path, name)
        write_tape(planted, files, spans)

        code, output = check(planted)

        assert code == 1, f"{name}: {output}"
        assert len(findings(output)) == 1, f"{name}: {output}"
        assert "finding tape_sequence" in findings(output)[0], f"{name}: {output}"
        assert "a hole of 1 records that no gap row covers" in findings(output)[0], output


def test_stamp_back_within_a_queue_is_a_finding(tmp_path: Path) -> None:
    """FITNESS: one queue is read in the order its producer pushed, so its stamps never go back.
    A trade of queue 0 whose stamps lie below the last trade of queue 0, at the first rows of the
    second file, is one finding; a spin of queue 1 above that trade, and so above the trade before
    it in the tape, is none."""
    first = [boot(), *messages(1, 6)]
    last_trade_us = first[-2][3]
    second: list[TapeRow] = [
        (7, SPIN, 1, last_trade_us + 1_500),
        (8, TRADE, 0, last_trade_us - 1),
        (9, SPIN, 1, last_trade_us + 1_600),
    ]
    run_dir = boot_copy(tmp_path, "stamps")
    write_tape(run_dir, [first, second])

    code, output = check(run_dir)

    assert code == 1, output
    assert len(findings(output)) == 1, output
    assert "finding tape_stamp" in findings(output)[0], output
    assert (
        f"seq 8 of queue 0 has received_ts_us {last_trade_us - 1}, below {last_trade_us}"
        in findings(output)[0]
    ), output
