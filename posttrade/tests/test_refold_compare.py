"""`posttrade compare RUN REPLAY` in exact mode: a re-fold equal to its run passes; one cell that
differs fails, named by table, row and column; a research row that only the re-fold holds is taken
while the run counted at least that many dropped.

Each re-fold here is a copy of a golden boot made into a run of kind `replay` of that boot, as `lab`
writes one: its `run.json` names the boot as its source, and each file's footer says `replay`.
"""

import io
import json
import shutil
from collections.abc import Callable
from pathlib import Path

import polars as pl

from posttrade import cli

from conftest import (
    FIRST_BOOT,
    GAP,
    SECOND_BOOT,
    boot,
    golden_dir,
    messages,
    rewrite,
    table_file,
    write_tape,
)


def replay_of(run_dir: Path, root: Path) -> Path:
    """A copy of `run_dir` below `root` that names it as its source, as the re-fold of `lab` does:
    `{root}/{strategy}/{te}/replay/{run_id}`."""
    te_dir = run_dir.parents[1]
    target = root / te_dir.parent.name / te_dir.name / "replay" / run_dir.name
    shutil.copytree(run_dir, target)
    manifest = json.loads((target / "run.json").read_text())
    manifest["source"] = {
        "run_dir": str(run_dir.relative_to(run_dir.parents[3])),
        "run_id": manifest["run_id"],
        "config_digest": manifest["config_digest"],
    }
    manifest["kind"] = "replay"
    manifest["identity_lock"] = None
    (target / "run.json").write_text(json.dumps(manifest))
    for path in target.glob("*/date=*/*.parquet"):
        rewrite(path, change_footer=lambda footer: {**footer, "run_kind": "replay"})
    return target


def boot_copy(tmp_path: Path, boot_dir: Path, name: str) -> Path:
    target = tmp_path / name / boot_dir
    shutil.copytree(golden_dir() / boot_dir, target)
    return target


def compare(run_dir: Path, replay_dir: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(["compare", str(run_dir), str(replay_dir)], cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def changed_cell(column: str, row: int, value: object) -> Callable[[pl.DataFrame], pl.DataFrame]:
    def change(frame: pl.DataFrame) -> pl.DataFrame:
        values = frame.get_column(column).to_list()
        values[row] = value
        return frame.with_columns(pl.Series(column, values, dtype=frame.schema[column]))

    return change


def with_copies_of_first_row(copies: int) -> Callable[[pl.DataFrame], pl.DataFrame]:
    def change(frame: pl.DataFrame) -> pl.DataFrame:
        return pl.concat([frame, *[frame.head(1)] * copies])

    return change


def test_refold_diff_names_cell(tmp_path: Path) -> None:
    """FITNESS (V7): a re-fold that equals its run passes in exact mode. One cell of a feature, or
    of an order, that the re-fold holds otherwise fails the comparison, and the line names the
    table, the row, the column and both values: a determinism breach, or look-ahead in a feature,
    that no sum over the run would show."""
    run_dir = golden_dir() / FIRST_BOOT
    equal = replay_of(run_dir, tmp_path / "equal")

    code, output = compare(run_dir, equal)

    assert code == 0, output
    assert "0 differences in exact mode" in output, output

    for name, table, column, row, value, run_value in [
        ("feature", "features", "value", 1, -1.0, "-1.25"),
        ("order", "orders", "price", 0, 10_000_000_000, None),
    ]:
        replay = replay_of(run_dir, tmp_path / name)
        rewrite(table_file(replay, table), changed_cell(column, row, value))

        code, output = compare(run_dir, replay)

        assert code == 1, f"{name}: {output}"
        assert f"table {table} row {row + 1} column {column}: the run holds " in output, output
        assert f"the re-fold holds {value!r}; comparison FAILED" in output, output
        if run_value is not None:
            assert f"the run holds {run_value}," in output, output


def test_rows_only_in_refold_are_taken_by_drop_count(tmp_path: Path) -> None:
    """FITNESS: the second golden boot counted 12 feature rows dropped on its research ring. A
    re-fold that holds 12 feature rows the run lacks is no difference, and the line names the
    count; 13 such rows are one more than the run dropped, a row the re-fold wrote on its own."""
    run_dir = golden_dir() / SECOND_BOOT
    for extra, code_wanted in [(12, 0), (13, 1)]:
        replay = replay_of(run_dir, tmp_path / f"extra-{extra}")
        rewrite(table_file(replay, "features"), with_copies_of_first_row(extra))

        code, output = compare(run_dir, replay)

        assert code == code_wanted, f"{extra} extra rows: {output}"
        if code_wanted == 0:
            assert "features: 12 rows only in the re-fold, dropped by the run" in output, output
        else:
            assert "table features row 3: the run holds None" in output, output


def test_unpaired_run_or_gapped_tape_stops_the_comparison(tmp_path: Path) -> None:
    """A re-fold is paired with its run by `run.json`, never by folder name, so a run that is not
    its re-fold is refused. A run whose tape lost records has no re-fold that can equal it, so the
    comparison halts and names the lost seqs."""
    run_dir = golden_dir() / FIRST_BOOT
    other = boot_copy(tmp_path, SECOND_BOOT, "other")

    code, output = compare(run_dir, other)

    assert code == 2, output
    assert "is no re-fold of" in output and "its kind is sim" in output, output

    gapped = boot_copy(tmp_path, FIRST_BOOT, "gapped")
    gap_row = (9, GAP, None, messages(8, 8)[0][3] + 1)
    write_tape(gapped, [[boot(), *messages(1, 8), gap_row], messages(13, 14)], {9: (9, 12)})
    replay = replay_of(gapped, tmp_path / "gapped-replay")

    code, output = compare(gapped, replay)

    assert code == 1, output
    assert "the tape lost seq 9 to 12" in output, output


def test_rows_past_the_stop_of_a_stopped_table_are_no_difference(tmp_path: Path) -> None:
    """FITNESS: a writer fault stopped the run's features table after its first row, and its
    `run.json` names the table in `stopped_tables`; the re-fold, whose writer did not fault, holds
    each row. The rows past the stop are no difference. The same short table with no stop named is
    a breach, because no drop count covers it (the first golden boot counts no drops)."""
    replay = replay_of(golden_dir() / FIRST_BOOT, tmp_path / "replay")
    for stopped, code_wanted in [(["features"], 0), ([], 1)]:
        run_dir = boot_copy(tmp_path, FIRST_BOOT, f"stopped-{len(stopped)}")
        rewrite(table_file(run_dir, "features"), lambda frame: frame.head(1))
        manifest = json.loads((run_dir / "run.json").read_text())
        manifest["stopped_tables"] = stopped
        (run_dir / "run.json").write_text(json.dumps(manifest))

        code, output = compare(run_dir, replay)

        assert code == code_wanted, f"stopped {stopped}: {output}"
        if code_wanted == 0:
            assert "features: 1 rows past the stop of the table in the run" in output, output
        else:
            assert "table features row 2: the run holds None" in output, output
