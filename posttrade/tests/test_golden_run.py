"""The reader decodes each cell of the golden run that the Rust writer made to its hand value."""

import json
from pathlib import Path
from typing import Any

import pytest

from posttrade import contract, runs, tables

REPO = Path(__file__).resolve().parents[2]
GOLDEN = REPO / "target" / "golden-run"


@pytest.fixture(scope="module")
def expected() -> Any:
    path = GOLDEN / "expected.json"
    if not path.is_file():
        pytest.fail(
            f"no golden run at {GOLDEN}: run "
            "`cargo run -p engine --bin golden-run -- target/golden-run` in the repo root first"
        )
    return json.loads(path.read_text())


def test_golden_run_cells_equal_expected(expected: Any) -> None:
    terms = contract.load_repo_contract()
    for want in expected["runs"]:
        run = runs.open_run(GOLDEN / want["run_dir"], terms)
        assert run.manifest.raw == want["manifest"], run.dir
        assert set(run.sealed_files) == set(want["tables"]), run.dir
        for name, cells in want["tables"].items():
            table = tables.read_table(run, name)
            assert table.columns[: len(cells["columns"])] == cells["columns"], (run.dir, name)
            got = [list(row) for row in table.select(cells["columns"]).rows()]
            assert got == cells["rows"], (run.dir, name)
        footers = {tables.read_footer(path, terms).raw_items() for path in run.all_sealed_files()}
        assert footers == {tuple(sorted(want["footer"].items()))}, run.dir


def test_each_file_decodes_ids_by_its_own_footer() -> None:
    terms = contract.load_repo_contract()
    first = runs.open_run(GOLDEN / "golden/te-golden/sim/1784000000000000", terms)
    second = runs.open_run(GOLDEN / "golden/te-golden/sim/1784000600000000", terms)
    names = {
        run.manifest.run_id: dict(
            tables.read_table(run, "features").select("feature_id", "feature").rows()
        )
        for run in (first, second)
    }
    assert names["1784000000000000"][1] == "beta"
    assert names["1784000600000000"][1] == "gamma"
