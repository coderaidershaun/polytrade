"""The walls of the package: it reads files only, and it never reads the holdout slice."""

import ast
import io
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest

from posttrade import cli, contract

from conftest import REPO, SECOND_BOOT, golden_dir

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "posttrade"
NETWORK_AND_PROCESS_MODULES = {"socket", "http", "urllib", "asyncio", "subprocess"}


def test_package_imports_no_network_or_process_module() -> None:
    found = []
    for path in sorted(PACKAGE.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                names = [node.module]
            else:
                continue
            found += [
                f"{path}:{node.lineno} imports {name}"
                for name in names
                if name.split(".")[0] in NETWORK_AND_PROCESS_MODULES
            ]
    assert found == [], "the tooling reads files only, so it must not import these"


@pytest.fixture
def repo_with_holdout(tmp_path: Path) -> Iterator[Path]:
    """A repo folder with the contract, one run in the holdout slice, and `data/runs`."""
    (tmp_path / "contract").mkdir()
    shutil.copy(REPO / "contract" / "tables.json", tmp_path / "contract" / "tables.json")
    day1 = tmp_path / "data" / "holdout" / "day1"
    shutil.copytree(golden_dir() / SECOND_BOOT.parent, day1 / SECOND_BOOT.parent)
    for path in day1.rglob("*"):
        if path.is_file():
            path.chmod(0)
    (tmp_path / "data" / "runs").mkdir()
    yield tmp_path
    for path in day1.rglob("*"):
        if path.is_file():
            path.chmod(0o644)


@pytest.mark.parametrize("root_below_data", ["runs/x", "runs/../holdout/day1", "runs"])
def test_holdout_symlink_is_refused(repo_with_holdout: Path, root_below_data: str) -> None:
    data = repo_with_holdout / "data"
    (data / "runs" / "x").symlink_to(data / "holdout" / "day1")
    (data / "runs" / "inside").mkdir()
    (data / "runs" / "inside" / "x").symlink_to(data / "holdout" / "day1")
    terms = contract.load(repo_with_holdout / "contract" / "tables.json")
    before = sorted(repo_with_holdout.rglob("*"))
    out = io.StringIO()
    env = cli.CommandEnv(out=out, err=out, load_contract=lambda: terms)

    code = cli.main(["runs", str(data / root_below_data)], env)

    assert code == 3
    (line,) = out.getvalue().splitlines()
    assert "holdout root" in line and str(data / root_below_data) in line
    assert sorted(repo_with_holdout.rglob("*")) == before
