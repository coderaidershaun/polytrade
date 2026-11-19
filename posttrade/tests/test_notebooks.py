"""The notebooks run on the golden run, hold no logic of their own, and are kept with no outputs."""

import ast
from pathlib import Path

import nbformat
import pytest
from nbclient import NotebookClient

from conftest import FIRST_BOOT, golden_dir

NOTEBOOKS = sorted((Path(__file__).resolve().parents[1] / "notebooks").glob("*.ipynb"))
LOGIC = (ast.FunctionDef, ast.ClassDef, ast.For, ast.While, ast.If, ast.Lambda, ast.ListComp)


def test_each_notebook_is_found() -> None:
    assert [path.name for path in NOTEBOOKS] == ["check.ipynb", "fills.ipynb"]


@pytest.mark.parametrize("path", NOTEBOOKS, ids=[path.name for path in NOTEBOOKS])
def test_notebook_runs_and_holds_no_logic_and_no_output(
    path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    notebook = nbformat.read(path, as_version=4)  # type: ignore[no-untyped-call]  # nbformat ships no types
    code = [cell for cell in notebook.cells if cell.cell_type == "code"]
    for cell in code:
        assert cell.outputs == [] and cell.execution_count is None, f"{path}: clear its outputs"
        logic = [type(n).__name__ for n in ast.walk(ast.parse(cell.source)) if isinstance(n, LOGIC)]
        assert logic == [], f"{path}: a cell holds {logic}; move it into the library"

    monkeypatch.setenv("POSTTRADE_RUN", str(golden_dir() / FIRST_BOOT))
    NotebookClient(notebook, timeout=120, kernel_name="python3").execute()

    assert all(cell.execution_count is not None for cell in code)
