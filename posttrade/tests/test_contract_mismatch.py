"""A file that differs from the contract is refused with an error that names the file."""

from pathlib import Path

import polars as pl
import pytest

from posttrade import contract, runs, tables
from posttrade.errors import ContractMismatch

from conftest import rewrite, table_file


def read_orders(run_dir: Path) -> pl.DataFrame:
    run = runs.open_run(run_dir, contract.load_repo_contract())
    return tables.read_table(run, "orders")


def test_planted_column_names_the_file(golden_copy: Path) -> None:
    path = table_file(golden_copy, "orders")
    rewrite(path, change_frame=lambda frame: frame.with_columns(pl.lit(1).alias("planted")))
    with pytest.raises(ContractMismatch) as refused:
        read_orders(golden_copy)
    assert str(path) in str(refused.value)
    assert "planted" in str(refused.value)


def test_planted_label_names_the_file(golden_copy: Path) -> None:
    path = table_file(golden_copy, "orders")
    rewrite(path, change_frame=lambda frame: frame.with_columns(pl.lit("sideways").alias("side")))
    with pytest.raises(ContractMismatch) as refused:
        read_orders(golden_copy)
    assert str(path) in str(refused.value)
    assert "sideways" in str(refused.value)


def test_planted_contract_version_names_the_file(golden_copy: Path) -> None:
    path = table_file(golden_copy, "orders")
    rewrite(path, change_footer=lambda footer: footer | {"contract_version": "2"})
    with pytest.raises(ContractMismatch) as refused:
        read_orders(golden_copy)
    assert str(path) in str(refused.value)
    assert "contract_version is '2'" in str(refused.value)
