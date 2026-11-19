"""The label enums in the package are those that the contract in git defines."""

from posttrade import contract


def test_generated_labels_equal_the_contract() -> None:
    written = contract.GENERATED_MODULE.read_text()
    assert written == contract.render_label_module(contract.load_repo_contract()), (
        "contract/tables.json changed: run `uv run python -m posttrade.generate` in posttrade/"
    )
