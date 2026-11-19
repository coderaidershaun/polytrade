"""Writes the label enums of `_generated.py` again from the contract in git."""

from posttrade import contract

if __name__ == "__main__":
    contract.GENERATED_MODULE.write_text(
        contract.render_label_module(contract.load_repo_contract())
    )
