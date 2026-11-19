"""Each input that the tooling cannot read is refused with one line and exit 2, never a traceback;
and a report never lands inside a run folder."""

import io
import json
import os
import shutil
from pathlib import Path

import pytest

from posttrade import cli, contract
from posttrade.errors import ContractMismatch

from conftest import table_file


def run_command(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(list(argv), cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def test_report_into_the_persistence_root_is_refused(first_boot_copy: Path) -> None:
    """F10. The report folder below the persistence root is the run folder itself."""
    root = first_boot_copy.parents[3]

    code, output = run_command("report", str(first_boot_copy), "--out", str(root))

    assert (code, "refused to write" in output) == (2, True), output
    assert not (first_boot_copy / "report.html").exists()
    assert not (first_boot_copy / "summary.json").exists()


def test_sealed_file_with_no_footer_is_a_truncated_file(first_boot_copy: Path) -> None:
    """F14. A sealed name over a file that a crash cut."""
    cut = table_file(first_boot_copy, "orders")
    cut.write_bytes(cut.read_bytes()[:100])

    code, output = run_command("check", str(first_boot_copy))

    assert code == 2, output
    (refusal,) = [line for line in output.splitlines() if line.startswith("posttrade: ")]
    assert str(cut) in refusal and "truncated" in refusal


def test_manifest_that_is_no_json_is_refused(first_boot_copy: Path) -> None:
    """F14."""
    (first_boot_copy / "run.json").write_text('{"contract_version": ')

    code, output = run_command("check", str(first_boot_copy))

    assert code == 2 and output.startswith("posttrade: ") and "run.json" in output, output


def test_fill_of_an_instrument_that_run_json_lacks_is_refused(first_boot_copy: Path) -> None:
    """F14."""
    manifest = json.loads((first_boot_copy / "run.json").read_text())
    manifest["instruments"] = []
    (first_boot_copy / "run.json").write_text(json.dumps(manifest))

    code, output = run_command("check", str(first_boot_copy))

    assert code == 2 and "no instrument 0" in output, output


def unreadable_lock_and_open_part(run_dir: Path) -> Path:
    lock = run_dir.parent / ".exec-golden.lock"
    lock.touch()
    lock.chmod(0)
    manifest = json.loads((run_dir / "run.json").read_text())
    manifest |= {"identity_lock": str(lock), "end_state": "open"}
    (run_dir / "run.json").write_text(json.dumps(manifest))
    part = table_file(run_dir, "orders").with_suffix(".parquet.part")
    part.write_bytes(b"rows still being written")
    return lock


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file of mode 0")
def test_lock_that_cannot_be_probed_is_a_run_ambiguous(first_boot_copy: Path) -> None:
    """F14. With the lock unreadable, an open file cannot be told from one that a crash cut."""
    lock = unreadable_lock_and_open_part(first_boot_copy)

    code, output = run_command("check", str(first_boot_copy))
    lock.chmod(0o644)

    assert code == 2 and "ambiguous" in output and str(lock) in output, output


def test_run_folder_that_names_no_run_id_is_a_run_ambiguous(first_boot_copy: Path) -> None:
    """F14. A copy of a run under another folder name cannot be placed among its siblings."""
    renamed = first_boot_copy.with_name("copy")
    shutil.move(first_boot_copy, renamed)
    manifest = json.loads((renamed / "run.json").read_text())
    manifest |= {"run_id": "copy", "end_state": "open", "identity_lock": str(renamed / "x.lock")}
    (renamed / "run.json").write_text(json.dumps(manifest))
    table_file(renamed, "orders").with_suffix(".parquet.part").write_bytes(b"open")

    code, output = run_command("check", str(renamed))

    assert code == 2 and "ambiguous" in output, output


def test_contract_that_is_no_json_is_refused(tmp_path: Path) -> None:
    """F14."""
    broken = tmp_path / "contract" / "tables.json"
    broken.parent.mkdir()
    broken.write_text("{")

    with pytest.raises(ContractMismatch) as refused:
        contract.load(broken)

    assert str(broken) in str(refused.value)
