"""A run that its engine still writes: the reader lists sealed files and never opens a `.part`."""

import fcntl
import json
import shutil
from pathlib import Path

from posttrade import contract, runs
from posttrade.runs import UnfinishedState

from conftest import golden_dir

RUN_ID = "1784410630178000"


def open_live_run(tmp_path: Path, lock: Path) -> Path:
    run_dir = tmp_path / "recorder" / "te-recorder" / "live" / RUN_ID
    orders = run_dir / "orders" / "date=2026-07-18"
    orders.mkdir(parents=True)
    golden = golden_dir() / "golden/te-golden/sim/1784000600000000"
    (sealed,) = (golden / "orders").glob("date=*/*.parquet")
    shutil.copy(sealed, orders / f"21-{RUN_ID}-000000.parquet")
    (orders / f"21-{RUN_ID}-000001.parquet.part").write_bytes(b"rows still being written")
    manifest = json.loads((golden / "run.json").read_text())
    manifest |= {
        "run_id": RUN_ID,
        "kind": "live",
        "boot_ts_us": int(RUN_ID),
        "identity_lock": str(lock),
        "end_state": "open",
        "end_reason": None,
    }
    (run_dir / "run.json").write_text(json.dumps(manifest))
    return run_dir


def folder_state(folder: Path) -> list[tuple[str, int, int]]:
    return sorted(
        (str(p), p.stat().st_size, p.stat().st_mtime_ns) for p in folder.rglob("*") if p.is_file()
    )


def test_open_file_is_skipped_and_named(tmp_path: Path) -> None:
    lock = tmp_path / ".exec-recorder.lock"
    lock.touch()
    run_dir = open_live_run(tmp_path, lock)
    before = folder_state(run_dir)
    terms = contract.load_repo_contract()
    part = run_dir / "orders" / "date=2026-07-18" / f"21-{RUN_ID}-000001.parquet.part"

    with lock.open("rb") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while_writing = runs.open_run(run_dir, terms)
    after_crash = runs.open_run(run_dir, terms)

    assert [p.name for p in while_writing.sealed_files["orders"]] == [f"21-{RUN_ID}-000000.parquet"]
    assert [(u.path, u.state) for u in while_writing.unfinished] == [(part, UnfinishedState.OPEN)]
    assert [(u.path, u.state) for u in after_crash.unfinished] == [(part, UnfinishedState.CUT)]
    assert folder_state(run_dir) == before
