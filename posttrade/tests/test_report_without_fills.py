"""A correct run that recorded no `fills` gets a report: its attribution is withheld by name,
because the attribution books each ledger fill row against its fill, and the rest shows."""

import io
import json
import shutil
from pathlib import Path
from typing import Any

from posttrade import cli
from posttrade.metrics import pnl


def test_run_without_fills_reports_no_attribution_and_passes(
    first_boot_copy: Path, tmp_path: Path
) -> None:
    """F44, the verifier's probe: golden boot 1 with `orders` and `fills` left out of `run.json`."""
    manifest_path = first_boot_copy / "run.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["tables"] = [t for t in manifest["tables"] if t not in ("orders", "fills")]
    manifest_path.write_text(json.dumps(manifest))
    shutil.rmtree(first_boot_copy / "orders")
    shutil.rmtree(first_boot_copy / "fills")
    out = tmp_path / "reports"
    stream = io.StringIO()

    code = cli.main(
        ["report", str(first_boot_copy), "--out", str(out)], cli.CommandEnv(stream, stream)
    )

    (summary_file,) = out.rglob("summary.json")
    summary: Any = json.loads(summary_file.read_text())
    page = (summary_file.parent / "report.html").read_text()
    assert code == 0, stream.getvalue()
    assert summary["status"] == "PASSED"
    assert summary["metrics"]["pnl.session_quote"] == 600_000_000
    assert not set(pnl.ATTRIBUTION_PARTS) & set(summary["metrics"])
    assert summary["notes"] == ["the run records no fills, so its P&L has no attribution"]
    assert "records no fills" in page
