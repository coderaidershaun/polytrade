"""Writes the results of one run as files for a person: one self-contained HTML page and a JSON
summary, in an output folder that lies outside each run folder, the holdout, and the code.

A report of a run whose records disagree is marked FAILED and holds the findings and no measure;
a report whose P&L parts leave a remainder is marked FAILED and holds no part.
"""

import html
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from posttrade.audit import FeatureHealth
from posttrade.contract import Contract
from posttrade.errors import ContractMismatch, OutputRefused
from posttrade.metrics.fills import HORIZONS_S, FillMeasures
from posttrade.runs import Run

__all__ = ["ReportFiles", "refuse_output", "report_folder", "write_report"]

CODE_FOLDERS: Final = ("engine", "lab", "strategies")
PER_FILL_COLUMNS: Final = (
    "received_ts_us",
    "instrument",
    "client_order_id",
    "side",
    "liquidity",
    "last_price",
    "last_qty",
    "effective_spread_bps",
    *(f"markout_{h}s_bps" for h in HORIZONS_S),
)
STYLE: Final = """
body { font-family: system-ui, sans-serif; margin: 2rem; color: #222; }
h1 { font-size: 1.4rem; } h2 { font-size: 1.1rem; margin-top: 2rem; }
table { border-collapse: collapse; font-variant-numeric: tabular-nums; }
td, th { border: 1px solid #ccc; padding: 0.2rem 0.5rem; text-align: right; }
th { background: #f3f3f3; } .failed { color: #a00; } .passed { color: #060; }
"""


@dataclass(frozen=True, slots=True)
class ReportFiles:
    html: Path
    summary: Path


def refuse_output(out: Path, terms: Contract) -> None:
    """The tooling writes only into its own output folder: not a run, the holdout, or code.

    Raises:
        OutputRefused: `out` resolves into a code folder, the holdout, or a run folder.
    """
    resolved = out.resolve()
    for name in CODE_FOLDERS:
        if resolved.is_relative_to((terms.root / name).resolve()):
            raise OutputRefused(out, f"it lies in the {name} folder of the repo")
    if terms.is_in_holdout(out):
        raise OutputRefused(out, "it lies below the holdout root")
    for folder in (resolved, *resolved.parents):
        if (folder / terms.manifest_file).is_file():
            raise OutputRefused(out, f"it lies in the run folder {folder}")


def report_folder(run: Run, out: Path) -> Path:
    """The folder that the report of `run` goes into below `out`, refused where the tooling must
    not write. Below a persistence root, that folder would be the run folder itself.

    Raises:
        OutputRefused: the folder lies in a code folder, the holdout, or a run folder.
    """
    manifest = run.manifest
    folder = out / manifest.strategy_id / manifest.te_id / manifest.kind.value / manifest.run_id
    refuse_output(folder, run.contract)
    if folder.resolve().is_relative_to(run.dir.resolve()):
        raise OutputRefused(folder, f"it lies in the run folder {run.dir}")
    return folder


def write_report(
    run: Run,
    out: Path,
    failures: Sequence[str],
    health: FeatureHealth,
    metrics: Mapping[str, object],
    fills: FillMeasures | None,
    *,
    notes: Sequence[str] = (),
) -> ReportFiles:
    """Writes `report.html` and `summary.json` into `out/<strategy>/<te>/<kind>/<run id>/`.

    Raises:
        OutputRefused: `out` lies where the tooling must not write.
        ContractMismatch: a measure has a name that the contract does not list.
    """
    folder = report_folder(run, out)
    unknown = sorted(set(metrics) - set(run.contract.metric_names))
    if unknown:
        raise ContractMismatch(run.contract.file, "metric names", unknown, "names of metrics")
    manifest = run.manifest
    folder.mkdir(parents=True, exist_ok=True)
    status = "FAILED" if failures else "PASSED"
    summary = {
        "contract_version": run.contract.version,
        "run_dir": str(run.dir),
        "run_id": manifest.run_id,
        "kind": manifest.kind.value,
        "status": status,
        "failures": list(failures),
        "notes": list(notes),
        "features_never_emitted": list(health.never_emitted),
        "unfinished_files": [f"{u.state.value} {u.path}" for u in run.unfinished],
        "metrics": dict(metrics),
    }
    files = ReportFiles(html=folder / "report.html", summary=folder / "summary.json")
    files.summary.write_text(json.dumps(summary, indent=2) + "\n")
    page = _page(run, status, failures, notes, health, metrics, fills)
    files.html.write_text(page)
    return files


def _page(
    run: Run,
    status: str,
    failures: Sequence[str],
    notes: Sequence[str],
    health: FeatureHealth,
    metrics: Mapping[str, object],
    fills: FillMeasures | None,
) -> str:
    manifest = run.manifest
    title = f"{manifest.strategy_id} / {manifest.te_id} / {manifest.kind.value} {manifest.run_id}"
    parts = [
        "<!doctype html>",
        '<html lang="en"><head><meta charset="utf-8">',
        f"<title>{_text(title)}</title><style>{STYLE}</style></head><body>",
        f"<h1>{_text(title)}</h1>",
        f'<p class="{status.lower()}"><strong>{status}</strong></p>',
    ]
    if failures:
        parts.append("<h2>Failures: the measures they touch are withheld</h2><ul>")
        parts += [f"<li>{_text(failure)}</li>" for failure in failures]
        parts.append("</ul>")
    if notes:
        parts.append("<h2>Withheld</h2><ul>")
        parts += [f"<li>{_text(note)}</li>" for note in notes]
        parts.append("</ul>")
    if run.unfinished:
        parts.append("<h2>Unfinished files</h2><ul>")
        parts += [f"<li>{_text(f'{u.state.value} {u.path}')}</li>" for u in run.unfinished]
        parts.append("</ul>")
    if health.never_emitted:
        parts.append(f"<p>Features never emitted: {_text(', '.join(health.never_emitted))}</p>")
    if metrics:
        rows = [[name, _cell(value)] for name, value in metrics.items()]
        parts += ["<h2>Measures</h2>", _table(["measure", "value"], rows)]
    if fills is not None:
        parts += _fill_sections(fills)
    parts.append("</body></html>")
    return "\n".join(parts) + "\n"


def _fill_sections(fills: FillMeasures) -> list[str]:
    bands = [
        [f"{h} s", "no band" if band is None else f"{band.low:.3f} to {band.high:.3f}"]
        for h, band in fills.bands.items()
    ]
    slices = [
        [s.side, s.liquidity, str(s.count), *(_cell(s.mean_bps[h]) for h in HORIZONS_S)]
        for s in fills.slices
    ]
    columns = [c for c in PER_FILL_COLUMNS if c in fills.per_fill.columns]
    per_fill = [[_cell(v) for v in row] for row in fills.per_fill.select(columns).iter_rows()]
    drops = {cause.value: count for cause, count in fills.dropped_by_cause.items()}
    return [
        "<h2>Fills</h2>",
        f"<p>Quoting time: {fills.quoting_seconds:.1f} s of sides quoting. "
        f"Markout pairs dropped by cause: {_text(json.dumps(drops))}. "
        "A markout is in basis points; negative is against us.</p>",
        "<h2>Mean markout band, resampling whole client orders</h2>",
        _table(["horizon", "95 % band, bps"], bands),
        "<h2>Markouts by side and liquidity</h2>",
        _table(["side", "liquidity", "fills", *(f"{h} s" for h in HORIZONS_S)], slices),
        "<h2>Each fill</h2>",
        _table(columns, per_fill),
    ]


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    head = "".join(f"<th>{_text(name)}</th>" for name in header)
    body = "".join("<tr>" + "".join(f"<td>{_text(c)}</td>" for c in row) + "</tr>" for row in rows)
    return f"<table><tr>{head}</tr>{body}</table>"


def _cell(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, Mapping):
        return json.dumps(value)
    return str(value)


def _text(value: str) -> str:
    return html.escape(value, quote=True)
