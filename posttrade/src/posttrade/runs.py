"""Lists the sealed files of each run that the tooling may read, and refuses the holdout slice.

A run folder holds `run.json` and one folder for each table. Only `*.parquet` names are complete
files; a `*.parquet.part` file is never opened, only named as still open or as cut by a crash.
"""

import fcntl
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Final

from posttrade._generated import EndState, RunKind
from posttrade.contract import Contract
from posttrade.errors import ContractMismatch, HoldoutRefused, RunAmbiguous

__all__ = [
    "Manifest",
    "Run",
    "UnfinishedFile",
    "UnfinishedState",
    "find_runs",
    "open_run",
]

# The quote asset pays a fee from the ledger's cash; the venue tells which instruments trade.
INSTRUMENT_TERMS: Final = ("instrument_id", "venue_id", "quote_asset_id")

JSON_TYPES: Final[Mapping[str, tuple[type, ...]]] = MappingProxyType(
    {"integer": (int,), "string": (str,), "boolean": (bool,), "array": (list,), "object": (dict,)}
)


class UnfinishedState(Enum):
    OPEN = "open"
    CUT = "cut"


@dataclass(frozen=True, slots=True)
class UnfinishedFile:
    """A `.part` file: OPEN while its engine still writes it, else CUT and its rows are lost."""

    path: Path
    table: str
    state: UnfinishedState


@dataclass(frozen=True, slots=True)
class Manifest:
    run_id: str
    kind: RunKind
    strategy_id: str
    te_id: str
    end_state: EndState
    identity_lock: Path | None
    tables: tuple[str, ...]
    quote_asset_ids: Mapping[int, int]
    trading_instrument_ids: frozenset[int]
    spin_interval_us: int
    raw: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class Run:
    dir: Path
    manifest: Manifest
    sealed_files: Mapping[str, tuple[Path, ...]]
    unfinished: tuple[UnfinishedFile, ...]
    contract: Contract

    def all_sealed_files(self) -> tuple[Path, ...]:
        return tuple(path for paths in self.sealed_files.values() for path in paths)


def find_runs(root: Path, contract: Contract) -> tuple[Run, ...]:
    """Each run folder below `root`, symlinks followed, in path order.

    Raises:
        HoldoutRefused: `root`, or a folder below it, resolves below the holdout root.
        ContractMismatch: a run folder differs from the contract.
    """
    _refuse_holdout(root, contract)
    found: list[Path] = []
    seen: set[Path] = set()
    for folder, subfolders, files in os.walk(root, followlinks=True):
        here = Path(folder)
        _refuse_holdout(here, contract)
        resolved = here.resolve()
        if resolved in seen:
            subfolders.clear()
            continue
        seen.add(resolved)
        if contract.manifest_file in files:
            found.append(here)
            subfolders.clear()
    return tuple(open_run(path, contract) for path in sorted(found))


def open_run(run_dir: Path, contract: Contract) -> Run:
    """Reads `run.json` and lists the files of each table of one run.

    Raises:
        HoldoutRefused: the run folder or one of its files resolves below the holdout root.
        ContractMismatch: `run.json` or the folder layout differs from the contract.
    """
    _refuse_holdout(run_dir, contract)
    manifest_path = run_dir / contract.manifest_file
    _refuse_holdout(manifest_path, contract)
    manifest = _read_manifest(manifest_path, run_dir, contract)
    sealed: dict[str, tuple[Path, ...]] = {}
    unfinished: list[UnfinishedFile] = []
    for entry in sorted(run_dir.iterdir()):
        if entry.name == contract.manifest_file or entry.name.startswith("."):
            continue
        if _is_manifest_being_written(entry.name, contract):
            continue
        if not entry.is_dir() or entry.name not in manifest.tables:
            raise ContractMismatch(entry, "entry of the run folder", entry.name, manifest.tables)
        files, parts = _table_files(entry, contract)
        if files:
            sealed[entry.name] = files
        unfinished += [
            UnfinishedFile(part, entry.name, _state_of(part, run_dir, manifest, manifest_path))
            for part in parts
        ]
    return Run(
        dir=run_dir,
        manifest=manifest,
        sealed_files=MappingProxyType(sealed),
        unfinished=tuple(unfinished),
        contract=contract,
    )


def _refuse_holdout(path: Path, contract: Contract) -> None:
    if contract.is_in_holdout(path):
        raise HoldoutRefused(path, contract.holdout_root)


def _table_files(table_dir: Path, contract: Contract) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    sealed: list[Path] = []
    parts: list[Path] = []
    for path in sorted(table_dir.glob("date=*/*")):
        if path.name.startswith("."):
            continue
        _refuse_holdout(path, contract)
        if path.name.endswith(".parquet"):
            sealed.append(path)
        elif path.name.endswith(".parquet" + contract.unfinished_suffix):
            parts.append(path)
        else:
            raise ContractMismatch(path, "file name", path.name, "*.parquet or *.parquet.part")
    return tuple(sorted(sealed, key=_seal_number)), tuple(parts)


def _seal_number(path: Path) -> int:
    """The number at the end of a file name is its place in seal order. The hour in front of it
    is the hour of the file's first row, which can lie before the hour of the file sealed before
    it: the fold can take an answer stamped before the spin that sealed."""
    number = path.name.removesuffix(".parquet").rsplit("-", 1)[-1]
    if not number.isdecimal():
        raise ContractMismatch(path, "file name", path.name, "{HH}-{run_id}-{seq:06}.parquet")
    return int(number)


def _is_manifest_being_written(name: str, contract: Contract) -> bool:
    """The engine writes `run.json` through `run.json.<pid>.tmp` and renames it; a kill in that
    write leaves the temporary file, which is no table and no manifest."""
    prefix = contract.manifest_file + "."
    pid = name.removeprefix(prefix).removesuffix(".tmp")
    return name.startswith(prefix) and name.endswith(".tmp") and pid.isdecimal()


def _state_of(
    part: Path, run_dir: Path, manifest: Manifest, manifest_path: Path
) -> UnfinishedState:
    is_open = (
        manifest.end_state is EndState.OPEN
        and manifest.identity_lock is not None
        and _is_newest_run(run_dir, manifest_path)
        and _is_lock_held(manifest.identity_lock, manifest_path)
    )
    return UnfinishedState.OPEN if is_open else UnfinishedState.CUT


def _is_newest_run(run_dir: Path, manifest_path: Path) -> bool:
    if not run_dir.name.isdecimal():
        reason = (
            f"its folder {run_dir.name!r} is no run id, so it is not known to be the newest run"
        )
        raise RunAmbiguous(manifest_path, reason)
    siblings = [int(p.name) for p in run_dir.parent.iterdir() if p.is_dir() and p.name.isdecimal()]
    return int(run_dir.name) == max(siblings)


def _is_lock_held(lock: Path, manifest_path: Path) -> bool:
    """Probes the engine's identity lock as the engine does: a shared try lock, let go at once."""
    try:
        fd = os.open(lock, os.O_RDONLY)
    except FileNotFoundError:
        return False
    except OSError as error:
        reason = f"its identity lock {lock} cannot be probed ({error.strerror})"
        raise RunAmbiguous(manifest_path, reason) from error
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _read_manifest(path: Path, run_dir: Path, contract: Contract) -> Manifest:
    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        expected = "the JSON that the engine writes"
        raise ContractMismatch(path, "JSON text", str(error), expected) from None
    if not isinstance(raw, dict):
        raise ContractMismatch(path, "top level", type(raw).__name__, "an object")
    names = [field.name for field in contract.manifest_fields]
    if set(raw) != set(names):
        raise ContractMismatch(path, "fields", sorted(raw), sorted(names))
    for field in contract.manifest_fields:
        value = raw[field.name]
        if value is None and field.nullable:
            continue
        is_bool_for_integer = field.json_type == "integer" and isinstance(value, bool)
        if is_bool_for_integer or not isinstance(value, JSON_TYPES[field.json_type]):
            raise ContractMismatch(path, field.name, value, f"a JSON {field.json_type}")
    _expect(path, "contract_version", raw["contract_version"], contract.version)
    _expect(path, "fixed_scale", raw["fixed_scale"], contract.fixed_scale)
    _expect(path, "run_id", raw["run_id"], run_dir.name)
    tables = tuple(str(name) for name in raw["tables"])
    for name in tables:
        if name not in contract.table_names:
            raise ContractMismatch(path, "tables", name, f"one of {contract.table_names}")
    lock = raw["identity_lock"]
    instruments = _integers(path, "instruments", raw["instruments"], INSTRUMENT_TERMS)
    trading_venues = {
        venue["venue_id"]
        for venue in _integers(path, "venues", raw["venues"], ("venue_id",))
        if _venue_trades(path, venue["venue_id"], raw["venues"])
    }
    return Manifest(
        run_id=str(raw["run_id"]),
        kind=_label(path, "kind", raw["kind"], RunKind),
        strategy_id=str(raw["strategy_id"]),
        te_id=str(raw["te_id"]),
        end_state=_label(path, "end_state", raw["end_state"], EndState),
        identity_lock=None if lock is None else Path(str(lock)),
        tables=tables,
        quote_asset_ids=MappingProxyType(
            {row["instrument_id"]: row["quote_asset_id"] for row in instruments}
        ),
        trading_instrument_ids=frozenset(
            row["instrument_id"] for row in instruments if row["venue_id"] in trading_venues
        ),
        spin_interval_us=int(str(raw["spin_interval_us"])),
        raw=MappingProxyType(raw),
    )


def _integers(path: Path, field: str, rows: object, keys: tuple[str, ...]) -> list[dict[str, int]]:
    """The named integer terms of each object of an array of `run.json`."""
    parsed: list[dict[str, int]] = []
    for row in rows if isinstance(rows, list) else ():
        if not isinstance(row, dict):
            raise ContractMismatch(path, field, row, "an object for each entry")
        terms: dict[str, int] = {}
        for key in keys:
            value = row.get(key)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ContractMismatch(path, f"{field} {key}", value, "an integer")
            terms[key] = value
        parsed.append(terms)
    return parsed


def _venue_trades(path: Path, venue_id: int, venues: object) -> bool:
    """A venue trades in this run when it holds a run nonce: one that only records holds none."""
    for venue in venues if isinstance(venues, list) else ():
        if isinstance(venue, dict) and venue.get("venue_id") == venue_id:
            nonce = venue.get("run_nonce")
            if nonce is not None and (isinstance(nonce, bool) or not isinstance(nonce, int)):
                raise ContractMismatch(path, "venues run_nonce", nonce, "an integer or null")
            return nonce is not None
    return False


def _expect(path: Path, field: str, seen: object, expected: object) -> None:
    if seen != expected:
        raise ContractMismatch(path, field, seen, expected)


def _label[L: (RunKind, EndState)](path: Path, field: str, value: object, labels: type[L]) -> L:
    if not isinstance(value, str):
        raise ContractMismatch(path, field, value, "a string")
    try:
        return labels(value)
    except ValueError:
        raise ContractMismatch(path, field, value, [label.value for label in labels]) from None
