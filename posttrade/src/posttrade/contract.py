"""Gives the definitions of the data contract that the engine exports.

Run `uv run python -m posttrade.generate` to write the label enums again after the contract changes.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import polars as pl

from posttrade.errors import ContractMismatch

__all__ = [
    "Column",
    "Contract",
    "Table",
    "load",
    "load_repo_contract",
    "render_label_module",
]

REPO_ROOT: Final = Path(__file__).resolve().parents[3]
REPO_CONTRACT: Final = REPO_ROOT / "contract" / "tables.json"
GENERATED_MODULE: Final = Path(__file__).with_name("_generated.py")

ARROW_TYPES: Final[Mapping[str, pl.DataType]] = {
    "int64": pl.Int64(),
    "int32": pl.Int32(),
    "uint8": pl.UInt8(),
    "uint16": pl.UInt16(),
    "uint32": pl.UInt32(),
    "uint64": pl.UInt64(),
    "float64": pl.Float64(),
    "bool": pl.Boolean(),
    "utf8": pl.String(),
    "binary": pl.Binary(),
}


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    kind: str
    dtype: pl.DataType
    nullable: bool
    labels: str | None


@dataclass(frozen=True, slots=True)
class Table:
    name: str
    partition_column: str
    columns: tuple[Column, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


@dataclass(frozen=True, slots=True)
class ManifestField:
    name: str
    json_type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class FooterKey:
    key: str
    form: str


@dataclass(frozen=True, slots=True)
class Contract:
    """The contract as the engine exported it; `root` is the folder that holds `contract/`."""

    file: Path
    root: Path
    version: int
    fixed_scale: int
    holdout_root: Path
    unfinished_suffix: str
    manifest_file: str
    manifest_fields: tuple[ManifestField, ...]
    footer_keys: tuple[FooterKey, ...]
    tables: tuple[Table, ...]
    label_sets: tuple[tuple[str, tuple[str, ...]], ...]
    metric_names: tuple[str, ...]
    tape_kinds: tuple[tuple[str, int], ...]

    def table(self, name: str) -> Table:
        for table in self.tables:
            if table.name == name:
                return table
        raise ContractMismatch(self.file, "table", name, f"one of {self.table_names}")

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(table.name for table in self.tables)

    def tape_kind(self, name: str) -> int:
        for kind_name, kind in self.tape_kinds:
            if kind_name == name:
                return kind
        raise ContractMismatch(self.file, "tape kind", name, "a kind of tape_kinds")

    def labels(self, set_name: str) -> tuple[str, ...]:
        for name, values in self.label_sets:
            if name == set_name:
                return values
        raise ContractMismatch(self.file, "label set", set_name, "a set of label_sets")

    def is_in_holdout(self, path: Path) -> bool:
        return path.resolve().is_relative_to(self.holdout_root.resolve())


def load_repo_contract() -> Contract:
    return load(REPO_CONTRACT)


def load(file: Path) -> Contract:
    """Reads `contract/tables.json`.

    Raises:
        ContractMismatch: the file has a field of another shape than this reader knows.
    """
    try:
        loaded: object = json.loads(file.read_text())
    except json.JSONDecodeError as error:
        expected = "the JSON that contract-export writes"
        raise ContractMismatch(file, "JSON text", str(error), expected) from None
    top = _Fields(file, "", loaded)
    root = file.resolve().parent.parent
    manifest = top.object("manifest")
    contract = Contract(
        file=file,
        root=root,
        version=top.integer("contract_version"),
        fixed_scale=top.integer("fixed_scale"),
        holdout_root=root / top.string("holdout_root"),
        unfinished_suffix=top.string("unfinished_suffix"),
        manifest_file=manifest.string("file"),
        manifest_fields=tuple(
            ManifestField(f.string("name"), f.string("type"), f.boolean("nullable"))
            for f in manifest.objects("fields")
        ),
        footer_keys=tuple(
            FooterKey(f.string("key"), f.string("form")) for f in top.objects("footer_keys")
        ),
        tables=tuple(_table(t) for t in top.objects("tables")),
        label_sets=top.object("label_sets").string_arrays(),
        metric_names=tuple(m.string("name") for m in top.objects("metrics")),
        tape_kinds=tuple((k.string("name"), k.integer("kind")) for k in top.objects("tape_kinds")),
    )
    for table in contract.tables:
        for column in table.columns:
            if column.labels is not None:
                contract.labels(column.labels)
    return contract


def _table(fields: "_Fields") -> Table:
    columns = []
    for column in fields.objects("columns"):
        arrow = column.string("arrow")
        if arrow not in ARROW_TYPES:
            raise ContractMismatch(column.file, f"{column.where}.arrow", arrow, list(ARROW_TYPES))
        labels = column.optional_string("labels")
        columns.append(
            Column(
                name=column.string("name"),
                kind=column.string("kind"),
                dtype=ARROW_TYPES[arrow],
                nullable=column.boolean("nullable"),
                labels=labels,
            )
        )
    return Table(
        name=fields.string("name"),
        partition_column=fields.string("partition_column"),
        columns=tuple(columns),
    )


class _Fields:
    """Typed access to one JSON object of the contract file; each wrong shape names its place."""

    def __init__(self, file: Path, where: str, value: object) -> None:
        if not isinstance(value, dict):
            raise ContractMismatch(file, where or "top level", type(value).__name__, "an object")
        self.file = file
        self.where = where
        self._value: dict[str, object] = value

    def string_arrays(self) -> tuple[tuple[str, tuple[str, ...]], ...]:
        arrays = []
        for key, value in self._value.items():
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                raise ContractMismatch(self.file, self._path(key), value, "an array of strings")
            arrays.append((key, tuple(str(item) for item in value)))
        return tuple(arrays)

    def _get(self, key: str) -> object:
        if key not in self._value:
            raise ContractMismatch(self.file, self._path(key), "absent", "a value")
        return self._value[key]

    def _path(self, key: str) -> str:
        return f"{self.where}.{key}" if self.where else key

    def string(self, key: str) -> str:
        value = self._get(key)
        if not isinstance(value, str):
            raise ContractMismatch(self.file, self._path(key), value, "a string")
        return value

    def optional_string(self, key: str) -> str | None:
        if key not in self._value:
            return None
        return self.string(key)

    def integer(self, key: str) -> int:
        value = self._get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ContractMismatch(self.file, self._path(key), value, "an integer")
        return value

    def boolean(self, key: str) -> bool:
        value = self._get(key)
        if not isinstance(value, bool):
            raise ContractMismatch(self.file, self._path(key), value, "true or false")
        return value

    def object(self, key: str) -> "_Fields":
        return _Fields(self.file, self._path(key), self._get(key))

    def objects(self, key: str) -> list["_Fields"]:
        value = self._get(key)
        if not isinstance(value, list):
            raise ContractMismatch(self.file, self._path(key), value, "an array")
        return [_Fields(self.file, f"{self._path(key)}[{i}]", item) for i, item in enumerate(value)]


def render_label_module(contract: Contract) -> str:
    """The source of `_generated.py`: one enum for each label set, values in contract order."""
    lines = [
        '"""The label sets of the data contract, written by `python -m posttrade.generate`."""',
        "",
        "from enum import StrEnum",
        "",
        "__all__ = [",
        *(f'    "{_class_name(name)}",' for name, _ in sorted(contract.label_sets)),
        "]",
    ]
    for name, values in contract.label_sets:
        lines += ["", "", f"class {_class_name(name)}(StrEnum):"]
        members = _members(contract.file, name, values)
        lines += [f'    {member} = "{value}"' for member, value in members]
    return "\n".join(lines) + "\n"


def _class_name(set_name: str) -> str:
    return "".join(part.capitalize() for part in set_name.split("_"))


def _members(file: Path, set_name: str, values: Sequence[str]) -> list[tuple[str, str]]:
    uppers = [value.upper() for value in values]
    members = []
    for value, upper in zip(values, uppers, strict=True):
        # `1m` and `1M` are two kline intervals: such a value keeps its case behind a prefix.
        is_plain = upper.isidentifier() and uppers.count(upper) == 1
        member = upper if is_plain else f"V_{value}"
        if not member.isidentifier():
            raise ContractMismatch(file, f"label_sets.{set_name}", value, "a name")
        members.append((member, value))
    return members
