"""Reads one table of one run into a frame that matches the contract, cell for cell.

Each file is decoded by its own footer: a feature id names the feature of the boot that wrote it.
Money stays an integer mantissa at the footer's `fixed_scale`.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

import polars as pl

from posttrade.contract import Contract, Table
from posttrade.errors import ContractMismatch, FileVanished, TruncatedFile
from posttrade.runs import Run

__all__ = ["Footer", "as_text", "fill_markets", "read_footer", "read_table", "timeline"]

# The arrow writer adds its own schema key to each footer; the contract does not list it.
ARROW_SCHEMA_KEY: Final = "ARROW:schema"

# Id columns that a footer dictionary names, and the name column the reader adds beside each.
DECODED_IDS: Final[Mapping[str, tuple[str, str]]] = MappingProxyType(
    {
        "feature_id": ("feature_dictionary", "feature"),
        "instrument_id": ("instrument_dictionary", "instrument"),
    }
)

JSON_FORMS: Final = ("json_string_array", "json_pair_array", "json_object", "json_object_array")
JSON_LABEL_ARRAY: Final = "json_label_array:"

MARKET_COLUMNS: Final = (
    "window_open_ts_us",
    "window_close_ts_us",
    "token_id_up",
    "token_id_down",
    "condition_id",
)


@dataclass(frozen=True, slots=True)
class Footer:
    path: Path
    values: Mapping[str, str]

    def raw_items(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(self.values.items()))

    def names(self, key: str) -> tuple[str, ...]:
        return tuple(str(name) for name in json.loads(self.values[key]))


def read_footer(path: Path, contract: Contract) -> Footer:
    """The footer of one sealed file, each key checked against the contract.

    Raises:
        FileVanished: the file was removed after it was listed.
        TruncatedFile: the file has no readable footer.
        ContractMismatch: a key is absent, unknown, or of another form; or the contract version or
            the fixed scale differs from the contract.
    """
    try:
        metadata = pl.read_parquet_metadata(path)
    except FileNotFoundError:
        raise FileVanished(path) from None
    except pl.exceptions.ComputeError as error:
        raise TruncatedFile(path, str(error).splitlines()[0]) from error
    values = {k: v for k, v in metadata.items() if k != ARROW_SCHEMA_KEY}
    keys = [key.key for key in contract.footer_keys]
    if set(values) != set(keys):
        raise ContractMismatch(path, "footer keys", sorted(values), sorted(keys))
    for key in contract.footer_keys:
        _check_form(path, key.key, values[key.key], key.form, contract)
    _expect(path, "footer contract_version", values["contract_version"], str(contract.version))
    _expect(path, "footer fixed_scale", values["fixed_scale"], str(contract.fixed_scale))
    return Footer(path, MappingProxyType(values))


def read_table(run: Run, name: str) -> pl.DataFrame:
    """Each sealed file of table `name`, in file order, with a name column beside each decoded id.

    Raises:
        FileVanished: a file was removed after it was listed.
        ContractMismatch: a file differs from the contract or belongs to another run.
    """
    table = run.contract.table(name)
    frames = [_read_file(path, table, run) for path in run.sealed_files.get(name, ())]
    if not frames:
        return _empty(table)
    return pl.concat(frames, how="vertical")


def fill_markets(run: Run) -> pl.DataFrame:
    """Each fill with the market window its order was placed in.

    A slot that rotates stands for one market in each window. A fill can arrive after its window
    closed, so the window is the one that covers the order's `placed` row, not the fill's stamp.
    Columns of the window are null for a fill whose order has no `placed` row in this run, or
    whose instrument has no rotation that covers the placement.
    """
    fills = read_table(run, "fills")
    placements = (
        read_table(run, "orders")
        .filter(pl.col("transition") == "placed")
        .group_by("instrument_id", "client_order_id")
        .agg(pl.col("received_ts_us").min().alias("placed_ts_us"))
    )
    windows = read_table(run, "rotations").select("instrument_id", *MARKET_COLUMNS)
    placed = fills.join(placements, on=["instrument_id", "client_order_id"], how="left")
    keyed = placed.with_row_index("fill_row")
    with_placement = keyed.filter(pl.col("placed_ts_us").is_not_null()).sort("placed_ts_us")
    matched = with_placement.join_asof(
        windows.sort("window_open_ts_us"),
        left_on="placed_ts_us",
        right_on="window_open_ts_us",
        by="instrument_id",
        strategy="backward",
    )
    covering = matched.filter(pl.col("placed_ts_us") < pl.col("window_close_ts_us"))
    return (
        keyed.join(covering.select("fill_row", *MARKET_COLUMNS), on="fill_row", how="left")
        .sort("fill_row")
        .drop("fill_row")
    )


def timeline(run: Run, start_us: int, end_us: int) -> list[tuple[int, str, str]]:
    """Each row of each table whose stamp lies in [start_us, end_us], as one list in stamp order.

    Each entry is (stamp, table, the row as `column=value` text); a table orders its own rows by its
    partition column, and rows of one stamp keep the table order of the contract.
    """
    events: list[tuple[int, int, int, str, str]] = []
    for table_order, table in enumerate(run.contract.tables):
        if table.name not in run.sealed_files:
            continue
        stamp = pl.col(table.partition_column)
        frame = as_text(read_table(run, table.name).filter(stamp.is_between(start_us, end_us)))
        for row_order, row in enumerate(frame.iter_rows(named=True)):
            text = " ".join(f"{column}={_cell(value)}" for column, value in row.items())
            at = int(row[table.partition_column])
            events.append((at, table_order, row_order, table.name, text))
    return [(at, name, text) for at, _, _, name, text in sorted(events)]


def _cell(value: object) -> object:
    return "null" if value is None else value


def as_text(frame: pl.DataFrame) -> pl.DataFrame:
    """The frame with each bytes column as lowercase hex, so that it prints and writes as text."""
    binary = [name for name, dtype in frame.schema.items() if dtype == pl.Binary]
    return frame.with_columns(pl.col(name).bin.encode("hex") for name in binary)


def _read_file(path: Path, table: Table, run: Run) -> pl.DataFrame:
    footer = read_footer(path, run.contract)
    _expect(path, "footer boot_ts_us", footer.values["boot_ts_us"], run.manifest.run_id)
    _expect(path, "footer run_kind", footer.values["run_kind"], run.manifest.kind.value)
    try:
        frame = pl.read_parquet(path)
    except FileNotFoundError:
        raise FileVanished(path) from None
    except pl.exceptions.ComputeError as error:
        raise TruncatedFile(path, str(error).splitlines()[0]) from error
    names = list(table.column_names)
    if frame.columns != names:
        extra = [name for name in frame.columns if name not in names]
        absent = [name for name in names if name not in frame.columns]
        seen = f"unknown {extra}, absent {absent}" if extra or absent else frame.columns
        raise ContractMismatch(path, "columns", seen, f"the columns of {table.name}, in order")
    for column in table.columns:
        series = frame.get_column(column.name)
        if series.dtype != column.dtype:
            raise ContractMismatch(path, f"type of {column.name}", series.dtype, column.dtype)
        if not column.nullable and series.null_count() > 0:
            raise ContractMismatch(path, f"nulls of {column.name}", series.null_count(), 0)
        if column.labels is not None:
            allowed = run.contract.labels(column.labels)
            unknown = series.drop_nulls().filter(~series.drop_nulls().is_in(allowed)).unique()
            if unknown.len() > 0:
                raise ContractMismatch(path, column.name, unknown.to_list(), f"labels {allowed}")
    return frame.with_columns(_decoded(path, frame, footer))


def _decoded(path: Path, frame: pl.DataFrame, footer: Footer) -> list[pl.Expr]:
    expressions = []
    for id_column, (key, name_column) in DECODED_IDS.items():
        if id_column not in frame.columns:
            continue
        names = footer.names(key)
        largest = frame.get_column(id_column).max()
        if isinstance(largest, int) and largest >= len(names):
            raise ContractMismatch(path, id_column, largest, f"an index of {key} {names}")
        mapping = dict(enumerate(names))
        expressions.append(
            pl.col(id_column).replace_strict(mapping, return_dtype=pl.String).alias(name_column)
        )
    return expressions


def _empty(table: Table) -> pl.DataFrame:
    schema = {column.name: column.dtype for column in table.columns}
    for id_column, (_, name_column) in DECODED_IDS.items():
        if id_column in schema:
            schema[name_column] = pl.String()
    return pl.DataFrame(schema=schema)


def _check_form(path: Path, key: str, value: str, form: str, contract: Contract) -> None:
    field = f"footer {key}"
    if form == "text":
        return
    if form == "integer":
        if not value.isdecimal():
            raise ContractMismatch(path, field, value, "a decimal integer")
        return
    if form.startswith("label:"):
        labels = contract.labels(form.removeprefix("label:"))
        if value not in labels:
            raise ContractMismatch(path, field, value, f"labels {labels}")
        return
    if form.startswith(JSON_LABEL_ARRAY):
        labels = contract.labels(form.removeprefix(JSON_LABEL_ARRAY))
        decoded = _json(path, field, value)
        if not isinstance(decoded, list) or not all(item in labels for item in decoded):
            raise ContractMismatch(path, field, value, f"a JSON array of labels {labels}")
        return
    if form not in JSON_FORMS:
        raise ContractMismatch(contract.file, f"form of footer key {key}", form, JSON_FORMS)
    if not _has_json_form(_json(path, field, value), form):
        raise ContractMismatch(path, field, value, form)


def _has_json_form(decoded: object, form: str) -> bool:
    if form == "json_string_array":
        return isinstance(decoded, list) and all(isinstance(item, str) for item in decoded)
    if form == "json_pair_array":
        return isinstance(decoded, list) and all(_is_string_pair(item) for item in decoded)
    if form == "json_object_array":
        return isinstance(decoded, list) and all(isinstance(item, dict) for item in decoded)
    return isinstance(decoded, dict) and all(isinstance(item, str) for item in decoded.values())


def _is_string_pair(item: object) -> bool:
    return isinstance(item, list) and len(item) == 2 and all(isinstance(s, str) for s in item)


def _json(path: Path, field: str, value: str) -> object:
    try:
        decoded: object = json.loads(value)
    except json.JSONDecodeError:
        raise ContractMismatch(path, field, value, "JSON text") from None
    return decoded


def _expect(path: Path, field: str, seen: object, expected: object) -> None:
    if seen != expected:
        raise ContractMismatch(path, field, seen, expected)
