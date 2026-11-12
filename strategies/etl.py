"""ETL over the parquet a strategy run records. Notebooks import this instead of
re-writing sealed-file discovery, footer decoding and the feature pivot."""

import json
from pathlib import Path

import polars as pl

MANTISSA_COLUMNS = {
    "fills": ["last_price", "last_qty", "booked_qty", "booked_quote", "commission"],
    "orders": ["price", "qty", "filled_qty", "filled_quote"],
}


def sealed(paths):
    """A live run's file has flushed row groups but no footer until close() — only a
    readable footer proves the file is done."""

    def has_footer(path):
        try:
            pl.read_parquet_metadata(path)
            return True
        except pl.exceptions.ComputeError:
            return False

    return [p for p in paths if has_footer(p)]


def find_run(data_dir, strategy_id, te_id, mode="auto"):
    """Directory holding a run's table folders. A sim run nests its artifacts under
    sim/ so simulated fills can never be grabbed by accident; live tables sit at the
    top level. mode="auto" picks whichever exists and refuses to guess between both."""
    te_dir = Path(data_dir) / strategy_id / te_id
    if not te_dir.is_dir():
        raise FileNotFoundError(f"no recording at {te_dir.resolve()}")
    if mode == "auto":
        has_sim = (te_dir / "sim").is_dir()
        has_live = any(p.is_dir() and p.name != "sim" for p in te_dir.iterdir())
        if has_sim and has_live:
            raise ValueError(f"{te_dir} holds both sim and live artifacts — pass mode explicitly")
        mode = "sim" if has_sim else "live"
    return te_dir / "sim" if mode == "sim" else te_dir


def table_files(run_dir, table):
    files = sealed(sorted(Path(run_dir).glob(f"{table}/date=*/*.parquet")))
    if not files:
        raise FileNotFoundError(
            f"no sealed {table} files under {Path(run_dir).resolve()} — "
            "a capture still writing has no footers; Ctrl-C the run and re-try"
        )
    return files


def read_footer(path):
    """Footer key-values, with the JSON dictionaries parsed and fixed_scale as a number."""
    footer = pl.read_parquet_metadata(path)
    for key in ("feature_dictionary", "instrument_dictionary", "asset_dictionary"):
        if key in footer:
            footer[key] = json.loads(footer[key])
    footer["fixed_scale"] = float(footer["fixed_scale"])
    return footer


def load_table(run_dir, table):
    """One table as a DataFrame plus its footer, mantissa columns scaled to floats."""
    files = table_files(run_dir, table)
    footer = read_footer(files[-1])
    df = pl.read_parquet([str(p) for p in files])
    scale = footer["fixed_scale"]
    scaled = [(pl.col(c) / scale).alias(c) for c in MANTISSA_COLUMNS.get(table, []) if c in df.columns]
    return df.with_columns(scaled), footer


def load_features(run_dir):
    """Long feature frame with id columns replaced by names, plus the footer and the
    list of declared-but-never-emitted feature names (dropped from the frame's world)."""
    df, footer = load_table(run_dir, "features")
    features = footer["feature_dictionary"]
    instruments = footer["instrument_dictionary"]
    df = df.with_columns(
        pl.col("feature_id").replace_strict(dict(enumerate(features))).alias("feature"),
        pl.col("instrument_id").replace_strict(dict(enumerate(instruments))).alias("instrument"),
    ).sort("event_ts_us")
    emitted = set(df["feature"].unique())
    empty = [name for name in features if name not in emitted]
    return df, footer, empty


def feature_series(long, name):
    """One feature as (event_ts_us, value) sorted for as-of joins."""
    out = long.filter(pl.col("feature") == name).select("event_ts_us", pl.col("value").alias(name))
    if out.is_empty():
        raise KeyError(f"feature {name!r} was never emitted in this capture")
    return out.sort("event_ts_us")


def pivot_wide(long, footer):
    """One row per event_ts_us, one column per instrument+feature, footer-dictionary
    order, hyphens to underscores. aggregate_function=None makes polars RAISE on a
    (tick, instrument, feature) duplicate — a duplicate-emission bug must fail loudly."""
    observed = set(long.select("instrument", "feature").unique().rows())
    columns = [
        f"{instrument.replace('-', '_')}_{feature}"
        for instrument in footer["instrument_dictionary"]
        for feature in footer["feature_dictionary"]
        if (instrument, feature) in observed
    ]
    wide = long.with_columns(
        pl.from_epoch("event_ts_us", time_unit="us").alias("event_ts"),
        (pl.col("instrument").str.replace_all("-", "_") + "_" + pl.col("feature")).alias("column"),
    ).pivot("column", index="event_ts", values="value", aggregate_function=None)
    return wide.select("event_ts", *columns).sort("event_ts")
