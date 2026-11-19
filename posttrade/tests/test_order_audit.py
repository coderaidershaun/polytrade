"""An order row lost from its chain is found, and the check withholds the P&L of the run."""

import io
from pathlib import Path

import polars as pl

from posttrade import audit, cli, contract, runs
from posttrade.errors import FindingKind

from conftest import FIRST_BOOT, golden_dir, rewrite, table_file

CHAIN_OF_CLIENT_7 = [
    ("placed", "free", "pending_new", 0, 0),
    ("ack_placed", "pending_new", "live", 0, 0),
    ("report_trade", "live", "live", 100_000_000, 10_000_000_000),
    ("report_canceled", "live", "closed_canceled", 100_000_000, 10_000_000_000),
]
CHAIN_START_US = 1_784_000_050_000_000
UNIT = 100_000_000


def with_chain_of_client_7(orders: pl.DataFrame) -> pl.DataFrame:
    template = orders.filter(pl.col("client_order_id") == 1).head(1)
    rows = [
        template.with_columns(
            pl.lit(7, dtype=pl.UInt64).alias("client_order_id"),
            pl.lit(CHAIN_START_US + step * 10_000).alias("received_ts_us"),
            pl.lit(transition).alias("transition"),
            pl.lit(previous).alias("previous_state"),
            pl.lit(state).alias("state"),
            pl.lit(200_000_000, dtype=pl.Int64).alias("qty"),
            pl.lit(filled, dtype=pl.Int64).alias("filled_qty"),
            pl.lit(quote, dtype=pl.Int64).alias("filled_quote"),
        )
        for step, (transition, previous, state, filled, quote) in enumerate(CHAIN_OF_CLIENT_7)
    ]
    return pl.concat([orders, *rows])


def with_fill_of_client_7(fills: pl.DataFrame) -> pl.DataFrame:
    fill = fills.filter(pl.col("client_order_id") == 1).with_columns(
        pl.lit(7, dtype=pl.UInt64).alias("client_order_id"),
        pl.lit(CHAIN_START_US + 20_000).alias("received_ts_us"),
        pl.lit(10_000_000_000).alias("last_price"),
        pl.lit(10_000_000_000).alias("booked_quote"),
        pl.lit(0, dtype=pl.Int64).alias("commission"),
    )
    return pl.concat([fills, fill])


def with_ledger_row_of_client_7(ledger: pl.DataFrame) -> pl.DataFrame:
    row = (
        ledger.filter(pl.col("kind") == "fill")
        .head(1)
        .with_columns(
            pl.lit(CHAIN_START_US + 20_000).alias("received_ts_us"),
            pl.lit(100_000_000, dtype=pl.Int64).alias("position_base"),
            pl.lit(-10_000_000_000).alias("cash_quote"),
            pl.lit(7, dtype=pl.UInt64).alias("client_order_id"),
        )
    )
    return pl.concat([ledger, row])


def without_row(transition: str, client: int) -> "pl.Expr":
    return ~((pl.col("client_order_id") == client) & (pl.col("transition") == transition))


def check(run_dir: Path) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.main(["check", str(run_dir)], cli.CommandEnv(out=out, err=out))
    return code, out.getvalue()


def test_broken_chain_withholds_pnl(first_boot_copy: Path) -> None:
    rewrite(table_file(first_boot_copy, "orders"), change_frame=with_chain_of_client_7)
    rewrite(table_file(first_boot_copy, "fills"), change_frame=with_fill_of_client_7)
    rewrite(table_file(first_boot_copy, "ledger"), change_frame=with_ledger_row_of_client_7)
    assert check(first_boot_copy)[0] == 0

    rewrite(
        table_file(first_boot_copy, "orders"),
        change_frame=lambda frame: frame.filter(without_row("ack_placed", 7)),
    )
    code, output = check(first_boot_copy)

    assert code == 1
    (finding,) = [line for line in output.splitlines() if line.startswith("finding")]
    assert "client order 7" in finding
    assert "report_trade" in finding
    assert "previous_state 'live'" in finding
    assert "P&L withheld" in output


def test_each_lost_row_that_changed_state_gives_one_finding(first_boot_copy: Path) -> None:
    """Each row of each golden chain but the last, whose state differs from its previous state.

    A lost last row, or a lost row that kept its state, leaves a chain that is whole: no record of
    the run tells it apart from a run that never had that row.
    """
    terms = contract.load_repo_contract()
    path = table_file(first_boot_copy, "orders")
    orders = pl.read_parquet(golden_dir() / FIRST_BOOT / path.relative_to(first_boot_copy))
    chains = orders.with_row_index("row").group_by("client_order_id", maintain_order=True)
    removed = 0
    for client, chain in chains:
        for position, row in enumerate(chain.iter_rows(named=True)):
            is_last = position == chain.height - 1
            if is_last or row["state"] == row["previous_state"]:
                continue
            lost = orders.filter(pl.int_range(pl.len()) != row["row"])

            def without_the_row(_: pl.DataFrame, lost: pl.DataFrame = lost) -> pl.DataFrame:
                return lost

            rewrite(path, change_frame=without_the_row)
            findings = audit.check_orders(runs.open_run(first_boot_copy, terms))
            assert [(f.kind, f.client_order_id) for f in findings] == [
                (FindingKind.ORDER_CHAIN, client[0])
            ], row
            removed += 1
    assert removed == 12


CLIENT_9_FILL_US = 1_784_000_040_900_000  # before the interval row at 41 s, folded after it
SPIN_US = 1_784_000_041_000_000


def with_cancel_sent_before_its_fill(orders: pl.DataFrame) -> pl.DataFrame:
    """The engine's real order: a spin at 41 s sends the cancel; the fill stamped 40.9 s by the
    venue comes in after that spin, so the fold takes it after the cancel."""
    template = orders.filter(pl.col("client_order_id") == 1).head(1)
    chain = [
        ("placed", "free", "pending_new", 1_784_000_040_000_000, 0, 0),
        ("ack_placed", "pending_new", "live", 1_784_000_040_010_000, 0, 0),
        ("cancel_sent", "live", "cancel_in_flight", SPIN_US, 0, 0),
        ("report_trade", "cancel_in_flight", "closed_filled", CLIENT_9_FILL_US, UNIT, 103 * UNIT),
    ]
    rows = [
        template.with_columns(
            pl.lit(9, dtype=pl.UInt64).alias("client_order_id"),
            pl.lit(received).alias("received_ts_us"),
            pl.lit(transition).alias("transition"),
            pl.lit(previous).alias("previous_state"),
            pl.lit(state).alias("state"),
            pl.lit(filled, dtype=pl.Int64).alias("filled_qty"),
            pl.lit(quote, dtype=pl.Int64).alias("filled_quote"),
        )
        for transition, previous, state, received, filled, quote in chain
    ]
    return pl.concat([orders, *rows])


def with_fill_of_client_9(fills: pl.DataFrame) -> pl.DataFrame:
    fill = fills.filter(pl.col("client_order_id") == 1).with_columns(
        pl.lit(9, dtype=pl.UInt64).alias("client_order_id"),
        pl.lit(CLIENT_9_FILL_US).alias("received_ts_us"),
        pl.lit(103 * UNIT, dtype=pl.Int64).alias("last_price"),
        pl.lit(103 * UNIT, dtype=pl.Int64).alias("booked_quote"),
        pl.lit(0, dtype=pl.Int64).alias("commission"),
    )
    return pl.concat([fills, fill])


def with_ledger_row_after_the_interval(ledger: pl.DataFrame) -> pl.DataFrame:
    """The interval row of the spin at 41 s holds the position before the fill; the fill's own
    row follows it in the file, and the rotation row at 45 s holds both."""
    before = ledger.filter(pl.col("kind") != "rotation")
    fill_row = (
        ledger.filter(pl.col("kind") == "fill")
        .head(1)
        .with_columns(
            pl.lit(CLIENT_9_FILL_US).alias("received_ts_us"),
            pl.lit(2 * UNIT, dtype=pl.Int64).alias("position_base"),
            pl.lit(-9_700_000_000 - 103 * UNIT, dtype=pl.Int64).alias("cash_quote"),
            pl.lit(9, dtype=pl.UInt64).alias("client_order_id"),
        )
    )
    rotation = ledger.filter(pl.col("kind") == "rotation").with_columns(
        pl.lit(2 * UNIT, dtype=pl.Int64).alias("position_base"),
        pl.lit(-9_700_000_000 - 103 * UNIT, dtype=pl.Int64).alias("cash_quote"),
    )
    return pl.concat([before, fill_row, rotation])


def test_cancel_sent_before_an_earlier_stamped_fill_is_no_break(first_boot_copy: Path) -> None:
    rewrite(table_file(first_boot_copy, "orders"), change_frame=with_cancel_sent_before_its_fill)
    rewrite(table_file(first_boot_copy, "fills"), change_frame=with_fill_of_client_9)
    rewrite(table_file(first_boot_copy, "ledger"), change_frame=with_ledger_row_after_the_interval)

    code, output = check(first_boot_copy)

    assert code == 0, output
