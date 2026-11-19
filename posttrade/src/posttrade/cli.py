"""The command line of the tooling: it composes the boxes and turns each error into an exit code.

Exit codes: 0 the checks passed; 1 an audit break, a re-fold that differs from its run, or a file
that kept vanishing; 2 a file, an output path or a pair of runs that the contract refuses, or a
wrong argument; 3 a path into the holdout slice.
"""

import argparse
import random
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, TextIO

import polars as pl

from posttrade import audit, compare, contract, report, runs, tables
from posttrade.audit import FeatureHealth
from posttrade.contract import Contract
from posttrade.errors import (
    AttributionRemainder,
    AuditBreak,
    FileVanished,
    Finding,
    PosttradeError,
)
from posttrade.metrics import fills, latency, orders, pnl, risk
from posttrade.runs import UnfinishedState

__all__ = ["CommandEnv", "main", "run"]

RETRY_WAITS_S: Final = (0.2, 2.0)
RETRY_JITTER: Final = 0.1
DEFAULT_REPORTS: Final = Path("data") / "reports"


@dataclass(frozen=True, slots=True)
class CommandEnv:
    """What a command reaches outside itself; tests pass their own."""

    out: TextIO = field(default_factory=lambda: sys.stdout)
    err: TextIO = field(default_factory=lambda: sys.stderr)
    load_contract: Callable[[], Contract] = contract.load_repo_contract
    open_run: Callable[[Path, Contract], runs.Run] = runs.open_run
    sleep: Callable[[float], None] = time.sleep


def run() -> None:
    sys.exit(main(sys.argv[1:]))


def main(argv: Sequence[str], env: CommandEnv | None = None) -> int:
    """Runs one command; the supervisor of the package: lower code only raises.

    Errors that reach here: `HoldoutRefused` halts with exit 3; `AuditBreak` prints its findings,
    withholds each measure and exits 1; `FileVanished` reaches here only past its last try and
    halts with exit 1; `RefoldDiff` and `TapeGapped` halt the comparison with exit 1;
    `ContractMismatch`, `TruncatedFile`, `RunAmbiguous`, `OutputRefused` and `RunsUnpaired`
    refuse with exit 2.
    """
    env = env or CommandEnv()
    args = _parser().parse_args(argv)
    try:
        terms = env.load_contract()
        if args.command == "runs":
            return _runs(Path(args.root), terms, env)
        if args.command == "timeline":
            return _timeline(Path(args.run), args.start_us, args.end_us, terms, env)
        if args.command == "export":
            return _export(Path(args.run), args.table, Path(args.out), terms, env)
        if args.command == "compare":
            return _retrying(
                lambda: _compare(Path(args.run), Path(args.replay), terms, env), args.seed, env
            )
        if args.command == "report":
            out = terms.root / DEFAULT_REPORTS if args.out is None else Path(args.out)
            return _retrying(
                lambda: _report(Path(args.run), out, args.seed, terms, env), args.seed, env
            )
        return _retrying(lambda: _check(Path(args.run), terms, env), args.seed, env)
    except AuditBreak as error:
        for finding in error.findings:
            print(finding, file=env.out)
        print(f"FAILED: {error}", file=env.out)
        return error.exit_code
    except PosttradeError as error:
        print(f"posttrade: {error}", file=env.err)
        return error.exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="posttrade", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("runs", help="list each run below a folder, with its files")
    listing.add_argument("root", help="a folder that holds run folders, or one run folder")
    check = commands.add_parser("check", help="audit the records of one run")
    check.add_argument("run", help="the run folder, which holds run.json")
    check.add_argument("--seed", type=int, default=0, help="seed of the waits between tries")
    compared = commands.add_parser("compare", help="compare a run with its re-fold")
    compared.add_argument("run", help="the run folder of the source run")
    compared.add_argument("replay", help="the run folder of its re-fold, of kind replay")
    compared.add_argument(
        "--mode",
        choices=[mode.value for mode in compare.CompareMode],
        default=compare.CompareMode.EXACT.value,
        help="exact: each table cell for cell, research rows the run dropped taken by count",
    )
    compared.add_argument("--seed", type=int, default=0, help="seed of the waits between tries")
    window = commands.add_parser("timeline", help="print each row of a time window as one list")
    window.add_argument("run", help="the run folder, which holds run.json")
    window.add_argument("--from", dest="start_us", type=int, required=True, help="first stamp, µs")
    window.add_argument("--to", dest="end_us", type=int, required=True, help="last stamp, µs")
    export = commands.add_parser("export", help="write one table of a run as a CSV file")
    export.add_argument("run", help="the run folder, which holds run.json")
    export.add_argument("table", help="a table of the contract")
    export.add_argument("--out", required=True, help="output folder, outside each run and code")
    written = commands.add_parser("report", help="check a run, then write its HTML report")
    written.add_argument("run", help="the run folder, which holds run.json")
    written.add_argument("--out", help=f"output folder, default {DEFAULT_REPORTS} of the repo")
    written.add_argument("--seed", type=int, default=0, help="seed of the bands and the waits")
    return parser


def _runs(root: Path, terms: Contract, env: CommandEnv) -> int:
    for found in runs.find_runs(root, terms):
        manifest = found.manifest
        files = sum(len(paths) for paths in found.sealed_files.values())
        print(
            f"{found.dir}: {manifest.kind.value} run {manifest.run_id}, end state "
            f"{manifest.end_state.value}, {files} sealed files in {len(found.sealed_files)} tables",
            file=env.out,
        )
        _print_unfinished(found, env)
    return 0


def _retrying(command: Callable[[], int], seed: int, env: CommandEnv) -> int:
    """Runs the command again, from a new listing, while a listed file vanishes before its read.

    Reading is safe to repeat: a command writes nothing until each read is done.
    """
    jitter = random.Random(seed)
    for try_number, wait in enumerate(RETRY_WAITS_S, start=1):
        try:
            return command()
        except FileVanished as error:
            delay = wait * (1 + jitter.uniform(0, RETRY_JITTER))
            print(
                f"{error.path} vanished while it was read; listing again in {delay:.3f} s "
                f"(try {try_number + 1} of {len(RETRY_WAITS_S) + 1}, seed {seed})",
                file=env.err,
            )
            env.sleep(delay)
    try:
        return command()
    except FileVanished as error:
        raise FileVanished(error.path, tries=len(RETRY_WAITS_S) + 1) from error


def _check(run_dir: Path, terms: Contract, env: CommandEnv) -> int:
    opened = env.open_run(run_dir, terms)
    findings, _ = _audited(opened, env)
    if findings:
        raise AuditBreak(opened.dir, findings, records_dropped=_audit_rows_lost(opened))
    print(f"PASSED: {opened.dir}", file=env.out)
    return 0


def _compare(run_dir: Path, replay_dir: Path, terms: Contract, env: CommandEnv) -> int:
    opened = env.open_run(run_dir, terms)
    replay = env.open_run(replay_dir, terms)
    matched = compare.compare_exact(opened, replay)
    print(f"PASSED: {replay.dir} equals {opened.dir}: {matched.summary()}", file=env.out)
    return 0


def _report(run_dir: Path, out: Path, seed: int, terms: Contract, env: CommandEnv) -> int:
    """The checks run first; a run whose records disagree gets a FAILED report with no measure."""
    opened = env.open_run(run_dir, terms)
    report.report_folder(opened, out)
    findings, health = _audited(opened, env)
    if findings:
        written = report.write_report(opened, out, [str(f) for f in findings], health, {}, None)
        print(f"wrote {written.html} and {written.summary}", file=env.out)
        raise AuditBreak(opened.dir, findings, records_dropped=_audit_rows_lost(opened))
    fill_measures = fills.measure(opened, fills.FillParams(seed=seed))
    money = pnl.measure(opened)
    measured = {
        **fill_measures.metrics,
        **money.metrics,
        **risk.measure(opened),
        **orders.measure(opened),
        **latency.measure(opened),
    }
    remainder = AttributionRemainder(opened.dir, money.remainder) if money.remainder else None
    if remainder is not None:
        measured = {k: v for k, v in measured.items() if k not in pnl.ATTRIBUTION_PARTS}
    failures = [] if remainder is None else [str(remainder)]
    notes = [] if money.withheld is None else [money.withheld]
    written = report.write_report(
        opened, out, failures, health, measured, fill_measures, notes=notes
    )
    print(f"wrote {written.html} and {written.summary}", file=env.out)
    if remainder is not None:
        raise remainder
    print(f"PASSED: {opened.dir}", file=env.out)
    return 0


def _audited(opened: runs.Run, env: CommandEnv) -> tuple[tuple[Finding, ...], FeatureHealth]:
    _print_unfinished(opened, env)
    tape = audit.tape_health(opened)
    print(tape.summary(), file=env.out)
    health = audit.feature_health(opened)
    if health.never_emitted:
        print(f"features never emitted: {', '.join(health.never_emitted)}", file=env.out)
    return (*tape.findings, *audit.check_orders(opened), *health.findings), health


def _timeline(run_dir: Path, start_us: int, end_us: int, terms: Contract, env: CommandEnv) -> int:
    opened = env.open_run(run_dir, terms)
    for at, table, text in tables.timeline(opened, start_us, end_us):
        print(f"{at} {table} {text}", file=env.out)
    return 0


def _export(run_dir: Path, table: str, out: Path, terms: Contract, env: CommandEnv) -> int:
    opened = env.open_run(run_dir, terms)
    report.refuse_output(out, terms)
    frame = tables.as_text(tables.read_table(opened, table))
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"{opened.manifest.run_id}-{table}.csv"
    frame.write_csv(target)
    print(f"wrote {frame.height} rows to {target}", file=env.out)
    return 0


def _print_unfinished(opened: runs.Run, env: CommandEnv) -> None:
    for unfinished in opened.unfinished:
        lost = "" if unfinished.state is UnfinishedState.OPEN else ": its rows are lost"
        print(f"{unfinished.state.value} file {unfinished.path}{lost}", file=env.out)


def _audit_rows_lost(opened: runs.Run) -> int:
    """The audit rows that the engine dropped at a full bank: each is a break the audit can see."""
    drops = tables.read_table(opened, "metrics_drops").filter(pl.col("sink") == "audit_lost")
    largest = drops.get_column("dropped_total").max()
    return largest if isinstance(largest, int) else 0
