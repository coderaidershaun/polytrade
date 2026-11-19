"""The errors of the package, each with the failure class that the command line acts on."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import ClassVar

__all__ = [
    "AttributionRemainder",
    "AuditBreak",
    "ContractMismatch",
    "FailureClass",
    "FileVanished",
    "Finding",
    "FindingKind",
    "HoldoutRefused",
    "OutputRefused",
    "PosttradeError",
    "RefoldDiff",
    "RunAmbiguous",
    "RunsUnpaired",
    "TapeGapped",
    "TruncatedFile",
]


class FailureClass(Enum):
    """What the supervisor of a command does after an error of this package."""

    HALT = "halt"
    RESET = "reset"
    RETRY = "retry"
    REJECT = "reject"


class PosttradeError(Exception):
    """Base of each error that this package raises."""

    failure_class: ClassVar[FailureClass]
    exit_code: ClassVar[int]


class ContractMismatch(PosttradeError):
    """A file that the engine wrote, or the contract itself, differs from the contract."""

    failure_class = FailureClass.REJECT
    exit_code = 2

    def __init__(self, file: Path, field: str, seen: object, expected: object) -> None:
        self.file = file
        self.field = field
        self.seen = seen
        self.expected = expected
        super().__init__(f"{file}: {field} is {seen!r}, the contract expects {expected}")


class HoldoutRefused(PosttradeError):
    """A path resolves below the holdout root, which no command of the tooling may read."""

    failure_class = FailureClass.HALT
    exit_code = 3

    def __init__(self, path: Path, holdout_root: Path) -> None:
        self.path = path
        self.holdout_root = holdout_root
        super().__init__(f"{path} resolves below the holdout root {holdout_root}: refused")


class FileVanished(PosttradeError):
    """A listed file was gone when it was read: an engine's retention removed it, or a person did.

    The command lists the run again and reads it again. Past the last try, the command halts.
    """

    failure_class = FailureClass.RETRY
    exit_code = 1

    def __init__(self, path: Path, tries: int = 1) -> None:
        self.path = path
        self.tries = tries
        super().__init__(f"{path} vanished while it was read, on each of {tries} tries")


class OutputRefused(PosttradeError):
    """An output path lies where the tooling must not write: a run folder, the holdout, or code."""

    failure_class = FailureClass.REJECT
    exit_code = 2

    def __init__(self, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"refused to write to {path}: {reason}")


class FindingKind(Enum):
    ORDER_CHAIN = "order_chain"
    ORDER_TOTALS = "order_totals"
    LEDGER = "ledger"
    DUPLICATE_FEATURE = "duplicate_feature"
    RECORDS_LOST = "records_lost"
    TAPE_BOOT = "tape_boot"
    TAPE_SEQUENCE = "tape_sequence"
    TAPE_STAMP = "tape_stamp"


@dataclass(frozen=True, slots=True)
class Finding:
    """One place where the records of a run disagree; `row` counts from 1 in file order."""

    kind: FindingKind
    table: str
    row: int
    client_order_id: int | None
    detail: str

    def __str__(self) -> str:
        order = "" if self.client_order_id is None else f", client order {self.client_order_id}"
        return f"finding {self.kind.value}: {self.table} row {self.row}{order}: {self.detail}"


class AuditBreak(PosttradeError):
    """The records of a run disagree, so no measure of the run may be shown."""

    failure_class = FailureClass.HALT
    exit_code = 1

    def __init__(self, run_dir: Path, findings: Sequence[Finding], records_dropped: int) -> None:
        self.run_dir = run_dir
        self.findings = tuple(findings)
        self.records_dropped = records_dropped
        dropped = f"; records dropped: {records_dropped}" if records_dropped > 0 else ""
        super().__init__(
            f"{run_dir}: {len(self.findings)} findings{dropped}; P&L withheld, report FAILED"
        )


class AttributionRemainder(PosttradeError):
    """The parts of the P&L do not sum to the ledger's session P&L, so the parts are withheld."""

    failure_class = FailureClass.HALT
    exit_code = 1

    def __init__(self, run_dir: Path, remainder: int) -> None:
        self.run_dir = run_dir
        self.remainder = remainder
        super().__init__(
            f"{run_dir}: the P&L parts leave a remainder {remainder} against the ledger's "
            "session P&L; attribution withheld, report FAILED"
        )


class RunsUnpaired(PosttradeError):
    """Two runs that a comparison was given are not a run and its re-fold, by their manifests."""

    failure_class = FailureClass.REJECT
    exit_code = 2

    def __init__(self, run_dir: Path, replay_dir: Path, reason: str) -> None:
        self.run_dir = run_dir
        self.replay_dir = replay_dir
        self.reason = reason
        super().__init__(f"{replay_dir} is no re-fold of {run_dir}: {reason}")


class TapeGapped(PosttradeError):
    """The tape of a run lost records, so no re-fold of the run exists to compare with it."""

    failure_class = FailureClass.HALT
    exit_code = 1

    def __init__(self, run_dir: Path, first_lost_seq: int, last_lost_seq: int) -> None:
        self.run_dir = run_dir
        self.first_lost_seq = first_lost_seq
        self.last_lost_seq = last_lost_seq
        super().__init__(
            f"{run_dir}: the tape lost seq {first_lost_seq} to {last_lost_seq}, and a re-fold "
            "never crosses a gap; comparison FAILED"
        )


class RefoldDiff(PosttradeError):
    """A run and its re-fold differ: a determinism breach, or look-ahead in a feature.

    `row` counts from 1 in the run's file order. `column` is None for a row that one side holds and
    the other does not.
    """

    failure_class = FailureClass.HALT
    exit_code = 1

    def __init__(
        self,
        table: str,
        row: int,
        column: str | None,
        run_value: object,
        replay_value: object,
    ) -> None:
        self.table = table
        self.row = row
        self.column = column
        self.run_value = run_value
        self.replay_value = replay_value
        cell = f"row {row}" if column is None else f"row {row} column {column}"
        super().__init__(
            f"table {table} {cell}: the run holds {run_value!r}, the re-fold holds "
            f"{replay_value!r}; comparison FAILED"
        )


class TruncatedFile(PosttradeError):
    """A sealed name holds no readable file: a crash or a copy cut it after its rename."""

    failure_class = FailureClass.REJECT
    exit_code = 2

    def __init__(self, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"{path} is truncated, its rows are lost: {reason}")


class RunAmbiguous(PosttradeError):
    """The reader cannot tell what state a run is in, so it reads none of it."""

    failure_class = FailureClass.REJECT
    exit_code = 2

    def __init__(self, manifest: Path, reason: str) -> None:
        self.manifest = manifest
        self.reason = reason
        super().__init__(f"{manifest}: the run is ambiguous: {reason}")
