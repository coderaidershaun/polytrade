"""Judges trading runs from the files that they record.

Priorities, in order: correctness, self-evident architecture,
adaptation (new measures, venues, run kinds).
Never: read the holdout slice.
Never: write into a run folder or reach an engine.
Never: compute money with floats.
"""

from posttrade import audit, cli, contract, metrics, runs, tables

__all__ = ["audit", "cli", "contract", "metrics", "runs", "tables"]
