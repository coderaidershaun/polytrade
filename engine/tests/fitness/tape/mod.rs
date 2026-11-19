//! Tests of the tape: the Parquet tables that the writer thread drains, keeps, and reads back.

mod e2e_replay;
mod e2e_scenario;
mod golden_run;
mod persist_exec;
mod persist_retention;
mod persist_tables;
mod seal_marker;
mod tape_files;
mod tape_kinds;
mod tape_rows;
mod tape_sync;
mod writer_fault;
