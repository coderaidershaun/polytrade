//! Tests of the tape: the Parquet tables that the writer thread drains, keeps, and reads back.

mod e2e_replay;
mod e2e_scenario;
mod persist_exec;
mod persist_retention;
mod persist_tables;
mod seal_marker;
mod tape_files;
mod writer_fault;
