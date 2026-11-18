//! What the tape writer does when a table cannot be written: a research table stops and is counted,
//! and an audit table also tells the fold once, so quoting halts at one message of the input.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::hot::spawn::QueueProducer;
use crate::msg::failure::FailureClass;
use crate::msg::inbound::{FileWriter, InboundMessage, MessageBody, WriterFailure};
use crate::msg::time::EngineClock;

use super::PersistError;
use super::schema::TableRow;
use super::table::TableWriter;

/// Where the tape writer reports a fault of an audit table: its own input queue of the hot thread.
pub struct TapeStatus {
    pub queue: QueueProducer,
    /// The writer stamps its message with the clock of the engine, as each producer of an input
    /// queue does, so the message takes its place in the order of the input.
    pub clock: EngineClock,
}

impl TapeStatus {
    /// The first fault of an audit table halts quoting, so the count of failed writes is always 1.
    #[expect(
        clippy::disallowed_methods,
        reason = "a writer that gives up stamps its one message as each producer of an input queue does"
    )]
    pub(super) fn report(mut self) {
        let failure = WriterFailure {
            writer: FileWriter::Tape,
            consecutive_failures: 1,
        };
        let message = InboundMessage::new(self.clock.now(), MessageBody::WriterFailed(failure));
        self.queue.push(message);
    }
}

/// The count of tables that the writer stopped after a fault, readable from outside its thread.
#[derive(Debug, Clone, Default)]
pub struct StoppedTables(Arc<AtomicU32>);

impl StoppedTables {
    pub fn count(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }

    pub(super) fn add_one(&self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

/// One table of the writer thread.
pub(super) enum Table<R> {
    /// `strategy.tables` does not name it, so it has no file.
    Unnamed,
    Writing(Box<TableWriter<R>>),
    /// A fault stopped it. Its open file keeps no footer, and each later row of it is lost.
    Stopped,
}

impl<R: TableRow> Table<R> {
    /// `false` when the config does not name the table.
    pub(super) fn push(&mut self, row: R, faults: &mut Faults) -> bool {
        match self {
            Table::Unnamed => return false,
            Table::Stopped => faults.rows_lost += 1,
            Table::Writing(writer) => {
                if let Err(error) = writer.push(row) {
                    self.stop(error, faults);
                }
            }
        }
        true
    }

    /// A fault of one table still leaves each other table to seal.
    pub(super) fn seal(&mut self, faults: &mut Faults) {
        let Table::Writing(writer) = self else {
            return;
        };
        if let Err(error) = writer.seal() {
            self.stop(error, faults);
        }
    }

    #[cold]
    fn stop(&mut self, error: PersistError, faults: &mut Faults) {
        *self = Table::Stopped;
        faults.table_stopped(error);
    }
}

/// What the faults of the tables of one run come to, read at the root of the writer thread.
pub(super) struct Faults {
    status: Option<TapeStatus>,
    stopped: StoppedTables,
    /// The first fault of an audit table: the drain returns it, so the run does not end clean.
    first_halt: Option<PersistError>,
    rows_lost: u64,
}

impl Faults {
    pub(super) fn new(status: Option<TapeStatus>, stopped: StoppedTables) -> Self {
        Self {
            status,
            stopped,
            first_halt: None,
            rows_lost: 0,
        }
    }

    fn table_stopped(&mut self, error: PersistError) {
        self.stopped.add_one();
        match error.failure_class() {
            FailureClass::Reject => {
                crate::error!(
                    "persistence stopped a research table, and each later row of it is lost: {error}"
                );
            }
            // A tape error has no retry and no reset, so those two take the higher class.
            FailureClass::Halt(_) | FailureClass::Retry | FailureClass::Reset => {
                crate::error!("persistence stopped an audit table, so quoting halts: {error}");
                if let Some(status) = self.status.take() {
                    status.report();
                }
                self.first_halt.get_or_insert(error);
            }
        }
    }

    /// # Errors
    /// The first fault of an audit table.
    pub(super) fn into_result(self) -> Result<(), PersistError> {
        if self.rows_lost > 0 {
            crate::error!(
                "persistence lost {} rows of {} stopped tables",
                self.rows_lost,
                self.stopped.count()
            );
        }
        match self.first_halt {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
