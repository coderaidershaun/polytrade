//! Banked execution commands and progress watermarks for the edge.

use rtrb::Producer;

use crate::msg::exec::{ExecCommand, ExecLaneItem, StampedExecCommand};
use crate::time::{DurationUs, TsUs};

use super::order::{MAX_ORDER_INSTRUMENTS, MAX_ORDER_SLOTS};

/// Commands one spin can produce for an instrument beyond the order table itself: an open-orders
/// request, a flatten, one action per side, the sweep a halt issues, and a prior-run cancel.
const COMMANDS_PER_INSTRUMENT_PER_SPIN: usize = 6;

/// Sized so a single spin can never fill it. Every producer banks at most one command per order
/// slot — a timeout chases a slot, a closing window pulls it, and no slot can be both at once —
/// plus [`COMMANDS_PER_INSTRUMENT_PER_SPIN`]. Anything smaller turns an ordinary busy spin, such as
/// a window closing on a full ladder, into a halt.
///
/// Anything larger buys nothing. Past one spin the bank holds whatever the lane below refused, so
/// an edge that stays stalled fills any bank there is — and THAT is the designed capacity event:
/// the engine halts rather than dropping a command whose state transition it has already applied.
pub(crate) const COMMAND_BANK_CAPACITY: usize =
    MAX_ORDER_SLOTS + COMMANDS_PER_INSTRUMENT_PER_SPIN * MAX_ORDER_INSTRUMENTS;

const COMMAND_SLOTS: usize = 1_024;

/// The two spans that decide how deep the lane to the edge has to be. Separate fields rather than
/// two arguments because both are [`DurationUs`] and transposing them at the call site compiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecLaneBudget {
    pub spin_interval: DurationUs,
    pub edge_stall: DurationUs,
}

/// Includes one watermark per spin through the longest edge stall.
pub fn exec_lane_capacity(budget: ExecLaneBudget) -> usize {
    let spin_us = budget.spin_interval.micros().max(1) as u64;
    let stall_us = budget.edge_stall.micros().max(0) as u64;
    COMMAND_SLOTS + stall_us.div_ceil(spin_us) as usize
}

pub struct ExecSink {
    producer: Producer<ExecLaneItem>,
}

impl ExecSink {
    pub fn new(producer: Producer<ExecLaneItem>) -> Self {
        Self { producer }
    }
}

pub(crate) struct PendingCommands {
    banked: Vec<ExecCommand>,
    banked_total: u64,
}

impl PendingCommands {
    pub(crate) fn new() -> Self {
        Self {
            banked: Vec::with_capacity(COMMAND_BANK_CAPACITY),
            banked_total: 0,
        }
    }

    /// False = bank full; caller must unwind (command not sent).
    #[inline]
    #[must_use]
    pub(crate) fn bank(&mut self, command: ExecCommand) -> bool {
        if self.banked.len() == self.banked.capacity() {
            return false;
        }
        self.banked.push(command);
        self.banked_total += 1;
        true
    }

    /// Pushes FIFO and appends a watermark only when every command was accepted.
    pub(crate) fn drain_into(&mut self, sink: &mut ExecSink, at: TsUs) {
        let items = self
            .banked
            .iter()
            .map(|command| {
                ExecLaneItem::Command(StampedExecCommand {
                    command: *command,
                    issued_ts_us: at,
                })
            })
            .chain(std::iter::once(ExecLaneItem::Watermark(at)));
        let pushed = items
            .take_while(|item| sink.producer.push(*item).is_ok())
            .count();
        let sent_commands = pushed.min(self.banked.len());
        self.banked.drain(..sent_commands);
    }

    #[inline]
    pub(crate) fn banked_total(&self) -> u64 {
        self.banked_total
    }
}
