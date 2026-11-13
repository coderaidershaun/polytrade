//! Banked execution commands and progress watermarks for the edge. Each command keeps the latency
//! tick of the moment it entered the bank, so a wait in the bank shows in its stage.

use rtrb::Producer;

use crate::hot::digest::{Digest, Digester, digest_fields};
use crate::msg::exec::{ExecCommand, ExecLaneItem, StampedExecCommand};
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::time::{DurationUs, LocalTs, Stamper, Tick};

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
    /// The latency counter that the bank reads when a command enters it.
    stamper: Stamper,
}

impl ExecSink {
    /// A full lane keeps each command in the bank, in order, for the next spin.
    pub const WHEN_FULL: FailureClass = FailureClass::Retry;

    pub fn new(producer: Producer<ExecLaneItem>, stamper: Stamper) -> Self {
        Self { producer, stamper }
    }
}

/// The write end of the command lane holds no fold state, so only whether the engine has one
/// counts, through the `Option` that holds it.
impl Digest for ExecSink {
    fn digest(&self, _digester: &mut Digester) {}
}

#[derive(Debug, Clone, Copy)]
struct BankedCommand {
    command: ExecCommand,
    /// The fold time of the banking. The bank is first in, first out, so the stamps on the lane
    /// never decrease.
    banked_ts_us: LocalTs,
    issue_tick: Tick,
}

pub(crate) struct PendingCommands {
    banked: Vec<BankedCommand>,
    /// The bank is full at this many commands, never at the capacity of its `Vec`.
    capacity: usize,
    banked_total: u64,
    stamper: Stamper,
}

digest_fields! {
    PendingCommands { capacity, banked_total }
    not_state {
        // The commands that the lane has not taken yet: how many wait depends on how fast the edge
        // reads, and the latency tick of each is a clock reading.
        banked,
        // The latency clock that stamps each banked command.
        stamper,
    }
}

impl PendingCommands {
    /// A full bank halts quoting with `HaltReason::CommandBankOverflow`.
    #[expect(
        dead_code,
        reason = "the class is named beside the type, and the halt of a refused command is its one reaction"
    )]
    pub(crate) const WHEN_FULL: FailureClass = FailureClass::Halt(Halt::Quoting);

    /// The bank reads the counter of the sink it drains into. With no sink no command is ever sent,
    /// so it reads none.
    pub(crate) fn for_sink(sink: Option<&ExecSink>, capacity: usize) -> Self {
        Self {
            banked: Vec::with_capacity(capacity),
            capacity,
            banked_total: 0,
            stamper: sink.map_or(Stamper::Off, |sink| sink.stamper.clone()),
        }
    }

    /// False = bank full; caller must unwind (command not sent).
    #[inline]
    #[must_use]
    pub(crate) fn bank(&mut self, command: ExecCommand, at: LocalTs) -> bool {
        if self.banked.len() == self.capacity {
            return false;
        }
        self.banked.push(BankedCommand {
            command,
            banked_ts_us: at,
            issue_tick: self.issue_tick(),
        });
        self.banked_total += 1;
        true
    }

    /// The one counter read of the bank. It lives apart from `bank`, so a clock read added to the
    /// bank's own logic fails the clock lint.
    #[inline]
    #[expect(
        clippy::disallowed_methods,
        reason = "the command sink stamps each banked command for its latency, and the fold gets nothing back"
    )]
    fn issue_tick(&self) -> Tick {
        self.stamper.stamp()
    }

    /// Pushes FIFO and appends a watermark only when every command was accepted.
    pub(crate) fn drain_into(&mut self, sink: &mut ExecSink, at: LocalTs) {
        let items = self
            .banked
            .iter()
            .map(|banked| {
                ExecLaneItem::Command(StampedExecCommand {
                    command: banked.command,
                    issued_ts_us: banked.banked_ts_us,
                    issue_tick: banked.issue_tick,
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
