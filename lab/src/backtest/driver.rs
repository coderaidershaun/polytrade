//! Feeds recorded messages one at a time into the fold, as the hot thread's driver does, and
//! empties each command lane after each message, as a live venue edge empties it.

use engine::hot::dispatch::{HotEngine, StateDigest};
use engine::msg::exec::{ExecLaneItem, StampedExecCommand};
use engine::msg::inbound::InboundMessage;
use rtrb::Consumer;

/// The fold of a replay and the read end of each command lane it writes, in venue order.
pub struct ReplayDriver {
    engine: HotEngine,
    commands: Vec<Consumer<ExecLaneItem>>,
    folded: u64,
}

impl ReplayDriver {
    pub fn new(engine: HotEngine, commands: Vec<Consumer<ExecLaneItem>>) -> ReplayDriver {
        ReplayDriver {
            engine,
            commands,
            folded: 0,
        }
    }

    /// Folds `message`, then moves each command of each lane, in venue order, into `sent`, and drops
    /// the watermarks.
    ///
    /// The lanes must be emptied here: a full lane keeps its commands in the bank, and a full bank
    /// halts quoting, a change of state that a live edge never causes.
    pub fn step(&mut self, message: &InboundMessage, sent: &mut Vec<StampedExecCommand>) {
        self.engine.dispatch(message);
        self.folded += 1;
        for lane in &mut self.commands {
            while let Ok(item) = lane.pop() {
                if let ExecLaneItem::Command(command) = item {
                    sent.push(command);
                }
            }
        }
    }

    /// How many messages this driver has folded.
    pub fn folded(&self) -> u64 {
        self.folded
    }

    pub fn state_digest(&self) -> StateDigest {
        self.engine.state_digest()
    }

    pub fn engine(&self) -> &HotEngine {
        &self.engine
    }
}
