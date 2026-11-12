//! Whether an engine trades or is parked, and the controller assertion that sets it. The link, the
//! venues, the hot thread, and the shutdown latch all read these types.

/// Trading or parked (assertion vs report = one comparison).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunState {
    Running,
    Idle,
}

/// The run state paired with a controller epoch. The highest epoch wins, so a stale or duplicate
/// controller cannot make the state oscillate. Epoch 0 means no controller yet; controller epochs
/// start at 1 and rise on each restart, stamped from the boot time in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunAssertion {
    pub state: RunState,
    pub epoch: u64,
}

impl RunAssertion {
    pub const INITIAL: RunAssertion = RunAssertion {
        state: RunState::Running,
        epoch: 0,
    };
}
