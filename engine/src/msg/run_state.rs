//! Whether an engine trades or is parked, in one type that the link, the venues, the hot thread, and
//! the shutdown latch all read.

/// Trading or parked (assertion vs report = one comparison).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunState {
    Running,
    Idle,
}
