//! What the engine does after an error, in one vocabulary, so each error type names its reaction in
//! code and the one supervisor of a thread acts on it.

/// What the supervisor of a thread does with an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureClass {
    Halt(Halt),
    /// Throw away the state of one part and build it again from a trusted source.
    Reset,
    /// Do the same action again after a delay, a limited number of times.
    Retry,
    /// Refuse this one input, count it, and carry on.
    Reject,
}

/// What a halt stops. Each keeps its state for a person to read, and only a person starts it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Halt {
    /// The fatal latch trips: the engine drains inside its deadline and exits with a code that is
    /// not 0.
    Engine,
    /// Quoting stops and the engine sweeps its own orders, while the process stays up.
    Quoting,
    /// The start is refused before the engine runs.
    Start,
}
