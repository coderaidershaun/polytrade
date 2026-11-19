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
    pub epoch: ControllerEpoch,
}

impl RunAssertion {
    pub const INITIAL: RunAssertion = RunAssertion {
        state: RunState::Running,
        epoch: ControllerEpoch::ZERO,
    };
}

/// A controller epoch, always below 2^63: the run-state cell keeps the state in the top bit of the
/// same word, so a larger epoch would read back as the other state at another epoch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControllerEpoch(u64);

impl ControllerEpoch {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self((1 << 63) - 1);

    /// For an epoch that code makes, never for one that arrives: that one goes through `TryFrom`.
    ///
    /// # Panics
    /// At 2^63 or above.
    pub const fn new(epoch: u64) -> Self {
        assert!(epoch <= Self::MAX.0, "a controller epoch is below 2^63");
        Self(epoch)
    }

    /// The epoch, or [`ControllerEpoch::MAX`] for one at 2^63 or above.
    pub const fn saturating(epoch: u64) -> Self {
        if epoch > Self::MAX.0 { Self::MAX } else { Self(epoch) }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The epoch one above, or `None` at [`ControllerEpoch::MAX`].
    pub const fn next(self) -> Option<Self> {
        if self.0 == Self::MAX.0 { None } else { Some(Self(self.0 + 1)) }
    }
}

impl TryFrom<u64> for ControllerEpoch {
    type Error = EpochOutOfRange;

    fn try_from(epoch: u64) -> Result<Self, EpochOutOfRange> {
        if epoch > Self::MAX.0 {
            return Err(EpochOutOfRange { found: epoch });
        }
        Ok(Self(epoch))
    }
}

impl From<ControllerEpoch> for u64 {
    fn from(epoch: ControllerEpoch) -> u64 {
        epoch.0
    }
}

impl std::fmt::Display for ControllerEpoch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[error("controller epoch {found} is 2^63 or above")]
pub struct EpochOutOfRange {
    pub found: u64,
}
