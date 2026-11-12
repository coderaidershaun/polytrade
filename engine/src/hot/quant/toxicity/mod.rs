//! Adverse selection w/o private fills: public print reaching quoted level => pseudo-execution.
//! [`markouts`] = pseudo-fill machinery. [`vpin`] = volume-clock order-flow imbalance.

mod markouts;
mod vpin;

pub use markouts::{MarkoutFill, MarkoutSide, MarkoutTracker, SideMarkouts};
pub use vpin::{VpinEstimate, vpin};

use crate::time::DurationUs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarkoutSpec {
    pub spin_interval: DurationUs,
    /// Max mids/sec: book commits + spins.
    pub max_mids_per_sec: u32,
}

/// Post-fill horizons. Ordering load-bearing: [`ForwardHorizon::ALL`] indexed by discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForwardHorizon {
    Secs1,
    Secs3,
    Secs5,
    Secs10,
    Secs30,
    Secs60,
}

impl ForwardHorizon {
    pub const ALL: [ForwardHorizon; 6] = [
        ForwardHorizon::Secs1,
        ForwardHorizon::Secs3,
        ForwardHorizon::Secs5,
        ForwardHorizon::Secs10,
        ForwardHorizon::Secs30,
        ForwardHorizon::Secs60,
    ];

    pub const fn duration(self) -> DurationUs {
        DurationUs::from_secs(match self {
            ForwardHorizon::Secs1 => 1,
            ForwardHorizon::Secs3 => 3,
            ForwardHorizon::Secs5 => 5,
            ForwardHorizon::Secs10 => 10,
            ForwardHorizon::Secs30 => 30,
            ForwardHorizon::Secs60 => 60,
        })
    }

    #[inline]
    const fn index(self) -> usize {
        self as usize
    }
}

/// Pre-fill horizons. Ordering load-bearing: [`ReverseHorizon::ALL`] indexed by discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReverseHorizon {
    Secs1,
    Secs5,
}

impl ReverseHorizon {
    pub const ALL: [ReverseHorizon; 2] = [ReverseHorizon::Secs1, ReverseHorizon::Secs5];

    pub const fn duration(self) -> DurationUs {
        DurationUs::from_secs(match self {
            ReverseHorizon::Secs1 => 1,
            ReverseHorizon::Secs5 => 5,
        })
    }

    #[inline]
    const fn index(self) -> usize {
        self as usize
    }
}
