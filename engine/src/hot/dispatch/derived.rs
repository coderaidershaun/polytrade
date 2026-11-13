//! What the fold derives from market data and can build again from it: the trackers and the EWMA
//! volatility of each market. A gap in the data makes them wrong, so a resume throws them away.

use crate::hot::digest::digest_fields;
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::tracker::MicroTracker;

/// The derived state of each market, indexed by instrument. Books and money are not derived: an
/// edge rebuilds a book from a snapshot of the venue, and a park keeps the position. A strategy
/// keeps the state that it derives for each market itself, and its resume callback resets it.
pub(super) struct Derived {
    pub(super) trackers: Vec<MicroTracker>,
    pub(super) ewma: Vec<Option<EwmaVol>>,
}

digest_fields! { Derived { trackers, ewma } }

impl Derived {
    /// The one reset for a gap in the data: each tracker and each EWMA back to its cold value, in
    /// place, so a resume allocates nothing.
    #[cold]
    pub(super) fn reset_for_gap(&mut self) {
        for tracker in &mut self.trackers {
            tracker.on_rotation();
        }
        for ewma in self.ewma.iter_mut().flatten() {
            ewma.reset();
        }
    }
}
