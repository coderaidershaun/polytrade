//! The waits and bounds that this venue imposes on its edge, in one value, so that the edge and a
//! simulator of the venue read the same numbers.

use crate::msg::time::DurationUs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolymarketFacts {
    /// How long the venue holds a marketable order before it matches. It refuses a cancel of the
    /// order until then, and that refusal counts against the reject streak.
    pub taker_hold: DurationUs,
    /// Added to the taker hold before a withheld cancel goes out, for the answer and the cancel on
    /// the wire.
    pub wire_margin: DurationUs,
    /// Stream frames held for an order that the venue has not named yet. The wait is one HTTP round
    /// trip, so more frames than this means a storm at the venue.
    pub held_frame_capacity: usize,
    /// Long enough for a placement answer and a resync, short enough to report a fill that no
    /// mapping explains while it can still be acted on.
    pub held_frame_lifetime: DurationUs,
}

pub const POLYMARKET_FACTS: PolymarketFacts = PolymarketFacts {
    taker_hold: DurationUs::from_millis(250),
    wire_margin: DurationUs::from_millis(50),
    held_frame_capacity: 128,
    held_frame_lifetime: DurationUs::from_secs(10),
};
