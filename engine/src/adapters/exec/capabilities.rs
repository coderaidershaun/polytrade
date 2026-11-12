//! What a venue physically does, stated once per venue instead of being rediscovered at each place
//! that has to care. Everything here is a fact about the exchange — never a number an operator
//! tunes — so the engine and the config gates can read it without naming a venue themselves.

use crate::hot::exec::{FeeModel, OrderBudget};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VenueCapabilities {
    pub(crate) holds_reservations_until_settled: bool,
    pub(crate) fee_model: FeeModel,
    /// How many placements the venue grants an account, over what windows. `NONE` where the venue
    /// meters requests per endpoint instead — a transport concern its own lanes carry.
    pub(crate) order_budget: OrderBudget,
    pub(crate) rotates_markets: bool,
    /// The base asset IS the position rather than a currency held beside it, which is what makes a
    /// non-zero floor under it subtract from every exit instead of protecting a reserve.
    pub(crate) base_asset_is_position: bool,
}
