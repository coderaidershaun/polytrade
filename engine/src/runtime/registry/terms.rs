//! A replay cannot probe a venue, so it stamps the terms that its source run recorded onto the
//! registry that its config builds.

use crate::msg::ids::{Notional, Price, Qty};
use crate::runtime::manifest::InstrumentTerms;

use super::Registry;

/// The recorded terms name other instruments than the config builds, so the replay would fold
/// another market than the run did.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error(
    "the recorded terms list {recorded} instruments and instrument {first_differing} as {found:?}, \
     but the config builds {built} instruments with {expected:?} there"
)]
pub struct TermsMismatch {
    pub recorded: usize,
    pub built: usize,
    pub first_differing: usize,
    pub found: Option<String>,
    pub expected: Option<String>,
}

impl Registry {
    /// Gives each instrument the terms that the source run read from its venue.
    ///
    /// # Errors
    /// [`TermsMismatch`] when the recorded instruments differ in count, symbol, venue or assets
    /// from those of the registry. The registry is then unchanged.
    pub fn stamp_terms(&mut self, terms: &[InstrumentTerms]) -> Result<(), TermsMismatch> {
        let built = InstrumentTerms::of(self);
        let differing = (0..built.len().max(terms.len())).find(|&index| {
            match (built.get(index), terms.get(index)) {
                (Some(ours), Some(theirs)) => !is_same_instrument(ours, theirs),
                _ => true,
            }
        });
        if let Some(index) = differing {
            return Err(TermsMismatch {
                recorded: terms.len(),
                built: built.len(),
                first_differing: index,
                found: terms.get(index).map(|terms| terms.venue_symbol.clone()),
                expected: built.get(index).map(|terms| terms.venue_symbol.clone()),
            });
        }
        for (row, terms) in self.instruments.iter_mut().zip(terms) {
            row.tick_size = terms.tick_size.map(Price::from_mantissa);
            row.lot_size = terms.lot_size.map(Qty::from_mantissa);
            row.min_qty = terms.min_qty.map(Qty::from_mantissa);
            row.min_notional = terms.min_notional.map(Notional::from_mantissa);
            row.max_num_orders = terms.max_num_orders;
            row.max_num_order_amends = terms.max_num_order_amends;
            row.max_price = terms.max_price.map(Price::from_mantissa);
            row.qty_scale = terms.qty_scale;
        }
        Ok(())
    }
}

fn is_same_instrument(ours: &InstrumentTerms, theirs: &InstrumentTerms) -> bool {
    ours.instrument_id == theirs.instrument_id
        && ours.venue_id == theirs.venue_id
        && ours.venue_symbol == theirs.venue_symbol
        && ours.base_asset_id == theirs.base_asset_id
        && ours.quote_asset_id == theirs.quote_asset_id
}
