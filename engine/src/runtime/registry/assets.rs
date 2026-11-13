//! The assets of the run, each with one dense [`AssetId`] given at start. A venue names its own
//! assets, so USDT at two venues is two assets.

use crate::msg::ids::{AssetId, VenueId};

use super::InstrumentRow;

/// Handed to adapter at spawn: balances and commissions as STRINGS; inward compares indices.
#[derive(Debug, Clone, Default)]
pub struct AssetDictionary {
    entries: Vec<(VenueId, Box<str>)>,
}

impl AssetDictionary {
    /// Finds an asset of `venue` by its name, in any case. A name that the venue does not hold
    /// gives [`AssetId::UNKNOWN`].
    pub fn id(&self, venue: VenueId, name: &str) -> AssetId {
        self.entries
            .iter()
            .position(|(known_venue, known)| {
                *known_venue == venue && known.eq_ignore_ascii_case(name)
            })
            .map_or(AssetId::UNKNOWN, |index| AssetId(index as u16))
    }

    pub fn name(&self, id: AssetId) -> Option<&str> {
        self.entries
            .get(usize::from(id.0))
            .map(|(_, name)| name.as_ref())
    }

    pub fn venue(&self, id: AssetId) -> Option<VenueId> {
        self.entries.get(usize::from(id.0)).map(|(venue, _)| *venue)
    }

    /// Dictionary in id order; persisted in footer for deserialization.
    pub fn entries(&self) -> &[(VenueId, Box<str>)] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn intern(&mut self, venue: VenueId, name: &str) -> AssetId {
        let known = self.id(venue, name);
        if known != AssetId::UNKNOWN {
            return known;
        }
        // A handful of assets per source (one per leg plus the shared quote) -> cannot hit the
        // sentinel unless interning is buggy.
        debug_assert!(
            self.entries.len() < usize::from(AssetId::UNKNOWN.0),
            "{} assets interned, the id space stops below the UNKNOWN sentinel",
            self.entries.len() + 1
        );
        self.entries.push((venue, name.into()));
        AssetId((self.entries.len() - 1) as u16)
    }
}

// Row order, base before quote, first seen wins -> deterministic ids for replay (shifted id = silent rename).
pub(super) fn intern_assets(instruments: &mut [InstrumentRow]) -> AssetDictionary {
    let mut assets = AssetDictionary::default();
    for row in instruments {
        row.base_asset = assets.intern(row.venue, &row.base);
        row.quote_asset = assets.intern(row.venue, &row.quote);
    }
    assets
}
