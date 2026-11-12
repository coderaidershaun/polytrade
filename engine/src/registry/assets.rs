//! Asset dictionary: base + quote across instruments, interned to dense [`AssetId`] at build.

use crate::ids::AssetId;

use super::InstrumentRow;

/// Handed to adapter at spawn: balances and commissions as STRINGS; inward compares indices.
#[derive(Debug, Clone, Default)]
pub struct AssetDictionary {
    names: Vec<Box<str>>,
}

impl AssetDictionary {
    /// Case-insensitive lookup; [`AssetId::UNKNOWN`] if not found.
    pub fn id(&self, name: &str) -> AssetId {
        self.names
            .iter()
            .position(|known| known.eq_ignore_ascii_case(name))
            .map_or(AssetId::UNKNOWN, |index| AssetId(index as u16))
    }

    pub fn name(&self, id: AssetId) -> Option<&str> {
        self.names.get(usize::from(id.0)).map(Box::as_ref)
    }

    /// Dictionary in id order; persisted in footer for deserialization.
    pub fn names(&self) -> &[Box<str>] {
        &self.names
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    fn intern(&mut self, name: &str) -> AssetId {
        let known = self.id(name);
        if known != AssetId::UNKNOWN {
            return known;
        }
        // A handful of assets per source (one per leg plus the shared quote) -> cannot hit the
        // sentinel unless interning is buggy.
        debug_assert!(
            self.names.len() < usize::from(AssetId::UNKNOWN.0),
            "{} assets interned, the id space stops below the UNKNOWN sentinel",
            self.names.len() + 1
        );
        self.names.push(name.into());
        AssetId((self.names.len() - 1) as u16)
    }
}

// Row order, base before quote, first seen wins -> deterministic ids for replay (shifted id = silent rename).
pub(super) fn intern_assets(instruments: &mut [InstrumentRow]) -> AssetDictionary {
    let mut assets = AssetDictionary::default();
    for row in instruments {
        row.base_asset = assets.intern(&row.base);
        row.quote_asset = assets.intern(&row.quote);
    }
    assets
}
