//! Strategy desired quotes per instrument/side. Level-triggered (survives one spin), not per-message (cleared every drain). Expired declaration = cancel. No escape hatch: declare every spin or absent.

use crate::config::MAX_QUOTE_LEVELS;
use crate::ids::{InstrumentId, Price, Qty, Side};
use crate::msg::exec::OrderStyle;

/// A stable ladder identity, not a position in a sorted list: a price change never makes one live
/// order masquerade as another level, and an absent level has exactly one meaning, which is to
/// withdraw the order carrying that identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QuoteLevel(u8);

impl QuoteLevel {
    pub const ZERO: Self = Self(0);

    /// Every rung, in ladder order. A pass over the ladder walks this rather than round-tripping
    /// each index through the fallible constructor.
    pub const ALL: [Self; MAX_QUOTE_LEVELS] = {
        let mut levels = [Self(0); MAX_QUOTE_LEVELS];
        let mut index = 0;
        while index < MAX_QUOTE_LEVELS {
            levels[index] = Self(index as u8);
            index += 1;
        }
        levels
    };

    #[inline]
    pub const fn new(value: u8) -> Option<Self> {
        if (value as usize) < MAX_QUOTE_LEVELS { Some(Self(value)) } else { None }
    }

    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DesiredQuote {
    pub price: Price,
    pub qty: Qty,
    /// Only PostOnly admitted. Reject (not downgrade) prevents unwanted position.
    pub style: OrderStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DesiredSide {
    quote: Option<DesiredQuote>,
    declared_at_spin: u64,
}

impl DesiredSide {
    const EMPTY: DesiredSide = DesiredSide {
        quote: None,
        declared_at_spin: 0,
    };
}

pub struct DesiredBook {
    sides: Vec<[[DesiredSide; MAX_QUOTE_LEVELS]; 2]>,
    /// Spin each instrument's flatten was last declared on. Same level-triggered contract as a
    /// quote, and for the same reason: a market order that outlives the intent behind it is one
    /// nobody asked for.
    flatten: Vec<Option<u64>>,
}

impl DesiredBook {
    pub fn new(instrument_count: usize) -> Self {
        Self {
            sides: vec![[[DesiredSide::EMPTY; MAX_QUOTE_LEVELS]; 2]; instrument_count],
            flatten: vec![None; instrument_count],
        }
    }

    #[inline]
    pub fn declare(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        quote: Option<DesiredQuote>,
        spin: u64,
    ) {
        self.sides[usize::from(instrument.0)][side.index()][level.index()] = DesiredSide {
            quote,
            declared_at_spin: spin,
        };
    }

    /// Expiry checked at read-time (not swept) -> stale never observed.
    #[inline]
    pub fn quote(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        spin: u64,
    ) -> Option<DesiredQuote> {
        let declared = self.sides[usize::from(instrument.0)][side.index()][level.index()];
        (declared.declared_at_spin == spin)
            .then_some(declared.quote)
            .flatten()
    }

    #[inline]
    pub fn declare_flatten(&mut self, instrument: InstrumentId, spin: u64) {
        self.flatten[usize::from(instrument.0)] = Some(spin);
    }

    /// Expiry checked at read-time, exactly as [`Self::quote`] does it.
    #[inline]
    pub fn is_flattening(&self, instrument: InstrumentId, spin: u64) -> bool {
        self.flatten[usize::from(instrument.0)] == Some(spin)
    }

    #[inline]
    pub fn instrument_count(&self) -> usize {
        self.sides.len()
    }
}
