//! What a strategy declares for each market in each spin: stand aside, get flat, or a ladder of
//! quotes. A declaration lives for its spin only, so a market that is not declared again stands
//! aside and each of its quotes is withdrawn.

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::msg::ids::{InstrumentId, Price, Qty, Side};
use crate::runtime::config::MAX_QUOTE_LEVELS;

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

/// A limit order that a strategy wants resting on the book. It has no order style: the engine
/// always places a quote so that it cannot take liquidity, and the only order that takes
/// liquidity is the one that gets a market flat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DesiredQuote {
    pub price: Price,
    pub qty: Qty,
}

type Ladder = [[Option<DesiredQuote>; MAX_QUOTE_LEVELS]; 2];

/// What a strategy wants of one market for one spin.
#[expect(
    clippy::large_enum_variant,
    reason = "the table keeps room for a whole ladder in each market from the start, so a smaller \
              variant saves no memory, and a boxed ladder would allocate on the hot thread"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Declaration {
    StandAside,
    /// Close the position, and hold no quote while doing it.
    GetFlat,
    /// The quote for each side and level. A level with no quote is withdrawn.
    Ladder(Ladder),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MarketDeclaration {
    spin: u64,
    declaration: Declaration,
}

const EMPTY_LADDER: Ladder = [[None; MAX_QUOTE_LEVELS]; 2];

pub struct DesiredBook {
    markets: Box<[MarketDeclaration]>,
}

digest_by_hash!(QuoteLevel, MarketDeclaration);
digest_fields! { DesiredBook { markets } }

impl DesiredBook {
    pub fn new(instrument_count: usize) -> Self {
        let undeclared = MarketDeclaration {
            spin: 0,
            declaration: Declaration::StandAside,
        };
        Self {
            markets: vec![undeclared; instrument_count].into_boxed_slice(),
        }
    }

    /// Declares one level of the ladder of this spin. In a spin that gets the market flat, the
    /// market holds no quote, so the quote is dropped whether it comes before or after.
    #[inline]
    pub fn declare(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        quote: Option<DesiredQuote>,
        spin: u64,
    ) {
        let market = &mut self.markets[usize::from(instrument.0)];
        if market.spin != spin || matches!(market.declaration, Declaration::StandAside) {
            *market = MarketDeclaration {
                spin,
                declaration: Declaration::Ladder(EMPTY_LADDER),
            };
        }
        if let Declaration::Ladder(ladder) = &mut market.declaration {
            ladder[side.index()][level.index()] = quote;
        }
    }

    /// Declares that the market gets flat in this spin, which withdraws each quote of it.
    #[inline]
    pub fn declare_flatten(&mut self, instrument: InstrumentId, spin: u64) {
        self.markets[usize::from(instrument.0)] = MarketDeclaration {
            spin,
            declaration: Declaration::GetFlat,
        };
    }

    #[inline]
    pub fn quote(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        spin: u64,
    ) -> Option<DesiredQuote> {
        match self.declaration(instrument, spin) {
            Declaration::Ladder(ladder) => ladder[side.index()][level.index()],
            Declaration::StandAside | Declaration::GetFlat => None,
        }
    }

    #[inline]
    pub fn is_flattening(&self, instrument: InstrumentId, spin: u64) -> bool {
        matches!(self.declaration(instrument, spin), Declaration::GetFlat)
    }

    #[inline]
    pub fn instrument_count(&self) -> usize {
        self.markets.len()
    }

    /// A declaration of an earlier spin has expired, which is read here rather than swept, so a
    /// stale one is never seen.
    #[inline]
    fn declaration(&self, instrument: InstrumentId, spin: u64) -> &Declaration {
        let market = &self.markets[usize::from(instrument.0)];
        if market.spin == spin { &market.declaration } else { &Declaration::StandAside }
    }
}
