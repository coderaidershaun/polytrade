//! The nouns a strategy reads in one callback: the market with its touch and window, my position,
//! and the state the strategy keeps for each market. Each is a plain value, so a replay or a
//! research run can build the same nouns from a tape.

use crate::hot::book::{Book, BookState};
use crate::hot::exec::TickGrid;
use crate::hot::ledger::LedgerRow;
use crate::msg::ids::{InstrumentId, fixed_mantissa};
use crate::msg::time::{DurationUs, LocalTs};

use super::ready::{NotReady, Ready};
use super::units::{Fair, Notional, Price, Qty};
use super::{StrategyGate, WindowInfo};

/// The best bid and the best ask of a market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Touch {
    pub bid: Price,
    pub ask: Price,
}

/// One market that the engine can trade now, as it stands after the message under fold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Market {
    instrument: InstrumentId,
    touch: Ready<Touch>,
    grid: TickGrid,
    window: Option<WindowInfo>,
    now: LocalTs,
}

impl Market {
    pub(crate) fn of(
        instrument: InstrumentId,
        book: &Book,
        gate: StrategyGate,
        grid: TickGrid,
        window: Option<WindowInfo>,
        now: LocalTs,
    ) -> Self {
        Self {
            instrument,
            touch: touch_of(book, gate),
            grid,
            window,
            now,
        }
    }

    /// The market this noun describes, for the state kept for it.
    pub fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Not ready while the market warms up or the run is parked, while the book is not valid, and
    /// while a side of the book is empty.
    pub fn touch(&self) -> Ready<Touch> {
        self.touch
    }

    /// Whether the window of the market ends within `span` of the time of the fold. A market with
    /// no window never closes.
    pub fn closes_within(&self, span: DurationUs) -> bool {
        self.window
            .is_some_and(|window| window.is_past_quote_stop(self.now, span))
    }

    /// The highest price of the grid at or below `fair`: a bid that rounds here is never more
    /// aggressive than the fair value.
    pub fn price_at_or_below(&self, fair: Fair) -> Ready<Price> {
        self.on_grid(fair, Rounding::Down)
    }

    /// The lowest price of the grid at or above `fair`, the mirror for an ask.
    pub fn price_at_or_above(&self, fair: Fair) -> Ready<Price> {
        self.on_grid(fair, Rounding::Up)
    }

    /// The price of the grid nearest `fair`; a fair value half a tick from two prices takes the
    /// higher one.
    pub fn nearest_price(&self, fair: Fair) -> Ready<Price> {
        self.on_grid(fair, Rounding::Nearest)
    }

    /// The fair value is first rounded to the nearest mantissa, which is finer than any grid, and
    /// the grid is then taken in whole mantissas, so a price of the grid comes back as itself. A
    /// division of two floats would put 0.29 on a 0.01 grid at 28.999… ticks.
    fn on_grid(&self, fair: Fair, rounding: Rounding) -> Ready<Price> {
        let tick = self.grid.tick;
        if tick <= 0 {
            return Err(NotReady::OffGrid);
        }
        let mantissa = fixed_mantissa(fair.value()).ok_or(NotReady::OffGrid)?;
        let index = match rounding {
            Rounding::Down => Some(mantissa.div_euclid(tick)),
            Rounding::Up => mantissa
                .checked_neg()
                .map(|negated| negated.div_euclid(tick))
                .and_then(i64::checked_neg),
            Rounding::Nearest => mantissa
                .checked_add(tick / 2)
                .map(|shifted| shifted.div_euclid(tick)),
        };
        index
            .filter(|index| *index >= 1)
            .and_then(|index| index.checked_mul(tick))
            .map(Price::from_mantissa)
            .ok_or(NotReady::OffGrid)
    }
}

/// The direction in which a fair value rounds onto the grid.
#[derive(Debug, Clone, Copy)]
enum Rounding {
    Down,
    Up,
    Nearest,
}

fn touch_of(book: &Book, gate: StrategyGate) -> Ready<Touch> {
    match gate {
        StrategyGate::Warming { .. } => return Err(NotReady::Warming),
        StrategyGate::Parked => return Err(NotReady::Parked),
        StrategyGate::Live => {}
    }
    if book.state() != BookState::Valid {
        return Err(NotReady::BookInvalid);
    }
    let bid = book.best_bid().ok_or(NotReady::NoBid)?;
    let ask = book.best_ask().ok_or(NotReady::NoAsk)?;
    Ok(Touch {
        bid: bid.price,
        ask: ask.price,
    })
}

/// What I hold in one market, from real fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    size: Qty,
    exposure: Ready<Notional>,
    lot_step: i64,
    min_qty: Qty,
}

impl Position {
    pub(crate) fn of(row: &LedgerRow, grid: TickGrid) -> Self {
        let exposure = match row.has_mark() {
            true => Ok(row.exposure_quote()),
            false => Err(NotReady::NoMark),
        };
        Self {
            size: row.position_base(),
            exposure,
            lot_step: grid.step,
            min_qty: grid.min_qty,
        }
    }

    pub fn is_flat(&self) -> bool {
        self.size.mantissa() == 0
    }

    pub fn is_long(&self) -> bool {
        self.size.mantissa() > 0
    }

    pub fn is_short(&self) -> bool {
        self.size.mantissa() < 0
    }

    /// The signed size: above zero is long.
    pub fn size(&self) -> Qty {
        self.size
    }

    /// What a sell can offer of a long position: the size floored to the lot step, and not ready
    /// below the venue's minimum order size, where no order could sell it.
    pub fn sellable(&self) -> Ready<Qty> {
        let held = self.size.mantissa().max(0);
        let floored = match self.lot_step > 0 {
            true => held - held.rem_euclid(self.lot_step),
            false => held,
        };
        let sellable = Qty::from_mantissa(floored);
        if sellable.mantissa() == 0 || sellable < self.min_qty {
            return Err(NotReady::BelowMinimum);
        }
        Ok(sellable)
    }

    /// The signed value of the position at the mark. Not ready before the market has a mark,
    /// where zero would read as flat.
    pub fn exposure(&self) -> Ready<Notional> {
        self.exposure
    }
}

/// State that a strategy keeps for each market. The engine resets a market's state to its
/// default on each resume and when the market rotates, before the next callback reads it, so a
/// strategy holds no reset code. A strategy hands it to the engine in
/// [`Strategy::market_states`](super::Strategy::market_states).
#[derive(Debug, Clone, Default)]
pub struct PerMarket<S> {
    states: Vec<S>,
}

impl<S: Default> PerMarket<S> {
    pub fn new() -> Self {
        Self { states: Vec::new() }
    }

    /// # Panics
    /// When the strategy does not return this state from `Strategy::market_states`, so the engine
    /// never sized it: a bug of the strategy, found at its first callback.
    pub fn of(&self, market: &Market) -> &S {
        &self.states[self.index(market.instrument)]
    }

    /// # Panics
    /// As [`Self::of`].
    pub fn of_mut(&mut self, market: &Market) -> &mut S {
        let index = self.index(market.instrument);
        &mut self.states[index]
    }

    fn index(&self, instrument: InstrumentId) -> usize {
        let index = usize::from(instrument.0);
        assert!(
            index < self.states.len(),
            "market {index} has no state: return this PerMarket from Strategy::market_states"
        );
        index
    }
}

/// How the engine sizes and resets the state of each market. A strategy names it once, in
/// `Strategy::market_states`, and never calls it.
pub trait MarketStates {
    fn size_for(&mut self, markets: usize);
    fn reset(&mut self, instrument: InstrumentId);
    fn reset_all(&mut self);
}

impl<S: Default> MarketStates for PerMarket<S> {
    fn size_for(&mut self, markets: usize) {
        self.states.resize_with(markets, S::default);
    }

    fn reset(&mut self, instrument: InstrumentId) {
        if let Some(state) = self.states.get_mut(usize::from(instrument.0)) {
            *state = S::default();
        }
    }

    fn reset_all(&mut self) {
        self.states.fill_with(S::default);
    }
}
