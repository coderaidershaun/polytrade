//! Guéant-Lehalle-Fernandez-Tapia optimal market-making quotes. Each depth carries its unit in its
//! type, ticks or basis points, so that only depths in ticks of the grid snap onto it.

use core::marker::PhantomData;

use crate::msg::ids::Price;

/// Depths in ticks of the price grid, the unit that a quote snaps in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Ticks;

/// Depths in basis points of fair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Bps;

/// The rates of one side that the closed form reads, in the depth unit `U`: A, the fill rate per
/// second at fair; k, its decay per unit of depth; and σ, the volatility per √second in that unit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideRates<U> {
    pub a_per_sec: f64,
    pub k_per_depth: f64,
    pub sigma_per_sqrt_sec: f64,
    pub unit: U,
}

/// The objective of the closed form. It fixes ξ, so ξ is never a parameter to tune.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Objective {
    /// Model A: constant absolute risk aversion over the marked-to-market wealth at the end
    /// (ξ = γ).
    CaraUtility,
    /// Model B: the wealth at the end minus a quadratic penalty on inventory (ξ = 0), which takes
    /// the limits c1 = 1/k and c2 = √(γe/(2AΔk)).
    InventoryPenalty,
}

/// The choices of the maker that set its risk budget: the risk aversion, the size of one fill,
/// and the objective.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GueantParams {
    gamma: f64,
    order_size: f64,
    objective: Objective,
}

impl GueantParams {
    /// `gamma` is γ, the risk aversion per unit of wealth in the depth unit of the rates, and
    /// `order_size` is Δ, the change of inventory that one fill makes.
    ///
    /// # Panics
    /// If `gamma` or `order_size` is not finite or not positive: both come from config, so either
    /// is a bug, not a market condition.
    pub fn new(gamma: f64, order_size: f64, objective: Objective) -> Self {
        assert!(
            gamma.is_finite() && gamma > 0.0,
            "gueant gamma must be finite and positive, got {gamma}"
        );
        assert!(
            order_size.is_finite() && order_size > 0.0,
            "gueant order_size must be finite and positive, got {order_size}"
        );
        Self {
            gamma,
            order_size,
            objective,
        }
    }

    /// The coefficients of one side, in the unit of its rates. `None` unless each rate is finite
    /// and positive and the closed form stays finite.
    pub fn coefficients<U>(&self, rates: SideRates<U>) -> Option<QuoteCoefficients<U>> {
        let SideRates {
            a_per_sec,
            k_per_depth,
            sigma_per_sqrt_sec,
            unit: _,
        } = rates;
        let is_positive_finite = |value: f64| value.is_finite() && value > 0.0;
        if !(is_positive_finite(a_per_sec)
            && is_positive_finite(k_per_depth)
            && is_positive_finite(sigma_per_sqrt_sec))
        {
            return None;
        }
        let (c1, c2) = self.c1_c2(a_per_sec, k_per_depth);
        let half_spread = c1 + 0.5 * self.order_size * sigma_per_sqrt_sec * c2;
        let skew_per_inventory = sigma_per_sqrt_sec * c2;
        if !(half_spread.is_finite() && skew_per_inventory.is_finite()) {
            return None;
        }
        Some(QuoteCoefficients {
            half_spread,
            skew_per_inventory,
            unit: PhantomData,
        })
    }

    fn c1_c2(&self, a_per_sec: f64, k_per_depth: f64) -> (f64, f64) {
        let denom = 2.0 * a_per_sec * self.order_size * k_per_depth;
        match self.objective {
            Objective::InventoryPenalty => {
                let c1 = 1.0 / k_per_depth;
                let c2 = (self.gamma * std::f64::consts::E / denom).sqrt();
                (c1, c2)
            }
            Objective::CaraUtility => {
                let xi_delta = self.gamma * self.order_size;
                let ratio = xi_delta / k_per_depth;
                // ln_1p keeps its precision as ξΔ/k goes to 0, where c1 tends to 1/k and the
                // exponent tends to 1, the factor e of Model B.
                let log1p_ratio = ratio.ln_1p();
                let c1 = log1p_ratio / xi_delta;
                let exponent = (k_per_depth / xi_delta + 1.0) * log1p_ratio;
                let c2 = (self.gamma / denom * exponent.exp()).sqrt();
                (c1, c2)
            }
        }
    }
}

/// Solved depth coefficients in the depth unit `U`: the half-spread h and the skew per unit of
/// inventory j.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteCoefficients<U> {
    half_spread: f64,
    skew_per_inventory: f64,
    unit: PhantomData<U>,
}

impl<U> QuoteCoefficients<U> {
    /// h = c1 + (Δ/2)·σ·c2, the half-spread at zero inventory, before any rounding.
    #[inline]
    pub fn half_spread(&self) -> f64 {
        self.half_spread
    }

    /// j = σ·c2, how far the centre of the quote moves for each unit of inventory.
    #[inline]
    pub fn skew_per_inventory(&self) -> f64 {
        self.skew_per_inventory
    }

    /// δᵇ(q) = h + j·q, the depth of the bid below fair, before rounding.
    #[inline]
    pub fn bid_depth(&self, inventory: f64) -> f64 {
        self.half_spread + self.skew_per_inventory * inventory
    }

    /// δᵃ(q) = h − j·q, the depth of the ask above fair, before rounding.
    #[inline]
    pub fn ask_depth(&self, inventory: f64) -> f64 {
        self.half_spread - self.skew_per_inventory * inventory
    }
}

impl QuoteCoefficients<Ticks> {
    /// The post-only quote on the grid, with its tick indices and depths. `None` for a fair, an
    /// inventory, or a band that is not finite, and for a grid tick that is not positive: each is
    /// a market or ledger value, so it is refused, not a panic. A side whose tick is not positive,
    /// or whose price passes the range of a mantissa, has no price.
    pub fn quote(&self, inputs: QuoteInputs) -> Option<Quotes> {
        let bid_depth = self.bid_depth(inputs.inventory);
        let ask_depth = self.ask_depth(inputs.inventory);
        quote_at_depths(bid_depth, ask_depth, inputs)
    }
}

impl QuoteCoefficients<Bps> {
    /// The same quote as for coefficients in ticks, for depths in basis points: each depth is taken
    /// at the live inventory first, then times `ticks_per_bp`.
    pub fn quote_in_ticks(&self, ticks_per_bp: f64, inputs: QuoteInputs) -> Option<Quotes> {
        let bid_depth = self.bid_depth(inputs.inventory) * ticks_per_bp;
        let ask_depth = self.ask_depth(inputs.inventory) * ticks_per_bp;
        quote_at_depths(bid_depth, ask_depth, inputs)
    }
}

fn quote_at_depths(bid_depth: f64, ask_depth: f64, inputs: QuoteInputs) -> Option<Quotes> {
    let is_band_finite = inputs.min_distance_ticks.is_none_or(f64::is_finite);
    if !(inputs.fair.is_finite() && inputs.inventory.is_finite() && is_band_finite)
        || inputs.tick.mantissa() <= 0
    {
        return None;
    }
    let tick = inputs.tick.mantissa();
    let fair_ticks = inputs.fair / inputs.tick.to_f64();
    let (mut bid_tick, mut ask_tick) = match inputs.snap {
        Snap::Outward => (
            (fair_ticks - bid_depth).floor() as i64,
            (fair_ticks + ask_depth).ceil() as i64,
        ),
        Snap::Nearest => (
            (fair_ticks - bid_depth).round() as i64,
            (fair_ticks + ask_depth).round() as i64,
        ),
    };
    if let Some(distance) = inputs.min_distance_ticks {
        bid_tick = bid_tick.min((fair_ticks - distance).floor() as i64);
        ask_tick = ask_tick.max((fair_ticks + distance).ceil() as i64);
    }

    // Post-only: a side may improve the touch, but never reaches the best price of the other
    // side. The tick index of the touch comes from its mantissa, so no float remainder of a price
    // can misplace it.
    if let Some(best_ask) = inputs.best_ask {
        bid_tick = bid_tick.min(best_ask.mantissa().div_euclid(tick).saturating_sub(1));
    }
    if let Some(best_bid) = inputs.best_bid {
        ask_tick = ask_tick.max(best_bid.mantissa().div_euclid(tick).saturating_add(1));
    }

    // When the sides collide, the side that sheds inventory stays and the other moves one tick
    // out.
    if ask_tick <= bid_tick {
        if inputs.inventory < 0.0 {
            ask_tick = bid_tick.saturating_add(1);
        } else {
            bid_tick = ask_tick.saturating_sub(1);
        }
    }

    Some(Quotes {
        bid: price_at(bid_tick, inputs.tick),
        ask: price_at(ask_tick, inputs.tick),
        bid_tick,
        ask_tick,
        bid_depth,
        ask_depth,
    })
}

/// The price of a tick index, or `None` for an index that is not positive or whose price passes
/// the range of a mantissa.
fn price_at(tick_index: i64, tick: Price) -> Option<Price> {
    if tick_index <= 0 {
        return None;
    }
    tick_index
        .checked_mul(tick.mantissa())
        .map(Price::from_mantissa)
}

/// How a quote rounds a depth onto the grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Snap {
    /// The bid floors and the ask ceils, so neither side comes nearer the market than its depth.
    Outward,
    /// Each side takes its nearest tick, which can be up to half a tick nearer the market than its
    /// depth.
    Nearest,
}

/// What a quote reads: fair, the signed inventory, the touch, and the grid, whose origin is zero.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteInputs {
    /// The fair price, a mid or a microprice. Its tick index is fair / tick.
    pub fair: f64,
    /// The signed inventory, in the unit of `order_size`; above zero is long.
    pub inventory: f64,
    /// The touch that a post-only quote stays behind: the bid below the best ask, and the ask
    /// above the best bid. `None` sets no such limit on that side.
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    /// The step of the grid. The tick index of a price is its mantissa over this mantissa, exactly.
    pub tick: Price,
    pub snap: Snap,
    /// The nearest that each side may come to fair, in ticks: the bid at most fair minus it,
    /// floored, and the ask at least fair plus it, ceiled. `None` sets no band.
    pub min_distance_ticks: Option<f64>,
}

/// The quote of both sides: the price of each, or `None`, with the tick indices and the depths
/// that gave them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quotes {
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    pub bid_tick: i64,
    pub ask_tick: i64,
    pub bid_depth: f64,
    pub ask_depth: f64,
}
