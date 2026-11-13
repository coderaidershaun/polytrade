//! Greeks taken from values at shifted states, so that one rule serves one option and a whole book.

use super::black76::POINTS_PER_UNIT;
use super::error::{
    require_finite_input, require_finite_output, require_not_negative, require_positive,
};
use super::{Black76, OptionRight, OptionsError};

/// One move of the market. The default is no move.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Shift {
    /// USD for each coin, added to the price that the caller moves.
    pub underlying: f64,
    /// Fraction, added to volatility.
    pub volatility: f64,
    /// Variance time that passes, in years, at least 0.
    pub years: f64,
}

/// Sizes of the bumps. They are a choice of the caller: no size is fixed here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bumps {
    /// USD for each coin, above 0.
    pub near_move: f64,
    /// USD for each coin, above `near_move`.
    pub far_move: f64,
    /// Fraction of volatility, above 0.
    pub volatility: f64,
    /// Years, above 0.
    pub day: f64,
}

/// Greeks from values at shifted states, for whatever the value function holds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BumpGreeks {
    /// Coins: the central difference over the near move.
    pub delta: f64,
    /// Coins: the difference over the near move up.
    pub delta_up: f64,
    /// Coins: the difference over the near move down.
    pub delta_down: f64,
    /// Coins for a move of 1 USD: the change of the up-move delta from the near move to the far
    /// move. Exact for a value that is quadratic on the up side, and never averaged with the down
    /// side.
    pub up_gamma: f64,
    /// As `up_gamma`, on the down side.
    pub down_gamma: f64,
    /// USD for one point of volatility.
    pub vega_per_point: f64,
    /// USD for the time that `Bumps::day` gives.
    pub theta_per_day: f64,
}

/// Second-order greeks from first-order greeks by bump at four states. A day is the time that
/// `Bumps::day` gives.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SecondOrderGreeks {
    /// Coins for one point of volatility.
    pub vanna_per_point: f64,
    /// USD for one point of volatility, squared.
    pub volga_per_point_squared: f64,
    /// Coins for one day.
    pub charm_per_day: f64,
    /// Coins for a move of 1 USD, squared. Exact for a value that is cubic in the move.
    pub speed: f64,
    /// Coins for a move of 1 USD, for one day.
    pub color_per_day: f64,
    /// Coins for a move of 1 USD, for one point of volatility.
    pub zomma_per_point: f64,
    /// USD for one point of volatility, for one day.
    pub vega_decay_per_day: f64,
}

/// Coins each: the delta of a book that is kept in USD, and of one that is kept in coin.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AccountDeltas {
    pub usd: f64,
    pub coin: f64,
}

/// Greeks of any value function in USD, from its values at 8 shifted states. They are taken in
/// this order: here, near up, far up, near down, far down, volatility up, volatility down, a day
/// later. The order is fixed, so that a closure that sums a book does the same work in the same
/// order on each call.
///
/// # Errors
///
/// `OutsideDomain` names the first bump size that is not inside its domain. An `Err` of `value`
/// comes back unchanged, from the first shift that fails. `NotFinite` if a greek overflows.
pub fn greeks_by_bump(
    value: impl Fn(Shift) -> Result<f64, OptionsError>,
    bumps: Bumps,
) -> Result<BumpGreeks, OptionsError> {
    bumps.check()?;
    let Bumps {
        near_move: near,
        far_move: far,
        volatility,
        day,
    } = bumps;
    let moved = |underlying| Shift {
        underlying,
        ..Shift::default()
    };
    let here = value(Shift::default())?;
    let near_up = value(moved(near))?;
    let far_up = value(moved(far))?;
    let near_down = value(moved(-near))?;
    let far_down = value(moved(-far))?;
    let volatility_up = value(Shift {
        volatility,
        ..Shift::default()
    })?;
    let volatility_down = value(Shift {
        volatility: -volatility,
        ..Shift::default()
    })?;
    let day_later = value(Shift {
        years: day,
        ..Shift::default()
    })?;
    BumpGreeks {
        delta: (near_up - near_down) / (2.0 * near),
        delta_up: (near_up - here) / near,
        delta_down: (here - near_down) / near,
        up_gamma: 2.0 * ((far_up - here) / far - (near_up - here) / near) / (far - near),
        down_gamma: 2.0 * ((far_down - here) / far - (near_down - here) / near) / (far - near),
        vega_per_point: (volatility_up - volatility_down) / (2.0 * volatility) / POINTS_PER_UNIT,
        theta_per_day: day_later - here,
    }
    .checked()
}

/// Second-order greeks of any value function, from `greeks_by_bump` at four states, in this
/// order: here, volatility up, volatility down, a day later. That makes 32 calls of `value`.
///
/// # Errors
///
/// As [`greeks_by_bump`].
pub fn second_order_by_bump(
    value: impl Fn(Shift) -> Result<f64, OptionsError>,
    bumps: Bumps,
) -> Result<SecondOrderGreeks, OptionsError> {
    let here = greeks_by_bump(&value, bumps)?;
    let volatility_up = greeks_by_bump(
        |shift| {
            value(Shift {
                volatility: shift.volatility + bumps.volatility,
                ..shift
            })
        },
        bumps,
    )?;
    let volatility_down = greeks_by_bump(
        |shift| {
            value(Shift {
                volatility: shift.volatility - bumps.volatility,
                ..shift
            })
        },
        bumps,
    )?;
    let day_later = greeks_by_bump(
        |shift| {
            value(Shift {
                years: shift.years + bumps.day,
                ..shift
            })
        },
        bumps,
    )?;
    let twice_volatility = 2.0 * bumps.volatility;
    SecondOrderGreeks {
        vanna_per_point: (volatility_up.delta - volatility_down.delta)
            / twice_volatility
            / POINTS_PER_UNIT,
        volga_per_point_squared: (volatility_up.vega_per_point - volatility_down.vega_per_point)
            / twice_volatility
            / POINTS_PER_UNIT,
        charm_per_day: day_later.delta - here.delta,
        speed: 3.0 * (here.up_gamma - here.down_gamma) / (2.0 * (bumps.near_move + bumps.far_move)),
        color_per_day: day_later.mean_gamma() - here.mean_gamma(),
        zomma_per_point: (volatility_up.mean_gamma() - volatility_down.mean_gamma())
            / twice_volatility
            / POINTS_PER_UNIT,
        vega_decay_per_day: day_later.vega_per_point - here.vega_per_point,
    }
    .checked()
}

/// Both deltas of an account from its equity in coin at the spot price, at spot plus the bump,
/// and at spot less the bump, called in that order.
///
/// # Errors
///
/// `OutsideDomain` names `spot` or `bump` when it is not finite or not above 0, and `bump` when it
/// is not below `spot`. An `Err` of `equity_in_coin` comes back unchanged, from the first call
/// that fails. `NotFinite` if a delta overflows.
pub fn account_deltas(
    equity_in_coin: impl Fn(f64) -> Result<f64, OptionsError>,
    spot: f64,
    bump: f64,
) -> Result<AccountDeltas, OptionsError> {
    require_positive("spot", spot)?;
    require_positive("bump", bump)?;
    if bump >= spot {
        return Err(OptionsError::OutsideDomain {
            input: "bump",
            value: bump,
        });
    }
    let here = equity_in_coin(spot)?;
    let up = equity_in_coin(spot + bump)?;
    let down = equity_in_coin(spot - bump)?;
    let coin = spot * ((up - down) / (2.0 * bump));
    Ok(AccountDeltas {
        usd: require_finite_output("usd", here + coin)?,
        coin: require_finite_output("coin", coin)?,
    })
}

impl Black76 {
    /// USD for each coin of notional at the shifted state. The forward and the volatility each
    /// gain their part of the shift. Time to expiry loses `shift.years`, down to 0, where the
    /// option is worth its payoff.
    ///
    /// # Errors
    ///
    /// As [`Black76::price`] for the option before the shift. Then `OutsideDomain` names a field
    /// of `shift` that is not finite, or `shift.years` below 0. A shifted forward at or below 0
    /// names `forward`, and a shifted volatility below 0 names `volatility`.
    pub fn shifted_price(
        &self,
        right: OptionRight,
        volatility: f64,
        shift: Shift,
    ) -> Result<f64, OptionsError> {
        self.check_market()?;
        require_not_negative("volatility", volatility)?;
        require_finite_input("shift.underlying", shift.underlying)?;
        require_finite_input("shift.volatility", shift.volatility)?;
        require_not_negative("shift.years", shift.years)?;
        let years_left = self.years - shift.years;
        let shifted = Black76 {
            forward: self.forward + shift.underlying,
            years: if years_left > 0.0 { years_left } else { 0.0 },
            ..*self
        };
        shifted.price(right, volatility + shift.volatility)
    }
}

impl Bumps {
    fn check(&self) -> Result<(), OptionsError> {
        require_positive("bumps.near_move", self.near_move)?;
        require_positive("bumps.far_move", self.far_move)?;
        if self.far_move <= self.near_move {
            return Err(OptionsError::OutsideDomain {
                input: "bumps.far_move",
                value: self.far_move,
            });
        }
        require_positive("bumps.volatility", self.volatility)?;
        require_positive("bumps.day", self.day)?;
        Ok(())
    }
}

impl BumpGreeks {
    fn mean_gamma(&self) -> f64 {
        0.5 * (self.up_gamma + self.down_gamma)
    }

    fn checked(self) -> Result<Self, OptionsError> {
        Ok(Self {
            delta: require_finite_output("delta", self.delta)?,
            delta_up: require_finite_output("delta_up", self.delta_up)?,
            delta_down: require_finite_output("delta_down", self.delta_down)?,
            up_gamma: require_finite_output("up_gamma", self.up_gamma)?,
            down_gamma: require_finite_output("down_gamma", self.down_gamma)?,
            vega_per_point: require_finite_output("vega_per_point", self.vega_per_point)?,
            theta_per_day: require_finite_output("theta_per_day", self.theta_per_day)?,
        })
    }
}

impl SecondOrderGreeks {
    fn checked(self) -> Result<Self, OptionsError> {
        Ok(Self {
            vanna_per_point: require_finite_output("vanna_per_point", self.vanna_per_point)?,
            volga_per_point_squared: require_finite_output(
                "volga_per_point_squared",
                self.volga_per_point_squared,
            )?,
            charm_per_day: require_finite_output("charm_per_day", self.charm_per_day)?,
            speed: require_finite_output("speed", self.speed)?,
            color_per_day: require_finite_output("color_per_day", self.color_per_day)?,
            zomma_per_point: require_finite_output("zomma_per_point", self.zomma_per_point)?,
            vega_decay_per_day: require_finite_output(
                "vega_decay_per_day",
                self.vega_decay_per_day,
            )?,
        })
    }
}
