//! Price × size → the venue's 6-decimal `makerAmount`/`takerAmount`, in exact integers.
//!
//! The rounding rule is the SDK's, not the docs' round-up-then-down prose: plain truncation at
//! `tick_decimals + 2` places. These amounts are an equality operand against the venue's own
//! arithmetic, so `f64` is ruled out and every step below stays in `i128`.

use crate::ids::{FIXED_SCALE, Price, Qty};

use super::order::OrderSide;

/// Decimal places in an engine mantissa, i.e. `FIXED_SCALE == 10^FIXED_DECIMALS`.
const FIXED_DECIMALS: u32 = 8;

/// pUSD and shares are both 6-decimal integers on the wire.
const VENUE_DECIMALS: u32 = 6;

/// A share size carries at most 2 decimal places whatever the tick is.
const LOT_DECIMALS: u32 = 2;

const _: () = assert!(FIXED_SCALE == 10_i64.pow(FIXED_DECIMALS));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmountRequest {
    pub side: OrderSide,
    pub price: Price,
    pub size: Qty,
    pub tick: Price,
}

/// Both scaled by 10^6. Which one is money and which is shares depends on the side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderAmounts {
    pub maker: u128,
    pub taker: u128,
}

/// # Errors
/// Any input the venue would refuse: a tick, price or size that is not positive, a tick too fine
/// for six decimals, a price off the tick grid or within one tick of $1, a size finer than a
/// hundredth of a share, or amounts that truncate to zero.
pub fn order_amounts(request: &AmountRequest) -> Result<OrderAmounts, AmountError> {
    let AmountRequest {
        side,
        price,
        size,
        tick,
    } = *request;

    if tick.0 <= 0 {
        return Err(AmountError::TickNotPositive { tick: tick.0 });
    }
    if price.0 <= 0 {
        return Err(AmountError::PriceNotPositive { price: price.0 });
    }
    if size.0 <= 0 {
        return Err(AmountError::SizeNotPositive { size: size.0 });
    }

    let tick_decimals = decimal_places(tick.0);
    let amount_decimals = tick_decimals + LOT_DECIMALS;
    if amount_decimals > VENUE_DECIMALS {
        return Err(AmountError::TickTooFine {
            tick: tick.0,
            decimals: tick_decimals,
        });
    }
    if price.0 % tick.0 != 0 {
        return Err(AmountError::PriceOffTick {
            price: price.0,
            tick: tick.0,
        });
    }
    if price.0 > FIXED_SCALE - tick.0 {
        return Err(AmountError::PriceOutOfBand {
            price: price.0,
            tick: tick.0,
        });
    }
    let lot = FIXED_SCALE / 10_i64.pow(LOT_DECIMALS);
    if size.0 % lot != 0 {
        return Err(AmountError::SizeOffLot { size: size.0 });
    }

    // The product carries 2×FIXED_DECIMALS places; the division truncates it to `amount_decimals`
    // and the multiplication reinflates to the venue's 6, both exactly because both are powers of
    // ten and `amount_decimals <= VENUE_DECIMALS`.
    let notional = i128::from(price.0) * i128::from(size.0);
    let truncate = 10_i128.pow(2 * FIXED_DECIMALS - amount_decimals);
    let reinflate = 10_i128.pow(VENUE_DECIMALS - amount_decimals);
    let money = notional / truncate * reinflate;
    let shares = i128::from(size.0) / 10_i128.pow(FIXED_DECIMALS - VENUE_DECIMALS);

    if money == 0 {
        return Err(AmountError::NotionalTruncatedToZero {
            price: price.0,
            size: size.0,
        });
    }

    let (maker, taker) = match side {
        OrderSide::Buy => (money, shares),
        OrderSide::Sell => (shares, money),
    };
    Ok(OrderAmounts {
        maker: maker as u128,
        taker: taker as u128,
    })
}

#[derive(thiserror::Error, Debug)]
pub enum AmountError {
    #[error("tick mantissa {tick} is not positive")]
    TickNotPositive { tick: i64 },
    #[error("price mantissa {price} is not positive")]
    PriceNotPositive { price: i64 },
    #[error("size mantissa {size} is not positive")]
    SizeNotPositive { size: i64 },
    #[error(
        "tick mantissa {tick} carries {decimals} decimal places — polymarket amounts hold at most {VENUE_DECIMALS} and a size adds {LOT_DECIMALS}"
    )]
    TickTooFine { tick: i64, decimals: u32 },
    #[error("price mantissa {price} is not a whole multiple of tick {tick}")]
    PriceOffTick { price: i64, tick: i64 },
    #[error("price mantissa {price} is outside [{tick}, 1 − {tick}] — a share cannot cost $1")]
    PriceOutOfBand { price: i64, tick: i64 },
    #[error(
        "size mantissa {size} is finer than {LOT_DECIMALS} decimal places — polymarket sizes are whole hundredths of a share"
    )]
    SizeOffLot { size: i64 },
    #[error("price {price} × size {size} truncates to zero at the venue's decimals")]
    NotionalTruncatedToZero { price: i64, size: i64 },
}

/// Decimal places actually carried by a mantissa: `0.01` at 1e-8 is `1_000_000`, i.e. 2 places.
fn decimal_places(mantissa: i64) -> u32 {
    let mut places = FIXED_DECIMALS;
    let mut value = mantissa;
    while places > 0 && value % 10 == 0 {
        value /= 10;
        places -= 1;
    }
    places
}
