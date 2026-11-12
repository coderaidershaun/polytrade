//! Startup scale preflight for Binance: confirm each instrument's max price and quantity fit the
//! i64 1e-8 range, stamp the tick/step grid and the order floors and counts onto the registry, and
//! read the account's ORDERS budget, which the hot side paces against without knowing whose buckets
//! they are. Anything out of range, unlisted, absent or unreadable refuses the start loudly.

use crate::config::{BinanceMarket, VenueMarket};
use crate::hot::exec::{MAX_ORDER_BUDGET_WINDOWS, OrderBudget, OrderBudgetWindow};
use crate::ids::{DecimalError, InstrumentId, Price, Qty};
use crate::info;
use crate::registry::{BinanceLimits, InstrumentRow, OrderRateLimit, RateInterval, Registry};
use crate::time::DurationUs;

use super::rest::{BinanceEnv, ExchangeInfo, RestClient, RestError, SymbolFilter};

/// Binance tags an order-count bucket with this `rateLimitType`; weight and raw-request buckets
/// ride the same array and are not ours to pace against.
const ORDERS_LIMIT_TYPE: &str = "ORDERS";

#[derive(thiserror::Error, Debug)]
pub enum ScaleError {
    #[error(
        "instrument {instrument} ({symbol}) {field} {value} is not representable as an i64 1e-8 mantissa"
    )]
    OutOfRange {
        instrument: u16,
        symbol: Box<str>,
        field: &'static str,
        value: Box<str>,
        #[source]
        source: DecimalError,
    },
    #[error(
        "instrument {instrument} ({symbol}) is not listed on binance {market} — check source.market, env, base and quote in config.yaml"
    )]
    SymbolUnknown {
        instrument: u16,
        symbol: Box<str>,
        market: &'static str,
    },
    #[error(
        "instrument {instrument} ({symbol}) exchangeInfo is missing {field} — cannot validate scale"
    )]
    FieldMissing {
        instrument: u16,
        symbol: Box<str>,
        field: &'static str,
    },
    #[error(
        "instrument {instrument} ({symbol}) {field} {value} is not positive — a zero grid cannot quantise"
    )]
    NotPositive {
        instrument: u16,
        symbol: Box<str>,
        field: &'static str,
        value: Box<str>,
    },
    #[error(
        "instrument {instrument} ({symbol}) {field} {value} is not positive — an order limit of zero means the venue would reject every order the engine could build"
    )]
    LimitNotPositive {
        instrument: u16,
        symbol: Box<str>,
        field: &'static str,
        value: Box<str>,
    },
    #[error(
        "{market} exchangeInfo carries no ORDERS rate limit — the venue has always published one, so a payload missing it is not the document this build knows how to read"
    )]
    OrderLimitsMissing { market: &'static str },
    #[error(
        "{market} ORDERS rate limit has an unreadable {field} {value} — dropping the bucket would overstate the order budget"
    )]
    RateLimitUnreadable {
        market: &'static str,
        field: &'static str,
        value: Box<str>,
    },
    #[error(
        "scale preflight could not fetch binance {market} exchangeInfo — refusing to start unvalidated"
    )]
    Unreachable {
        market: &'static str,
        #[source]
        source: RestError,
    },
    #[error(
        "the venue declares {found} ORDERS rate-limit buckets and the engine paces against {max} — pacing against only some of them would spend a budget the venue never granted"
    )]
    TooManyOrderWindows { found: usize, max: usize },
}

pub(crate) async fn preflight_scales(
    registry: &mut Registry,
    env: BinanceEnv,
) -> Result<(), ScaleError> {
    for binance_market in [BinanceMarket::Spot, BinanceMarket::Perpetual] {
        let members: Vec<InstrumentId> = registry
            .instruments()
            .iter()
            .filter(|row| row.market == VenueMarket::Binance(binance_market))
            .map(|row| row.instrument_id)
            .collect();
        if members.is_empty() {
            continue;
        }
        let mut client =
            RestClient::new(binance_market, env).map_err(|source| ScaleError::Unreachable {
                market: binance_market.as_str(),
                source,
            })?;
        for id in members {
            let row = registry.instrument(id);
            let info = client
                .exchange_info(&row.venue_symbol)
                .await
                .map_err(|source| exchange_info_failure(row, binance_market, source))?;
            let scales = check_symbol_scale(row, &info)?;
            registry.set_scales(id, scales.tick_size, scales.step_size);
            registry.set_binance_limits(id, scales.limits);
            registry.set_order_rate_limits(check_order_rate_limits(binance_market, &info)?);
        }
    }
    info!(
        "scale preflight ok on binance {} — every instrument fits the i64 1e-8 range",
        env.as_str()
    );
    Ok(())
}

/// The trading grid and order limits the scale preflight parsed from one instrument's
/// `exchangeInfo` entry — the venue increments a strategy quantises to, plus the floors and counts
/// below which the venue rejects an order outright.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SymbolScales {
    pub tick_size: Price,
    pub step_size: Qty,
    pub limits: BinanceLimits,
}

/// Confirms an instrument's `exchangeInfo` entry carries a max price and quantity that fit the
/// i64 1e-8 range, and returns its grid and order limits. It does no I/O.
///
/// # Errors
/// [`ScaleError::SymbolUnknown`], [`ScaleError::FieldMissing`], [`ScaleError::OutOfRange`],
/// [`ScaleError::NotPositive`], [`ScaleError::LimitNotPositive`].
pub fn check_symbol_scale(
    row: &InstrumentRow,
    info: &ExchangeInfo,
) -> Result<SymbolScales, ScaleError> {
    // SMELL: a row from another venue is reported as a symbol Binance does not list, and the error
    // points at Binance config fields that row does not have. This cannot happen today because only
    // Binance rows are passed in. Taking the Binance market as a parameter would remove the branch.
    let VenueMarket::Binance(binance_market) = row.market else {
        return Err(symbol_unknown(row));
    };
    let Some(symbol) = info
        .symbols
        .iter()
        .find(|entry| entry.symbol.eq_ignore_ascii_case(&row.venue_symbol))
    else {
        return Err(symbol_unknown(row));
    };

    let Some(max_price) = filter_field(&symbol.filters, "PRICE_FILTER", |f| f.max_price.as_deref())
    else {
        return Err(scale_field_missing(row, "PRICE_FILTER.maxPrice"));
    };
    Price::parse_decimal(max_price)
        .map_err(|source| scale_out_of_range(row, "maxPrice", max_price, source))?;

    let Some(max_qty) = filter_field(&symbol.filters, "LOT_SIZE", |f| f.max_qty.as_deref()) else {
        return Err(scale_field_missing(row, "LOT_SIZE.maxQty"));
    };
    Qty::parse_decimal(max_qty)
        .map_err(|source| scale_out_of_range(row, "maxQty", max_qty, source))?;

    let Some(tick) = filter_field(&symbol.filters, "PRICE_FILTER", |f| f.tick_size.as_deref())
    else {
        return Err(scale_field_missing(row, "PRICE_FILTER.tickSize"));
    };
    let tick_size = Price::parse_decimal(tick)
        .map_err(|source| scale_out_of_range(row, "tickSize", tick, source))?;
    if tick_size.0 <= 0 {
        return Err(scale_not_positive(row, "tickSize", tick));
    }

    let Some(step) = filter_field(&symbol.filters, "LOT_SIZE", |f| f.step_size.as_deref()) else {
        return Err(scale_field_missing(row, "LOT_SIZE.stepSize"));
    };
    let step_size = Qty::parse_decimal(step)
        .map_err(|source| scale_out_of_range(row, "stepSize", step, source))?;
    if step_size.0 <= 0 {
        return Err(scale_not_positive(row, "stepSize", step));
    }

    let limits = check_symbol_limits(row, binance_market, &symbol.filters)?;

    Ok(SymbolScales {
        tick_size,
        step_size,
        limits,
    })
}

/// The order floors and counts below which the venue rejects an order. Spot and futures publish
/// them under different filter names, so the market decides which names are read.
fn check_symbol_limits(
    row: &InstrumentRow,
    market: BinanceMarket,
    filters: &[SymbolFilter],
) -> Result<BinanceLimits, ScaleError> {
    // LOT_SIZE, not MARKET_LOT_SIZE: spot quotes the latter as 0.00000000, which would read as no
    // floor at all.
    let Some(min_qty) = filter_field(filters, "LOT_SIZE", |f| f.min_qty.as_deref()) else {
        return Err(scale_field_missing(row, "LOT_SIZE.minQty"));
    };
    let min_qty = Qty::parse_decimal(min_qty)
        .map_err(|source| scale_out_of_range(row, "minQty", min_qty, source))?;
    if min_qty.0 <= 0 {
        return Err(scale_limit_not_positive(
            row,
            "minQty",
            min_qty.0.to_string(),
        ));
    }

    let (notional, notional_field) = match market {
        BinanceMarket::Spot => (
            filter_field(filters, "NOTIONAL", |f| f.min_notional.as_deref()),
            "NOTIONAL.minNotional",
        ),
        BinanceMarket::Perpetual => (
            filter_field(filters, "MIN_NOTIONAL", |f| f.notional.as_deref()),
            "MIN_NOTIONAL.notional",
        ),
    };
    let Some(notional) = notional else {
        return Err(scale_field_missing(row, notional_field));
    };
    // A notional is an amount of quote money, so it uses the same 1e-8 mantissa as quote_volume.
    let min_notional = Qty::parse_decimal(notional)
        .map(|value| value.0)
        .map_err(|source| scale_out_of_range(row, "minNotional", notional, source))?;
    if min_notional <= 0 {
        return Err(scale_limit_not_positive(
            row,
            "minNotional",
            min_notional.to_string(),
        ));
    }

    let (max_num_orders, orders_field) = match market {
        BinanceMarket::Spot => (
            filter_field(filters, "MAX_NUM_ORDERS", |f| f.max_num_orders),
            "MAX_NUM_ORDERS.maxNumOrders",
        ),
        BinanceMarket::Perpetual => (
            filter_field(filters, "MAX_NUM_ORDERS", |f| f.limit),
            "MAX_NUM_ORDERS.limit",
        ),
    };
    let Some(max_num_orders) = max_num_orders else {
        return Err(scale_field_missing(row, orders_field));
    };
    if max_num_orders == 0 {
        return Err(scale_limit_not_positive(
            row,
            "maxNumOrders",
            "0".to_owned(),
        ));
    }

    let max_num_order_amends = match market {
        BinanceMarket::Spot => Some(spot_amend_cap(row, filters)?),
        // The futures schema carries no amend limit, so this is not a field left unread.
        BinanceMarket::Perpetual => None,
    };

    Ok(BinanceLimits {
        min_qty,
        min_notional,
        max_num_orders,
        max_num_order_amends,
    })
}

fn spot_amend_cap(row: &InstrumentRow, filters: &[SymbolFilter]) -> Result<u32, ScaleError> {
    let Some(amends) = filter_field(filters, "MAX_NUM_ORDER_AMENDS", |f| f.max_num_order_amends)
    else {
        return Err(scale_field_missing(
            row,
            "MAX_NUM_ORDER_AMENDS.maxNumOrderAmends",
        ));
    };
    if amends == 0 {
        return Err(scale_limit_not_positive(
            row,
            "maxNumOrderAmends",
            "0".to_owned(),
        ));
    }
    Ok(amends)
}

/// The picked field of the first filter of this type, when both the filter and the field exist.
fn filter_field<'a, T>(
    filters: &'a [SymbolFilter],
    filter_type: &str,
    pick: impl Fn(&'a SymbolFilter) -> Option<T>,
) -> Option<T> {
    filters
        .iter()
        .find(|filter| filter.filter_type.as_ref() == filter_type)
        .and_then(pick)
}

fn exchange_info_failure(
    row: &InstrumentRow,
    market: BinanceMarket,
    source: RestError,
) -> ScaleError {
    if source.is_symbol_rejected() {
        return symbol_unknown(row);
    }
    ScaleError::Unreachable {
        market: market.as_str(),
        source,
    }
}

fn symbol_unknown(row: &InstrumentRow) -> ScaleError {
    ScaleError::SymbolUnknown {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        market: row.market.as_str(),
    }
}

fn scale_field_missing(row: &InstrumentRow, field: &'static str) -> ScaleError {
    ScaleError::FieldMissing {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        field,
    }
}

fn scale_out_of_range(
    row: &InstrumentRow,
    field: &'static str,
    value: &str,
    source: DecimalError,
) -> ScaleError {
    ScaleError::OutOfRange {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        field,
        value: value.into(),
        source,
    }
}

fn scale_not_positive(row: &InstrumentRow, field: &'static str, value: &str) -> ScaleError {
    ScaleError::NotPositive {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        field,
        value: value.into(),
    }
}

// Takes the parsed value, not the venue's text: a limit that refuses here has already survived the
// parse, so the message carries the number the engine would have acted on.
fn scale_limit_not_positive(row: &InstrumentRow, field: &'static str, value: String) -> ScaleError {
    ScaleError::LimitNotPositive {
        instrument: row.instrument_id.0,
        symbol: row.venue_symbol.clone(),
        field,
        value: value.into(),
    }
}

/// Reads every ORDERS bucket the payload carries. Purely a function of its input, with no
/// I/O of its own. An unreadable or absent bucket refuses the run: a budget the engine never read
/// is one it cannot avoid exhausting.
///
/// # Errors
/// [`ScaleError::OrderLimitsMissing`], [`ScaleError::RateLimitUnreadable`].
pub fn check_order_rate_limits(
    market: BinanceMarket,
    info: &ExchangeInfo,
) -> Result<Vec<OrderRateLimit>, ScaleError> {
    let mut limits = Vec::new();
    for entry in info
        .rate_limits
        .iter()
        .filter(|entry| entry.rate_limit_type.as_ref() == ORDERS_LIMIT_TYPE)
    {
        let Some(interval) = RateInterval::parse(&entry.interval) else {
            return Err(unreadable(market, "interval", &entry.interval));
        };
        if entry.interval_num == 0 {
            return Err(unreadable(
                market,
                "intervalNum",
                &entry.interval_num.to_string(),
            ));
        }
        if entry.limit == 0 {
            return Err(unreadable(market, "limit", &entry.limit.to_string()));
        }
        limits.push(OrderRateLimit {
            interval,
            interval_num: entry.interval_num,
            limit: entry.limit,
        });
    }
    if limits.is_empty() {
        return Err(ScaleError::OrderLimitsMissing {
            market: market.as_str(),
        });
    }
    Ok(limits)
}

/// What the engine paces against, from the buckets the venue published.
///
/// # Errors
/// [`ScaleError::TooManyOrderWindows`] when the venue declares more buckets than the
/// engine models.
pub fn order_budget(limits: &[OrderRateLimit]) -> Result<OrderBudget, ScaleError> {
    let windows: Vec<OrderBudgetWindow> = limits
        .iter()
        .map(|limit| OrderBudgetWindow {
            window: DurationUs::from_secs(
                limit.interval.as_secs() as i64 * i64::from(limit.interval_num),
            ),
            // Saturating a cap DOWN refuses earlier, which is the safe direction; no venue
            // publishes an order count anywhere near this, so nothing real is being clamped.
            max_places: u32::try_from(limit.limit).unwrap_or(u32::MAX),
        })
        .collect();
    OrderBudget::of(&windows).ok_or(ScaleError::TooManyOrderWindows {
        found: windows.len(),
        max: MAX_ORDER_BUDGET_WINDOWS,
    })
}

fn unreadable(market: BinanceMarket, field: &'static str, value: &str) -> ScaleError {
    ScaleError::RateLimitUnreadable {
        market: market.as_str(),
        field,
        value: value.into(),
    }
}
