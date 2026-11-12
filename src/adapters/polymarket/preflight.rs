//! Startup scale preflight for Polymarket: resolve the current and next window through Gamma,
//! confirm each quotes on a tick the engine accepts with a positive minimum size, and stamp that
//! grid and the order limits onto every Polymarket row. Any failure refuses the start loudly.

use crate::config::{MAX_QUOTE_LEVELS, PolySeries, VenueMarket};
use crate::ids::{FIXED_SCALE, InstrumentId, Price, Qty};
use crate::info;
use crate::registry::{PolyLimits, Registry};
use crate::time::EngineClock;

use super::rest::{GammaError, GammaMarket, PolyRest};

/// Quote ticks for btc-updown-5m: 0.01 normally, and 0.001 near the 0/1 bounds at resolution.
/// Any other tick refuses the start.
const POLY_ACCEPTED_TICKS: [Price; 2] = [Price(FIXED_SCALE / 100), Price(FIXED_SCALE / 1000)];

/// Polymarket sizes in shares to two decimals, whatever the tick.
const POLY_SHARE_STEP: Qty = Qty(FIXED_SCALE / 100);

/// Our own ceiling, not the venue's: Polymarket publishes no per-market order count, and an
/// unstamped one leaves the edge's mirror sized zero and the first placement fatal. Four full
/// ladders is room for the working set plus orders the venue has not yet confirmed gone.
const POLY_MAX_ORDERS: u32 = 4 * MAX_QUOTE_LEVELS as u32;

#[derive(thiserror::Error, Debug)]
pub enum ScaleError {
    #[error("gamma preflight failed for {series} — refusing to start unvalidated")]
    Unreachable {
        series: &'static str,
        #[source]
        source: GammaError,
    },
    #[error("polymarket {series} not resolvable — gamma returned no current/next window")]
    SeriesUnknown {
        series: &'static str,
        #[source]
        source: GammaError,
    },
    #[error(
        "polymarket {symbol} tick {} not in accepted set {{{}, {}}} — book capacity and 1e-8 scale assume these",
        actual.to_f64(),
        expected[0].to_f64(),
        expected[1].to_f64()
    )]
    TickNotAccepted {
        symbol: Box<str>,
        expected: [Price; 2],
        actual: Price,
    },
    #[error(
        "polymarket {symbol} min order size {} is not positive — orderMinSize absent or malformed",
        value.to_f64()
    )]
    MinSizeNotPositive { symbol: Box<str>, value: Qty },
}

/// Resolves the current and next btc-updown-5m windows through Gamma, confirms both quote on an
/// accepted tick, and stamps the current grid onto every Polymarket row. A run with no Polymarket
/// source returns at once. The stamp is taken once and a tick change mid-run is not re-stamped, so
/// a price off that grid shows as invalid rather than being silently rounded.
pub(crate) async fn preflight_poly(registry: &mut Registry) -> Result<(), ScaleError> {
    let Some(series) = poly_series(registry) else {
        return Ok(());
    };
    let rest = PolyRest::new(series).map_err(|error| classify_poly_resolve(series, error))?;
    let now = EngineClock::start().now();
    let (current, next) = rest
        .resolve_current_and_next(now)
        .await
        .map_err(|error| classify_poly_resolve(series, error))?;
    check_poly_market(&current)?;
    check_poly_market(&next)?;
    stamp_poly_scales(registry, &current);
    info!(
        "polymarket preflight ok — {} current tick {} next tick {} (current grid stamped on all slots)",
        series.as_str(),
        current.tick_size.to_f64(),
        next.tick_size.to_f64()
    );
    Ok(())
}

/// Stamps the trading grid and order limits onto every Polymarket row, one grid across all slots.
///
/// `orderMinSize` lands on `min_qty`, where a floor belongs, and `lot_size` carries the venue's
/// two-decimal share step. Putting the floor in `lot_size` instead would quantise every size to
/// whole multiples of the minimum and leave the engine reading no floor at all.
pub fn stamp_poly_scales(registry: &mut Registry, market: &GammaMarket) {
    let poly_ids: Vec<InstrumentId> = registry
        .instruments()
        .iter()
        .filter(|row| matches!(row.market, VenueMarket::Polymarket(_)))
        .map(|row| row.instrument_id)
        .collect();
    for id in poly_ids {
        registry.set_scales(id, market.tick_size, POLY_SHARE_STEP);
        registry.set_poly_limits(
            id,
            PolyLimits {
                min_qty: market.min_order_size,
                max_num_orders: POLY_MAX_ORDERS,
                max_price: Price(FIXED_SCALE - market.tick_size.0),
            },
        );
    }
}

/// Confirms a resolved market quotes on an accepted tick with a positive minimum order size. It
/// does no I/O.
///
/// # Errors
/// [`ScaleError::TickNotAccepted`], [`ScaleError::MinSizeNotPositive`].
pub fn check_poly_market(market: &GammaMarket) -> Result<(), ScaleError> {
    if !POLY_ACCEPTED_TICKS.contains(&market.tick_size) {
        return Err(ScaleError::TickNotAccepted {
            symbol: market.slug.clone(),
            expected: POLY_ACCEPTED_TICKS,
            actual: market.tick_size,
        });
    }
    if market.min_order_size.0 <= 0 {
        return Err(ScaleError::MinSizeNotPositive {
            symbol: market.slug.clone(),
            value: market.min_order_size,
        });
    }
    Ok(())
}

/// A missing market or too few windows means the series is unknown; any other Gamma failure means
/// Gamma was unreachable.
fn classify_poly_resolve(series: PolySeries, error: GammaError) -> ScaleError {
    match error {
        GammaError::MarketNotFound { .. } | GammaError::FallbackTooFew { .. } => {
            ScaleError::SeriesUnknown {
                series: series.as_str(),
                source: error,
            }
        }
        other => ScaleError::Unreachable {
            series: series.as_str(),
            source: other,
        },
    }
}

/// The Polymarket series this run trades, if it trades one.
fn poly_series(registry: &Registry) -> Option<PolySeries> {
    registry
        .instruments()
        .iter()
        .find_map(|row| match row.market {
            VenueMarket::Polymarket(series) => Some(series),
            VenueMarket::Binance(_) => None,
        })
}
