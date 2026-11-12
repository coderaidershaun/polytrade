//! `SourceSpec` -> instrument rows: one venue's config block becomes the dense, frozen rows every
//! later stage addresses by [`InstrumentId`]. Row order fixes those ids, so it is replay-visible.

use crate::config::{
    BinanceEnv, ConfigError, Instruments, KlineInterval, SourceSpec, Subscriptions, TrackerSpec,
    VenueMarket, validated_mantissa,
};
use crate::ids::{AssetId, FIXED_SCALE, InstrumentId};

use super::InstrumentRow;
use super::assets::{AssetDictionary, intern_assets};
use super::validate::{validate_binance_tracker, validate_poly_tracker};

const MAX_INSTRUMENTS: usize = u16::MAX as usize + 1;

// Symbol naming no row = silently void config promise; reject here where config + rows coexist. Case-insensitive.
pub(super) fn check_strategy_instruments(
    filter: &Instruments,
    instruments: &[InstrumentRow],
) -> Result<(), ConfigError> {
    let Instruments::Explicit(symbols) = filter else {
        return Ok(());
    };
    for symbol in symbols {
        let wanted = symbol.to_lowercase();
        if !instruments
            .iter()
            .any(|row| row.venue_symbol.to_lowercase() == wanted)
        {
            return Err(ConfigError::UnknownStrategyInstrument {
                symbol: symbol.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn build_instrument_rows(
    source: &SourceSpec,
) -> Result<(Vec<InstrumentRow>, AssetDictionary), ConfigError> {
    let mut instruments = Vec::new();
    match source {
        SourceSpec::Binance {
            market,
            env: _,
            base,
            quote,
            subscriptions,
            kline_intervals,
            book_capacity,
            max_exposure_quote,
            tracker,
        } => {
            validate_binance_tracker(tracker, kline_intervals)?;
            let spec = SourceRowSpec {
                market: VenueMarket::Binance(*market),
                subscriptions: *subscriptions,
                kline_intervals: kline_intervals.clone(),
                book_capacity: *book_capacity,
                max_exposure_quote: validated_mantissa(*max_exposure_quote),
                tracker: tracker.clone(),
            };
            let names = RowNames {
                venue_symbol: format!("{}{}", base.to_lowercase(), quote.to_lowercase()).into(),
                display: format!(
                    "{}/{} {}",
                    base.to_uppercase(),
                    quote.to_uppercase(),
                    market.as_str()
                )
                .into(),
                base: base.to_uppercase().into(),
                quote: quote.to_uppercase().into(),
            };
            spec.push_row(&mut instruments, names);
        }
        SourceSpec::Polymarket {
            series,
            subscriptions,
            book_capacity,
            max_exposure_quote,
            tracker,
        } => {
            validate_poly_tracker(tracker)?;
            let spec = SourceRowSpec {
                market: VenueMarket::Polymarket(*series),
                subscriptions: Subscriptions::from(*subscriptions),
                kline_intervals: Vec::new(),
                book_capacity: *book_capacity,
                max_exposure_quote: validated_mantissa(*max_exposure_quote),
                tracker: tracker.clone(),
            };
            // The base asset is the LEG, not the underlying. Every slot here is a distinct
            // conditional token with its own share balance, and a shared "BTC" would have all four
            // legs reading one balance — the sell-side funds gate would then see another leg's
            // inventory as this one's, or zero forever.
            for venue_symbol in series.slot_symbols() {
                let names = RowNames {
                    display: format!("polymarket {venue_symbol}").into(),
                    base: venue_symbol.clone(),
                    quote: "USD".into(),
                    venue_symbol,
                };
                spec.push_row(&mut instruments, names);
            }
        }
    }
    let assets = intern_assets(&mut instruments);
    Ok((instruments, assets))
}

pub(super) fn binance_env(source: &SourceSpec) -> Option<BinanceEnv> {
    match source {
        SourceSpec::Binance { env, .. } => Some(*env),
        SourceSpec::Polymarket { .. } => None,
    }
}

/// Everything the rows of one source share; only the names differ between them.
struct SourceRowSpec {
    market: VenueMarket,
    subscriptions: Subscriptions,
    kline_intervals: Vec<KlineInterval>,
    book_capacity: usize,
    max_exposure_quote: i64,
    tracker: TrackerSpec,
}

struct RowNames {
    venue_symbol: Box<str>,
    display: Box<str>,
    base: Box<str>,
    quote: Box<str>,
}

impl SourceRowSpec {
    // Tick, lot and the venue's order limits are left unset here. The venue's preflight stamps
    // them once, before the registry freezes.
    fn push_row(&self, instruments: &mut Vec<InstrumentRow>, names: RowNames) {
        let instrument_id = issue_instrument_id(instruments);
        instruments.push(InstrumentRow {
            instrument_id,
            market: self.market,
            venue_symbol: names.venue_symbol,
            display: names.display,
            base: names.base,
            quote: names.quote,
            base_asset: AssetId::UNKNOWN,
            quote_asset: AssetId::UNKNOWN,
            tick_size: None,
            lot_size: None,
            min_qty: None,
            min_notional: None,
            max_num_orders: None,
            max_num_order_amends: None,
            max_price: None,
            qty_scale: FIXED_SCALE,
            subscriptions: self.subscriptions,
            kline_intervals: self.kline_intervals.clone(),
            book_capacity: self.book_capacity,
            max_exposure_quote: self.max_exposure_quote,
            tracker: self.tracker.clone(),
        });
    }
}

// u16 id space unreachable by config; only row-building bug can violate.
fn issue_instrument_id(instruments: &[InstrumentRow]) -> InstrumentId {
    debug_assert!(
        instruments.len() < MAX_INSTRUMENTS,
        "{} instrument rows from one source, max {MAX_INSTRUMENTS}",
        instruments.len() + 1
    );
    InstrumentId(instruments.len() as u16)
}
