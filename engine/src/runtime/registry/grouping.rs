//! Instruments that share one venue connection form one producer group with one input queue.
//! Binance opens a connection per market and category; Polymarket opens one per series.

use std::ops::Range;

use crate::msg::ids::{InstrumentId, QueueId, SourceId};
use crate::msg::labelled_enum::labelled_enum;
use crate::runtime::config::{ConfigError, Subscriptions, VenueMarket};

use super::{InstrumentRow, queue_id_at};

labelled_enum! {
    /// Binance: 3 categories (one depth carries updates + snapshots). Polymarket: single `Market` channel.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ConnectionCategory {
        Trades = "trades",
        Depth = "depth",
        Klines = "klines",
        Market = "market",
    }
    pub fn as_str;
}

impl ConnectionCategory {
    fn serving(market: VenueMarket) -> &'static [ConnectionCategory] {
        match market {
            VenueMarket::Binance(_) => &[
                ConnectionCategory::Trades,
                ConnectionCategory::Depth,
                ConnectionCategory::Klines,
            ],
            VenueMarket::Polymarket(_) => &[ConnectionCategory::Market],
        }
    }

    fn is_active_for(self, subscriptions: &Subscriptions) -> bool {
        match self {
            ConnectionCategory::Trades => subscriptions.trades,
            ConnectionCategory::Depth => subscriptions.book_updates || subscriptions.book_snapshots,
            ConnectionCategory::Klines => subscriptions.klines,
            ConnectionCategory::Market => {
                subscriptions.trades || subscriptions.book_updates || subscriptions.book_snapshots
            }
        }
    }
}

/// Instruments sharing (market, category) -> one connection + queue. Binance: 3 groups; Poly: 1.
#[derive(Debug, Clone)]
pub struct ProducerGroup {
    pub source_id: SourceId,
    pub queue_id: QueueId,
    pub market: VenueMarket,
    pub category: ConnectionCategory,
    pub instruments: Vec<InstrumentId>,
}

// Group order fixes the queue and source ids, and those ids are recorded on every message, so a
// new order would re-attribute every recorded tape.
/// The connections of each source, in source order: each source reads its own rows through its own
/// connection for each category its market serves.
pub(super) fn group_producers(
    instruments: &[InstrumentRow],
    source_rows: &[Range<usize>],
) -> Result<Vec<ProducerGroup>, ConfigError> {
    let mut groups = Vec::new();
    for rows in source_rows {
        let Some(first) = instruments.get(rows.start) else {
            continue;
        };
        push_market_groups(&mut groups, &instruments[rows.clone()], first.market)?;
    }
    Ok(groups)
}

fn push_market_groups(
    groups: &mut Vec<ProducerGroup>,
    instruments: &[InstrumentRow],
    market: VenueMarket,
) -> Result<(), ConfigError> {
    for &category in ConnectionCategory::serving(market) {
        let subscribed: Vec<InstrumentId> = instruments
            .iter()
            .filter(|row| row.market == market && category.is_active_for(&row.subscriptions))
            .map(|row| row.instrument_id)
            .collect();
        if subscribed.is_empty() {
            continue;
        }
        let ordinal = groups.len();
        groups.push(ProducerGroup {
            source_id: SourceId(ordinal as u16),
            queue_id: queue_id_at(ordinal)?,
            market,
            category,
            instruments: subscribed,
        });
    }
    Ok(())
}
