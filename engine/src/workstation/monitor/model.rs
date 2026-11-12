//! The feature store and the channel histories: pure data that folds events in event time and never
//! reads the wall clock.

use crate::hot::exec::{OrderState, QuoteLevel, RejectOrigin};
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::Liquidity;
use crate::msg::persist::FeatureId;
use crate::msg::ui::{UiBookState, UiEvent};
use crate::time::{DurationUs, TsUs};
use crate::workstation::history::{BoundedHistory, grow_to};

/// A few hundred public trades is about five minutes of tape: scrollback to glance at, not a
/// ledger.
const TRADE_TAPE_CAPACITY: usize = 256;

/// The other channels keep their newest 256 rows each.
const SYSTEM_CAPACITY: usize = 256;
const ORDER_CAPACITY: usize = 256;
const FILL_CAPACITY: usize = 256;

/// Every emission moves `last_update_ts`. Only a different value moves `last_changed_ts`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeatureCell {
    pub value: f64,
    pub last_update_ts: TsUs,
    pub last_changed_ts: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeRow {
    pub at: TsUs,
    pub aggressor: Side,
    pub price: Price,
    pub qty: Qty,
}

/// An order's state change or a refusal. Both answer what happened to an order, so they share the
/// Orders tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderEvent {
    Transition {
        client_id: ClientOrderId,
        state: OrderState,
        price: Price,
        qty: Qty,
        filled: Qty,
    },
    Refused {
        origin: RejectOrigin,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderRow {
    pub at: TsUs,
    pub instrument: InstrumentId,
    pub quote_level: Option<QuoteLevel>,
    pub side: Side,
    pub event: OrderEvent,
}

/// One of this engine's fills as the venue reported it. The commission is in `commission_asset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillRow {
    pub at: TsUs,
    pub instrument: InstrumentId,
    pub quote_level: Option<QuoteLevel>,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub commission: i64,
    pub commission_asset: AssetId,
    /// `None` when the venue did not say. That is not the same as taker, which pays a different
    /// fee.
    pub liquidity: Option<Liquidity>,
}

/// An engine lifecycle note for the System channel. The workstation adds it, and nothing in the
/// model reads it.
// SMELL: nothing the engine sends produces `Starting`. Only the dom-fixture scenes note it, so the
// fixture shows a System row the live workstation never shows. Without it this enum has one value
// and should fold into `SystemEvent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemNote {
    Starting,
    Ready,
}

/// What a System row reports, kept typed rather than as text so its words live in one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemEvent {
    Lifecycle(SystemNote),
    Rotation {
        instrument: InstrumentId,
    },
    /// The book went from awaiting a snapshot back to valid.
    BookResynced {
        instrument: InstrumentId,
    },
    EventsLost {
        count: u64,
    },
    BooksLost {
        count: u64,
    },
}

/// `at` is the event time of what the row reports, or `None` for a lifecycle note, which has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemRow {
    pub at: Option<TsUs>,
    pub event: SystemEvent,
}

/// Per-instrument storage grows on demand, because events can arrive before the catalog.
pub struct MonitorModel {
    feature_count: usize,
    features: Vec<Option<FeatureCell>>,
    latest_feed_ts_us: Vec<Option<TsUs>>,
    book_state: Vec<Option<UiBookState>>,
    trades: Vec<BoundedHistory<TradeRow>>,
    system: BoundedHistory<SystemRow>,
    orders: BoundedHistory<OrderRow>,
    fills: BoundedHistory<FillRow>,
    spin_interval: DurationUs,
}

impl MonitorModel {
    pub fn with_capacity(
        instrument_count: usize,
        feature_count: usize,
        spin_interval: DurationUs,
    ) -> Self {
        let mut model = Self {
            feature_count,
            features: Vec::new(),
            latest_feed_ts_us: Vec::new(),
            book_state: Vec::new(),
            trades: Vec::new(),
            system: BoundedHistory::new(SYSTEM_CAPACITY),
            orders: BoundedHistory::new(ORDER_CAPACITY),
            fills: BoundedHistory::new(FILL_CAPACITY),
            spin_interval,
        };
        model.configure(instrument_count, feature_count, spin_interval);
        model
    }

    /// Takes the run's dimensions once the catalog arrives. Rebuilding the feature grid loses
    /// nothing, because no feature can be stored before then.
    pub(crate) fn configure(
        &mut self,
        instrument_count: usize,
        feature_count: usize,
        spin_interval: DurationUs,
    ) {
        self.spin_interval = spin_interval;
        self.feature_count = feature_count;
        self.ensure_instruments(instrument_count);
        self.features = vec![None; self.latest_feed_ts_us.len() * feature_count];
    }

    pub(crate) fn apply_event(&mut self, event: &UiEvent) {
        match *event {
            UiEvent::Quote {
                instrument,
                event_ts_us,
                ..
            } => self.touch_feed(instrument, event_ts_us),
            UiEvent::Trade {
                instrument,
                event_ts_us,
                aggressor,
                price,
                qty,
                ..
            } => {
                self.touch_feed(instrument, event_ts_us);
                self.trades[instrument.0 as usize].push(TradeRow {
                    at: event_ts_us,
                    aggressor,
                    price,
                    qty,
                });
            }
            UiEvent::OrderUpdate {
                instrument,
                event_ts_us,
                client_id,
                quote_level,
                side,
                state,
                price,
                qty,
                filled,
                ..
            } => {
                self.touch_feed(instrument, event_ts_us);
                self.orders.push(OrderRow {
                    at: event_ts_us,
                    instrument,
                    quote_level,
                    side,
                    event: OrderEvent::Transition {
                        client_id,
                        state,
                        price,
                        qty,
                        filled,
                    },
                });
            }
            UiEvent::OrderSnapshot {
                instrument,
                event_ts_us,
                ..
            } => self.touch_feed(instrument, event_ts_us),
            UiEvent::Reject {
                instrument,
                event_ts_us,
                side,
                origin,
                ..
            } => {
                self.touch_feed(instrument, event_ts_us);
                self.orders.push(OrderRow {
                    at: event_ts_us,
                    instrument,
                    quote_level: None,
                    side,
                    event: OrderEvent::Refused { origin },
                });
            }
            UiEvent::Balance { .. } | UiEvent::Execution { .. } | UiEvent::Latency { .. } => {}
            UiEvent::Feature {
                instrument,
                event_ts_us,
                feature,
                value,
                ..
            } => {
                self.touch_feed(instrument, event_ts_us);
                self.apply_feature(instrument, feature, value, event_ts_us);
            }
            UiEvent::Fill {
                instrument,
                event_ts_us,
                quote_level,
                side,
                price,
                qty,
                commission,
                commission_asset,
                liquidity,
                ..
            } => {
                self.touch_feed(instrument, event_ts_us);
                self.fills.push(FillRow {
                    at: event_ts_us,
                    instrument,
                    quote_level,
                    side,
                    price,
                    qty,
                    commission,
                    commission_asset,
                    liquidity,
                });
            }
            UiEvent::Rotation {
                instrument,
                event_ts_us,
                ..
            } => {
                self.touch_feed(instrument, event_ts_us);
                self.system.push(SystemRow {
                    at: Some(event_ts_us),
                    event: SystemEvent::Rotation { instrument },
                });
            }
            UiEvent::Position {
                instrument,
                event_ts_us,
                ..
            } => self.touch_feed(instrument, event_ts_us),
        }
    }

    /// Adds one System row when a book comes back from awaiting a snapshot to valid. Losing the
    /// book adds no row of its own, so each repair reads as one line.
    pub(crate) fn observe_book_state(
        &mut self,
        instrument: InstrumentId,
        state: UiBookState,
        at: TsUs,
    ) {
        let index = instrument.0 as usize;
        self.ensure_instruments(index + 1);
        let previous = self.book_state[index].replace(state);
        if previous != Some(UiBookState::AwaitingSnapshot) || state != UiBookState::Valid {
            return;
        }
        self.system.push(SystemRow {
            at: Some(at),
            event: SystemEvent::BookResynced { instrument },
        });
    }

    pub(crate) fn note_events_lost(&mut self, count: u64, at: TsUs) {
        self.system.push(SystemRow {
            at: Some(at),
            event: SystemEvent::EventsLost { count },
        });
    }

    pub(crate) fn note_books_lost(&mut self, count: u64, at: TsUs) {
        self.system.push(SystemRow {
            at: Some(at),
            event: SystemEvent::BooksLost { count },
        });
    }

    pub(crate) fn note_lifecycle(&mut self, note: SystemNote) {
        self.system.push(SystemRow {
            at: None,
            event: SystemEvent::Lifecycle(note),
        });
    }

    /// `None` when the feature was never emitted or the id is out of range.
    pub fn feature(&self, instrument: InstrumentId, feature: FeatureId) -> Option<FeatureCell> {
        if self.feature_count == 0 || feature.0 as usize >= self.feature_count {
            return None;
        }
        let index = instrument.0 as usize * self.feature_count + feature.0 as usize;
        self.features.get(index).copied().flatten()
    }

    pub fn feature_count(&self) -> usize {
        self.feature_count
    }

    /// Feature freshness is measured against this event time, so it never depends on the
    /// workstation's own clock.
    pub(crate) fn latest_feed_ts_us(&self, instrument: InstrumentId) -> Option<TsUs> {
        self.latest_feed_ts_us
            .get(instrument.0 as usize)
            .copied()
            .flatten()
    }

    pub fn spin_interval(&self) -> DurationUs {
        self.spin_interval
    }

    /// Newest first. Empty for an instrument not seen yet.
    pub fn trades(&self, instrument: InstrumentId) -> impl Iterator<Item = &TradeRow> {
        self.trades
            .get(instrument.0 as usize)
            .into_iter()
            .flat_map(BoundedHistory::iter_newest_first)
    }

    pub fn system(&self) -> impl Iterator<Item = &SystemRow> {
        self.system.iter_newest_first()
    }

    pub fn orders(&self) -> impl Iterator<Item = &OrderRow> {
        self.orders.iter_newest_first()
    }

    pub fn fills(&self) -> impl Iterator<Item = &FillRow> {
        self.fills.iter_newest_first()
    }

    /// It only grows, so the Pub trades tab can count the rows it has not shown yet.
    pub fn trades_appended(&self, instrument: InstrumentId) -> u64 {
        self.trades
            .get(instrument.0 as usize)
            .map_or(0, BoundedHistory::appended)
    }

    pub fn system_appended(&self) -> u64 {
        self.system.appended()
    }

    pub fn orders_appended(&self) -> u64 {
        self.orders.appended()
    }

    pub(crate) fn fills_appended(&self) -> u64 {
        self.fills.appended()
    }

    fn touch_feed(&mut self, instrument: InstrumentId, at: TsUs) {
        let index = instrument.0 as usize;
        self.ensure_instruments(index + 1);
        let slot = &mut self.latest_feed_ts_us[index];
        *slot = (*slot).max(Some(at));
    }

    fn apply_feature(
        &mut self,
        instrument: InstrumentId,
        feature: FeatureId,
        value: f64,
        at: TsUs,
    ) {
        if self.feature_count == 0 || feature.0 as usize >= self.feature_count {
            return;
        }
        let index = instrument.0 as usize * self.feature_count + feature.0 as usize;
        match self.features[index] {
            Some(ref mut cell) => {
                // Compare bits, not values: a re-emitted NaN is unchanged, and -0.0 differs from
                // +0.0.
                let changed = cell.value.to_bits() != value.to_bits();
                cell.value = value;
                cell.last_update_ts = at;
                if changed {
                    cell.last_changed_ts = at;
                }
            }
            None => {
                self.features[index] = Some(FeatureCell {
                    value,
                    last_update_ts: at,
                    last_changed_ts: at,
                });
            }
        }
    }

    fn ensure_instruments(&mut self, len: usize) {
        if self.latest_feed_ts_us.len() >= len {
            return;
        }
        self.latest_feed_ts_us.resize(len, None);
        self.book_state.resize(len, None);
        grow_to(&mut self.trades, len, || {
            BoundedHistory::new(TRADE_TAPE_CAPACITY)
        });
        self.features.resize(len * self.feature_count, None);
    }
}
