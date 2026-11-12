//! Builds the monitor scenes by feeding ordered events through the real model, as the link would.

use polysim::hot::exec::{CloseReason, ExecHalt, OrderState, RejectOrigin};
use polysim::ids::{AssetId, ClientOrderId};
use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::Liquidity;
use polysim::msg::inbound::Level;
use polysim::msg::persist::FeatureId;
use polysim::msg::ui::{DomQuote, UiBookState, UiCatalog, UiEvent};
use polysim::time::DurationUs;
use polysim::time::TsUs;
use polysim::workstation::model::UiModel;
use polysim::workstation::monitor::model::SystemNote;
use polysim::workstation::monitor::view::MonitorUiState;

use crate::fixture::{BASE_TS, book, qm};

pub const SPIN: DurationUs = DurationUs::from_micros(100_000);
pub const QUOTE_QTY_MILLI: i64 = 9_000;

pub struct MonitorScene {
    pub name: &'static str,
    pub model: UiModel,
    pub feature_names: Vec<Box<str>>,
    pub instrument_names: Vec<Box<str>>,
    pub instrument: InstrumentId,
    pub tick: Price,
    pub qty_scale: i64,
    pub qty_decimals: usize,
    pub state: MonitorUiState,
}

/// A commission in units of 1e-8, picked to look real rather than round.
const FILL_COMMISSION: i64 = 944_000;

struct OrderStep {
    ts: i64,
    id: u64,
    side: Side,
    tick: i64,
    state: OrderState,
    filled: Qty,
}

/// Every price a feed carries is a whole number of ticks on its grid, so a scene sets its scale
/// once.
pub struct Feed {
    model: UiModel,
    tick: Price,
    seq: u64,
    book_seq: u64,
}

impl Feed {
    /// Prices on a grid of one whole unit, the grid the first four scenes use.
    pub fn new(instruments: usize, features: usize) -> Self {
        Self::with_tick(instruments, features, TICK)
    }

    pub fn with_tick(instruments: usize, features: usize, tick: Price) -> Self {
        Self {
            model: UiModel::with_monitor_capacity(instruments, features, SPIN),
            tick,
            seq: 0,
            book_seq: 0,
        }
    }

    /// Reconfigures the monitor's grid and spin interval, so it comes before any event.
    pub fn set_catalog(&mut self, catalog: UiCatalog) {
        self.model.set_catalog(catalog);
    }

    fn price(&self, tick_index: i64) -> Price {
        Price(tick_index * self.tick.0)
    }

    fn levels(&self, rows: &[(i64, i64)]) -> Vec<Level> {
        rows.iter()
            .map(|&(tick_index, milli)| Level {
                price: self.price(tick_index),
                qty: qm(milli),
            })
            .collect()
    }

    pub fn lifecycle(&mut self, note: SystemNote) {
        self.model.note_lifecycle(note);
    }

    pub fn book(&mut self, state: UiBookState, ts: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        self.book_seq += 1;
        self.model.apply_book(book(
            self.book_seq,
            BASE_TS + ts,
            state,
            &self.levels(bids),
            &self.levels(asks),
        ));
    }

    pub fn quote(&mut self, ts: i64, bid: Option<i64>, ask: Option<i64>) {
        let quote = DomQuote::top(
            bid.map(|tick| (self.price(tick), qm(QUOTE_QTY_MILLI))),
            ask.map(|tick| (self.price(tick), qm(QUOTE_QTY_MILLI))),
        );
        self.apply(UiEvent::Quote {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(ts),
            quote,
        });
    }

    pub fn trade(&mut self, ts: i64, aggressor: Side, tick: i64, milli: i64) {
        self.apply(UiEvent::Trade {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(ts),
            aggressor,
            price: self.price(tick),
            qty: qm(milli),
        });
    }

    pub fn feature(&mut self, feature: u16, ts: i64, value: f64) {
        self.apply(UiEvent::Feature {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(ts),
            feature: FeatureId(feature),
            value,
        });
    }

    pub fn order(&mut self, ts: i64, id: u64, side: Side, tick: i64, state: OrderState) {
        let filled = if matches!(state, OrderState::Closed(CloseReason::Filled)) {
            qm(QUOTE_QTY_MILLI)
        } else {
            Qty(0)
        };
        self.send_order(OrderStep {
            ts,
            id,
            side,
            tick,
            state,
            filled,
        });
    }

    /// A resting order that has filled `filled_milli` of its size.
    pub fn partial_order(&mut self, ts: i64, id: u64, side: Side, tick: i64, filled_milli: i64) {
        self.send_order(OrderStep {
            ts,
            id,
            side,
            tick,
            state: OrderState::Live,
            filled: qm(filled_milli),
        });
    }

    fn send_order(&mut self, step: OrderStep) {
        self.apply(UiEvent::OrderUpdate {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(step.ts),
            client_id: ClientOrderId(step.id),
            side: step.side,
            state: step.state,
            price: self.price(step.tick),
            qty: qm(QUOTE_QTY_MILLI),
            filled: step.filled,
            quote_level: None,
        });
    }

    pub fn refusal(&mut self, ts: i64, side: Side, origin: RejectOrigin) {
        self.apply(UiEvent::Reject {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(ts),
            side,
            origin,
        });
    }

    pub fn fill(&mut self, ts: i64, side: Side, tick: i64, milli: i64) {
        self.apply(UiEvent::Fill {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(ts),
            side,
            price: self.price(tick),
            qty: qm(milli),
            commission: FILL_COMMISSION,
            commission_asset: AssetId(1),
            liquidity: Some(Liquidity::Maker),
            quote_level: None,
        });
    }

    pub fn balance(&mut self, ts: i64, asset: AssetId, free: i64, locked: i64) {
        self.apply(UiEvent::Balance {
            asset,
            seq: 0,
            event_ts_us: ts_at(ts),
            free,
            locked,
        });
    }

    pub fn execution(&mut self, ts: i64, halt: ExecHalt) {
        self.apply(UiEvent::Execution {
            seq: 0,
            event_ts_us: ts_at(ts),
            halt,
        });
    }

    pub fn rotation(&mut self, ts: i64) {
        self.apply(UiEvent::Rotation {
            instrument: InstrumentId(0),
            seq: 0,
            event_ts_us: ts_at(ts),
        });
    }

    pub fn skip(&mut self, count: u64) {
        self.seq += count;
    }

    pub fn system_appended(&self) -> u64 {
        self.model.monitor().system_appended()
    }

    pub fn orders_appended(&self) -> u64 {
        self.model.monitor().orders_appended()
    }

    fn apply(&mut self, mut event: UiEvent) {
        event.set_seq(self.seq);
        self.seq += 1;
        self.model.apply_event(event);
    }

    pub fn finish(
        self,
        name: &'static str,
        features: &[&str],
        state: MonitorUiState,
    ) -> MonitorScene {
        MonitorScene {
            name,
            model: self.model,
            feature_names: features
                .iter()
                .map(|name| Box::<str>::from(*name))
                .collect(),
            instrument_names: vec![Box::<str>::from("BTCUSDT")],
            instrument: InstrumentId(0),
            tick: self.tick,
            qty_scale: FIXED_SCALE,
            qty_decimals: 3,
            state,
        }
    }
}

const TICK: Price = Price(FIXED_SCALE);

fn ts_at(relative: i64) -> TsUs {
    TsUs::from_micros(BASE_TS + relative)
}
