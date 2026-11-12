//! What the engine tells a workstation: its lifecycle, its instrument catalog, and the live book
//! and event feed. Nothing here names a GUI toolkit, so the engine never depends on one.

mod latency;

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use rtrb::{Consumer, RingBuffer};

use crate::config::{ExecutionMode, StrategyId};
use crate::hot::exec::{ExecHalt, OrderState, QuoteLevel, RejectOrigin};
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::Liquidity;
use crate::msg::inbound::Level;
use crate::msg::persist::FeatureId;
use crate::registry::Registry;
use crate::sink::{UiBookSink, UiEventSink};
use crate::time::TsUs;

pub use latency::{UiLatencyCell, UiLatencyRow, UiLatencySummary};

/// A run sends only a handful of lifecycle messages, and each is sent with `try_send`, so a full
/// channel drops a message instead of stalling the engine.
const LIFECYCLE_CAPACITY: usize = 16;

/// Book levels per side. The ladder shows 12 rows a side, but a grouped row sums several ticks, so
/// a 16-level feed would leave a grouped ladder half empty.
pub const UI_BOOK_LEVELS: usize = 32;

/// Quote levels per side, equal to the execution ladder's fixed capacity.
pub const UI_QUOTE_LEVELS: usize = 8;

/// How many working orders one snapshot lists in full, one per quote level. A side holding more
/// still reports its true total, so the excess stays visible.
pub const UI_ORDER_SNAPSHOT_CAPACITY: usize = UI_QUOTE_LEVELS;

/// The most working orders one side can report: 32 from this run plus 256 repair slots for orders
/// left by an earlier run. A side normally holds one.
pub const UI_ORDER_SNAPSHOT_MAX_TOTAL: u16 = 288;

// SMELL: the two ring capacities below are public only because the fitness suite builds an engine
// with the real ring sizes. A test is not a reason to make an item public.
/// The book ring takes tens of commits a second. When it is full a snapshot is dropped and counted,
/// so the hot thread never stalls.
pub const UI_BOOK_RING_CAPACITY: usize = 1024;

/// The event ring takes every public trade plus at least 63 features a spin, which is hundreds of
/// events a second. When it is full an event is dropped and counted.
pub const UI_EVENT_RING_CAPACITY: usize = 16_384;

/// The engine's own rare state changes. There is no starting phase, because a workstation only ever
/// attaches to an engine that is already running.
#[derive(Debug, Clone)]
pub(crate) enum UiLifecycle {
    Ready(UiCatalog),
    Draining,
}

/// The fixed shape of a run, copied from the registry once preflight has frozen it. Public because
/// a workstation rebuilds it from link frames.
#[derive(Debug, Clone)]
pub struct UiCatalog {
    pub strategy_id: Box<str>,
    pub window_title: Box<str>,
    /// Drives the execution-mode badge.
    pub execution_mode: Option<ExecutionMode>,
    pub spin_interval_us: u64,
    pub instruments: Vec<UiInstrument>,
    /// Indexed by `FeatureId`, as the strategy declared them at startup, so the monitor can label
    /// each feature.
    pub feature_names: Vec<Box<str>>,
}

impl UiCatalog {
    pub fn instrument(&self, instrument: InstrumentId) -> Option<&UiInstrument> {
        self.instruments
            .iter()
            .find(|candidate| candidate.instrument_id == instrument)
    }

    /// Takes `feature_names` as given, because by the time the catalog is built the strategy has
    /// moved into the hot engine.
    pub(crate) fn from_registry(
        strategy_id: &StrategyId,
        execution_mode: Option<ExecutionMode>,
        spin_interval_us: u64,
        feature_names: Vec<Box<str>>,
        registry: &Registry,
    ) -> Self {
        let instruments = registry
            .instruments()
            .iter()
            .map(|row| UiInstrument {
                instrument_id: row.instrument_id,
                display: row.display.clone(),
                base: row.base.clone(),
                quote: row.quote.clone(),
                base_asset: row.base_asset,
                quote_asset: row.quote_asset,
                tick_size: row.tick_size,
                lot_size: row.lot_size,
                qty_scale: row.qty_scale,
            })
            .collect();
        Self {
            strategy_id: strategy_id.as_str().into(),
            window_title: format!("Polysim — {}", strategy_id.as_str()).into(),
            execution_mode,
            spin_interval_us,
            instruments,
            feature_names,
        }
    }
}

/// A registry row cut down to what a workstation shows. `tick_size` and `lot_size` stay `None`
/// until the venue reports them. `base_asset` and `quote_asset` say which balance events are this
/// instrument's.
#[derive(Debug, Clone)]
pub struct UiInstrument {
    pub instrument_id: InstrumentId,
    pub display: Box<str>,
    pub base: Box<str>,
    pub quote: Box<str>,
    pub base_asset: AssetId,
    pub quote_asset: AssetId,
    pub tick_size: Option<Price>,
    pub lot_size: Option<Qty>,
    pub qty_scale: i64,
}

/// Whether the book can be trusted yet. It mirrors the engine's own state, so a workstation can dim
/// a book that is still waiting for its snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UiBookState {
    AwaitingSnapshot,
    Valid,
}

/// The top of the book, copied at each commit. It is fixed size and `Copy`, so the hot thread
/// writes it to the ring without allocating. `seq` counts per instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiBookSnapshot {
    pub instrument: InstrumentId,
    pub seq: u64,
    pub event_ts_us: TsUs,
    pub state: UiBookState,
    pub bid_len: u16,
    pub ask_len: u16,
    pub bids: [Level; UI_BOOK_LEVELS],
    pub asks: [Level; UI_BOOK_LEVELS],
}

// The 32-level cap is what keeps the cost of each copy bounded.
const _: () = assert!(size_of::<UiBookSnapshot>() <= 1048);

/// The ladder the strategy wants for one instrument. The array index is the stable quote level, and
/// `None` withdraws that level. This is intent, not fact: an `OrderUpdate` arrives once the venue
/// confirms, and the two are shown apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DomQuote {
    pub bids: [Option<(Price, Qty)>; UI_QUOTE_LEVELS],
    pub asks: [Option<(Price, Qty)>; UI_QUOTE_LEVELS],
}

impl DomQuote {
    #[inline]
    pub fn top(bid: Option<(Price, Qty)>, ask: Option<(Price, Qty)>) -> Self {
        let mut quote = Self::default();
        quote.bids[0] = bid;
        quote.asks[0] = ask;
        quote
    }
}

/// One order in a working-order snapshot. `quote_level` is `None` for an order inherited from an
/// earlier run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UiWorkingOrder {
    pub client_id: ClientOrderId,
    pub quote_level: Option<QuoteLevel>,
    pub state: OrderState,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
}

impl UiWorkingOrder {
    pub const EMPTY: Self = Self {
        client_id: ClientOrderId(0),
        quote_level: None,
        state: OrderState::Free,
        price: Price(0),
        qty: Qty(0),
        filled: Qty(0),
    };
}

/// An event for the workstation, in engine order. `seq` rises by one across every kind, and each
/// event is a pure function of the engine's input sequence. Not `Eq`, because `Feature` and
/// `Position` carry an `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UiEvent {
    Quote {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        quote: DomQuote,
    },
    /// A public trade. It is copied out ahead of the warmup gate, so trades show during warmup too.
    Trade {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        aggressor: Side,
        price: Price,
        qty: Qty,
    },
    /// An engine order after a venue event. `state` comes from the engine's own order table, not
    /// the wire, so it tells an order the venue confirmed from one still in flight.
    OrderUpdate {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        client_id: ClientOrderId,
        /// The strategy's stable ladder level. `None` for an order inherited from an earlier run.
        quote_level: Option<QuoteLevel>,
        side: Side,
        state: OrderState,
        price: Price,
        qty: Qty,
        filled: Qty,
    },
    /// One side's complete order set at the end of a spin. Only the first orders are listed in
    /// full, but `total_working` is exact. It fits in one datagram, so it is never half lost, and
    /// the previous snapshot stands until the next arrives.
    OrderSnapshot {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        side: Side,
        detail_len: u8,
        total_working: u16,
        orders: [UiWorkingOrder; UI_ORDER_SNAPSHOT_CAPACITY],
    },
    /// A feature value, copied from the same row that Parquet records, so the monitor shows exactly
    /// the recorded value.
    Feature {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        feature: FeatureId,
        value: f64,
    },
    /// A real execution against an engine order. `price` and `qty` are the fill's, not the order's,
    /// and `commission` is in `commission_asset`, which need not be the quote asset.
    Fill {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        /// The strategy's stable ladder level. `None` when the filled order was inherited from an
        /// earlier run.
        quote_level: Option<QuoteLevel>,
        side: Side,
        price: Price,
        qty: Qty,
        commission: i64,
        commission_asset: AssetId,
        /// `None` when the venue did not say. Absent does not mean taker.
        liquidity: Option<Liquidity>,
    },
    /// An asset's absolute balance at the venue, sent whole whenever it moves. Never a delta,
    /// because one lost delta would leave the balance wrong for good.
    Balance {
        asset: AssetId,
        seq: u64,
        event_ts_us: TsUs,
        free: i64,
        locked: i64,
    },
    /// A quote refused by the engine's gate or by the venue. It has its own kind because a refusal
    /// is not an order transition.
    Reject {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        side: Side,
        origin: RejectOrigin,
    },
    /// The execution kill switch, sent every spin while execution is wired. It is absolute state,
    /// so a dropped frame heals on the next spin.
    Execution {
        seq: u64,
        event_ts_us: TsUs,
        halt: ExecHalt,
    },
    /// An instrument's window rotated; the monitor's System channel records the handover.
    Rotation {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
    },
    /// An instrument's marked position, sent every spin. It is ABSOLUTE, not an increment, so a
    /// dropped frame is as if never sent. The `f64` values are for display only; the exact amounts
    /// stay in the engine.
    Position {
        instrument: InstrumentId,
        seq: u64,
        event_ts_us: TsUs,
        exposure_quote: f64,
        pnl_quote: f64,
    },
    /// The engine's timing of itself, sent every spin. The figures are absolute, so a dropped frame
    /// heals on the next spin.
    Latency {
        seq: u64,
        event_ts_us: TsUs,
        summary: UiLatencySummary,
    },
}

/// Named once so the three accessors below cannot drift apart on which kinds they cover.
macro_rules! every_ui_event {
    ($field:ident) => {
        UiEvent::Quote { $field, .. }
            | UiEvent::Trade { $field, .. }
            | UiEvent::OrderUpdate { $field, .. }
            | UiEvent::OrderSnapshot { $field, .. }
            | UiEvent::Feature { $field, .. }
            | UiEvent::Fill { $field, .. }
            | UiEvent::Balance { $field, .. }
            | UiEvent::Reject { $field, .. }
            | UiEvent::Execution { $field, .. }
            | UiEvent::Rotation { $field, .. }
            | UiEvent::Position { $field, .. }
            | UiEvent::Latency { $field, .. }
    };
}

impl UiEvent {
    /// The sequence shared by every kind, which a consumer reads to count dropped events.
    #[inline]
    pub fn seq(&self) -> u64 {
        match self {
            every_ui_event!(seq) => *seq,
        }
    }

    /// The fixture page builds events by kind and numbers them afterwards. Without this it would
    /// rebuild every variant to change one field.
    #[inline]
    pub fn set_seq(&mut self, seq: u64) {
        let slot = match self {
            every_ui_event!(seq) => seq,
        };
        *slot = seq;
    }

    /// Event time for any kind, read to stamp a note when the sequence jumps.
    #[inline]
    pub fn event_ts_us(&self) -> TsUs {
        match self {
            every_ui_event!(event_ts_us) => *event_ts_us,
        }
    }
}

// Quote is the widest variant. Its 16 levels make it large, but they publish the whole ladder at a
// fixed size without allocating.
const _: () = assert!(size_of::<UiEvent>() <= 416);

/// The engine's end of the workstation feed.
pub(crate) struct UiWiring {
    pub lifecycle: SyncSender<UiLifecycle>,
    pub books: UiBookSink,
    pub events: UiEventSink,
}

/// The link actor's end of the workstation feed.
pub(crate) struct UiChannels {
    pub lifecycle: Receiver<UiLifecycle>,
    pub books: Consumer<UiBookSnapshot>,
    pub events: Consumer<UiEvent>,
}

/// Both rings are allocated here at startup, so the hot thread never allocates to publish.
pub(crate) fn ui_channel() -> (UiWiring, UiChannels) {
    let (lifecycle_tx, lifecycle_rx) = sync_channel(LIFECYCLE_CAPACITY);
    let (book_producer, book_consumer) = RingBuffer::<UiBookSnapshot>::new(UI_BOOK_RING_CAPACITY);
    let (event_producer, event_consumer) = RingBuffer::<UiEvent>::new(UI_EVENT_RING_CAPACITY);
    (
        UiWiring {
            lifecycle: lifecycle_tx,
            books: UiBookSink::new(book_producer),
            events: UiEventSink::new(event_producer),
        },
        UiChannels {
            lifecycle: lifecycle_rx,
            books: book_consumer,
            events: event_consumer,
        },
    )
}
