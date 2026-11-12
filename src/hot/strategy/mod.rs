//! Strategy seam: hot-path market-state consumer via StrategyCtx (post-event, no wall clock → replay-safe).
//! Output gated by strategy.tables: config is sole authority (read once, checked at dispatch).
//! emit() exception: features tee to UI regardless of tables (only Parquet half gated).

mod actions;
mod ctx;
mod declare;

use std::collections::HashSet;

use crate::config::{Instruments, StrategySpec};
use crate::hot::book::Book;
use crate::hot::exec::{DesiredBook, ExecEngine};
use crate::hot::ledger::PositionLedger;
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::tracker::MicroTracker;
use crate::ids::InstrumentId;
use crate::msg::inbound::{BookChunk, KlineEvent, MarketRotation, SpinTick, TradeEvent};
use crate::time::{DurationUs, TsUs};

pub use crate::hot::exec::{
    Balance, DesiredQuote, Fill, MAX_QUOTE_LEVELS, OrderReject, OrderUpdate, OrderView, QuoteLevel,
    TickGrid,
};
/// The style field a [`StrategyCtx::quote`] declaration cannot be built without; re-exported so a
/// strategy names it through this seam rather than reaching into `msg::exec`.
pub use crate::msg::exec::OrderStyle;
/// The strategy-facing DOM quote type; re-exported for the reason above.
pub use crate::msg::ui::DomQuote;

/// Types a strategy must name to work this seam — a callback argument, a registration slot, a
/// declared column's id, what a book read reports — each homed in engine plumbing rather than in
/// strategy vocabulary; re-exported for the reason above.
pub use crate::hot::book::BookState;
pub use crate::hot::tracker::VolumeBar;
pub use crate::link::{LinkFrame, TopicId};
pub use crate::msg::persist::FeatureId;
pub use crate::registry::InstrumentRow;

pub(crate) use actions::{Actions, ActionsSetup, DrainSinks};
pub use actions::{ClientOrderId, LaneDrops};

/// Callbacks default no-ops; override only events of interest. State pre-applied; read via ctx, don't re-derive.
pub trait Strategy: Send {
    /// Feature names in index order; engine assigns dense FeatureIds in same order.
    fn features(&self) -> &'static [&'static str] {
        &[]
    }

    /// Link payload slot names in order. Digested into schema_hash → peer disagreement rejects on first frame.
    /// Names asserted at LINK_NAME_LEN bytes.
    fn link_fields(&self) -> &'static [&'static str] {
        &[]
    }

    /// Topic names in index order; engine assigns dense TopicIds above FIRST_STRATEGY.
    fn link_topics(&self) -> &'static [&'static str] {
        &[]
    }

    /// Resolved declarations (once at construction, before callbacks).
    fn register(&mut self, _registration: Registration<'_>) {}

    /// Peer frame (gated at edge, recorded). **STATE only, never deltas/events**: ring drops not kill-engine.
    fn on_link(&mut self, _ctx: &mut StrategyCtx<'_>, _frame: &LinkFrame) {}

    fn on_trade(&mut self, _ctx: &mut StrategyCtx<'_>, _event: &TradeEvent) {}
    /// Once/chunk; mid-update book may uncommit. Derived-state: key on is_last_chunk + BookState::Valid.
    fn on_book_update(&mut self, _ctx: &mut StrategyCtx<'_>, _chunk: &BookChunk) {}
    fn on_book_reset(&mut self, _ctx: &mut StrategyCtx<'_>, _instrument: InstrumentId) {}
    /// New window; ctx.window() reflects it. Slot derived state (tracker, EwmaVol) pre-reset.
    fn on_market_rotation(&mut self, _ctx: &mut StrategyCtx<'_>, _rotation: &MarketRotation) {}
    /// Engine left IDLE: wipe engine-side derived state (tracker, EwmaVol). Far side of park = unknown hole;
    /// estimators across it are poison. Wipe own series in-place. Driven by recorded marker (replay-exact).
    fn on_resume(&mut self, _ctx: &mut StrategyCtx<'_>) {}
    fn on_kline(&mut self, _ctx: &mut StrategyCtx<'_>, _event: &KlineEvent) {}

    /// REAL fill: position/cash/PnL pre-moved in ctx. Read, don't re-derive.
    fn on_fill(&mut self, _ctx: &mut StrategyCtx<'_>, _fill: &Fill) {}

    /// State transition (no money moved).
    fn on_order_update(&mut self, _ctx: &mut StrategyCtx<'_>, _update: &OrderUpdate) {}

    /// Quote rejected (engine gates or venue). One event = one callback, only here.
    fn on_reject(&mut self, _ctx: &mut StrategyCtx<'_>, _reject: &OrderReject) {}
    fn on_spin(&mut self, _ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {}
    /// Once/closed volume bar (after on_trade that closed it). Big trade closes k bars → k calls (oldest first).
    fn on_volume(
        &mut self,
        _ctx: &mut StrategyCtx<'_>,
        _instrument: InstrumentId,
        _bar: &VolumeBar,
    ) {
    }
}

/// Dense ids assigned to declarations (index-for-index). `instruments` = dispatched registry (always populated).
#[derive(Debug, Clone, Copy)]
pub struct Registration<'a> {
    pub features: &'a [FeatureId],
    pub feature_names: &'a [&'static str],
    pub instruments: &'a [InstrumentRow],
    pub link_topics: &'a [TopicId],
}

impl Registration<'_> {
    /// Ids for `names`, joined by position — a strategy asks for the columns it means instead of
    /// counting declaration order, so a typo lands as a boot panic rather than a mislabelled column.
    ///
    /// # Panics
    /// If a name is not among [`Strategy::features`]; registration runs before the first callback.
    pub fn feature_ids_of<const N: usize>(&self, names: &[&str; N]) -> [FeatureId; N] {
        names.map(|name| {
            let Some(index) = self.feature_names.iter().position(|each| *each == name) else {
                panic!("strategy declares no feature named {name:?} — nothing to resolve it to")
            };
            self.features[index]
        })
    }
}

/// Engine settings (spin interval for buffer sizing, init alloc).
#[derive(Debug, Clone, Copy)]
pub struct EngineView {
    pub spin_interval: DurationUs,
}

/// Config-to-strategy construction (off Strategy trait for dyn-compat).
pub trait StrategyConfig: Sized {
    /// strategy.params mapping (define w/ deny_unknown_fields; derive Default). Clone+Debug required.
    type Params: serde::de::DeserializeOwned + Default + Clone + std::fmt::Debug;

    fn from_spec(spec: &StrategySpec<Self::Params>, engine: EngineView) -> Self;
}

/// Slot's current window (latest MarketRotation). `None` until first rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WindowInfo {
    pub open_ts_us: TsUs,
    pub close_ts_us: TsUs,
}

impl WindowInfo {
    /// Whether a quote may rest here at `at` — the stamp on the message being processed, never a
    /// clock read, or the answer would stop being a function of the message sequence. Quoting stops
    /// `margin` before the close because an order still resting at a rotation belongs to a market
    /// that is about to stop existing, and the position it fills is one nothing can trade out of.
    #[inline]
    pub fn admits_quote_at(self, at: TsUs, margin: DurationUs) -> bool {
        at >= self.open_ts_us && !self.is_past_quote_stop(at, margin)
    }

    #[inline]
    pub fn is_past_quote_stop(self, at: TsUs, margin: DurationUs) -> bool {
        at >= self.close_ts_us - margin
    }
}

/// Post-event state (immutable) + emission surface (mutable) per callback.
/// Rows/orders/logs banked, drained after callback (nothing leaves hot thread mid-callback).
pub struct StrategyCtx<'a> {
    pub(crate) books: &'a [Book],
    pub(crate) trackers: &'a [MicroTracker],
    pub(crate) ewma: &'a [Option<EwmaVol>],
    pub(crate) windows: &'a [Option<WindowInfo>],
    pub(crate) ledger: &'a PositionLedger,
    /// Read-only: view orders, cannot transmit.
    pub(crate) exec: &'a ExecEngine,
    pub(crate) desired: &'a mut DesiredBook,
    pub(crate) actions: &'a mut Actions,
    pub(crate) event_ts: TsUs,
    /// Declaration's current spin (see quote).
    pub(crate) spin_seq: u64,
    /// How many topics [`Strategy::link_topics`] declared — the range `link_send` accepts.
    pub(crate) declared_link_topics: usize,
}

/// Which instruments a config filter selected, keyed by the dense ids the engine dispatches on.
pub struct InstrumentMask {
    selected: Vec<bool>,
}

impl InstrumentMask {
    /// # Panics
    /// If `instrument` is outside the registry the mask was resolved against. A mask and a registry
    /// that disagree would otherwise answer plausibly for every id they happen to share, and the
    /// strategy would quietly record the wrong instruments.
    pub fn contains(&self, instrument: InstrumentId) -> bool {
        let index = usize::from(instrument.0);
        assert!(
            index < self.selected.len(),
            "instrument {index} is outside the {} this filter was resolved against",
            self.selected.len()
        );
        self.selected[index]
    }
}

/// Resolved once at registration: symbol matching is case-insensitive and allocates, so it must not
/// happen per callback.
pub fn resolve_filter(filter: &Instruments, instruments: &[InstrumentRow]) -> InstrumentMask {
    let selected = match filter {
        Instruments::All => vec![true; instruments.len()],
        Instruments::Explicit(symbols) => {
            let wanted: HashSet<String> =
                symbols.iter().map(|symbol| symbol.to_lowercase()).collect();
            instruments
                .iter()
                .map(|row| wanted.contains(&row.venue_symbol.to_lowercase()))
                .collect()
        }
    };
    InstrumentMask { selected }
}

/// Bank strategy INFO line (event time stamp). fmt! syntax; ctx is first arg.
#[macro_export]
macro_rules! strategy_info {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.log_record(
            $crate::log::Level::Info,
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
            ::core::format_args!($($arg)*),
        )
    };
}

/// Bank strategy WARN line (see strategy_info!).
#[macro_export]
macro_rules! strategy_warn {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.log_record(
            $crate::log::Level::Warn,
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
            ::core::format_args!($($arg)*),
        )
    };
}

/// Bank strategy ERROR line (file/line but no backtrace — capture allocs, priority).
#[macro_export]
macro_rules! strategy_error {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.log_record(
            $crate::log::Level::Error,
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
            ::core::format_args!($($arg)*),
        )
    };
}
