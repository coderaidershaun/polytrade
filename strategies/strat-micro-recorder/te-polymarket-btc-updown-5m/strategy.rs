//! Publishes both up legs over one link topic, keyed by the role each plays this spin: quotes,
//! top-of-book depth, per-side (A, k) trade intensity and volume traded since rotation.
//!
//! And makes a naive market on the leg whose window is OPEN: one post-only bid a few ticks behind
//! the touch while flat, one post-only offer against whatever that bid bought, then nothing at all
//! for the last few seconds of the window, where the only thing worth wanting is to be out. Both
//! quotes lapse by not being redeclared — the seam is level-triggered — and that is what frees the
//! side's single order budget for the flatten that follows.

use polysim::config::StrategySpec;
use polysim::hot::quant::intensity::{PacedIntensity, SideEstimate};
use polysim::hot::strategy::{
    BookState, DesiredQuote, EngineView, InstrumentRow, OrderStyle, QuoteLevel, Registration,
    Strategy, StrategyConfig, StrategyCtx, TickGrid, TopicId, WindowInfo,
};
use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::{MarketRotation, SpinTick, TradeEvent};
use polysim::time::{DurationUs, TsUs};
use serde::Deserialize;

#[path = "../common.rs"]
pub(crate) mod common;

const UP_SUFFIX: &str = "-up";

/// The venue's own floor is 5 shares; anything under it is an order no market accepts.
const DEFAULT_ORDER_SHARES: f64 = 5.0;
const DEFAULT_EDGE_TICKS: u32 = 2;

/// Comfortably past `execution.quote_stop_margin_ms`, whose shipped value is 3000. Quoting has to
/// stop FIRST: reaching the engine's gate means every spin declares something it refuses.
const DEFAULT_QUOTE_STOP_LEAD_MS: u32 = 3_500;

/// Operator knobs. Quoting needs `enabled` AND `execution.mode`, so a config that arms execution for
/// another reason does not start a market maker by omission.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PolyUpParams {
    pub enabled: bool,
    /// Order size in outcome shares.
    pub order_shares: f64,
    /// How far behind the touch a quote rests.
    pub edge_ticks: u32,
    /// How long before the window closes quoting stops and flattening starts.
    pub quote_stop_lead_ms: u32,
}

impl Default for PolyUpParams {
    fn default() -> Self {
        Self {
            enabled: false,
            order_shares: DEFAULT_ORDER_SHARES,
            edge_ticks: DEFAULT_EDGE_TICKS,
            quote_stop_lead_ms: DEFAULT_QUOTE_STOP_LEAD_MS,
        }
    }
}

pub struct PolyUpPublisher {
    topic: Option<TopicId>,
    slots: [Option<UpSlot>; 2],
    /// `None` leaves every intensity slot NaN — the source configured no reach histogram.
    refit_interval: Option<DurationUs>,
    /// `None` while the config leaves quoting off, which is what keeps the publishing half of this
    /// engine reachable on its own.
    maker: Option<Maker>,
}

/// The knobs above in engine units, resolved once.
#[derive(Debug, Clone, Copy)]
struct Maker {
    order_qty: Qty,
    edge_ticks: i64,
    quote_stop_lead: DurationUs,
}

/// One up leg's publishable state. Volumes fold as exact mantissas; f64 appears only on the wire.
struct UpSlot {
    instrument: InstrumentId,
    intensity: PacedIntensity,
    buy_qty: Qty,
    sell_qty: Qty,
}

impl StrategyConfig for PolyUpPublisher {
    type Params = PolyUpParams;

    fn from_spec(spec: &StrategySpec<PolyUpParams>, _engine: EngineView) -> Self {
        Self {
            topic: None,
            slots: [None, None],
            refit_interval: None,
            maker: spec.params.enabled.then(|| Maker::new(spec.params)),
        }
    }
}

impl Strategy for PolyUpPublisher {
    fn link_fields(&self) -> &'static [&'static str] {
        &common::LINK_FIELDS
    }

    fn link_topics(&self) -> &'static [&'static str] {
        common::LINK_TOPICS
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.topic = registration.link_topics.first().copied();
        let up_legs: Vec<&InstrumentRow> = registration
            .instruments
            .iter()
            .filter(|row| row.venue_symbol.ends_with(UP_SUFFIX))
            .collect();
        // Two roles, two slots. A series with a third up leg needs a wider wire and a role for it, and
        // zipping would drop it in silence — as a market that simply never appears in the data.
        assert!(
            up_legs.len() <= self.slots.len(),
            "polymarket source carries {} up legs, this publisher publishes {}",
            up_legs.len(),
            self.slots.len()
        );
        self.refit_interval = up_legs
            .iter()
            .find_map(|row| row.tracker.intensity.as_ref())
            .map(PacedIntensity::refit_interval);
        let refit_interval = self.refit_interval;
        for (slot, row) in self.slots.iter_mut().zip(&up_legs) {
            *slot = Some(UpSlot::new(row.instrument_id, refit_interval));
        }
    }

    fn on_trade(&mut self, _ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        let Some(slot) = self.slot_mut(event.instrument) else {
            return;
        };
        match event.side {
            Side::Buy => slot.buy_qty.0 += event.qty.0,
            Side::Sell => slot.sell_qty.0 += event.qty.0,
        }
    }

    /// The engine wipes the reach histogram at rotation. A cached estimate would republish a dead
    /// market's (A, k) as live, and its warm start would seed the next fit in the wrong basin.
    fn on_market_rotation(&mut self, _ctx: &mut StrategyCtx<'_>, rotation: &MarketRotation) {
        let refit_interval = self.refit_interval;
        if let Some(slot) = self.slot_mut(rotation.instrument) {
            *slot = UpSlot::new(slot.instrument, refit_interval);
        }
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let now = ctx.event_ts();
        self.publish(ctx, now);
        self.trade_the_open_window(ctx, now);
    }
}

impl PolyUpPublisher {
    fn publish(&mut self, ctx: &mut StrategyCtx<'_>, now: TsUs) {
        let Some(topic) = self.topic else { return };
        let mut frame = common::UpFrame::ABSENT;
        let mut has_role = false;
        for slot in self.slots.iter_mut().flatten() {
            let Some(block) = slot.role_block(ctx, now, &mut frame) else {
                continue;
            };
            has_role = true;
            slot.write(ctx, now, block);
        }
        // An all-NaN frame reads as silence on the far side, so send nothing rather than say nothing.
        if !has_role {
            return;
        }
        ctx.link_send(topic, &frame.to_array());
    }

    /// Only the leg hosting the open window is tradeable. The other one is either pre-open — where
    /// the engine refuses a quote anyway, and asking would fill the log with its refusals — or past
    /// its close, where nothing can be traded out of what it settles into.
    fn trade_the_open_window(&mut self, ctx: &mut StrategyCtx<'_>, now: TsUs) {
        let Some(maker) = self.maker else { return };
        let Some((instrument, window)) = self.open_window(ctx, now) else {
            return;
        };
        maker.declare(ctx, instrument, window, now);
    }

    fn open_window(&self, ctx: &StrategyCtx<'_>, now: TsUs) -> Option<(InstrumentId, WindowInfo)> {
        self.slots.iter().flatten().find_map(|slot| {
            let window = ctx.window(slot.instrument)?;
            (role_at(window, now)? == Role::Current).then_some((slot.instrument, window))
        })
    }

    fn slot_mut(&mut self, instrument: InstrumentId) -> Option<&mut UpSlot> {
        self.slots
            .iter_mut()
            .flatten()
            .find(|slot| slot.instrument == instrument)
    }
}

impl Maker {
    fn new(params: PolyUpParams) -> Self {
        assert!(
            params.order_shares.is_finite() && params.order_shares > 0.0,
            "order_shares must be a positive number of outcome shares, got {}",
            params.order_shares
        );
        assert!(
            params.quote_stop_lead_ms > 0,
            "quote_stop_lead_ms must leave time to get out before the window closes"
        );
        Self {
            order_qty: Qty((params.order_shares * FIXED_SCALE as f64).round() as i64),
            edge_ticks: i64::from(params.edge_ticks),
            quote_stop_lead: DurationUs::from_millis(i64::from(params.quote_stop_lead_ms)),
        }
    }

    /// One side, one order, every spin — or nothing, which withdraws whatever was declared last.
    fn declare(
        self,
        ctx: &mut StrategyCtx<'_>,
        instrument: InstrumentId,
        window: WindowInfo,
        now: TsUs,
    ) {
        let position = ctx.position_base(instrument);
        // Past the stop there is no quote worth having: this market is minutes from settling to 0
        // or 1 and a fill here is one nothing can be traded out of. Declaring nothing withdraws the
        // ladder, which is what frees the side for the flatten below.
        if window.is_past_quote_stop(now, self.quote_stop_lead) {
            if position.0 != 0 {
                ctx.flatten(instrument);
            }
            return;
        }
        let book = ctx.book(instrument);
        if book.state() != BookState::Valid {
            return;
        }
        let (Some(best_bid), Some(best_ask)) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        let grid = ctx.tick_grid(instrument);
        let (side, quote) = match position.0.signum() {
            0 => (Side::Buy, Some(self.bid(best_bid.price, grid))),
            // Long: offer it back and stop bidding. One position at a time is the whole strategy —
            // an engine that never adds to a fill cannot lose more than one order's worth per window.
            1 => (Side::Sell, self.ask(best_ask.price, position, grid)),
            // A ladder that only ever buys cannot go short, so a short is an inherited position and
            // guessing how to close it is not this strategy's job.
            _ => return,
        };
        ctx.quote(instrument, side, QuoteLevel::ZERO, quote);
    }

    /// A bid `edge_ticks` behind the touch, floored at the venue's own lowest price — an outcome
    /// share never trades at zero, and a clamp is the difference between a deep quote and a rejected
    /// one on a market already trading at a tick.
    fn bid(self, best_bid: Price, grid: TickGrid) -> DesiredQuote {
        DesiredQuote {
            price: Price((best_bid.0 - self.edge_ticks * grid.tick).max(grid.tick)),
            qty: self.order_qty,
            style: OrderStyle::PostOnly,
        }
    }

    /// An offer sized to what is actually held, or none at all. A residue under the venue's minimum
    /// order size cannot be sold by any order, so asking would only produce a refusal per spin; it
    /// rides to the flatten, which says so once and loudly.
    fn ask(self, best_ask: Price, position: Qty, grid: TickGrid) -> Option<DesiredQuote> {
        let qty = Qty(position.0 - position.0.rem_euclid(grid.step));
        if qty < grid.min_qty {
            return None;
        }
        let ceiling = grid.max_price.map_or(i64::MAX, |max| max.0);
        Some(DesiredQuote {
            price: Price((best_ask.0 + self.edge_ticks * grid.tick).min(ceiling)),
            qty,
            style: OrderStyle::PostOnly,
        })
    }
}

impl UpSlot {
    fn new(instrument: InstrumentId, refit_interval: Option<DurationUs>) -> Self {
        Self {
            instrument,
            intensity: PacedIntensity::new(refit_interval),
            buy_qty: Qty(0),
            sell_qty: Qty(0),
        }
    }

    fn role_block<'f>(
        &self,
        ctx: &StrategyCtx<'_>,
        now: TsUs,
        frame: &'f mut common::UpFrame,
    ) -> Option<&'f mut common::UpRole> {
        match role_at(ctx.window(self.instrument)?, now)? {
            Role::Current => Some(&mut frame.cur),
            Role::Next => Some(&mut frame.next),
        }
    }

    fn write(&mut self, ctx: &StrategyCtx<'_>, now: TsUs, block: &mut common::UpRole) {
        block.buy_vol = self.buy_qty.to_f64();
        block.sell_vol = self.sell_qty.to_f64();
        self.write_book(ctx, block);
        self.write_intensity(ctx, now, block);
    }

    fn write_book(&self, ctx: &StrategyCtx<'_>, block: &mut common::UpRole) {
        let book = ctx.book(self.instrument);
        if book.state() != BookState::Valid {
            return;
        }
        if let Some(bid) = book.best_bid() {
            block.bid = bid.price.to_f64();
            block.bid_qty = bid.qty.to_f64();
        }
        if let Some(ask) = book.best_ask() {
            block.ask = ask.price.to_f64();
            block.ask_qty = ask.qty.to_f64();
        }
    }

    fn write_intensity(&mut self, ctx: &StrategyCtx<'_>, now: TsUs, block: &mut common::UpRole) {
        let counts = ctx.tracker(self.instrument).intensity();
        let Some(estimate) = self.intensity.refit(counts, now) else {
            return;
        };
        write_side(
            (&mut block.intensity_a_bid, &mut block.intensity_k_bid),
            estimate.bid,
        );
        write_side(
            (&mut block.intensity_a_ask, &mut block.intensity_k_ask),
            estimate.ask,
        );
    }
}

/// Which of the wire's two blocks a slot fills, and equally which one this engine may trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Current,
    Next,
}

/// Half-open `[open, close)`: window N's close IS window N+1's open, so the two slots tile the
/// timeline with no instant where both are current. After close a slot has no role — the market only
/// settles towards 0 or 1, which answers a different question from the one either caller asks.
fn role_at(window: WindowInfo, now: TsUs) -> Option<Role> {
    if window.open_ts_us <= now && now < window.close_ts_us {
        return Some(Role::Current);
    }
    (now < window.open_ts_us).then_some(Role::Next)
}

fn write_side((a, k): (&mut f64, &mut f64), side: Option<SideEstimate>) {
    let Some(estimate) = side.and_then(SideEstimate::fresh) else {
        return;
    };
    *a = estimate.a_per_sec;
    *k = estimate.k_per_tick;
}
