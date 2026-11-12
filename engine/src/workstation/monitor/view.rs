//! Pure projections from the model to what the monitor shows, free of egui so they test natively:
//! quote distances, feature freshness, channel histories and names, and the tab and scroll state a
//! caller owns.

use super::model::FeatureCell;
use crate::hot::exec::ExecHalt;
use crate::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use crate::labelled_enum::labelled_enum;
use crate::msg::persist::FeatureId;
use crate::time::{DurationUs, TsUs};
use crate::workstation::dom_view::{DomUnit, delta_from_mid, snapshot_mid, tick_index};
use crate::workstation::exec_model::{BalanceCell, OrderStatus, RejectCell};
use crate::workstation::model::UiModel;

/// How long a changed feature stays highlighted: long enough to notice, short enough not to flash.
const CHANGED_WITHIN_SPINS: i64 = 2;

/// The recorder emits every feature every spin, so five silent spins means the feed is lagging.
/// Fewer would flag ordinary jitter.
const STALE_AFTER_SPINS: i64 = 5;

/// The summary band's three cells, each absent on its own. The deltas are unsigned distances from
/// the mid, in half-ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QuoteSummary {
    pub bid_delta_half_ticks: Option<i64>,
    pub mid_half_ticks: Option<i64>,
    pub ask_delta_half_ticks: Option<i64>,
}

/// `value` is `None` only for a feature never emitted. A real zero is `Some(0.0)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeatureRowView {
    pub feature: FeatureId,
    pub value: Option<f64>,
    pub changed: bool,
    pub stale: bool,
}

/// The mid comes from the book's best bid and ask. The deltas show only while the quote is live and
/// the book has a mid.
pub fn quote_summary(model: &UiModel, instrument: InstrumentId, tick: Price) -> QuoteSummary {
    let mid = model
        .book(instrument)
        .and_then(|book| snapshot_mid(book, tick));
    let live = model.is_quote_live(instrument);
    let quote = model.quote(instrument).map(|(quote, _)| quote);
    let delta = |leg: Option<(Price, Qty)>| -> Option<i64> {
        if !live {
            return None;
        }
        let mid = mid?;
        let (price, _) = leg?;
        Some(delta_from_mid(tick_index(price, tick)?, mid))
    };
    QuoteSummary {
        bid_delta_half_ticks: delta(
            quote.and_then(|quote| quote.bids.iter().flatten().next().copied()),
        ),
        mid_half_ticks: mid,
        ask_delta_half_ticks: delta(
            quote.and_then(|quote| quote.asks.iter().flatten().next().copied()),
        ),
    }
}

/// One row per catalog feature, in catalog order and never re-sorted, so a row never jumps.
pub fn feature_rows(
    model: &UiModel,
    instrument: InstrumentId,
) -> impl Iterator<Item = FeatureRowView> + '_ {
    let monitor = model.monitor();
    let freshest = monitor.latest_feed_ts_us(instrument);
    let spin_interval = monitor.spin_interval();
    (0..monitor.feature_count()).map(move |index| {
        let feature = FeatureId(index as u16);
        feature_row(
            feature,
            monitor.feature(instrument, feature),
            freshest,
            spin_interval,
        )
    })
}

/// An empty `label` means the catalog named no asset, and the painter shows the role instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetRowView<'a> {
    pub label: &'a str,
    pub role: AssetRole,
    /// `None` when no balance has arrived, which is not the same as a zero balance.
    pub balance: Option<BalanceCell>,
    /// The whole holding, free plus locked, in quote-asset amounts at [`FIXED_SCALE`]. `None` with
    /// no balance, or for the base asset with no mid to value it at.
    pub value: Option<i64>,
}

/// Both rows show before any balance arrives, so the band keeps its shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetRole {
    Base,
    Quote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SideCountView {
    pub open: usize,
    pub in_flight: usize,
    /// Orders whose state at the venue is unknown. Unlike in-flight ones, nothing is outstanding.
    pub lost: usize,
    /// Working orders the model could not keep details for. Non-zero means the counts beside it are
    /// short.
    pub leaked: u64,
}

/// Each field can be absent on its own, and the band keeps its shape either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountView<'a> {
    pub base: AssetRowView<'a>,
    pub quote: AssetRowView<'a>,
    pub bid: SideCountView,
    pub ask: SideCountView,
    pub last_reject: Option<RejectCell>,
    /// `None` until the engine's first execution frame.
    pub halt: Option<ExecHalt>,
    /// Balances in assets the catalog does not name. They share one id, so only their count shows.
    pub unknown_asset_balances: u64,
    /// Exposure frames an engine sent that could not be used. It shows beside the count above, so
    /// neither loss is silent.
    pub rejected_position_frames: u64,
}

/// The tick values the base holding at the same book mid the quote summary reads, so the band and
/// the MID cell cannot disagree.
pub fn account(model: &UiModel, instrument: InstrumentId, tick: Price) -> AccountView<'_> {
    let row = model
        .catalog()
        .and_then(|catalog| catalog.instrument(instrument));
    let exec = model.exec();
    let mid = model
        .book(instrument)
        .and_then(|book| snapshot_mid(book, tick));
    let base = row.and_then(|row| exec.balance(row.base_asset));
    let quote = row.and_then(|row| exec.balance(row.quote_asset));
    AccountView {
        base: AssetRowView {
            label: row.map_or("", |row| row.base.as_ref()),
            role: AssetRole::Base,
            balance: base,
            value: holding(base)
                .zip(mid)
                .and_then(|(holding, mid)| value_at_mid(holding, mid, tick)),
        },
        quote: AssetRowView {
            label: row.map_or("", |row| row.quote.as_ref()),
            role: AssetRole::Quote,
            balance: quote,
            // The quote asset's worth in quote terms is its own total.
            value: holding(quote),
        },
        bid: side_counts(model, instrument, Side::Buy),
        ask: side_counts(model, instrument, Side::Sell),
        last_reject: exec.last_reject(),
        halt: exec.halt(),
        unknown_asset_balances: exec.unknown_asset_balances(),
        rejected_position_frames: model.positions().rejected_frames(),
    }
}

/// Free plus locked: a coin held against a resting order is still one you own.
fn holding(balance: Option<BalanceCell>) -> Option<i64> {
    let balance = balance?;
    i64::try_from(i128::from(balance.free) + i128::from(balance.locked)).ok()
}

/// The holding valued at the mid, in quote amounts. One division covers both the tick and the half
/// in the half-tick mid, so the value rounds once. A result too large for `i64` is absent rather
/// than clamped, because a clamped money figure is wrong and nothing on screen would say so.
fn value_at_mid(holding: i64, mid_half_ticks: i64, tick: Price) -> Option<i64> {
    if tick.0 <= 0 {
        return None;
    }
    let scaled = i128::from(holding)
        .checked_mul(i128::from(mid_half_ticks))?
        .checked_mul(i128::from(tick.0))?;
    i64::try_from(scaled / (2 * i128::from(FIXED_SCALE))).ok()
}

fn side_counts(model: &UiModel, instrument: InstrumentId, side: Side) -> SideCountView {
    let Some(orders) = model.exec().side(instrument, side) else {
        return SideCountView::default();
    };
    SideCountView {
        open: orders.count(OrderStatus::Confirmed),
        in_flight: orders.count(OrderStatus::InFlight),
        lost: orders.count(OrderStatus::Lost),
        leaked: orders.leaked(),
    }
}

fn feature_row(
    feature: FeatureId,
    cell: Option<FeatureCell>,
    freshest: Option<TsUs>,
    spin_interval: DurationUs,
) -> FeatureRowView {
    let Some(cell) = cell else {
        return FeatureRowView {
            feature,
            value: None,
            changed: false,
            stale: false,
        };
    };
    let (changed, stale) = match freshest {
        Some(freshest) => {
            let since_change_us = freshest.diff(cell.last_changed_ts).micros();
            let since_update_us = freshest.diff(cell.last_update_ts).micros();
            let changed = since_change_us <= spins(spin_interval, CHANGED_WITHIN_SPINS);
            let stale = since_update_us > spins(spin_interval, STALE_AFTER_SPINS);
            (changed, stale)
        }
        None => (false, false),
    };
    FeatureRowView {
        feature,
        value: Some(cell.value),
        changed,
        stale,
    }
}

fn spins(spin_interval: DurationUs, count: i64) -> i64 {
    spin_interval.micros().saturating_mul(count)
}

labelled_enum! {
    /// Monitor channels in tab order; System is the tab a fresh session opens on.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Channel {
        System = "System",
        Orders = "Orders",
        PubTrades = "Pub trades",
        TradeFills = "Trade fills",
    }
    /// The tab's label. The two-word names keep their space so they read at a glance.
    pub fn label;
    /// Every channel in tab order, which the tab strip and the channel cycler walk.
    pub const ALL;
}

impl Channel {
    pub(crate) fn index(self) -> usize {
        self as usize
    }
}

/// While following, a channel stays pinned to its newest row and `seen` tracks every row appended.
/// Scrolled away, it holds still, and a pending offset forces one scroll on the next frame.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ChannelSeen {
    pub(crate) is_following: bool,
    pub(crate) seen: u64,
    pending_offset: Option<f32>,
}

impl ChannelSeen {
    const FOLLOWING: Self = Self {
        is_following: true,
        seen: 0,
        pending_offset: None,
    };

    pub(crate) fn take_pending_offset(&mut self) -> Option<f32> {
        self.pending_offset.take()
    }

    /// Follows again, and scrolls to the newest row on the next frame.
    pub(crate) fn resume_follow(&mut self) {
        self.is_following = true;
        self.pending_offset = Some(0.0);
    }
}

/// What a tap can pin. A row is named by its place in its channel's append order, counted from
/// the first row ever appended, so the pin stays on that row while newer rows arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MonitorDetail {
    Delta(Side),
    Feature(FeatureId),
    Row { channel: Channel, ordinal: u64 },
}

/// The tab, scroll and pinned-detail state the caller owns. It holds no market data.
#[derive(Debug, Clone, PartialEq)]
pub struct MonitorUiState {
    pub active_tab: Channel,
    channels: [ChannelSeen; Channel::ALL.len()],
    pinned: Option<MonitorDetail>,
}

impl Default for MonitorUiState {
    fn default() -> Self {
        Self {
            active_tab: Channel::System,
            channels: [ChannelSeen::FOLLOWING; Channel::ALL.len()],
            pinned: None,
        }
    }
}

impl MonitorUiState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Puts a channel in the scrolled-away state, as the operator's scroll or a fixture scene
    /// would.
    pub fn set_scrolled_away(&mut self, channel: Channel, seen: u64, offset: f32) {
        let slot = &mut self.channels[channel.index()];
        slot.is_following = false;
        slot.seen = seen;
        slot.pending_offset = Some(offset);
    }

    /// `Some` pins that detail in place of any other, or lets go when it is already pinned. `None`
    /// is a tap on nothing and clears the pin.
    pub fn tap(&mut self, target: Option<MonitorDetail>) {
        self.pinned = target.filter(|detail| self.pinned != Some(*detail));
    }

    pub(crate) fn pinned(&self) -> Option<MonitorDetail> {
        self.pinned
    }

    pub(crate) fn channel(&mut self, channel: Channel) -> &mut ChannelSeen {
        &mut self.channels[channel.index()]
    }
}

/// What the monitor reads for one frame. The names come as slices, so the monitor never depends on
/// the catalog's own types.
pub struct MonitorFrame<'a> {
    pub model: &'a UiModel,
    pub instrument: InstrumentId,
    pub tick: Price,
    pub qty_scale: i64,
    pub qty_decimals: usize,
    /// The ladder's unit selector, which the summary deltas follow.
    pub dom_unit: DomUnit,
    pub feature_names: &'a [Box<str>],
    pub instrument_names: &'a [Box<str>],
}

impl<'a> MonitorFrame<'a> {
    pub(crate) fn for_selected(model: &'a UiModel, instrument_names: &'a [Box<str>]) -> Self {
        let scales = model.instrument_scales();
        Self {
            model,
            instrument: model.selected(),
            tick: scales.tick,
            qty_scale: scales.qty_scale,
            qty_decimals: scales.qty_decimals,
            dom_unit: model.dom_unit(),
            feature_names: model
                .catalog()
                .map_or(&[], |catalog| &catalog.feature_names),
            instrument_names,
        }
    }

    /// The catalog's display name, or `i<id>` for an id the catalog does not name. A named
    /// instrument costs no allocation.
    pub(crate) fn instrument_label(&self, id: InstrumentId, scratch: &mut String) {
        scratch.clear();
        match self.instrument_names.get(id.0 as usize) {
            Some(name) => scratch.push_str(name),
            None => {
                scratch.push('i');
                scratch.push_str(&id.0.to_string());
            }
        }
    }
}

/// The catalog's display names in id order. The catalog keeps each inside its instrument record, so
/// a frame cannot borrow them as one slice.
pub(crate) fn instrument_names(model: &UiModel) -> Vec<Box<str>> {
    model.catalog().map_or_else(Vec::new, |catalog| {
        catalog
            .instruments
            .iter()
            .map(|instrument| instrument.display.clone())
            .collect()
    })
}
