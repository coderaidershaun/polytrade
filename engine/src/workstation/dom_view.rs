//! The ladder as a pure projection, in exact integer tick maths around the mid, with N ticks to a
//! row. One bucket ladder places the public levels, the desired quote and the real orders, so they
//! agree on rows, and the desired quote and the real orders never merge. How many rows fit is
//! projection too, so [`fit_rows`] lives here and takes its row floor as an argument.

use super::exec_model::{OrderCell, OrderStatus};
use super::format::BASIS_POINTS_PER_UNIT;
use crate::ids::{Price, Qty};
use crate::msg::inbound::Level;
use crate::msg::ui::{DomQuote, UiBookSnapshot, UiBookState};

/// Rows per side the ladder can hold. Fixes [`DomView`]'s arrays, so a request beyond it clamps.
pub const MAX_ROWS_PER_SIDE: usize = 30;

/// Range the level control offers; below the minimum a ladder stops being one.
pub const MIN_ROWS_PER_SIDE: usize = 12;

pub const DEFAULT_ROWS_PER_SIDE: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomUnit {
    Ticks,
    Bps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomGrouping {
    Ticks { per_bucket: i64 },
    Bps { numerator: i64, denominator: i64 },
}

/// One bucket per tick, the venue's own grid. The workstation opens on a different grouping, which
/// [`UiModel`](super::model::UiModel) chooses.
impl Default for DomGrouping {
    fn default() -> Self {
        Self::Ticks { per_bucket: 1 }
    }
}

impl DomGrouping {
    #[inline]
    pub fn unit(self) -> DomUnit {
        match self {
            Self::Ticks { .. } => DomUnit::Ticks,
            Self::Bps { .. } => DomUnit::Bps,
        }
    }

    pub fn ticks_per_bucket(self, mid_half_ticks: i64) -> i64 {
        let (numerator, denominator) = match self {
            Self::Ticks { per_bucket } => return per_bucket.max(1),
            Self::Bps {
                numerator,
                denominator,
            } => (numerator, denominator),
        };
        // The mid is in half-ticks, so the share of it in whole ticks divides by two as well.
        let divisor = 2 * BASIS_POINTS_PER_UNIT * i128::from(denominator.max(1));
        let ticks = i128::from(mid_half_ticks) * i128::from(numerator) / divisor;
        i64::try_from(ticks).unwrap_or(i64::MAX).max(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowFit {
    pub rows: usize,
    pub row_height: f32,
}

/// Rows a ladder side `side_height` points tall gives when asked for `requested`, and the height each
/// gets. Asking for more than fit above `floor_height` clamps the COUNT: a short ladder is honest,
/// overlapping text is not.
pub fn fit_rows(side_height: f32, requested: usize, floor_height: f32) -> RowFit {
    let height = side_height.max(0.0);
    let whole = if floor_height > 0.0 { (height / floor_height).floor() } else { 0.0 };
    let fits = if whole.is_finite() && whole > 0.0 { whole as usize } else { 0 };
    let rows = requested.min(fits).min(MAX_ROWS_PER_SIDE);
    let row_height = if rows == 0 { height } else { height / rows as f32 };
    RowFit { rows, row_height }
}

#[inline]
pub fn bucket_low_edge(tick_index: i64, ticks_per_bucket: i64) -> i64 {
    let bucket = i128::from(tick_index.div_euclid(ticks_per_bucket));
    saturating_i64(bucket * i128::from(ticks_per_bucket))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DomRow {
    pub tick_index: i64,
    pub public_qty: Option<Qty>,
    pub strategy_qty: Option<Qty>,
    pub order_qty: Option<Qty>,
    pub order_status: Option<OrderStatus>,
    pub is_quoted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StrategyCell {
    Order { qty: Qty, status: OrderStatus },
    Desired { qty: Qty },
}

impl DomRow {
    pub(crate) fn strategy_cell(self) -> Option<StrategyCell> {
        match (self.order_qty, self.order_status, self.strategy_qty) {
            (Some(qty), Some(status), _) => Some(StrategyCell::Order { qty, status }),
            (_, _, Some(qty)) => Some(StrategyCell::Desired { qty }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DomOverlay<'a> {
    pub desired: Option<DomQuote>,
    pub bid_orders: &'a [OrderCell],
    pub ask_orders: &'a [OrderCell],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotePlacement {
    None,
    Visible,
    OffScreenAbove { delta_half_ticks: i64 },
    OffScreenBelow { delta_half_ticks: i64 },
}

impl QuotePlacement {
    /// The first placement a side produces is the one the chevron reports; later ones do not
    /// override it. Both the desired quote and the real orders answer that question, so they answer
    /// it through the same rule.
    #[inline]
    fn or(self, candidate: Self) -> Self {
        if self == Self::None { candidate } else { self }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomStatus {
    Live,
    Stale,
    AwaitingBook,
    Disconnected,
}

/// What the link knows about the feed behind the ladder, before the book itself is consulted. One
/// three-state reading rather than two flags: "disconnected AND stale" was never a fourth thing to
/// paint, and neither flag read as a question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FeedStatus {
    #[default]
    Live,
    Stale,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomView {
    pub mid_half_ticks: Option<i64>,
    pub ticks_per_bucket: i64,
    pub status: DomStatus,
    pub bid_placement: QuotePlacement,
    pub ask_placement: QuotePlacement,
    pub bid_order_placement: QuotePlacement,
    pub ask_order_placement: QuotePlacement,
    rows_per_side: usize,
    ask_rows: [DomRow; MAX_ROWS_PER_SIDE],
    bid_rows: [DomRow; MAX_ROWS_PER_SIDE],
}

impl DomView {
    pub fn ask_rows(&self) -> &[DomRow] {
        &self.ask_rows[..self.rows_per_side]
    }

    pub fn bid_rows(&self) -> &[DomRow] {
        &self.bid_rows[..self.rows_per_side]
    }
}

#[inline]
pub fn tick_index(price: Price, tick: Price) -> Option<i64> {
    if tick.0 <= 0 || price.0.rem_euclid(tick.0) != 0 {
        return None;
    }
    Some(price.0.div_euclid(tick.0))
}

#[inline]
pub fn price_for_row(tick: Price, tick_index: i64) -> Option<Price> {
    tick.0.checked_mul(tick_index).map(Price)
}

#[inline]
pub fn mid_half_ticks(best_bid: Price, best_ask: Price, tick: Price) -> Option<i64> {
    let bid = tick_index(best_bid, tick)?;
    let ask = tick_index(best_ask, tick)?;
    bid.checked_add(ask)
}

#[inline]
pub fn ask_anchor(mid_half_ticks: i64) -> i64 {
    mid_half_ticks.div_euclid(2) + 1
}

#[inline]
pub fn bid_anchor(mid_half_ticks: i64) -> i64 {
    (mid_half_ticks + 1).div_euclid(2) - 1
}

/// Everything the ladder projection reads. The painter's own concerns — decimals, quantity scale —
/// stay out: this is the book, the overlay and the grid it lands on.
pub struct DomViewInput<'a> {
    pub snapshot: Option<&'a UiBookSnapshot>,
    pub overlay: DomOverlay<'a>,
    pub tick: Price,
    pub grouping: DomGrouping,
    pub rows_per_side: usize,
    pub feed: FeedStatus,
}

pub fn build_dom_view(input: DomViewInput<'_>) -> DomView {
    let DomViewInput {
        snapshot,
        overlay,
        tick,
        grouping,
        rows_per_side,
        feed,
    } = input;
    let rows = rows_per_side.min(MAX_ROWS_PER_SIDE);
    let status = derive_status(snapshot, feed);

    let mid = snapshot.and_then(|snapshot| snapshot_mid(snapshot, tick));
    let (Some(snapshot), Some(mid_half_ticks)) = (snapshot, mid) else {
        return DomView {
            mid_half_ticks: None,
            ticks_per_bucket: 1,
            status,
            bid_placement: QuotePlacement::None,
            ask_placement: QuotePlacement::None,
            bid_order_placement: QuotePlacement::None,
            ask_order_placement: QuotePlacement::None,
            rows_per_side: 0,
            ask_rows: [DomRow::default(); MAX_ROWS_PER_SIDE],
            bid_rows: [DomRow::default(); MAX_ROWS_PER_SIDE],
        };
    };

    let ticks_per_bucket = grouping.ticks_per_bucket(mid_half_ticks);
    let asks = BucketLadder::new(
        ask_anchor(mid_half_ticks),
        ticks_per_bucket,
        rows,
        AnchorSide::Ask,
    );
    let bids = BucketLadder::new(
        bid_anchor(mid_half_ticks),
        ticks_per_bucket,
        rows,
        AnchorSide::Bid,
    );

    let mut ask_rows = [DomRow::default(); MAX_ROWS_PER_SIDE];
    let mut bid_rows = [DomRow::default(); MAX_ROWS_PER_SIDE];
    for (offset, row) in ask_rows[..rows].iter_mut().enumerate() {
        row.tick_index = asks.edge(offset);
    }
    for (offset, row) in bid_rows[..rows].iter_mut().enumerate() {
        row.tick_index = bids.edge(offset);
    }

    let ask_levels = &snapshot.asks[..snapshot.ask_len as usize];
    let bid_levels = &snapshot.bids[..snapshot.bid_len as usize];
    place_public(&mut ask_rows[..rows], ask_levels, tick, &asks);
    place_public(&mut bid_rows[..rows], bid_levels, tick, &bids);

    let desired = overlay.desired.unwrap_or_default();
    let ask_placement = place_desired(
        &mut ask_rows[..rows],
        &desired.asks,
        tick,
        &asks,
        mid_half_ticks,
    );
    let bid_placement = place_desired(
        &mut bid_rows[..rows],
        &desired.bids,
        tick,
        &bids,
        mid_half_ticks,
    );
    let ask_order_placement = place_orders(
        &mut ask_rows[..rows],
        overlay.ask_orders,
        tick,
        &asks,
        mid_half_ticks,
    );
    let bid_order_placement = place_orders(
        &mut bid_rows[..rows],
        overlay.bid_orders,
        tick,
        &bids,
        mid_half_ticks,
    );

    DomView {
        mid_half_ticks: Some(mid_half_ticks),
        ticks_per_bucket,
        status,
        bid_placement,
        ask_placement,
        bid_order_placement,
        ask_order_placement,
        rows_per_side: rows,
        ask_rows,
        bid_rows,
    }
}

fn place_desired(
    rows: &mut [DomRow],
    levels: &[Option<(Price, Qty)>],
    tick: Price,
    ladder: &BucketLadder,
    mid_half_ticks: i64,
) -> QuotePlacement {
    let mut primary = QuotePlacement::None;
    for &level in levels {
        primary = primary.or(place_quote(rows, level, tick, ladder, mid_half_ticks));
    }
    primary
}

fn derive_status(snapshot: Option<&UiBookSnapshot>, feed: FeedStatus) -> DomStatus {
    if feed == FeedStatus::Disconnected {
        return DomStatus::Disconnected;
    }
    match snapshot.map(|snapshot| snapshot.state) {
        Some(UiBookState::Valid) if feed == FeedStatus::Stale => DomStatus::Stale,
        Some(UiBookState::Valid) => DomStatus::Live,
        _ => DomStatus::AwaitingBook,
    }
}

pub(crate) fn snapshot_mid(snapshot: &UiBookSnapshot, tick: Price) -> Option<i64> {
    if snapshot.state != UiBookState::Valid || snapshot.bid_len == 0 || snapshot.ask_len == 0 {
        return None;
    }
    mid_half_ticks(snapshot.bids[0].price, snapshot.asks[0].price, tick)
}

pub(crate) fn delta_from_mid(tick_index: i64, mid_half_ticks: i64) -> i64 {
    let delta = (2 * tick_index as i128 - mid_half_ticks as i128).unsigned_abs();
    i64::try_from(delta).unwrap_or(i64::MAX)
}

enum AnchorSide {
    Ask,
    Bid,
}

struct BucketLadder {
    anchor: i64,
    ticks_per_bucket: i64,
    rows: usize,
    side: AnchorSide,
}

impl BucketLadder {
    fn new(anchor_tick: i64, ticks_per_bucket: i64, rows: usize, side: AnchorSide) -> Self {
        Self {
            anchor: bucket_low_edge(anchor_tick, ticks_per_bucket),
            ticks_per_bucket,
            rows,
            side,
        }
    }

    fn edge(&self, offset: usize) -> i64 {
        let distance = offset as i128 * i128::from(self.ticks_per_bucket);
        let anchor = i128::from(self.anchor);
        saturating_i64(match self.side {
            AnchorSide::Ask => anchor + distance,
            AnchorSide::Bid => anchor - distance,
        })
    }

    fn lowest_edge(&self) -> i64 {
        match self.side {
            AnchorSide::Ask => self.anchor,
            AnchorSide::Bid => self.edge(self.rows.saturating_sub(1)),
        }
    }

    fn row_of(&self, tick_index: i64) -> Option<usize> {
        let edge = i128::from(bucket_low_edge(tick_index, self.ticks_per_bucket));
        let anchor = i128::from(self.anchor);
        let distance = match self.side {
            AnchorSide::Ask => edge - anchor,
            AnchorSide::Bid => anchor - edge,
        };
        if distance < 0 {
            return None;
        }
        let offset = distance / i128::from(self.ticks_per_bucket);
        usize::try_from(offset)
            .ok()
            .filter(|offset| *offset < self.rows)
    }

    fn off_screen(&self, tick_index: i64, mid_half_ticks: i64) -> QuotePlacement {
        let delta_half_ticks = delta_from_mid(tick_index, mid_half_ticks);
        if tick_index < self.lowest_edge() {
            QuotePlacement::OffScreenBelow { delta_half_ticks }
        } else {
            QuotePlacement::OffScreenAbove { delta_half_ticks }
        }
    }
}

fn saturating_i64(value: i128) -> i64 {
    i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
}

fn place_public(rows: &mut [DomRow], levels: &[Level], tick: Price, ladder: &BucketLadder) {
    for level in levels {
        let Some(level_tick) = tick_index(level.price, tick) else { continue };
        let Some(offset) = ladder.row_of(level_tick) else { continue };
        add_qty(&mut rows[offset].public_qty, level.qty);
    }
}

fn add_qty(slot: &mut Option<Qty>, qty: Qty) {
    *slot = Some(slot.map_or(qty, |seen| Qty(seen.0.saturating_add(qty.0))));
}

fn place_quote(
    rows: &mut [DomRow],
    quote: Option<(Price, Qty)>,
    tick: Price,
    ladder: &BucketLadder,
    mid_half_ticks: i64,
) -> QuotePlacement {
    let Some((price, qty)) = quote else { return QuotePlacement::None };
    let Some(quote_tick) = tick_index(price, tick) else { return QuotePlacement::None };
    let Some(offset) = ladder.row_of(quote_tick) else {
        return ladder.off_screen(quote_tick, mid_half_ticks);
    };
    let row = &mut rows[offset];
    add_qty(&mut row.strategy_qty, qty);
    row.is_quoted = true;
    QuotePlacement::Visible
}

fn place_orders(
    rows: &mut [DomRow],
    orders: &[OrderCell],
    tick: Price,
    ladder: &BucketLadder,
    mid_half_ticks: i64,
) -> QuotePlacement {
    let mut placement = QuotePlacement::None;
    for order in orders {
        let Some(order_tick) = tick_index(order.price, tick) else { continue };
        let Some(offset) = ladder.row_of(order_tick) else {
            placement = placement.or(ladder.off_screen(order_tick, mid_half_ticks));
            continue;
        };
        let row = &mut rows[offset];
        add_qty(&mut row.order_qty, order.remaining());
        row.order_status = Some(match row.order_status {
            Some(held) => most_alarming(held, order.status),
            None => order.status,
        });
        placement = placement.or(QuotePlacement::Visible);
    }
    placement
}

fn most_alarming(left: OrderStatus, right: OrderStatus) -> OrderStatus {
    match (left, right) {
        (OrderStatus::Lost, _) | (_, OrderStatus::Lost) => OrderStatus::Lost,
        (OrderStatus::InFlight, _) | (_, OrderStatus::InFlight) => OrderStatus::InFlight,
        _ => OrderStatus::Confirmed,
    }
}
