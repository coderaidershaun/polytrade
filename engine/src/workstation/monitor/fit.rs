//! Which channel columns show, and how tall each band is, for a monitor pane of a given size. The
//! rules take their sizes as arguments instead of reading the theme, which exists only in the
//! browser build, so a native test can pin them.

use super::view::Channel;

/// Past this a wider pane only stretches the gaps, because even the widest row, a fill with its
/// fee, fits well inside it.
const MAX_CONTENT_WIDTH: f32 = 480.0;

/// The width in points of the centred column that holds the monitor's content, for a monitor rect
/// `pane_width` points wide.
pub fn content_width(pane_width: f32) -> f32 {
    pane_width.min(MAX_CONTENT_WIDTH)
}

/// Column widths in points. `pad` is the margin on each side of a row.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnWidths {
    pub pad: f32,
    pub time: f32,
    pub short_time: f32,
    pub liquidity: f32,
    pub side: f32,
    pub price: f32,
    pub qty: f32,
    pub fee: f32,
    pub system_description: f32,
    pub order_description: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeColumn {
    Full,
    /// The clock reading without its milliseconds.
    Short,
    Hidden,
}

/// The columns a row shows besides the ones its channel always needs. A channel without a
/// liquidity or a fee column never has them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnPlan {
    pub time: TimeColumn,
    pub has_liquidity: bool,
    pub has_fee: bool,
}

impl ColumnPlan {
    pub(crate) fn time_width(self, widths: &ColumnWidths) -> f32 {
        match self.time {
            TimeColumn::Full => widths.time,
            TimeColumn::Short => widths.short_time,
            TimeColumn::Hidden => 0.0,
        }
    }

    pub(crate) fn fee_width(self, widths: &ColumnWidths) -> f32 {
        if self.has_fee { widths.fee } else { 0.0 }
    }
}

/// What a channel's row always needs, and which optional columns it has to start with.
struct ChannelShape {
    widest: ColumnPlan,
    always_width: f32,
}

impl ChannelShape {
    fn of(channel: Channel, widths: &ColumnWidths) -> Self {
        let plain = ColumnPlan {
            time: TimeColumn::Full,
            has_liquidity: false,
            has_fee: false,
        };
        let numbers = widths.side + widths.price + widths.qty;
        match channel {
            Channel::System => Self {
                widest: plain,
                always_width: widths.system_description,
            },
            Channel::Orders => Self {
                widest: plain,
                always_width: widths.side + widths.order_description,
            },
            Channel::PubTrades => Self {
                widest: plain,
                always_width: numbers,
            },
            Channel::TradeFills => Self {
                widest: ColumnPlan {
                    has_liquidity: true,
                    has_fee: true,
                    ..plain
                },
                always_width: numbers,
            },
        }
    }

    fn width_of(&self, plan: ColumnPlan, widths: &ColumnWidths) -> f32 {
        let liquidity = if plan.has_liquidity { widths.liquidity } else { 0.0 };
        self.always_width + plan.time_width(widths) + liquidity + plan.fee_width(widths)
    }
}

/// The columns a `channel` row shows in a monitor pane `pane_width` points wide. The row spans the
/// content column less its pad on each side. Columns drop in a fixed order until the row fits:
/// first the fee, then the liquidity note, then the time is shortened, and last the time is hidden
/// even if the row still does not fit.
pub fn fit_columns(channel: Channel, pane_width: f32, widths: &ColumnWidths) -> ColumnPlan {
    let row_width = content_width(pane_width) - 2.0 * widths.pad;
    let shape = ChannelShape::of(channel, widths);
    let no_fee = ColumnPlan {
        has_fee: false,
        ..shape.widest
    };
    let no_liquidity = ColumnPlan {
        has_liquidity: false,
        ..no_fee
    };
    let short_time = ColumnPlan {
        time: TimeColumn::Short,
        ..no_liquidity
    };
    let no_time = ColumnPlan {
        time: TimeColumn::Hidden,
        ..no_liquidity
    };
    [shape.widest, no_fee, no_liquidity, short_time]
        .into_iter()
        .find(|plan| shape.width_of(*plan, widths) <= row_width)
        .unwrap_or(no_time)
}

/// The fewest rows the feature list keeps while the bands show whole, so it still reads as a list.
const FEATURE_MIN_ROWS: f32 = 3.0;

/// The fewest rows the channel keeps, whatever the pane height.
const CHANNEL_MIN_ROWS: f32 = 4.0;

/// Band heights and row pitches in points. The pitch is a row's height plus the spacing under it.
/// `feature_fraction` is the share of the free height the feature list takes, up to the rows it
/// needs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BandSizes {
    pub status: f32,
    pub account: f32,
    pub summary: f32,
    pub tabs: f32,
    pub feature_pitch: f32,
    pub channel_pitch: f32,
    pub feature_fraction: f32,
}

/// The bands, top to bottom: the status strip, which never scrolls, then the account, summary,
/// features, tabs and channel, which are one column of content.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BandHeights {
    pub status: f32,
    pub account: f32,
    pub summary: f32,
    pub features: f32,
    pub tabs: f32,
    pub channel: f32,
    /// The pane is too short to show every band whole, so the content is taller than the space
    /// under the status strip and scrolls.
    pub is_scrolled: bool,
}

impl BandHeights {
    /// The height of everything under the status strip. It equals the space there when the bands
    /// show whole, so nothing scrolls, and exceeds it when they do not.
    pub fn content_height(&self) -> f32 {
        self.account + self.summary + self.features + self.tabs + self.channel
    }
}

/// The band heights for a `pane_height` that includes the status strip.
///
/// With room for three feature rows and four channel rows besides the other bands, the bands show
/// whole. The features take their share of the free height, never fewer than three rows and never
/// more than they need, and the channel takes the rest. Below that the bands keep a useful size
/// and the content scrolls. Neither list then fills the view, so something that is not a list
/// always stays in reach to drag.
pub fn fit_bands(pane_height: f32, feature_count: usize, sizes: &BandSizes) -> BandHeights {
    let feature_floor = FEATURE_MIN_ROWS * sizes.feature_pitch;
    let channel_floor = CHANNEL_MIN_ROWS * sizes.channel_pitch;
    let needed = feature_count.max(1) as f32 * sizes.feature_pitch;
    let fixed = sizes.account + sizes.summary + sizes.tabs;
    let whole_min = sizes.status + fixed + feature_floor + channel_floor;
    let viewport = pane_height - sizes.status;

    let is_scrolled = pane_height < whole_min;
    let (features, channel) = if is_scrolled {
        let room = (viewport - sizes.tabs).max(0.0);
        (needed.min(room), room.max(channel_floor))
    } else {
        let flexible = viewport - fixed;
        let share = (flexible * sizes.feature_fraction).max(feature_floor);
        let features = needed.min(share).min(flexible - channel_floor);
        (features, flexible - features)
    };
    BandHeights {
        status: sizes.status,
        account: sizes.account,
        summary: sizes.summary,
        features,
        tabs: sizes.tabs,
        channel,
        is_scrolled,
    }
}
