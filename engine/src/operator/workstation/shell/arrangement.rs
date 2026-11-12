//! Where everything goes: the bars, the panes and the top bar's slots for one canvas, as plain
//! numbers. A painter never computes geometry of its own.

use crate::hot::exec::ExecHalt;

use super::navigation::{Navigation, Selection, SizeClass, View};

/// The smallest height at which a control is still easy to hit with a finger.
pub(crate) const TOUCH_HEIGHT: f32 = 36.0;
const TOP_BAR_HEIGHT: f32 = 40.0;
const TAB_BAR_HEIGHT: f32 = 48.0;
const LINK_BAR_HEIGHT: f32 = 24.0;
const PANE_HEADER_HEIGHT: f32 = 30.0;

const MODE_WIDTH: f32 = 60.0;
const HALT_WIDTH: f32 = 72.0;
const PICKER_WIDTH: f32 = 220.0;
const SWITCHER_WIDTH: f32 = 256.0;
const STRATEGY_MIN_WIDTH: f32 = 64.0;

const WIDE_CHART_END: f32 = 0.50;
const WIDE_MONITOR_END: f32 = 0.75;
const MEDIUM_LADDER_SHARE: f32 = 0.40;
const MEDIUM_LADDER_MIN_WIDTH: f32 = 320.0;
const MEDIUM_LADDER_MAX_WIDTH: f32 = 440.0;

const MAX_PANE_WIDTH: f32 = 2_400.0;
const MAX_PANE_HEIGHT: f32 = 1_600.0;

const MIN_CSS_WIDTH: f32 = 320.0;
const MIN_CSS_HEIGHT: f32 = 400.0;
const MIN_ZOOM: f32 = 0.5;
/// Rounding to whole pixels moves the measured size by under a pixel each frame. A change smaller
/// than this is that jitter, and acting on it would resize the page every frame.
const ZOOM_TOLERANCE: f32 = 0.002;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Area {
    pub left: f32,
    pub top: f32,
    pub width: f32,
    pub height: f32,
}

impl Area {
    /// Each `cut_*` takes a strip off one side and leaves the rest. A strip is never larger than the
    /// area, so a canvas shorter than its bars clips them instead of turning a size negative.
    fn cut_top(&mut self, height: f32) -> Self {
        let height = height.min(self.height);
        let strip = Self { height, ..*self };
        self.top += height;
        self.height -= height;
        strip
    }

    fn cut_left(&mut self, width: f32) -> Self {
        let width = width.min(self.width);
        let strip = Self { width, ..*self };
        self.left += width;
        self.width -= width;
        strip
    }

    fn cut_right(&mut self, width: f32) -> Self {
        let width = width.min(self.width);
        self.width -= width;
        Self {
            left: self.left + self.width,
            width,
            ..*self
        }
    }

    fn cut_bottom(&mut self, height: f32) -> Self {
        let height = height.min(self.height);
        self.height -= height;
        Self {
            top: self.top + self.height,
            height,
            ..*self
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pane {
    /// The strip for the pane's own controls. The monitor has none.
    pub header: Option<Area>,
    pub body: Area,
}

/// The slots run left to right: picker, switcher, strategy id, then the badges. On a short bar the
/// strategy id gives way first and its room goes to the picker, then the picker shrinks, and the
/// badges never give way. Below 320 points the badges keep their width first, then the switcher,
/// and the picker gets what is left, so no slot overlaps another or turns negative.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopBar {
    pub area: Area,
    pub picker: Area,
    /// Absent on a compact canvas, where the tab bar is the switcher.
    pub switcher: Option<Area>,
    /// Absent when less than a readable slice is left for it.
    pub strategy: Option<Area>,
    /// Present exactly while the halt latch is set.
    pub halt: Option<Area>,
    pub mode: Area,
}

impl TopBar {
    fn of(area: Area, class: SizeClass, halt: Option<ExecHalt>) -> Self {
        let mut room = area;
        let mode = room.cut_right(MODE_WIDTH);
        let halt = halt
            .is_some_and(ExecHalt::is_halted)
            .then(|| room.cut_right(HALT_WIDTH));
        let switcher_width = (class != SizeClass::Compact).then(|| SWITCHER_WIDTH.min(room.width));
        let reserved = switcher_width.unwrap_or(0.0);
        let has_strategy = room.width - PICKER_WIDTH - reserved >= STRATEGY_MIN_WIDTH;
        // Without room for the strategy id the picker takes what is left, so no blank cell sits
        // before the badges.
        let picker_width = if has_strategy { PICKER_WIDTH } else { room.width - reserved };
        let picker = room.cut_left(picker_width);
        let switcher = switcher_width.map(|width| room.cut_left(width));
        let strategy = has_strategy.then_some(room);
        Self {
            area,
            picker,
            switcher,
            strategy,
            halt,
            mode,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Arrangement {
    pub class: SizeClass,
    /// What the canvas actually shows, which on a compact canvas is always a single view.
    pub shown: Selection,
    pub top_bar: TopBar,
    pub tab_bar: Option<Area>,
    pub link_bar: Area,
    canvas: Area,
    chart: Option<Pane>,
    ladder: Option<Pane>,
    monitor: Option<Pane>,
}

impl Arrangement {
    /// `halt` is the engine's latch, once it has reported one. The top bar opens a halt slot only
    /// while the latch is set.
    pub fn new(canvas: Area, navigation: &Navigation, halt: Option<ExecHalt>) -> Self {
        let class = SizeClass::of_width(canvas.width);
        let shown = navigation.shown(class);

        let mut rest = canvas;
        let link_bar = rest.cut_bottom(link_bar_height(class));
        let top_bar = rest.cut_top(TOP_BAR_HEIGHT);
        let tab_bar = (class == SizeClass::Compact).then(|| rest.cut_bottom(TAB_BAR_HEIGHT));
        let content = rest;

        let header = pane_header_height(class);
        let slots = Slots::of(content, class, shown);
        Self {
            class,
            shown,
            top_bar: TopBar::of(top_bar, class, halt),
            tab_bar,
            link_bar,
            canvas,
            chart: slots.chart.map(|slot| Pane::fit(slot, Some(header))),
            ladder: slots.ladder.map(|slot| Pane::fit(slot, Some(header))),
            monitor: slots.monitor.map(|slot| Pane::fit(slot, None)),
        }
    }

    pub fn pane(&self, view: View) -> Option<Pane> {
        match view {
            View::Chart => self.chart,
            View::Ladder => self.ladder,
            View::Monitor => self.monitor,
        }
    }

    pub(crate) fn above_link_bar(&self) -> Area {
        Area {
            height: self.link_bar.top - self.canvas.top,
            ..self.canvas
        }
    }
}

/// The part of the content each view gets. A view that is not shown gets none.
#[derive(Default)]
struct Slots {
    chart: Option<Area>,
    ladder: Option<Area>,
    monitor: Option<Area>,
}

impl Slots {
    fn of(content: Area, class: SizeClass, shown: Selection) -> Self {
        let mut slots = Self::default();
        let mut rest = content;
        match (shown, class) {
            (Selection::View(View::Chart), _) => slots.chart = Some(content),
            (Selection::View(View::Ladder), _) => slots.ladder = Some(content),
            (Selection::View(View::Monitor), _) => slots.monitor = Some(content),
            (Selection::Desk, SizeClass::Wide) => {
                // Split at whole points, because rounding can leave the ladder a fraction under the
                // 300 points its painter needs.
                let chart_width = (content.width * WIDE_CHART_END).floor();
                let monitor_end = (content.width * WIDE_MONITOR_END).floor();
                slots.chart = Some(rest.cut_left(chart_width));
                slots.monitor = Some(rest.cut_left(monitor_end - chart_width));
                slots.ladder = Some(rest);
            }
            (Selection::Desk, SizeClass::Medium) => {
                let ladder_width = (content.width * MEDIUM_LADDER_SHARE)
                    .clamp(MEDIUM_LADDER_MIN_WIDTH, MEDIUM_LADDER_MAX_WIDTH)
                    .floor();
                slots.ladder = Some(rest.cut_right(ladder_width));
                slots.chart = Some(rest);
            }
            (Selection::Desk, SizeClass::Compact) => {
                unreachable!("a compact canvas shows a view, because the desk falls back to one")
            }
        }
        slots
    }
}

impl Pane {
    /// A slot larger than the largest pane keeps the pane at that size, centred across the slot and
    /// at its top, so a big screen does not stretch the type and the gaps.
    fn fit(slot: Area, header_height: Option<f32>) -> Self {
        let width = slot.width.min(MAX_PANE_WIDTH);
        let mut column = Area {
            left: slot.left + (slot.width - width) / 2.0,
            width,
            ..slot
        };
        let header = header_height.map(|height| column.cut_top(height));
        column.height = column.height.min(MAX_PANE_HEIGHT);
        Self {
            header,
            body: column,
        }
    }
}

fn link_bar_height(class: SizeClass) -> f32 {
    match class {
        SizeClass::Compact => TOUCH_HEIGHT,
        SizeClass::Medium | SizeClass::Wide => LINK_BAR_HEIGHT,
    }
}

fn pane_header_height(class: SizeClass) -> f32 {
    match class {
        SizeClass::Compact => TOUCH_HEIGHT,
        SizeClass::Medium | SizeClass::Wide => PANE_HEADER_HEIGHT,
    }
}

/// The zoom to switch to, or `None` when the current one is right. A page of at least 320 by 400
/// CSS pixels is never zoomed out, because every pane fits at that size. A smaller page zooms out
/// to fit, to half at most. A page with no size, such as a hidden frame, is left alone.
pub fn zoom_to_fit(css_width: f32, css_height: f32, current: f32) -> Option<f32> {
    if css_width <= 0.0 || css_height <= 0.0 {
        return None;
    }
    let target = (css_width / MIN_CSS_WIDTH)
        .min(css_height / MIN_CSS_HEIGHT)
        .clamp(MIN_ZOOM, 1.0);
    ((target - current).abs() > ZOOM_TOLERANCE).then_some(target)
}
