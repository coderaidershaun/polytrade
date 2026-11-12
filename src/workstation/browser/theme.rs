//! The design system: named colour and size tokens, so no painter writes a raw value, and the
//! pixel snapping, shapes and cut-off text that every painter shares.

use eframe::egui;

use crate::workstation::shell::TOUCH_HEIGHT;
use crate::workstation::text::Tone;

/// Colours named by role, not hue, so a painter asks for what a thing is and the palette decides
/// how it looks. Values are gamma-space sRGBA.
#[derive(Clone, Copy)]
pub(crate) struct Palette {
    pub canvas: egui::Color32,
    pub panel: egui::Color32,
    pub panel_raised: egui::Color32,
    pub border: egui::Color32,
    pub grid: egui::Color32,
    pub text_primary: egui::Color32,
    pub text_secondary: egui::Color32,
    pub bid: egui::Color32,
    pub ask: egui::Color32,
    /// Behind the halt banner's text, not behind an ask: a red wash the invalid colour reads on.
    pub halt_fill: egui::Color32,
    /// Our own sell fills are orange so they never read as the ask side's red. Buy fills reuse the
    /// bid green.
    pub sell_fill: egui::Color32,
    pub positive: egui::Color32,
    pub negative: egui::Color32,
    pub warning: egui::Color32,
    pub invalid: egui::Color32,
    pub stale: egui::Color32,
    pub selected: egui::Color32,
    pub focus: egui::Color32,
}

pub(crate) const DARK: Palette = Palette {
    canvas: egui::Color32::from_rgb(10, 13, 18),
    panel: egui::Color32::from_rgb(15, 19, 26),
    panel_raised: egui::Color32::from_rgb(21, 27, 36),
    border: egui::Color32::from_rgb(44, 53, 67),
    grid: egui::Color32::from_rgb(31, 39, 50),
    text_primary: egui::Color32::from_rgb(229, 234, 241),
    text_secondary: egui::Color32::from_rgb(145, 157, 174),
    bid: egui::Color32::from_rgb(64, 205, 150),
    ask: egui::Color32::from_rgb(244, 101, 118),
    halt_fill: egui::Color32::from_rgba_unmultiplied_const(190, 54, 74, 58),
    sell_fill: egui::Color32::from_rgb(233, 138, 45),
    positive: egui::Color32::from_rgb(72, 211, 153),
    negative: egui::Color32::from_rgb(255, 105, 120),
    warning: egui::Color32::from_rgb(245, 184, 72),
    invalid: egui::Color32::from_rgb(255, 92, 92),
    stale: egui::Color32::from_rgb(221, 165, 66),
    selected: egui::Color32::from_rgb(76, 139, 245),
    focus: egui::Color32::from_rgb(126, 178, 255),
};

impl Palette {
    pub(crate) fn tone(&self, tone: Tone) -> egui::Color32 {
        match tone {
            Tone::Text => self.text_primary,
            Tone::Text2 => self.text_secondary,
            Tone::Bid => self.bid,
            Tone::Ask => self.ask,
            Tone::SellFill => self.sell_fill,
            Tone::Positive => self.positive,
            Tone::Negative => self.negative,
            Tone::Warning => self.warning,
            Tone::Invalid => self.invalid,
            Tone::Stale => self.stale,
            Tone::Selected => self.selected,
        }
    }
}

/// Sizes in logical points, kept beside the palette so every panel draws to one scale.
#[derive(Clone, Copy)]
pub(crate) struct Metrics {
    pub space_1: f32,
    pub space_2: f32,
    pub space_3: f32,
    pub corner_radius: f32,
    /// The bar on one edge that marks a selected cell, shared by the top bar, the pane headers and
    /// the tabs.
    pub accent_thickness: f32,
    pub segment_font: f32,
    pub dom_separator_fraction: f32,
    pub dom_row_height: f32,
    pub dom_row_height_floor: f32,
    pub dom_min_font: f32,
    pub dom_cell_pad: f32,
    pub dom_price_font: f32,
    pub dom_qty_font: f32,
    pub dom_mid_font: f32,
    pub dom_tag_font: f32,
    pub monitor_summary_height: f32,
    pub monitor_tab_height: f32,
    pub monitor_row_height: f32,
    pub monitor_feature_fraction: f32,
    /// The mid's share of the summary band; the two deltas split the rest. At the narrowest
    /// monitor, 300 points, any split from 0.36 to 0.46 keeps a ten-glyph mid such as 118234.505
    /// and a nine-glyph delta inside their cells. 0.40 is the round split that leaves about one
    /// glyph of spare room in both.
    pub monitor_mid_fraction: f32,
    pub monitor_mid_font: f32,
    pub monitor_delta_font: f32,
    pub monitor_micro_font: f32,
    pub monitor_feature_font: f32,
    pub monitor_channel_font: f32,
    pub monitor_tab_font: f32,
    pub monitor_badge_font: f32,
    pub monitor_value_col_w: f32,
    pub monitor_time_col_w: f32,
    pub monitor_qty_col_w: f32,
    pub monitor_intent_tag_w: f32,
    pub monitor_asset_label_w: f32,
    pub monitor_fill_tag_w: f32,
    /// The most of the chart pane the latency grid may take. The grid is only as tall as its rows
    /// need, and when that is more than this share of the pane it folds to a one-line summary, so
    /// the charts below keep the height their axes need.
    pub latency_fraction: f32,
    pub latency_label_col_w: f32,
    pub latency_header_font: f32,
    pub latency_row_font: f32,
    pub latency_value_font: f32,
    pub chart_pad: f32,
    pub chart_line_width: f32,
    pub chart_wick_width: f32,
    pub chart_min_body_width: f32,
    pub chart_body_gap: f32,
    pub chart_marker_radius: f32,
    pub chart_marker_outline: f32,
    pub chart_note_font: f32,
    pub chart_caption_font: f32,
    pub chart_axis_gutter: f32,
    pub chart_axis_font: f32,
}

pub(crate) const METRICS: Metrics = Metrics {
    space_1: 4.0,
    space_2: 8.0,
    space_3: 12.0,
    corner_radius: 3.0,
    accent_thickness: 2.0,
    segment_font: 12.0,
    dom_separator_fraction: 0.08,
    // Reference row: fonts are full size here and shrink proportionally below it, down to the
    // floor that clamps how many levels the operator can ask for.
    dom_row_height: 18.0,
    dom_row_height_floor: 8.5,
    dom_min_font: 7.0,
    dom_cell_pad: 8.0,
    dom_price_font: 13.0,
    dom_qty_font: 13.0,
    dom_mid_font: 16.0,
    dom_tag_font: 9.0,
    // The monitor's account, summary and tab bands are fixed. A pane tall enough to show every band
    // gives the feature list its fraction of what is left, up to the rows it needs, and the channel
    // the rest. A shorter pane scrolls.
    monitor_summary_height: 56.0,
    monitor_tab_height: TOUCH_HEIGHT,
    monitor_row_height: 18.0,
    monitor_feature_fraction: 0.43,
    monitor_mid_fraction: 0.40,
    monitor_mid_font: 18.0,
    monitor_delta_font: 15.0,
    monitor_micro_font: 9.0,
    monitor_feature_font: 12.0,
    monitor_channel_font: 11.0,
    monitor_tab_font: 12.0,
    monitor_badge_font: 10.0,
    monitor_value_col_w: 108.0,
    monitor_time_col_w: 84.0,
    monitor_qty_col_w: 72.0,
    monitor_intent_tag_w: 42.0,
    monitor_asset_label_w: 56.0,
    monitor_fill_tag_w: 30.0,
    latency_fraction: 0.25,
    latency_label_col_w: 112.0,
    latency_header_font: 10.0,
    latency_row_font: 11.0,
    latency_value_font: 12.0,
    chart_pad: 10.0,
    chart_line_width: 1.25,
    chart_wick_width: 1.0,
    chart_min_body_width: 1.0,
    chart_body_gap: 1.0,
    chart_marker_radius: 3.0,
    chart_marker_outline: 1.0,
    chart_note_font: 13.0,
    chart_caption_font: 10.0,
    chart_axis_gutter: 72.0,
    chart_axis_font: 11.0,
};

/// One physical pixel in logical points.
pub(crate) fn one_physical_pixel(painter: &egui::Painter) -> f32 {
    1.0 / painter.pixels_per_point()
}

pub(crate) fn hairline(painter: &egui::Painter, color: egui::Color32) -> egui::Stroke {
    egui::Stroke::new(one_physical_pixel(painter), color)
}

/// Snaps to a pixel centre, so a one-pixel line stays sharp instead of smearing over two.
pub(crate) fn crisp(painter: &egui::Painter, value: f32) -> f32 {
    painter.round_to_pixel_center(value)
}

/// Snap a region's OWN bottom edge to the last pixel row inside it. Plain [`crisp`] lands a
/// boundary on the row beyond the edge, which the scissor of a painter clipped to that region
/// discards — the separator would then draw or vanish depending on the window's height.
pub(crate) fn crisp_bottom_edge(painter: &egui::Painter, bottom: f32) -> f32 {
    crisp(painter, bottom - one_physical_pixel(painter))
}

/// [`egui::Painter::text`] with the anchor's y snapped to a pixel centre; x stays where it is.
pub(crate) fn text_at(
    painter: &egui::Painter,
    anchor: egui::Pos2,
    align: egui::Align2,
    text: &str,
    font: egui::FontId,
    color: egui::Color32,
) -> egui::Rect {
    let anchor = egui::pos2(anchor.x, crisp(painter, anchor.y));
    painter.text(anchor, align, text, font, color)
}

/// The column of `width` points centred across `rect`, at the full height of `rect`. Its offset
/// from the left of `rect` is a whole number of points, so it starts on a whole point whenever
/// `rect` does.
pub(crate) fn centred_column(rect: egui::Rect, width: f32) -> egui::Rect {
    let left = rect.left() + ((rect.width() - width) / 2.0).floor();
    egui::Rect::from_min_size(
        egui::pos2(left, rect.top()),
        egui::vec2(width, rect.height()),
    )
}

/// A border hairline at each x across `y_range`, each snapped to a pixel centre.
pub(crate) fn rules_v(
    painter: &egui::Painter,
    xs: impl IntoIterator<Item = f32>,
    y_range: egui::Rangef,
) {
    let stroke = hairline(painter, DARK.border);
    for x in xs {
        painter.vline(crisp(painter, x), y_range, stroke);
    }
}

/// A border hairline at each y across `x_range`, each snapped to a pixel centre.
pub(crate) fn rules_h(
    painter: &egui::Painter,
    ys: impl IntoIterator<Item = f32>,
    x_range: egui::Rangef,
) {
    let stroke = hairline(painter, DARK.border);
    for y in ys {
        painter.hline(x_range, crisp(painter, y), stroke);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Up,
    Down,
}

/// Painted as a shape rather than set as a glyph: the bundled default fonts have no face for the
/// arrows and triangles this would otherwise be written with.
pub(crate) fn triangle(
    center: egui::Pos2,
    size: f32,
    direction: Direction,
    color: egui::Color32,
) -> egui::Shape {
    let half = size / 2.0;
    let points = match direction {
        Direction::Up => vec![
            egui::pos2(center.x, center.y - half),
            egui::pos2(center.x - half, center.y + half),
            egui::pos2(center.x + half, center.y + half),
        ],
        Direction::Down => vec![
            egui::pos2(center.x, center.y + half),
            egui::pos2(center.x - half, center.y - half),
            egui::pos2(center.x + half, center.y - half),
        ],
    };
    egui::Shape::convex_polygon(points, color, egui::Stroke::NONE)
}

pub(crate) fn fill_pill(painter: &egui::Painter, rect: egui::Rect, border: egui::Color32) {
    painter.rect_filled(rect, METRICS.corner_radius, DARK.panel_raised);
    painter.rect_stroke(
        rect,
        METRICS.corner_radius,
        egui::Stroke::new(1.0, border),
        egui::StrokeKind::Inside,
    );
}

pub(crate) fn clamp_rect(bounds: egui::Rect, rect: egui::Rect) -> egui::Rect {
    let mut min = rect.min;
    if rect.right() > bounds.right() {
        min.x = bounds.right() - rect.width();
    }
    if rect.bottom() > bounds.bottom() {
        min.y = bounds.bottom() - rect.height();
    }
    min.x = min.x.max(bounds.left());
    min.y = min.y.max(bounds.top());
    egui::Rect::from_min_size(min, rect.size())
}

/// Paints `text` left-aligned in `rect`, cut with an ellipsis at its right end when it does not
/// fit, and returns the width it took.
pub(crate) fn paint_ellipsized(
    painter: &egui::Painter,
    rect: egui::Rect,
    text: &str,
    font: egui::FontId,
    color: egui::Color32,
) -> f32 {
    let mut job = egui::text::LayoutJob::single_section(
        text.to_owned(),
        egui::text::TextFormat {
            font_id: font,
            color,
            ..Default::default()
        },
    );
    job.halign = egui::Align::LEFT;
    job.wrap = egui::text::TextWrapping::truncate_at_width(rect.width().max(0.0));
    let galley = painter.layout_job(job);
    let width = galley.size().x;
    let pos = egui::pos2(rect.left(), rect.center().y - galley.size().y / 2.0);
    painter.galley(pos, galley, color);
    width
}

/// The colour behind the first frame, so the page never flashes grey while it starts.
pub(crate) fn clear_color() -> [f32; 4] {
    DARK.canvas.to_normalized_gamma_f32()
}

pub(crate) fn install_style(ctx: &egui::Context) {
    ctx.all_styles_mut(|style| {
        style.visuals.override_text_color = Some(DARK.text_primary);
        style.visuals.panel_fill = DARK.panel;
        style.visuals.window_fill = DARK.panel_raised;
        style.visuals.extreme_bg_color = DARK.canvas;
        style.spacing.item_spacing = egui::vec2(METRICS.space_2, METRICS.space_1);
        style.animation_time = 0.10;
    });
}
