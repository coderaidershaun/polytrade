//! The live workstation page: the top bar, the panes the arrangement shows, and the tab bar. The
//! pane headers hold display switches only, which never reach the engine: the chart's line or
//! candles mode, and the ladder's unit, grouping and depth.

use eframe::egui;

use super::chrome::{self, ChromeAction, TopBarFrame};
use super::controls::{self, DropdownSpec};
use super::latency::{self, LatencyFrame};
use super::theme;
use crate::workstation::attachment::Attachment;
use crate::workstation::browser::chart::stack::{self, ChartStackFrame};
use crate::workstation::browser::dom::{self, DomFrame, ladder};
use crate::workstation::browser::monitor::bands;
use crate::workstation::chart::view::ChartMode;
use crate::workstation::dom_view::{DomUnit, FeedStatus, MAX_ROWS_PER_SIDE, MIN_ROWS_PER_SIDE};
use crate::workstation::format;
use crate::workstation::model::UiModel;
use crate::workstation::monitor::view::{MonitorFrame, MonitorUiState, instrument_names};
use crate::workstation::shell::{Arrangement, Pane, Selection, View};
use crate::workstation::text::{grouping_label, grouping_table};

const CHART_MODES: [(ChartMode, &str); 2] =
    [(ChartMode::Line, "Line"), (ChartMode::Candles, "Candles")];

const DOM_UNITS: [(DomUnit, &str); 2] = [(DomUnit::Ticks, "Ticks"), (DomUnit::Bps, "bps")];

const CHART_MODE_WIDTH: f32 = 112.0;
// Together these three cells are as wide as the narrowest ladder pane, 300 points.
const DOM_UNIT_WIDTH: f32 = 84.0;
const DOM_GROUPING_WIDTH: f32 = 88.0;
const DOM_LEVELS_WIDTH: f32 = 128.0;

/// Paints everything but the link bar. Returns the view the operator picked on the top bar or the
/// tab bar, for the caller to apply.
pub(crate) fn workstation(
    ui: &mut egui::Ui,
    arrangement: &Arrangement,
    attachment: &mut Attachment,
) -> Option<Selection> {
    let feed = attachment.feed_status();
    let (model, monitor_state) = attachment.panels();
    let mut picked_view = None;

    let frame = TopBarFrame {
        catalog: model.catalog(),
        selected: model.selected(),
    };
    match chrome::paint_top_bar(ui, arrangement, &frame) {
        ChromeAction::SelectInstrument(instrument) => model.select(instrument),
        ChromeAction::Navigate(selection) => picked_view = Some(selection),
        ChromeAction::None => {}
    }

    chrome::paint_panes(ui, arrangement, |ui, view, pane| match view {
        View::Chart => paint_chart_pane(ui, pane, model),
        View::Monitor => paint_monitor_pane(ui, pane, model, monitor_state),
        View::Ladder => paint_ladder_pane(ui, pane, model, feed),
    });

    if let ChromeAction::Navigate(selection) = chrome::paint_tab_bar(ui, arrangement) {
        picked_view = Some(selection);
    }
    picked_view
}

fn paint_chart_pane(ui: &mut egui::Ui, pane: Pane, model: &mut UiModel) {
    if let Some(header) = pane.header {
        paint_chart_header(ui, header.into(), model);
    }
    paint_chart_body(ui, pane.body.into(), model);
}

fn paint_chart_header(ui: &mut egui::Ui, rect: egui::Rect, model: &mut UiModel) {
    let painter = ui.painter_at(rect);
    let toggle = rect.with_max_x((rect.left() + CHART_MODE_WIDTH).min(rect.right()));
    let current = model.chart_mode();
    if let Some(mode) =
        controls::paint_segmented_toggle(ui, toggle, "chart-mode", &CHART_MODES, current)
    {
        model.set_chart_mode(mode);
    }
    theme::rules_v(&painter, [toggle.right()], rect.y_range());
}

fn paint_chart_body(ui: &mut egui::Ui, body: egui::Rect, model: &mut UiModel) {
    let (latency_rect, charts) = latency::split(body);
    latency::paint(
        ui,
        latency_rect,
        &LatencyFrame {
            summary: model.latency(),
        },
    );

    let picked = {
        let frame = ChartStackFrame {
            chart: model.chart(),
            positions: model.positions(),
            instrument: model.selected(),
            tick: model.instrument_scales().tick,
            mode: model.chart_mode(),
            series: model.risk_series(),
            pane: body,
        };
        stack::paint(ui, charts, &frame)
    };
    if let Some(series) = picked {
        model.set_risk_series(series);
    }
}

fn paint_monitor_pane(
    ui: &mut egui::Ui,
    pane: Pane,
    model: &UiModel,
    monitor_state: &mut MonitorUiState,
) {
    let instrument_names = instrument_names(model);
    let frame = MonitorFrame::for_selected(model, &instrument_names);
    bands::paint(ui, pane.body.into(), &frame, monitor_state);
}

/// Hands out the next full-height cell in a control strip, so the controls touch instead of
/// floating apart.
fn strip_cell(strip: egui::Rect, cursor: &mut f32, width: f32) -> egui::Rect {
    let left = *cursor;
    *cursor += width;
    strip.with_min_x(left).with_max_x(left + width)
}

fn paint_ladder_pane(ui: &mut egui::Ui, pane: Pane, model: &mut UiModel, feed: FeedStatus) {
    let body = egui::Rect::from(pane.body);
    if let Some(header) = pane.header {
        let strip = egui::Rect::from(header);
        let block = dom::ladder_rect(body);
        let controls = strip.with_min_x(block.left()).with_max_x(block.right());
        paint_dom_header(ui, controls, model, dom::rows_that_fit(body));
        // The ladder rules its outer edges when it is narrower than its pane, so its header does too.
        if controls.width() < strip.width() {
            theme::rules_v(
                &ui.painter_at(strip),
                [controls.left(), controls.right()],
                strip.y_range(),
            );
        }
    }

    let scales = model.instrument_scales();
    let source = model.dom_source(feed);
    let frame = DomFrame {
        snapshot: model.book(model.selected()),
        overlay: source.overlay,
        tick: scales.tick,
        grouping: model.dom_grouping(),
        levels: model.dom_levels(),
        price_decimals: format::price_decimals(scales.tick),
        qty_scale: scales.qty_scale,
        qty_decimals: scales.qty_decimals,
        feed: source.feed,
        stale_age: source.stale_age,
    };
    ladder::paint(ui, body, &frame);
}

fn paint_dom_header(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    model: &mut UiModel,
    rows_that_fit: usize,
) {
    let painter = ui.painter_at(rect);

    let mut cursor = rect.left();
    let toggle = strip_cell(rect, &mut cursor, DOM_UNIT_WIDTH);
    let current = model.dom_unit();
    if let Some(unit) =
        controls::paint_segmented_toggle(ui, toggle, "dom-unit", &DOM_UNITS, current)
    {
        model.set_dom_unit(unit);
    }

    let combo = strip_cell(rect, &mut cursor, DOM_GROUPING_WIDTH);
    paint_grouping_dropdown(ui, combo, model);

    // Right-aligned, but never allowed to run over the grouping dropdown on a narrow pane.
    let levels_rect = rect.with_min_x((rect.right() - DOM_LEVELS_WIDTH).max(combo.right()));
    let spec = controls::StepperSpec {
        label: "LVL",
        value: model.dom_levels(),
        min: MIN_ROWS_PER_SIDE,
        max: MAX_ROWS_PER_SIDE,
        capped_to: rows_that_fit,
    };
    if let Some(chosen) = controls::paint_stepper(ui, levels_rect, "dom-levels", spec) {
        model.set_dom_levels(chosen);
    }

    let seams = [toggle.right(), combo.right(), levels_rect.left()];
    theme::rules_v(&painter, seams, rect.y_range());
}

fn paint_grouping_dropdown(ui: &mut egui::Ui, rect: egui::Rect, model: &mut UiModel) {
    let current = model.dom_grouping();
    let mut chosen = current;
    let table = grouping_table(current.unit());
    let spec = DropdownSpec {
        rect,
        id_salt: "polysim-dom-grouping",
        selected_text: grouping_label(current),
        row_count: table.len(),
    };
    controls::paint_dropdown(ui, spec, |ui| {
        for (grouping, label) in table {
            ui.selectable_value(&mut chosen, *grouping, *label);
        }
    });
    if chosen != current {
        model.set_dom_grouping(chosen);
    }
}
