//! The text band over the page: which scene each panel shows, what the shell shows, the keys that
//! step them, and a row for each pane that reports its fit.

use eframe::egui;

use polysim::workstation::chart::position_view::RiskSeries;
use polysim::workstation::chart::view::ChartMode;
use polysim::workstation::monitor::view::Channel;
use polysim::workstation::shell::{Selection, SizeClass};

use crate::LeftPanel;
use crate::scenes::Scene;

const TEXT: egui::Color32 = egui::Color32::from_rgb(145, 157, 174);
const TEXT_DIM: egui::Color32 = egui::Color32::from_rgb(96, 106, 120);

const PAD: f32 = 10.0;
const LINE_GAP: f32 = 3.0;

pub struct Hud<'a> {
    pub dom_index: usize,
    pub dom_count: usize,
    pub dom: &'a Scene,
    pub variant: &'a str,
    pub monitor: &'a str,
    pub tab: Channel,
    pub chart: &'a str,
    pub mode: ChartMode,
    pub position: &'a str,
    pub check: &'a str,
    pub series: RiskSeries,
    pub left_panel: LeftPanel,
    pub shown: Selection,
    pub class: SizeClass,
    pub size: egui::Vec2,
    pub zoom: f32,
    pub link_index: usize,
    pub link_count: usize,
    pub link: &'a str,
    /// How many panes report a fit readout. The band keeps a row for each.
    pub readout_count: usize,
}

/// The band's height and the rows it keeps for the panes' readouts. The rows are filled after the
/// panes paint, because a readout reports what its pane measured.
pub struct HudBand {
    pub height: f32,
    rows: Vec<egui::Rect>,
}

impl HudBand {
    /// Pairs readouts with rows in order, so a readout without a row is dropped and a row without a
    /// readout stays blank. A readout wider than its row ends in an ellipsis on a whole glyph.
    pub fn paint_readouts(&self, painter: &egui::Painter, readouts: &[String]) {
        for (row, readout) in self.rows.iter().zip(readouts) {
            let mut job =
                egui::text::LayoutJob::simple_singleline(readout.clone(), body_font(), TEXT);
            job.wrap = egui::text::TextWrapping::truncate_at_width(row.width());
            painter.galley(row.min, painter.layout_job(job), TEXT);
        }
    }
}

/// Paints the band at the top of `canvas`, every line wrapped to its width, and leaves one blank
/// row, a text line tall, for each readout. The page starts below the band.
pub fn paint(painter: &egui::Painter, canvas: egui::Rect, hud: &Hud<'_>) -> HudBand {
    let wrap_width = (canvas.width() - 2.0 * PAD).max(1.0);
    let mut top = canvas.top() + PAD;
    for (font, color, text) in lines(hud) {
        let galley = painter.layout(text, font, color, wrap_width);
        let height = galley.size().y;
        painter.galley(egui::pos2(canvas.left() + PAD, top), galley, color);
        top += height + LINE_GAP;
    }
    let row_height = painter
        .layout_no_wrap("0".to_owned(), body_font(), TEXT)
        .size()
        .y;
    let mut rows = Vec::with_capacity(hud.readout_count);
    for _ in 0..hud.readout_count {
        let size = egui::vec2(wrap_width, row_height);
        let row = egui::Rect::from_min_size(egui::pos2(canvas.left() + PAD, top), size);
        rows.push(row);
        top += row_height + LINE_GAP;
    }
    HudBand {
        height: top + PAD - LINE_GAP - canvas.top(),
        rows,
    }
}

fn body_font() -> egui::FontId {
    egui::FontId::monospace(11.0)
}

fn lines(hud: &Hud<'_>) -> Vec<(egui::FontId, egui::Color32, String)> {
    let title = (
        egui::FontId::monospace(13.0),
        TEXT,
        format!(
            "dom {}/{}  {}",
            hud.dom_index + 1,
            hud.dom_count,
            hud.dom.name
        ),
    );
    let mut lines = vec![title];
    let mut line = |color: egui::Color32, text: String| {
        lines.push((body_font(), color, text));
    };
    if !hud.variant.is_empty() {
        line(TEXT_DIM, format!("variant: {}", hud.variant));
    }
    line(
        TEXT,
        format!("monitor: {}  |  tab {}", hud.monitor, hud.tab.label()),
    );
    // The line for the left panel that is not on screen is dimmed, so a reviewer sees at a glance
    // which panel a key press switched to.
    line(
        active_text(hud.left_panel, LeftPanel::Chart),
        format!("chart: {}  |  {}", hud.chart, mode_label(hud.mode)),
    );
    line(
        active_text(hud.left_panel, LeftPanel::Positions),
        format!("risk: {}  |  {}", hud.position, series_label(hud.series)),
    );
    // While the positions panel is on screen, show what its scene asks the reviewer to verify, so
    // they do not have to look it up.
    if hud.left_panel == LeftPanel::Positions {
        line(TEXT, format!("check: {}", hud.check));
    }
    line(
        TEXT,
        format!(
            "view {}  class {}  {:.0}x{:.0} pt  zoom {:.2}  link {}/{} {}",
            hud.shown.fragment(),
            class_label(hud.class),
            hud.size.x,
            hud.size.y,
            hud.zoom,
            hud.link_index + 1,
            hud.link_count,
            hud.link
        ),
    );
    line(
        TEXT_DIM,
        "1-9 dom | left/right var | up/down step | M mon | T tab | C chart | V line/candles | P risk | X series | L link | N view | K halt | E exec | H hud".to_string(),
    );
    lines
}

fn active_text(active: LeftPanel, line: LeftPanel) -> egui::Color32 {
    if active == line { TEXT } else { TEXT_DIM }
}

fn series_label(series: RiskSeries) -> &'static str {
    match series {
        RiskSeries::Exposure => "exposure",
        RiskSeries::Pnl => "pnl",
    }
}

fn mode_label(mode: ChartMode) -> &'static str {
    match mode {
        ChartMode::Line => "line",
        ChartMode::Candles => "candles",
    }
}

fn class_label(class: SizeClass) -> &'static str {
    match class {
        SizeClass::Compact => "compact",
        SizeClass::Medium => "medium",
        SizeClass::Wide => "wide",
    }
}
