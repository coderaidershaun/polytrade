//! The fixture page: the workstation's own arrangement, top bar and tab bar around every panel on a
//! fixed scene, stepped by keys.

use eframe::egui;

use polysim::workstation::browser::chart::mid::{self, ChartFrame};
use polysim::workstation::browser::chart::stack::{self, ChartStackFrame};
use polysim::workstation::browser::dom::{DomFrame, ladder};
use polysim::workstation::browser::latency::{self, LatencyFrame};
use polysim::workstation::browser::monitor::bands;
use polysim::workstation::browser::{announce_touch_screen, chrome};
use polysim::workstation::chart::position_view::RiskSeries;
use polysim::workstation::chart::view::ChartMode;
use polysim::workstation::dom_view::{DEFAULT_ROWS_PER_SIDE, DomOverlay};
use polysim::workstation::monitor::view::{Channel, MonitorFrame};
use polysim::workstation::shell::{Arrangement, View};

use crate::chart_scenes::{ChartScene, chart_scenes, latency_summary};
use crate::fit_readout;
use crate::hud::{self, HudBand};
use crate::link_scenes::{LinkScene, link_scenes};
use crate::monitor_feed::MonitorScene;
use crate::monitor_scenes::monitor_scenes;
use crate::position_scenes::{PositionScene, position_scenes};
use crate::scenes::{Scene, scenes};
use crate::shell_scene::{self, ShellScene};
use crate::{LeftPanel, query};

// SMELL: this colour, like the HUD's text colour, repeats one from the workstation's palette, which
// the library keeps private, so a palette change leaves the fixture's background and the HUD's text
// behind.
const CANVAS: egui::Color32 = egui::Color32::from_rgb(10, 13, 18);

/// The panes that report a fit readout. The ladder has none.
const READOUT_VIEWS: [View; 2] = [View::Chart, View::Monitor];

pub struct DomFixture {
    scenes: Panel<Scene>,
    variant: usize,
    monitors: Panel<MonitorScene>,
    charts: Panel<ChartScene>,
    chart_mode: ChartMode,
    positions: Panel<PositionScene>,
    risk_series: RiskSeries,
    left_panel: LeftPanel,
    latency: LatencyFrame,
    links: Panel<LinkScene>,
    shell: ShellScene,
}

/// The scenes of one panel and the one on show.
struct Panel<T> {
    items: Vec<T>,
    index: usize,
}

impl<T> Panel<T> {
    /// `number` is the operator's pick, counted from one. With no pick the panel starts on the
    /// first scene, and a pick past the end shows the last.
    fn new(items: Vec<T>, number: Option<usize>) -> Self {
        let index = number
            .map_or(0, |number| number.saturating_sub(1))
            .min(items.len() - 1);
        Self { items, index }
    }

    fn step(&mut self, delta: i32) {
        self.index = wrapped(self.index, delta, self.items.len());
    }

    fn current(&self) -> &T {
        &self.items[self.index]
    }

    fn current_mut(&mut self) -> &mut T {
        &mut self.items[self.index]
    }
}

impl DomFixture {
    /// Reads the starting selection from the page's query string, such as `?DOM_SCENE=3&MON_TAB=1`.
    pub fn new() -> Self {
        let scenes = Panel::new(scenes(), number("DOM_SCENE"));
        let variant = number("DOM_VARIANT")
            .unwrap_or(0)
            .min(scenes.current().variants.len() - 1);
        let mut monitors = Panel::new(monitor_scenes(), number("MON_SCENE"));
        if let Some(tab) = Channel::ALL.get(number("MON_TAB").unwrap_or(0)) {
            monitors.current_mut().state.active_tab = *tab;
        }
        // Choosing a risk scene also puts the stacked chart and positions view in the chart pane.
        let risk_scene = number("RISK_SCENE");
        Self {
            scenes,
            variant,
            monitors,
            charts: Panel::new(chart_scenes(), number("CHART_SCENE")),
            // The query can start in either chart mode, so each one can be captured without a key
            // press.
            chart_mode: match query("CHART_MODE").as_deref() {
                Some("candles") => ChartMode::Candles,
                _ => ChartMode::Line,
            },
            positions: Panel::new(position_scenes(), risk_scene),
            risk_series: match query("RISK_SERIES").as_deref() {
                Some("pnl") => RiskSeries::Pnl,
                _ => RiskSeries::Exposure,
            },
            left_panel: match risk_scene {
                Some(_) => LeftPanel::Positions,
                None => LeftPanel::Chart,
            },
            latency: LatencyFrame {
                summary: Some(latency_summary()),
            },
            links: Panel::new(link_scenes(), number("LINK")),
            shell: ShellScene::new(),
        }
    }

    fn handle_keys(&mut self, ctx: &egui::Context) {
        ctx.input(|input| {
            for (offset, key) in DIGIT_KEYS.iter().enumerate() {
                if input.key_pressed(*key) && offset < self.scenes.items.len() {
                    self.scenes.index = offset;
                    self.variant = 0;
                }
            }
            if input.key_pressed(egui::Key::ArrowDown) {
                self.step_scene(1);
            }
            if input.key_pressed(egui::Key::ArrowUp) {
                self.step_scene(-1);
            }
            if input.key_pressed(egui::Key::ArrowRight) {
                self.step_variant(1);
            }
            if input.key_pressed(egui::Key::ArrowLeft) {
                self.step_variant(-1);
            }
            if input.key_pressed(egui::Key::M) {
                self.monitors.step(1);
            }
            if input.key_pressed(egui::Key::T) {
                self.cycle_tab();
            }
            if input.key_pressed(egui::Key::C) {
                self.charts.step(1);
                self.left_panel = LeftPanel::Chart;
            }
            if input.key_pressed(egui::Key::V) {
                self.flip_chart_mode();
            }
            if input.key_pressed(egui::Key::P) {
                self.positions.step(1);
                self.left_panel = LeftPanel::Positions;
            }
            if input.key_pressed(egui::Key::X) {
                self.flip_risk_series();
            }
            if input.key_pressed(egui::Key::L) {
                self.links.step(1);
            }
        });
    }

    fn step_scene(&mut self, delta: i32) {
        self.scenes.step(delta);
        self.variant = 0;
    }

    fn step_variant(&mut self, delta: i32) {
        self.variant = wrapped(self.variant, delta, self.scenes.current().variants.len());
    }

    fn flip_chart_mode(&mut self) {
        self.chart_mode = match self.chart_mode {
            ChartMode::Line => ChartMode::Candles,
            ChartMode::Candles => ChartMode::Line,
        };
    }

    fn flip_risk_series(&mut self) {
        self.risk_series = match self.risk_series {
            RiskSeries::Exposure => RiskSeries::Pnl,
            RiskSeries::Pnl => RiskSeries::Exposure,
        };
    }

    /// Paints through the workstation's own chart stack, which sits in `charts` under the latency
    /// band of the chart pane `pane`. Returns the series the operator clicked.
    fn paint_stack(
        &self,
        ui: &mut egui::Ui,
        charts: egui::Rect,
        pane: egui::Rect,
    ) -> Option<RiskSeries> {
        let scene = self.positions.current();
        stack::paint(
            ui,
            charts,
            &ChartStackFrame {
                chart: &scene.chart,
                positions: &scene.positions,
                instrument: scene.instrument,
                tick: scene.tick,
                mode: self.chart_mode,
                series: self.risk_series,
                pane,
            },
        )
    }

    fn cycle_tab(&mut self) {
        let state = &mut self.monitors.current_mut().state;
        let index = Channel::ALL
            .iter()
            .position(|channel| *channel == state.active_tab)
            .unwrap_or(0);
        state.active_tab = Channel::ALL[(index + 1) % Channel::ALL.len()];
    }
}

impl DomFixture {
    /// Paints the HUD band first and arranges the panes in what is left, so no pane sits under the
    /// band. The band keeps a row for each pane that reports a fit, and fills them last.
    fn paint_page(&mut self, ui: &mut egui::Ui) {
        let full = ui.available_rect_before_wrap();
        ui.painter_at(full).rect_filled(full, 0.0, CANVAS);

        // The size class and the shown view depend on the width and the selection alone, so an
        // arrangement of the whole canvas can name them before the band's height is known. A key
        // can change the selection, so the rows are counted from an arrangement made after it.
        let page = self.shell.arrange(full);
        self.shell.handle_keys(ui.ctx(), &page);
        let page = self.shell.arrange(full);
        let band = self
            .shell
            .is_hud_shown()
            .then(|| self.paint_hud(ui, full, &page));
        let band_height = band.as_ref().map_or(0.0, |band| band.height);
        let below = full.with_min_y((full.top() + band_height).min(full.bottom()));
        let arrangement = self.shell.arrange(below);

        let action = chrome::paint_top_bar(ui, &arrangement, &self.shell.top_bar_frame());
        self.shell.apply(action, arrangement.class);
        let readouts = self.paint_panes(ui, &arrangement);
        let action = chrome::paint_tab_bar(ui, &arrangement);
        self.shell.apply(action, arrangement.class);
        self.links.current().paint(ui, arrangement.link_bar.into());
        if let Some(band) = band {
            band.paint_readouts(&ui.painter_at(full), &readouts);
        }
    }

    fn paint_hud(&self, ui: &egui::Ui, canvas: egui::Rect, page: &Arrangement) -> HudBand {
        let dom_scene = self.scenes.current();
        let monitor = self.monitors.current();
        let position_scene = self.positions.current();
        hud::paint(
            &ui.painter_at(canvas),
            canvas,
            &hud::Hud {
                dom_index: self.scenes.index,
                dom_count: self.scenes.items.len(),
                dom: dom_scene,
                variant: dom_scene.variants[self.variant].label,
                monitor: monitor.name,
                tab: monitor.state.active_tab,
                chart: self.charts.current().name,
                mode: self.chart_mode,
                position: position_scene.name,
                check: position_scene.check,
                series: self.risk_series,
                left_panel: self.left_panel,
                shown: page.shown,
                class: page.class,
                size: canvas.size(),
                zoom: ui.ctx().zoom_factor(),
                link_index: self.links.index,
                link_count: self.links.items.len(),
                link: self.links.current().name,
                readout_count: READOUT_VIEWS
                    .iter()
                    .filter(|view| page.pane(**view).is_some())
                    .count(),
            },
        )
    }

    /// The pane headers stay empty, because their controls change a live model the fixture does
    /// not have. Returns the fit readout of each pane that has one, in the order of
    /// `READOUT_VIEWS`.
    fn paint_panes(&mut self, ui: &mut egui::Ui, arrangement: &Arrangement) -> Vec<String> {
        let mut readouts = Vec::new();
        chrome::paint_panes(ui, arrangement, |ui, view, pane| match view {
            View::Chart => readouts.push(self.paint_chart(ui, pane.body.into())),
            View::Monitor => readouts.push(self.paint_monitor(ui, pane.body.into())),
            View::Ladder => self.paint_ladder(ui, pane.body.into()),
        });
        readouts
    }

    /// Paints the latency band and the charts under it through the workstation's own painters, as
    /// the page does.
    fn paint_chart(&self, ui: &mut egui::Ui, body: egui::Rect) -> String {
        let (band, charts) = latency::split(body);
        latency::paint(ui, band, &self.latency);
        let fit = match self.left_panel {
            LeftPanel::Chart => {
                let chart_scene = self.charts.current();
                let painted = mid::paint(
                    ui,
                    charts,
                    &ChartFrame {
                        chart: &chart_scene.chart,
                        instrument: chart_scene.instrument,
                        mode: self.chart_mode,
                        tick: chart_scene.tick,
                        domain: None,
                    },
                );
                fit_readout::mid_axis(painted.geometry.as_ref(), self.chart_mode)
            }
            // The workstation applies the operator's click. The fixture switches series by key.
            LeftPanel::Positions => {
                let _picked = self.paint_stack(ui, charts, body);
                fit_readout::stack(charts)
            }
        };
        format!("chart {} | {fit}", fit_readout::latency(band))
    }

    fn paint_monitor(&mut self, ui: &mut egui::Ui, body: egui::Rect) -> String {
        // The ladder variant's own unit, so cycling scenes shows both delta captions.
        let dom_unit = self.scenes.current().variants[self.variant].grouping.unit();
        let monitor = self.monitors.current_mut();
        let monitor_frame = MonitorFrame {
            model: &monitor.model,
            instrument: monitor.instrument,
            tick: monitor.tick,
            qty_scale: monitor.qty_scale,
            qty_decimals: monitor.qty_decimals,
            dom_unit,
            feature_names: &monitor.feature_names,
            instrument_names: &monitor.instrument_names,
        };
        bands::paint(ui, body, &monitor_frame, &mut monitor.state);
        fit_readout::monitor(
            body,
            monitor.state.active_tab,
            monitor.model.monitor().feature_count(),
        )
    }

    fn paint_ladder(&self, ui: &mut egui::Ui, body: egui::Rect) {
        let variant = &self.scenes.current().variants[self.variant];
        let dom_frame = DomFrame {
            snapshot: variant.snapshot.as_ref(),
            overlay: DomOverlay {
                desired: variant.quote,
                bid_orders: &variant.bid_orders,
                ask_orders: &variant.ask_orders,
            },
            tick: variant.tick,
            grouping: variant.grouping,
            price_decimals: variant.price_decimals,
            qty_scale: variant.qty_scale,
            qty_decimals: variant.qty_decimals,
            feed: variant.feed,
            stale_age: variant.stale_age,
            levels: dom_levels(),
        };
        ladder::paint(ui, body, &dom_frame);
    }
}

impl eframe::App for DomFixture {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        shell_scene::fit_zoom(ctx);
    }

    fn raw_input_hook(&mut self, ctx: &egui::Context, raw_input: &mut egui::RawInput) {
        announce_touch_screen(ctx, raw_input);
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.handle_keys(&ui.ctx().clone());
        self.paint_page(ui);
    }
}

/// `DOM_LEVELS` reproduces any position of the header's level stepper, which the fixture does not
/// paint — the point is to eyeball the ladder at the dense end without a live engine.
fn dom_levels() -> usize {
    number("DOM_LEVELS").unwrap_or(DEFAULT_ROWS_PER_SIDE)
}

/// A value that is not a number gives no pick.
fn number(name: &str) -> Option<usize> {
    query(name)?.parse().ok()
}

fn wrapped(index: usize, delta: i32, count: usize) -> usize {
    (index as i32 + delta).rem_euclid(count as i32) as usize
}

const DIGIT_KEYS: [egui::Key; 9] = [
    egui::Key::Num1,
    egui::Key::Num2,
    egui::Key::Num3,
    egui::Key::Num4,
    egui::Key::Num5,
    egui::Key::Num6,
    egui::Key::Num7,
    egui::Key::Num8,
    egui::Key::Num9,
];
