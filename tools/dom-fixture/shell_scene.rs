//! The fixture's share of the shell: the catalog the top bar reads, the halt latch, the view on
//! show and the HUD switch. The query string sets each and a key flips it, so every size class and
//! every badge can be seen on fixed scenes.

use eframe::egui;

use polysim::config::ExecutionMode;
use polysim::hot::exec::{ExecHalt, HaltReason};
use polysim::ids::{AssetId, InstrumentId};
use polysim::msg::ui::{UiCatalog, UiInstrument};
use polysim::time::TsUs;
use polysim::workstation::browser::chrome::{ChromeAction, TopBarFrame};
use polysim::workstation::shell::{self, Arrangement, Navigation, Selection, SizeClass};

use crate::query;

const STRATEGY_ID: &str = "strat-micro-recorder";
// SMELL: this repeats the library's list of execution modes, so a mode added there is missing here
// until someone adds it by hand. The library's enum can publish its own list of every mode instead.
const EXECUTION_MODES: [ExecutionMode; 3] =
    [ExecutionMode::Off, ExecutionMode::Sim, ExecutionMode::Live];

pub struct ShellScene {
    catalog: UiCatalog,
    selected: InstrumentId,
    halt: Option<ExecHalt>,
    navigation: Navigation,
    is_hud_shown: bool,
}

impl ShellScene {
    /// Reads `VIEW=desk|chart|ladder|monitor`, `HALT=1`, `EXEC=off|sim|live` and `HUD=0` from the
    /// page's query string.
    pub fn new() -> Self {
        let selection =
            query("VIEW").map_or(Selection::Desk, |name| Selection::from_fragment(&name));
        let name = query("EXEC");
        let mode = EXECUTION_MODES
            .into_iter()
            .find(|mode| Some(mode.as_str()) == name.as_deref())
            .unwrap_or(ExecutionMode::Sim);
        let instruments = [(0, "BTC/USDT spot", "BTC"), (1, "ETH/USDT spot", "ETH")]
            .map(|(index, display, base)| instrument(InstrumentId(index), display, base));
        Self {
            selected: instruments[0].instrument_id,
            catalog: UiCatalog {
                strategy_id: STRATEGY_ID.into(),
                window_title: STRATEGY_ID.into(),
                execution_mode: Some(mode),
                spin_interval_us: 1_000_000,
                instruments: instruments.into(),
                feature_names: Vec::new(),
            },
            halt: (query("HALT").as_deref() == Some("1")).then(halted),
            navigation: Navigation::new(selection),
            is_hud_shown: query("HUD").as_deref() != Some("0"),
        }
    }

    pub fn arrange(&self, canvas: egui::Rect) -> Arrangement {
        Arrangement::new(canvas.into(), &self.navigation, self.halt)
    }

    pub fn top_bar_frame(&self) -> TopBarFrame<'_> {
        TopBarFrame {
            catalog: Some(&self.catalog),
            selected: self.selected,
        }
    }

    pub fn is_hud_shown(&self) -> bool {
        self.is_hud_shown
    }

    pub fn apply(&mut self, action: ChromeAction, class: SizeClass) {
        match action {
            ChromeAction::None => {}
            ChromeAction::SelectInstrument(instrument) => self.selected = instrument,
            ChromeAction::Navigate(selection) => self.navigation.select(selection, class),
        }
    }

    /// `N` steps to the next view, `K` flips the halt latch, `E` cycles the execution mode and `H`
    /// shows or hides the HUD. No scene uses any of these keys.
    pub fn handle_keys(&mut self, ctx: &egui::Context, arrangement: &Arrangement) {
        let [next, halt, mode, hud] = [egui::Key::N, egui::Key::K, egui::Key::E, egui::Key::H]
            .map(|key| ctx.input(|input| input.key_pressed(key)));
        if next {
            self.step_view(arrangement);
        }
        if halt {
            self.halt = self.halt.is_none().then(halted);
        }
        if mode {
            self.cycle_execution_mode();
        }
        if hud {
            self.is_hud_shown = !self.is_hud_shown;
        }
        if next || halt || mode || hud {
            ctx.request_repaint();
        }
    }

    /// The steps wrap, so the desk follows the monitor. A compact canvas has no desk, so the step
    /// skips it there.
    fn step_view(&mut self, arrangement: &Arrangement) {
        let at = Selection::ALL
            .iter()
            .position(|selection| *selection == arrangement.shown)
            .unwrap_or(0);
        let next = Selection::ALL
            .into_iter()
            .cycle()
            .skip(at + 1)
            .find(|selection| {
                *selection != Selection::Desk || arrangement.class != SizeClass::Compact
            });
        if let Some(selection) = next {
            self.navigation.select(selection, arrangement.class);
        }
    }

    fn cycle_execution_mode(&mut self) {
        let at = EXECUTION_MODES
            .iter()
            .position(|mode| Some(*mode) == self.catalog.execution_mode)
            .unwrap_or(0);
        self.catalog.execution_mode = Some(EXECUTION_MODES[(at + 1) % EXECUTION_MODES.len()]);
    }
}

/// The same zoom rule as the page, so a fixture window below 320 by 400 behaves like it.
pub fn fit_zoom(ctx: &egui::Context) {
    let css = ctx.content_rect().size() * ctx.zoom_factor();
    if let Some(zoom) = shell::zoom_to_fit(css.x, css.y, ctx.zoom_factor()) {
        ctx.set_zoom_factor(zoom);
    }
}

fn halted() -> ExecHalt {
    ExecHalt::Halted {
        reason: HaltReason::RejectStreak,
        halted_ts_us: TsUs::from_micros(0),
    }
}

fn instrument(instrument_id: InstrumentId, display: &str, base: &str) -> UiInstrument {
    UiInstrument {
        instrument_id,
        display: display.into(),
        base: base.into(),
        quote: "USDT".into(),
        base_asset: AssetId(0),
        quote_asset: AssetId(1),
        tick_size: None,
        lot_size: None,
        qty_scale: 1,
    }
}
