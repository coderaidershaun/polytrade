//! Where the ladder and its rows sit inside the pane, and the size their text starts at. The
//! painter, the overlay and the touch pin all measure from these same rectangles.

use eframe::egui;

use crate::workstation::browser::theme::{self, METRICS};
use crate::workstation::dom_view::{DomRow, DomView, MAX_ROWS_PER_SIDE, fit_rows};

/// The widest a column grows. A wider pane gets a centred ladder of three full columns instead of
/// columns so wide that a quantity sits a long way from its price.
const COLUMN_CEILING: f32 = 160.0;

/// The separator band is never shorter than this, so the mid's own line fits inside its borders
/// even where the band's share of a short pane would cut it off.
const SEPARATOR_FLOOR: f32 = 26.0;

/// The column of one side's rows that holds the public size, and the one that holds the strategy's
/// own orders, counted from the left. The price column sits between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SideColumns {
    pub(super) public: usize,
    pub(super) strategy: usize,
}

pub(super) const ASK_COLUMNS: SideColumns = SideColumns {
    public: 2,
    strategy: 0,
};

pub(super) const BID_COLUMNS: SideColumns = SideColumns {
    public: 0,
    strategy: 2,
};

pub(super) struct DomLayout {
    pub(super) rect: egui::Rect,
    pub(super) separator: egui::Rect,
    pub(super) column_width: f32,
    pub(super) rows_per_side: usize,
    pub(super) row_height: f32,
    pub(super) fonts: RowFonts,
    /// This row's height against the reference row. Everything sized per row shrinks by it.
    pub(super) scale: f32,
}

/// Row fonts shrink with the row so a dense ladder stays legible rather than overlapping. `tag` is
/// `None` once the status word would fall under the legible floor: colour and the quote ring still
/// carry status there, and the hover readout still spells it out in words.
pub(super) struct RowFonts {
    pub(super) price: f32,
    pub(super) qty: f32,
    pub(super) tag: Option<f32>,
}

impl RowFonts {
    fn at(scale: f32) -> Self {
        let scaled = |size: f32| (size * scale).max(METRICS.dom_min_font);
        Self {
            price: scaled(METRICS.dom_price_font),
            qty: scaled(METRICS.dom_qty_font),
            tag: Some(METRICS.dom_tag_font * scale).filter(|size| *size >= METRICS.dom_min_font),
        }
    }
}

fn separator_of(rect: egui::Rect) -> egui::Rect {
    let height = (rect.height() * METRICS.dom_separator_fraction)
        .max(SEPARATOR_FLOOR)
        .min(rect.height());
    let center_y = rect.center().y;
    rect.with_min_y(center_y - height / 2.0)
        .with_max_y(center_y + height / 2.0)
}

fn side_height(rect: egui::Rect) -> f32 {
    (separator_of(rect).top() - rect.top()).max(0.0)
}

/// Rows per side `body` can paint without going under the legibility floor. The header's level
/// control reports this when it is below the operator's choice, so a clamped ladder is visible
/// rather than silently short.
pub(crate) fn rows_that_fit(body: egui::Rect) -> usize {
    fit_rows(
        side_height(body),
        MAX_ROWS_PER_SIDE,
        METRICS.dom_row_height_floor,
    )
    .rows
}

/// The block the ladder takes in `body`: it fills the height, and once its three columns reach their
/// widest, it sits centred across the width. The header's controls sit over the same block.
pub(crate) fn ladder_rect(body: egui::Rect) -> egui::Rect {
    theme::centred_column(body, body.width().min(3.0 * COLUMN_CEILING))
}

impl DomLayout {
    pub(super) fn new(body: egui::Rect, levels: usize) -> Self {
        let rect = ladder_rect(body);
        let fit = fit_rows(side_height(rect), levels, METRICS.dom_row_height_floor);
        let scale = (fit.row_height / METRICS.dom_row_height).min(1.0);
        Self {
            rect,
            separator: separator_of(rect),
            column_width: rect.width() / 3.0,
            rows_per_side: fit.rows,
            row_height: fit.row_height,
            fonts: RowFonts::at(scale),
            scale,
        }
    }

    pub(super) fn ask_row(&self, index: usize) -> egui::Rect {
        let bottom = self.separator.top() - index as f32 * self.row_height;
        self.rect
            .with_min_y(bottom - self.row_height)
            .with_max_y(bottom)
    }

    pub(super) fn bid_row(&self, index: usize) -> egui::Rect {
        let top = self.separator.bottom() + index as f32 * self.row_height;
        self.rect.with_min_y(top).with_max_y(top + self.row_height)
    }

    pub(super) fn cell(&self, row: egui::Rect, column: usize) -> egui::Rect {
        let left = self.rect.left() + column as f32 * self.column_width;
        row.with_min_x(left).with_max_x(left + self.column_width)
    }

    /// Finds the row under `pointer`, with its rectangle. A pointer on the separator, in the margin
    /// beside the ladder or past the last row of the book finds none.
    pub(super) fn row_at<'view>(
        &self,
        view: &'view DomView,
        pointer: egui::Pos2,
    ) -> Option<(egui::Rect, &'view DomRow)> {
        if !self.rect.contains(pointer) {
            return None;
        }
        if pointer.y < self.separator.top() {
            let index = ((self.separator.top() - pointer.y) / self.row_height).floor() as usize;
            return Some((self.ask_row(index), view.ask_rows().get(index)?));
        }
        if pointer.y > self.separator.bottom() {
            let index = ((pointer.y - self.separator.bottom()) / self.row_height).floor() as usize;
            return Some((self.bid_row(index), view.bid_rows().get(index)?));
        }
        None
    }
}
