//! Hand-painted controls the panels share, so every control looks and behaves alike. Cells fill
//! their container and are parted by hairlines the container draws, and a selected cell gets a fill
//! and an accent bar on one edge, never a box.

use eframe::egui;

use super::theme::{self, DARK, METRICS};

/// A dropdown that fills `rect`. The popup's rows are as tall as the cell, so a cell sized as a
/// touch target gets touch-sized rows too, and the popup is tall enough to show all `row_count`
/// rows.
pub(crate) struct DropdownSpec<'a> {
    pub rect: egui::Rect,
    pub id_salt: &'a str,
    pub selected_text: &'a str,
    pub row_count: usize,
}

pub(crate) fn paint_dropdown(
    ui: &mut egui::Ui,
    spec: DropdownSpec<'_>,
    rows: impl FnOnce(&mut egui::Ui),
) {
    let DropdownSpec {
        rect,
        id_salt,
        selected_text,
        row_count,
    } = spec;
    let height = rect.height();
    let rows_height = row_count as f32 * (height + METRICS.space_1);
    // egui builds the popup from the page-wide style, not this scope's, so the popup needs its own
    // row height. It floats above the panes, so it keeps rounded corners while inline cells stay
    // square.
    let popup_style = egui::style::StyleModifier::new(move |style| {
        style.spacing.interact_size.y = height;
        style.visuals.menu_corner_radius = egui::CornerRadius::same(METRICS.corner_radius as u8);
    });
    let builder = egui::UiBuilder::new()
        .max_rect(rect)
        .layout(egui::Layout::left_to_right(egui::Align::Center));
    ui.scope_builder(builder, |ui| {
        style_combo(ui.style_mut(), height);
        egui::ComboBox::from_id_salt(id_salt)
            .width(rect.width())
            .height(rows_height)
            .truncate()
            .popup_style(popup_style)
            .selected_text(selected_text)
            .show_ui(ui, rows);
    });
}

fn style_combo(style: &mut egui::Style, height: f32) {
    style.spacing.interact_size.y = height;
    let widgets = &mut style.visuals.widgets;
    for state in [
        &mut widgets.inactive,
        &mut widgets.hovered,
        &mut widgets.active,
        &mut widgets.open,
    ] {
        state.bg_stroke = egui::Stroke::NONE;
        state.corner_radius = egui::CornerRadius::ZERO;
    }
    widgets.inactive.weak_bg_fill = DARK.panel_raised;
    widgets.hovered.weak_bg_fill = DARK.panel;
    widgets.active.weak_bg_fill = DARK.panel;
    widgets.open.weak_bg_fill = DARK.panel;
}

pub(crate) fn paint_segmented_toggle<T: Copy + PartialEq>(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    id_salt: &str,
    segments: &[(T, &str)],
    current: T,
) -> Option<T> {
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DARK.panel);

    let mut clicked = None;
    let cell_width = rect.width() / segments.len() as f32;
    for (index, (value, label)) in segments.iter().enumerate() {
        let cell = egui::Rect::from_min_size(
            egui::pos2(rect.left() + index as f32 * cell_width, rect.top()),
            egui::vec2(cell_width, rect.height()),
        );
        let response = ui.interact(cell, egui::Id::new((id_salt, index)), egui::Sense::click());

        let active = *value == current;
        if active {
            fill_selected(&painter, cell);
        } else if response.hovered() {
            painter.rect_filled(cell, 0.0, DARK.panel_raised);
        }
        painter.text(
            cell.center(),
            egui::Align2::CENTER_CENTER,
            *label,
            egui::FontId::proportional(METRICS.segment_font),
            if active { DARK.text_primary } else { DARK.text_secondary },
        );
        if index > 0 {
            theme::rules_v(&painter, [cell.left()], rect.y_range());
        }

        if response.clicked() {
            clicked = Some(*value);
        }
    }

    enclose(&painter, rect);
    clicked
}

/// A value the operator steps or drags. `capped_to` is what the consumer will actually honour —
/// shown beside the value when it is lower, so a clamped choice is never silently ignored.
pub(crate) struct StepperSpec<'a> {
    pub label: &'a str,
    pub value: usize,
    pub min: usize,
    pub max: usize,
    pub capped_to: usize,
}

/// Three cells side by side: step down, a draggable track, step up. Returns the newly chosen value.
pub(crate) fn paint_stepper(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    id_salt: &str,
    spec: StepperSpec<'_>,
) -> Option<usize> {
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DARK.panel);

    let step_width = rect.height();
    let track = rect
        .with_min_x(rect.left() + step_width)
        .with_max_x(rect.right() - step_width);
    let down = rect.with_max_x(track.left());
    let up = rect.with_min_x(track.right());

    let dragged = paint_track(ui, &painter, track, id_salt, &spec);
    // Both step cells paint every frame: short-circuiting on the first click would leave the other
    // unpainted and un-hovered for that frame.
    let down_clicked = paint_step(ui, &painter, down, (id_salt, "down"), "-");
    let up_clicked = paint_step(ui, &painter, up, (id_salt, "up"), "+");
    let stepped = match (down_clicked, up_clicked) {
        (true, _) => Some(spec.value.saturating_sub(1)),
        (_, true) => Some(spec.value + 1),
        _ => None,
    };

    theme::rules_v(&painter, [track.left(), track.right()], rect.y_range());
    enclose(&painter, rect);

    stepped
        .or(dragged)
        .map(|chosen| chosen.clamp(spec.min, spec.max))
        .filter(|chosen| *chosen != spec.value)
}

fn paint_track(
    ui: &mut egui::Ui,
    painter: &egui::Painter,
    track: egui::Rect,
    id_salt: &str,
    spec: &StepperSpec<'_>,
) -> Option<usize> {
    let response = ui.interact(
        track,
        egui::Id::new((id_salt, "track")),
        egui::Sense::click_and_drag(),
    );
    if response.hovered() {
        painter.rect_filled(track, 0.0, DARK.panel_raised);
    }

    let span = spec.max.saturating_sub(spec.min).max(1) as f32;
    let filled = (spec.value.saturating_sub(spec.min) as f32 / span).clamp(0.0, 1.0);
    let grown = track.with_max_x(track.left() + track.width() * filled);
    fill_selected(painter, grown);

    paint_readout(painter, track, spec);

    let pointer = response.interact_pointer_pos()?;
    let along = ((pointer.x - track.left()) / track.width().max(1.0)).clamp(0.0, 1.0);
    Some(spec.min + (along * span).round() as usize)
}

/// The value sits on the right and the label on the left. The clamp reads as `50 (34)`: what was
/// asked for, then what the consumer can actually give. The value is the load-bearing half, so a track too narrow for both drops the
/// label rather than painting them through each other.
fn paint_readout(painter: &egui::Painter, track: egui::Rect, spec: &StepperSpec<'_>) {
    let font = egui::FontId::proportional(METRICS.segment_font);
    let value = painter.layout_no_wrap(spec.value.to_string(), font.clone(), DARK.text_primary);
    let label = painter.layout_no_wrap(spec.label.to_owned(), font, DARK.text_secondary);

    let pad = METRICS.space_1;
    let mut left = track.right() - pad - value.size().x;
    if spec.capped_to < spec.value {
        let capped = painter.layout_no_wrap(
            format!("({})", spec.capped_to),
            egui::FontId::proportional(METRICS.segment_font - 2.0),
            DARK.warning,
        );
        left -= capped.size().x + pad;
        let top = track.center().y - capped.size().y / 2.0;
        painter.galley(
            egui::pos2(track.right() - pad - capped.size().x, top),
            capped,
            DARK.warning,
        );
    }

    if track.left() + pad + label.size().x + pad < left {
        let top = track.center().y - label.size().y / 2.0;
        painter.galley(
            egui::pos2(track.left() + pad, top),
            label,
            DARK.text_secondary,
        );
    }
    let top = track.center().y - value.size().y / 2.0;
    painter.galley(egui::pos2(left, top), value, DARK.text_primary);
}

fn paint_step(
    ui: &mut egui::Ui,
    painter: &egui::Painter,
    cell: egui::Rect,
    id_salt: (&str, &str),
    glyph: &str,
) -> bool {
    let response = ui.interact(cell, egui::Id::new(id_salt), egui::Sense::click());
    if response.hovered() {
        painter.rect_filled(cell, 0.0, DARK.panel_raised);
    }
    painter.text(
        cell.center(),
        egui::Align2::CENTER_CENTER,
        glyph,
        egui::FontId::proportional(METRICS.segment_font),
        DARK.text_primary,
    );
    response.clicked()
}

/// A fill plus a bar along the cell's bottom edge. It is the only way the page marks a selected
/// cell.
pub(crate) fn fill_selected(painter: &egui::Painter, cell: egui::Rect) {
    painter.rect_filled(cell, 0.0, DARK.selected.gamma_multiply(0.28));
    let accent = cell.with_min_y(cell.bottom() - METRICS.accent_thickness);
    painter.rect_filled(accent, 0.0, DARK.selected);
}

fn enclose(painter: &egui::Painter, rect: egui::Rect) {
    painter.rect_stroke(
        rect,
        0.0,
        theme::hairline(painter, DARK.border),
        egui::StrokeKind::Inside,
    );
}
