//! The words and colour roles the workstation shows, toolkit-free, so they test natively and the
//! painters only draw them.

mod chart;
mod dom;
mod latency;
mod link;
mod monitor;
mod rows;
mod screen;
mod tone;

pub(crate) use chart::{AxisLabels, axis_labels, mid_note, risk_note, sign_tone};
pub(crate) use dom::{
    Ring, grouping_label, grouping_table, mid_tone, offscreen_of, order_tone, ring, row_price,
    status_pill,
};
pub(crate) use latency::{LATENCY_ROWS, LATENCY_SHORT_TITLE, LATENCY_TITLE, summary_cells};
pub(crate) use link::{
    ChipWords, NO_ENGINE, PEER_HOVER, button_label, confirm_label, connection_words, control_words,
    is_peer_clickable, peer_words, phase_words, run_words,
};
pub(crate) use monitor::{
    BALANCE_DECIMALS, Lane, VALUE_DECIMALS, account_captions, account_notes, asset_label,
    cell_tone, delta_hover, feature_hover, feature_name, reject_display, summary_caption,
    write_amount, write_delta, write_feature_cell,
};
pub(crate) use rows::{Cell, ChannelRow, record_row};
pub(crate) use screen::{HALT_BADGE, HALT_WORD, STARTING_SCREEN, execution_badge, selection_label};
pub(crate) use tone::Tone;

// Only the painters name these types.
#[cfg(target_arch = "wasm32")]
pub(crate) use latency::{Column, Flow};
#[cfg(target_arch = "wasm32")]
pub(crate) use rows::TagColumn;
#[cfg(target_arch = "wasm32")]
pub(crate) use screen::ScreenText;
