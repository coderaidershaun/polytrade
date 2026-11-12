//! The monitor panel's painters: the status strip and the five bands, the account, summary and
//! channel painters they call, and the pins that make a tap show what a hover shows.

mod account;
pub mod bands;
mod channels;
mod pin;
mod row_list;
mod sizes;
mod summary;

pub use sizes::{band_sizes, column_widths, pane_height};
