//! The volatility that the quotes read: an EGARCH fit over closed candles that starts from its last
//! optimum, the realised volatility of a price window, and an EWMA that folds one return at a time.

mod egarch;
mod ewma;
mod realised;

pub use egarch::{Egarch, EgarchEstimate};
pub(crate) use ewma::EwmaVol;
pub use realised::{Returns, realised_vol_per_sec};
