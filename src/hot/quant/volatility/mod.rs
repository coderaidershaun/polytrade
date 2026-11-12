//! Volatility estimators: EGARCH and realised volatility read over a rolling [`FastQueue<f64>`]
//! window, and [`EwmaVol`] folds one return at a time. Each is a pure function of its input.

mod egarch;
mod ewma;
mod realised;

pub use egarch::{Egarch, EgarchEstimate};
pub(crate) use ewma::EwmaVol;
pub use realised::{Returns, realised_vol_per_sec};

use crate::hot::series::FastQueue;
use crate::time::DurationUs;

/// Written here rather than beside the series type so the dependency runs one way: the series
/// module never imports quant.
impl FastQueue<f64> {
    pub fn egarch(&self, state: &mut Egarch) -> Option<EgarchEstimate> {
        state.fit(self.as_slice())
    }

    pub fn realised_volatility(&self, returns: Returns, interval: DurationUs) -> Option<f64> {
        realised_vol_per_sec(self.iter(), returns, interval)
    }
}
