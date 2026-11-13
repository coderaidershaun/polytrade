//! Per-aggressor-side arrival monitor: the rolling window the fitter reads, the paced re-solve that
//! keeps it current, and the resident evaluator answering λ between fits.

use super::{HawkesEstimate, HawkesEvents, HawkesMle, UnivariateHawkes};
use crate::msg::time::{LocalTs, VenueTs};

/// Hawkes monitor rolling window, fit floor, refit cadence. Strategy constants, not config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HawkesChoice {
    pub max_events: usize,
    pub min_events: usize,
    /// Fresh arrivals before kernel re-solve.
    pub refit_arrivals: usize,
}

impl HawkesChoice {
    /// # Errors
    /// The first rule that the choice breaks, in this order: an empty window, a fit floor below
    /// two events, a fit floor above the window, no refit cadence.
    pub const fn new(
        max_events: usize,
        min_events: usize,
        refit_arrivals: usize,
    ) -> Result<Self, HawkesChoiceError> {
        if max_events == 0 {
            return Err(HawkesChoiceError::EmptyWindow);
        }
        if min_events < 2 {
            return Err(HawkesChoiceError::TooFewEvents { min_events });
        }
        if min_events > max_events {
            return Err(HawkesChoiceError::FloorAboveWindow {
                min_events,
                max_events,
            });
        }
        if refit_arrivals == 0 {
            return Err(HawkesChoiceError::NoRefitCadence);
        }
        Ok(Self {
            max_events,
            min_events,
            refit_arrivals,
        })
    }
}

/// A Hawkes choice that no side could run, named by the rule it breaks, with the values seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HawkesChoiceError {
    #[error("hawkes max_events is 0, so the event window could hold no event")]
    EmptyWindow,
    #[error(
        "hawkes min_events = {min_events} is below 2, and a fit needs at least one gap between \
         two events"
    )]
    TooFewEvents { min_events: usize },
    #[error(
        "hawkes min_events = {min_events} exceeds max_events = {max_events}, so the window could \
         never hold the fit floor; lower min_events or raise max_events"
    )]
    FloorAboveWindow {
        min_events: usize,
        max_events: usize,
    },
    #[error("hawkes refit_arrivals is 0, so a refit would run on every read; set it to 1 or more")]
    NoRefitCadence,
}

/// Aggressor side Hawkes monitor: rolling event window, warm-start fitter, current fit, O(1) live intensity.
/// Buy -> ask, sells -> bid (intensity convention).
pub struct HawkesSide {
    choice: HawkesChoice,
    events: HawkesEvents,
    fitter: HawkesMle,
    /// Fit all columns read. Re-solved on arrival cadence, not per spin.
    estimate: Option<HawkesEstimate>,
    arrivals_since_fit: usize,
    live: Option<UnivariateHawkes>,
    last_exchange_ts_us: Option<VenueTs>,
}

impl HawkesSide {
    /// # Panics
    /// When min_events > max_events: window can't hold fit floor, every fit stale forever. Config bug.
    pub fn new(choice: HawkesChoice) -> Self {
        assert!(
            choice.min_events <= choice.max_events,
            "hawkes min_events = {} exceeds max_events = {}, so the window could never hold the fit \
             floor and every fit would be stale — lower min_events or raise max_events",
            choice.min_events,
            choice.max_events
        );
        Self {
            choice,
            events: HawkesEvents::new(choice.max_events),
            fitter: HawkesMle::new(choice.min_events),
            estimate: None,
            arrivals_since_fit: 0,
            live: None,
            last_exchange_ts_us: None,
        }
    }

    /// Fold trade arrival. Multi-sweep shares exchange_ts_us -> same-stamp folds (prevent zero-gap).
    /// received_ts_us is replay-pure clock. Evaluator advances every arrival (O(1), ungated). Fit gated.
    pub fn on_arrival(&mut self, exchange_ts_us: VenueTs, received_ts_us: LocalTs) {
        if self.last_exchange_ts_us == Some(exchange_ts_us) {
            return;
        }
        self.last_exchange_ts_us = Some(exchange_ts_us);
        self.events.push(received_ts_us);
        self.arrivals_since_fit += 1;
        if let Some(live) = self.live.as_mut() {
            live.on_event(received_ts_us);
        }
    }

    /// Before the first estimate the fitter is cheap — it returns without optimising until
    /// `min_events` have banked — so asking every spin costs nothing and gets the cold fit out the
    /// moment the floor is crossed.
    pub fn is_refit_due(&self) -> bool {
        self.estimate.is_none() || self.arrivals_since_fit >= self.choice.refit_arrivals
    }

    /// A full Nelder-Mead simplex whose objective is one O(window) pass, so this is the expensive
    /// half of the side and the reason [`HawkesSide::is_refit_due`] exists.
    ///
    /// A fresh fit re-anchors the resident: seed it once from the buffered window, then only nudge
    /// its params — the excitation carries across a param change, decaying the mismatch away.
    pub fn refit(&mut self, now: LocalTs) {
        self.arrivals_since_fit = 0;
        self.estimate = self.fitter.fit(&self.events, now);
        let Some(estimate) = self.estimate.filter(|fit| !fit.is_stale) else {
            return;
        };
        match self.live.as_mut() {
            Some(live) => live.set_params(estimate.params()),
            None => {
                let mut live = UnivariateHawkes::new(estimate.params());
                live.reseed_from(&self.events);
                self.live = Some(live);
            }
        }
    }

    #[inline]
    pub fn estimate(&self) -> Option<HawkesEstimate> {
        self.estimate
    }

    /// Present only once a non-stale fit has seeded it, which is why λ is recorded on a gate of its
    /// own rather than the parameter columns'.
    #[inline]
    pub fn live(&self) -> Option<&UnivariateHawkes> {
        self.live.as_ref()
    }

    /// A lifetime count, so [`HawkesSide::reset`] does not zero it.
    #[inline]
    pub fn out_of_order_count(&self) -> u64 {
        self.events.out_of_order_count()
    }

    /// Reset to post-new state keeping allocator. Window + fitter (warm-start inline) rebuilt from floor.
    pub fn reset(&mut self) {
        self.events.clear();
        self.fitter = HawkesMle::new(self.choice.min_events);
        self.estimate = None;
        self.arrivals_since_fit = 0;
        self.live = None;
        self.last_exchange_ts_us = None;
    }
}
