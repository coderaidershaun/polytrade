//! The trade arrivals of one aggressor side: the window that the fitter reads, the fit that each
//! read paces, and the live intensity between fits.

use core::num::NonZeroUsize;

use super::{HawkesEstimate, HawkesEvents, HawkesFit, HawkesMle, UnivariateHawkes};
use crate::msg::time::{LocalTs, VenueTs};

/// The window, the fit floor, and the refit cadence of one side. A strategy fixes them as
/// constants, not as config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HawkesChoice {
    max_events: NonZeroUsize,
    min_events: usize,
    refit_arrivals: usize,
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
        let Some(window) = NonZeroUsize::new(max_events) else {
            return Err(HawkesChoiceError::EmptyWindow);
        };
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
            max_events: window,
            min_events,
            refit_arrivals,
        })
    }

    /// The most events that the window keeps; a full window evicts its oldest.
    #[inline]
    pub const fn max_events(&self) -> NonZeroUsize {
        self.max_events
    }

    /// The fewest events in the window for which a fit runs.
    #[inline]
    pub const fn min_events(&self) -> usize {
        self.min_events
    }

    /// The new arrivals between two fits.
    #[inline]
    pub const fn refit_arrivals(&self) -> usize {
        self.refit_arrivals
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

/// The Hawkes model of one aggressor side. Buys feed the ask side and sells the bid side, as the
/// intensity calculator counts them.
pub struct HawkesSide {
    choice: HawkesChoice,
    events: HawkesEvents,
    fitter: HawkesMle,
    fit: Option<HawkesFit>,
    arrivals_since_fit: usize,
    live: Option<UnivariateHawkes>,
    last_exchange_ts_us: Option<VenueTs>,
}

impl HawkesSide {
    pub fn new(choice: HawkesChoice) -> Self {
        Self {
            choice,
            events: HawkesEvents::new(choice.max_events),
            fitter: HawkesMle::new(choice),
            fit: None,
            arrivals_since_fit: 0,
            live: None,
            last_exchange_ts_us: None,
        }
    }

    /// The prints of one sweep share an exchange stamp, so only the first of them counts: the others
    /// would add gaps of zero. The window takes the received stamp, which a replay gives again. The
    /// live sum moves with each arrival, and only the fit waits for its cadence.
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

    /// A full simplex search whose objective is one pass over the window, so it runs only when a
    /// fit is due. Each fresh fit reseeds the live sum from the window under its own params, so no
    /// excitation that was summed under an older decay stays.
    fn refit(&mut self, now: LocalTs) {
        self.arrivals_since_fit = 0;
        self.fit = self.fitter.fit(&self.events, now);
        let Some(estimate) = self.fit.and_then(HawkesFit::fresh) else {
            return;
        };
        let mut live = UnivariateHawkes::new(estimate.params);
        live.reseed_from(&self.events);
        self.live = Some(live);
    }

    /// Refits when a fit is due, then gives the fresh fit in force. A fit is due before the first
    /// one and after `refit_arrivals` new arrivals; below `min_events` the fitter returns at once,
    /// so a read on each spin is cheap. `None` before the first fresh fit, and while the window
    /// cannot support a new one.
    pub fn estimate(&mut self, now: LocalTs) -> Option<HawkesEstimate> {
        let is_due = self.fit.is_none() || self.arrivals_since_fit >= self.choice.refit_arrivals;
        if is_due {
            self.refit(now);
        }
        self.fit.and_then(HawkesFit::fresh)
    }

    /// The live intensity, per second. `None` until a fresh fit seeds the live sum. It then moves
    /// with each arrival, also while the fit in force is stale.
    #[inline]
    pub fn intensity(&self, now: LocalTs) -> Option<f64> {
        self.live.as_ref().map(|live| live.intensity(now))
    }

    /// A lifetime count, so [`HawkesSide::reset`] does not zero it.
    #[inline]
    pub fn out_of_order_count(&self) -> u64 {
        self.events.out_of_order_count()
    }

    /// Back to the state of `new`, keeping the buffer of the window. The next fit starts cold.
    pub fn reset(&mut self) {
        self.events.clear();
        self.fitter = HawkesMle::new(self.choice);
        self.fit = None;
        self.arrivals_since_fit = 0;
        self.live = None;
        self.last_exchange_ts_us = None;
    }
}
