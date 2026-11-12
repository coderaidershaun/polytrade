//! Slug + window-grid arithmetic: pure fns place 5-min windows on 300s grid, assign A/B slots,
//! derive subscribe/prefetch/probe instants. No I/O or clock reads.

use crate::config::PolySeries;
use crate::time::{DurationUs, TsUs};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Slot {
    A,
    B,
}

impl Slot {
    /// In [`Slot::as_usize`] order, so an array built by iterating this is indexable by it — which
    /// is how the driver routes a resolved window to its legs.
    pub(crate) const ALL: [Slot; 2] = [Slot::A, Slot::B];

    pub(crate) fn from_window_index(index: i64) -> Slot {
        if index.rem_euclid(2) == 0 { Slot::A } else { Slot::B }
    }

    pub(crate) const fn as_usize(self) -> usize {
        self as usize
    }
}

const _: () = assert!(Slot::ALL[0].as_usize() == 0 && Slot::ALL[1].as_usize() == 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolySchedule {
    pub window_len: DurationUs,
    pub subscribe_lead: DurationUs,
    pub grace_probe_delay: DurationUs,
    pub probe_cadence: DurationUs,
    pub silence_threshold: DurationUs,
}

impl PolySchedule {
    pub const BTC_5M: PolySchedule = PolySchedule {
        window_len: PolySeries::BtcUpDown5m.window_len(),
        subscribe_lead: DurationUs::from_secs(60),
        grace_probe_delay: DurationUs::from_secs(60),
        probe_cadence: DurationUs::from_secs(5),
        silence_threshold: DurationUs::from_secs(2),
    };

    pub(crate) fn for_series(series: PolySeries) -> PolySchedule {
        match series {
            PolySeries::BtcUpDown5m => PolySchedule::BTC_5M,
        }
    }

    pub(crate) fn window_index_containing(&self, now: TsUs) -> i64 {
        now.micros().div_euclid(self.window_len.micros())
    }

    fn window_start_containing(&self, now: TsUs) -> TsUs {
        self.window_start(self.window_index_containing(now))
    }

    pub(crate) fn window_start(&self, index: i64) -> TsUs {
        TsUs::from_micros(index * self.window_len.micros())
    }

    fn window_close(&self, window_start: TsUs) -> TsUs {
        window_start + self.window_len
    }

    pub fn subscribe_at(&self, window_start: TsUs) -> TsUs {
        window_start - self.subscribe_lead
    }

    /// When grace-tail `/book` probing arms for a window that nominally ended at `window_close`.
    pub fn grace_probe_start(&self, window_close: TsUs) -> TsUs {
        window_close + self.grace_probe_delay
    }

    /// The window covering `now`.
    pub fn current_window(&self, now: TsUs) -> PolyWindow {
        self.window_at(self.window_start_containing(now))
    }

    pub fn next_window(&self, now: TsUs) -> PolyWindow {
        self.window_at(self.window_start_containing(now) + self.window_len)
    }

    /// # Panics
    /// `window_start` is not on the window grid. Every caller mints it from this schedule, so an
    /// off-grid value means a window was computed somewhere that does not own the grid.
    pub fn window_at(&self, window_start: TsUs) -> PolyWindow {
        let len = self.window_len.micros();
        assert!(
            window_start.micros().rem_euclid(len) == 0,
            "window start {} is not aligned to the {len}us grid",
            window_start.micros()
        );
        let index = self.window_index_containing(window_start);
        PolyWindow {
            window_start_ts_us: window_start,
            window_close_ts_us: self.window_close(window_start),
            index,
            slot: Slot::from_window_index(index),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolyWindow {
    pub window_start_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
    pub index: i64,
    pub slot: Slot,
}
