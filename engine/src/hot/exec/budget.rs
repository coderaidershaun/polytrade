//! The venue's declared placement budget, and what this run has spent of it.
//!
//! A venue that meters order placements account-wide will start refusing once the budget is gone,
//! and by then the refusals fall on whatever is placing next — including the marketable order that
//! closes a position. Metering locally turns that into a choice: quoting stops first, and the
//! headroom left over belongs to the way out.

use crate::time::{DurationUs, TsUs};

/// The venues wired so far declare at most two order buckets per market. Four leaves room for one
/// that declares more without the meter's memory depending on anything but this constant.
pub const MAX_ORDER_BUDGET_WINDOWS: usize = 4;

/// At most `max_places` order placements in any `window` of venue time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderBudgetWindow {
    pub window: DurationUs,
    pub max_places: u32,
}

/// Every placement bucket a venue declares. [`OrderBudget::NONE`] is the venue that declares none —
/// whose rate limits are per-endpoint request concerns rather than an account-wide order count — and
/// a run under it places exactly as it would with no meter at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderBudget {
    windows: [OrderBudgetWindow; MAX_ORDER_BUDGET_WINDOWS],
    len: usize,
}

impl OrderBudget {
    pub const NONE: OrderBudget = OrderBudget {
        windows: [OrderBudgetWindow {
            window: DurationUs::ZERO,
            max_places: 0,
        }; MAX_ORDER_BUDGET_WINDOWS],
        len: 0,
    };

    /// `None` when the venue declares more buckets than [`MAX_ORDER_BUDGET_WINDOWS`]: keeping a
    /// subset would pace against a budget larger than the one the venue granted, so the caller
    /// refuses the run instead.
    pub fn of(windows: &[OrderBudgetWindow]) -> Option<OrderBudget> {
        if windows.len() > MAX_ORDER_BUDGET_WINDOWS {
            return None;
        }
        let mut budget = OrderBudget::NONE;
        budget.windows[..windows.len()].copy_from_slice(windows);
        budget.len = windows.len();
        Some(budget)
    }

    pub fn windows(&self) -> &[OrderBudgetWindow] {
        &self.windows[..self.len]
    }
}

/// Each window is cut into this many slots, and the meter sums one MORE than that. The newest slot
/// has only partly elapsed, so counting it whole keeps the counted span at or above the venue's own
/// window — never below it. That asymmetry is the design: the meter refuses early rather than
/// admitting a placement the venue's limiter would already have counted.
const SLOTS_PER_WINDOW: i64 = 4;
const COUNTED_SLOTS: usize = SLOTS_PER_WINDOW as usize + 1;

/// What this run has placed, measured in message time. A daily bucket of six figures rules out one
/// stamp per placement, so each window keeps counts over coarse slots instead — fixed memory
/// whatever the window or the cap.
pub(super) struct BudgetMeter {
    windows: [WindowMeter; MAX_ORDER_BUDGET_WINDOWS],
    len: usize,
}

impl BudgetMeter {
    pub(super) fn new(budget: OrderBudget) -> Self {
        let mut windows = [WindowMeter::EMPTY; MAX_ORDER_BUDGET_WINDOWS];
        for (meter, declared) in windows.iter_mut().zip(budget.windows()) {
            *meter = WindowMeter::of(*declared);
        }
        Self {
            windows,
            len: budget.windows().len(),
        }
    }

    /// The only place message time enters, which is what leaves [`BudgetMeter::admits_place`] a
    /// pure read: every placement in a spin is judged against the same stamp.
    pub(super) fn observe_spin(&mut self, now: TsUs) {
        for window in &mut self.windows[..self.len] {
            window.advance_to(now);
        }
    }

    #[inline]
    pub(super) fn admits_place(&self) -> bool {
        self.windows[..self.len]
            .iter()
            .all(WindowMeter::has_headroom)
    }

    pub(super) fn record_place(&mut self) {
        for window in &mut self.windows[..self.len] {
            window.record_place();
        }
    }
}

#[derive(Clone, Copy)]
struct WindowMeter {
    slot_width_us: i64,
    max_places: u32,
    /// Slot index of the newest stamp seen, so a gap of message time clears what it passed.
    newest_slot: i64,
    places: [u32; COUNTED_SLOTS],
}

impl WindowMeter {
    /// Nothing declared and nothing counted. The one-microsecond slot width is a divisor rather
    /// than a policy: a window narrower than its own slot count would otherwise divide by zero.
    const EMPTY: WindowMeter = WindowMeter {
        slot_width_us: 1,
        max_places: 0,
        newest_slot: i64::MIN,
        places: [0; COUNTED_SLOTS],
    };

    fn of(declared: OrderBudgetWindow) -> WindowMeter {
        WindowMeter {
            slot_width_us: (declared.window.micros() / SLOTS_PER_WINDOW).max(1),
            max_places: declared.max_places,
            ..WindowMeter::EMPTY
        }
    }

    /// A stamp older than the newest one is left in the newest slot rather than reopening a slot
    /// already retired — the conservative direction, since the count only ever stays higher.
    fn advance_to(&mut self, now: TsUs) {
        let slot = now.micros().div_euclid(self.slot_width_us);
        let advanced = slot.saturating_sub(self.newest_slot);
        if advanced <= 0 {
            return;
        }
        for step in 1..=advanced.min(COUNTED_SLOTS as i64) {
            let retired = self.newest_slot.saturating_add(step);
            self.places[slot_index(retired)] = 0;
        }
        self.newest_slot = slot;
    }

    fn has_headroom(&self) -> bool {
        self.used() < self.max_places
    }

    fn used(&self) -> u32 {
        self.places
            .iter()
            .fold(0, |total, count| total.saturating_add(*count))
    }

    fn record_place(&mut self) {
        let index = slot_index(self.newest_slot);
        self.places[index] = self.places[index].saturating_add(1);
    }
}

#[inline]
fn slot_index(slot: i64) -> usize {
    slot.rem_euclid(COUNTED_SLOTS as i64) as usize
}
