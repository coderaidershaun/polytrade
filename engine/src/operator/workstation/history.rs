//! The one bounded ring behind every rolling series the workstation shows: the monitor's four channel
//! histories, the chart's per-instrument bucket ring and its fill ring. One eviction policy shared by
//! all of them, so no panel invents its own retention rule, and one way per-instrument storage grows.

/// A fixed-capacity ring of the freshest `capacity` items: pushes past capacity silently evict the
/// oldest, iteration runs from either end, and `appended` counts every push ever so a viewer derives
/// an unseen count from a stored watermark. The backing store is reserved once at construction —
/// steady state never reallocates.
#[derive(Debug, Clone)]
pub(crate) struct BoundedHistory<T> {
    slots: Vec<T>,
    capacity: usize,
    oldest: usize,
    appended: u64,
}

impl<T> BoundedHistory<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            capacity,
            oldest: 0,
            appended: 0,
        }
    }

    pub(crate) fn push(&mut self, item: T) {
        self.appended += 1;
        if self.capacity == 0 {
            return;
        }
        if self.slots.len() < self.capacity {
            self.slots.push(item);
            return;
        }
        self.slots[self.oldest] = item;
        self.oldest = (self.oldest + 1) % self.capacity;
    }

    pub(crate) fn iter_newest_first(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.iter_oldest_first().rev()
    }

    /// The `i`-th item counted from the oldest sits at physical index `(oldest + i) % capacity`, so
    /// the walk is right whether or not the ring has wrapped.
    pub(crate) fn iter_oldest_first(&self) -> impl DoubleEndedIterator<Item = &T> {
        let oldest = self.oldest;
        let capacity = self.capacity.max(1);
        (0..self.slots.len()).map(move |i| &self.slots[(oldest + i) % capacity])
    }

    /// The newest item, mutable: a chart bucket keeps folding in place while further commits land
    /// inside its own spin interval.
    pub(crate) fn last_mut(&mut self) -> Option<&mut T> {
        let newest = self.slots.len().checked_sub(1)?;
        let index = (self.oldest + newest) % self.capacity.max(1);
        self.slots.get_mut(index)
    }

    pub(crate) fn appended(&self) -> u64 {
        self.appended
    }
}

pub(crate) fn grow_to<T>(slots: &mut Vec<T>, len: usize, make: impl Fn() -> T) {
    while slots.len() < len {
        slots.push(make());
    }
}
