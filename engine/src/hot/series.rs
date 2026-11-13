//! Rolling windows of a fixed size for the hot thread. Each window is one contiguous slice that
//! never allocates after it is built, and its sum is kept as samples arrive.

pub(crate) mod sealed {
    pub trait Sealed {}
    impl Sealed for i64 {}
    impl Sealed for f64 {}
}

/// The defaults keep no sum, which suits plain records. The numeric elements override them to keep
/// the window sum as samples arrive.
pub trait Element: Copy + sealed::Sealed {
    #[doc(hidden)]
    fn accumulate(self, _sum_exact: &mut i128, _sum_running: &mut f64) {}
    #[doc(hidden)]
    fn deaccumulate(self, _sum_exact: &mut i128, _sum_running: &mut f64) {}
}

/// A numeric [`Element`] with a window sum and statistics: `i64` or `f64` only.
pub trait SeriesElem: Element {
    #[doc(hidden)]
    fn to_f64(self) -> f64;
    #[doc(hidden)]
    fn resolve_sum(exact: i128, running: f64) -> Self;
}

// An i128 sum of any window of i64 samples never wraps, and the narrowing to i64 in `sum` panics
// rather than wraps.
impl Element for i64 {
    #[inline]
    fn accumulate(self, sum_exact: &mut i128, sum_running: &mut f64) {
        *sum_exact += i128::from(self);
        *sum_running += self as f64;
    }
    #[inline]
    fn deaccumulate(self, sum_exact: &mut i128, sum_running: &mut f64) {
        *sum_exact -= i128::from(self);
        *sum_running -= self as f64;
    }
}

impl SeriesElem for i64 {
    #[inline]
    fn to_f64(self) -> f64 {
        self as f64
    }
    #[inline]
    fn resolve_sum(exact: i128, _running: f64) -> i64 {
        i64::try_from(exact)
            .unwrap_or_else(|_| panic!("window sum overflows i64 mantissa: {exact}"))
    }
}

impl Element for f64 {
    #[inline]
    fn accumulate(self, _sum_exact: &mut i128, sum_running: &mut f64) {
        *sum_running += self;
    }
    #[inline]
    fn deaccumulate(self, _sum_exact: &mut i128, sum_running: &mut f64) {
        *sum_running -= self;
    }
}

impl SeriesElem for f64 {
    #[inline]
    fn to_f64(self) -> f64 {
        self
    }
    #[inline]
    fn resolve_sum(_exact: i128, running: f64) -> f64 {
        running
    }
}

/// Quickselect workspace for [`FastQueue::median_over_window`], sized once at init and reused. A
/// fixed slice cannot grow, so a median never reaches the allocator.
#[derive(Debug, Clone, PartialEq)]
pub struct MedianScratch {
    values: Box<[f64]>,
}

impl MedianScratch {
    pub fn for_window(window: usize) -> Self {
        Self {
            values: vec![0.0; window].into_boxed_slice(),
        }
    }
}

/// Exponential moving average at a fixed cost for each sample, with decay 2^(−1/halflife): the
/// same recursion as the RiskMetrics volatility. It weighs each sample since it was built, where
/// [`FastQueue::ema_over_window`] weighs only the samples of its window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ema {
    decay: f64,
    value: Option<f64>,
}

impl Ema {
    /// A zero halflife keeps only the newest sample.
    pub fn with_halflife(halflife_samples: u32) -> Self {
        Self {
            decay: (-std::f64::consts::LN_2 / f64::from(halflife_samples)).exp(),
            value: None,
        }
    }

    /// The first sample is the seed. Each later sample moves the average by the share that the
    /// halflife gives it.
    #[inline]
    pub fn push(&mut self, sample: f64) {
        let next = match self.value {
            None => sample,
            Some(previous) => self.decay * previous + (1.0 - self.decay) * sample,
        };
        self.value = Some(next);
    }

    #[inline]
    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

/// The live window is one contiguous slice, oldest first, and nothing allocates after `new`.
/// `PartialEq` compares the backing buffer and its cursors too, which makes it strict enough for
/// the replay tests.
#[derive(Debug, Clone, PartialEq)]
pub struct FastQueue<T: Copy> {
    buf: Vec<T>,
    window: usize,
    backing: usize,
    start: usize,
    head: usize,
    sum_exact: i128,
    sum_running: f64,
}

impl<T: Copy> FastQueue<T> {
    /// The backing buffer holds `window * backing_multiple` samples. A larger multiple makes the
    /// copy back to the front rarer.
    ///
    /// # Panics
    /// When `window` is 0 or `backing_multiple` is below 2. This runs at init only, after the
    /// config check.
    pub fn new(window: usize, backing_multiple: usize) -> Self {
        assert!(window != 0, "fastqueue window must be non-zero");
        assert!(
            backing_multiple >= 2,
            "fastqueue backing_multiple must be >= 2, got {backing_multiple}"
        );
        let backing = window * backing_multiple;
        Self {
            buf: Vec::with_capacity(backing),
            window,
            backing,
            start: 0,
            head: 0,
            sum_exact: 0,
            sum_running: 0.0,
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.buf[self.start..self.head]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.head - self.start
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == self.start
    }

    #[inline]
    pub fn window(&self) -> usize {
        self.window
    }

    #[inline]
    pub fn get(&self, i: usize) -> Option<T> {
        self.as_slice().get(i).copied()
    }

    #[inline]
    pub fn first(&self) -> Option<T> {
        self.as_slice().first().copied()
    }

    #[inline]
    pub fn last(&self) -> Option<T> {
        self.as_slice().last().copied()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.as_slice().iter().copied()
    }

    /// Keeps the allocation. A cleared queue compares equal to a new one.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.start = 0;
        self.head = 0;
        self.sum_exact = 0;
        self.sum_running = 0.0;
    }

    #[inline]
    fn write_head(&mut self, item: T) {
        // The first pass over the backing buffer appends within its capacity, and later passes
        // overwrite.
        if self.head == self.buf.len() {
            self.buf.push(item);
        } else {
            self.buf[self.head] = item;
        }
    }
}

impl<T: Element> FastQueue<T> {
    /// Evicts the oldest sample when the window is full. The cost is constant, apart from a copy
    /// back to the front once each time the backing buffer fills.
    #[inline]
    pub fn push(&mut self, item: T) {
        if self.head == self.backing {
            self.copy_back();
        }
        self.write_head(item);
        item.accumulate(&mut self.sum_exact, &mut self.sum_running);
        if self.len() == self.window {
            let evicted = self.buf[self.start];
            evicted.deaccumulate(&mut self.sum_exact, &mut self.sum_running);
        }
        self.head += 1;
        if self.head > self.window {
            self.start += 1;
        }
    }

    #[cold]
    fn copy_back(&mut self) {
        let live = self.len();
        self.buf.copy_within(self.start..self.head, 0);
        self.start = 0;
        self.head = live;
        // Moving the window cannot change its sum. The recompute is the only bound on the rounding
        // that a float running sum gathers over each push and eviction since the last copy-back.
        // The integer sum is exact and needs none.
        self.recompute_sums();
    }

    fn recompute_sums(&mut self) {
        let mut sum_exact = 0i128;
        let mut sum_running = 0.0f64;
        for &item in &self.buf[self.start..self.head] {
            item.accumulate(&mut sum_exact, &mut sum_running);
        }
        self.sum_exact = sum_exact;
        self.sum_running = sum_running;
    }
}

impl<T: SeriesElem> FastQueue<T> {
    /// Exact for `i64`, the running sum for `f64`.
    ///
    /// # Panics
    /// For `i64`, when the sum passes `i64::MAX`, because a wrapped sum would corrupt each
    /// statistic after it.
    #[inline]
    pub fn sum(&self) -> T {
        T::resolve_sum(self.sum_exact, self.sum_running)
    }

    /// Walks each sample of the window, oldest first, through an [`Ema`], so the cost grows with
    /// the window. `None` when the window is empty.
    pub fn ema_over_window(&self, halflife_samples: u32) -> Option<f64> {
        let mut ema = Ema::with_halflife(halflife_samples);
        for value in self.iter() {
            ema.push(value.to_f64());
        }
        ema.value()
    }

    /// Copies each sample of the window into the scratch and selects the middle there, so the cost
    /// grows with the window and the queue itself is never reordered. An even count gives the mean
    /// of the two middle samples. `None` when the window is empty.
    ///
    /// # Panics
    /// When the scratch was sized for a shorter window than this queue's, in each build, even
    /// before the window is full.
    pub fn median_over_window(&self, scratch: &mut MedianScratch) -> Option<f64> {
        assert!(
            scratch.values.len() >= self.window,
            "median scratch holds {} samples but this window holds up to {}: build it with \
             MedianScratch::for_window({})",
            scratch.values.len(),
            self.window,
            self.window
        );
        let n = self.len();
        if n == 0 {
            return None;
        }

        let values = &mut scratch.values[..n];
        for (slot, item) in values.iter_mut().zip(self.iter()) {
            *slot = item.to_f64();
        }
        let mid = n / 2;
        let (below, pivot, _above) = values.select_nth_unstable_by(mid, f64::total_cmp);
        if n % 2 == 1 {
            return Some(*pivot);
        }

        let lower_middle = below
            .iter()
            .copied()
            .max_by(f64::total_cmp)
            .expect("even window has a non-empty below partition");
        Some((lower_middle + *pivot) / 2.0)
    }
}

impl FastQueue<i64> {
    /// Divides the exact sum, so the mean depends only on the samples in the window.
    #[inline]
    pub fn mean(&self) -> Option<f64> {
        let n = self.len();
        if n == 0 {
            return None;
        }
        Some(self.sum_exact as f64 / n as f64)
    }
}

impl FastQueue<f64> {
    /// Divides the running sum, which carries the rounding of each push and eviction since the
    /// last copy-back. Equal push history gives equal bits, and a cleared queue answers as a new
    /// one. Two queues that reach the same window by a different history can differ in the last
    /// bits.
    #[inline]
    pub fn running_mean(&self) -> Option<f64> {
        let n = self.len();
        if n == 0 {
            return None;
        }
        Some(self.sum_running / n as f64)
    }
}
