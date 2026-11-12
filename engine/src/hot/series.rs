//! Fixed-window rolling series: always one contiguous slice. O(1) rolling stats via [`SeriesElem`].

pub(crate) mod sealed {
    pub trait Sealed {}
    impl Sealed for i64 {}
    impl Sealed for f64 {}
}

/// Default no-ops (POD); numeric elements override for O(1) rolling sums. Sealed.
pub trait Element: Copy + sealed::Sealed {
    #[doc(hidden)]
    fn accumulate(self, _sum_exact: &mut i128, _sum_running: &mut f64) {}
    #[doc(hidden)]
    fn deaccumulate(self, _sum_exact: &mut i128, _sum_running: &mut f64) {}
}

/// Numeric [`Element`] w/ O(1) rolling stats; i64/f64 only.
pub trait SeriesElem: Element {
    #[doc(hidden)]
    fn to_f64(self) -> f64;
    #[doc(hidden)]
    fn resolve_sum(exact: i128, running: f64) -> Self;
}

// i128 never wraps for any i64-window; narrowing to i64 at sum() is loud.
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

/// Quickselect workspace for [`FastQueue::median`]. Sized once at init and reused, because the
/// alternative — a bare `Vec` the caller is told to preallocate — is a precondition every new call
/// site has to be told about again, and forgetting it allocates on the hot path.
#[derive(Debug, Clone, PartialEq)]
pub struct MedianScratch {
    values: Vec<f64>,
}

impl MedianScratch {
    pub fn for_window(window: usize) -> Self {
        Self {
            values: Vec::with_capacity(window),
        }
    }
}

/// Rolling window: always one contiguous slice (oldest-first). Zero alloc after new.
/// PartialEq is physical (backing+cursors) — strict for replay-determinism tests.
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
    /// Backing = window × multiple. Larger multiple = rarer copy-backs.
    /// # Panics
    /// window==0 or multiple<2 (init-time only, config validates first).
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

    /// Live window, oldest-first, always contiguous.
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

    /// Empty in place (keep allocation). Cleared queue physically ≡ fresh construction.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.start = 0;
        self.head = 0;
        self.sum_exact = 0;
        self.sum_running = 0.0;
    }

    #[inline]
    fn write_head(&mut self, item: T) {
        // First append within prealloc; later passes overwrite.
        if self.head == self.buf.len() {
            self.buf.push(item);
        } else {
            self.buf[self.head] = item;
        }
    }
}

impl<T: Element> FastQueue<T> {
    /// Append, evict oldest when full. O(1) amortised (periodic copy-back).
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

    /// Relocate live window to front (once per backing-length run).
    #[cold]
    fn copy_back(&mut self) {
        let live = self.len();
        self.buf.copy_within(self.start..self.head, 0);
        self.start = 0;
        self.head = live;
        // Not relocation bookkeeping — moving the window cannot change its sum. This is the only
        // thing bounding the rounding drift an f64 running sum accumulates over every push and
        // eviction since the last copy-back. The i64 side goes through exact i128 and is unaffected.
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
    /// Window sum: exact for i64, running for f64.
    /// # Panics
    /// i64: sum > i64::MAX (wrap corrupts all downstream stats).
    #[inline]
    pub fn sum(&self) -> T {
        T::resolve_sum(self.sum_exact, self.sum_running)
    }

    #[inline]
    pub fn mean(&self) -> Option<f64> {
        let n = self.len();
        if n == 0 {
            return None;
        }
        Some(self.sum_running / n as f64)
    }

    /// EMA over window, λ = 2^(−1/halflife): same RiskMetrics recursion (series can't depend on quant).
    /// Zero halflife → newest sample only. `None` if empty.
    pub fn ema(&self, halflife_samples: u32) -> Option<f64> {
        let decay = (-std::f64::consts::LN_2 / f64::from(halflife_samples)).exp();
        let mut values = self.iter().map(SeriesElem::to_f64);
        let seed = values.next()?;
        Some(values.fold(seed, |acc, value| decay * acc + (1.0 - decay) * value))
    }

    /// Median via quickselect over scratch (queue never reordered — PartialEq invariant).
    /// Even counts: average lower/upper middle. `None` if empty.
    ///
    /// # Panics
    /// Debug builds, when the scratch was sized for a shorter window than this queue's — the call
    /// would still answer correctly, by allocating on the hot path.
    pub fn median(&self, scratch: &mut MedianScratch) -> Option<f64> {
        let n = self.len();
        if n == 0 {
            return None;
        }

        let scratch = &mut scratch.values;
        debug_assert!(
            scratch.capacity() >= self.window,
            "median scratch holds {} of the {} this window can reach, so the call would allocate",
            scratch.capacity(),
            self.window
        );
        scratch.clear();
        scratch.extend(self.iter().map(SeriesElem::to_f64));
        let mid = n / 2;
        let (below, pivot, _above) = scratch.select_nth_unstable_by(mid, f64::total_cmp);
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
