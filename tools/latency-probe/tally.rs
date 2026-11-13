//! The numbers of a run: percentiles of each timed region, with the count, the drops and the
//! backward pairs beside them, and never a mean. Every line says "development" until it comes from
//! the host that trades.

use hdrhistogram::Histogram;

use crate::clock::Probe;

/// One nanosecond to ten seconds, three significant figures: a major page fault still records.
const LOWEST_NS: u64 = 1;
const HIGHEST_NS: u64 = 10_000_000_000;
const SIGNIFICANT_FIGURES: u8 = 3;

/// The first regions of a run are reported apart, because cold caches and first touches of memory
/// slow them.
pub const WARM_UP: u64 = 10_000;

/// The tick pairs of one run, turned into nanoseconds off the measured thread.
pub struct Tally {
    probe_cost_ns: u64,
    send_interval_ns: u64,
    warm: Histogram<u64>,
    steady: Histogram<u64>,
    seen: u64,
    backward: u64,
}

impl Tally {
    /// `send_interval_ns` is the gap between two sends at the fixed rate of the run. A region that
    /// stalls longer stands for the sends it held up too.
    pub fn new(probe_cost_ns: u64, send_interval_ns: u64) -> Tally {
        Tally {
            probe_cost_ns,
            send_interval_ns,
            warm: histogram(),
            steady: histogram(),
            seen: 0,
            backward: 0,
        }
    }

    pub fn record(&mut self, probe: &Probe, start: u64, end: u64) {
        let Some(nanos) = probe.nanos(start, end) else {
            self.backward += 1;
            return;
        };
        let nanos = nanos.saturating_sub(self.probe_cost_ns).max(LOWEST_NS);
        self.seen += 1;
        let histogram = if self.seen <= WARM_UP { &mut self.warm } else { &mut self.steady };
        let nanos = nanos.min(HIGHEST_NS);
        if histogram
            .record_correct(nanos, self.send_interval_ns)
            .is_err()
        {
            histogram.saturating_record(nanos);
        }
    }

    pub fn print(&self, label: &str, dropped: u64) {
        println!(
            "development {label}: {} dropped={dropped} backward={} probe_cost={}ns",
            spread(&self.steady),
            self.backward,
            self.probe_cost_ns
        );
        println!("development {label} warm-up: {}", spread(&self.warm));
    }
}

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(LOWEST_NS, HIGHEST_NS, SIGNIFICANT_FIGURES)
        .expect("the histogram bounds are constants that the crate accepts")
}

fn spread(histogram: &Histogram<u64>) -> String {
    if histogram.is_empty() {
        return "count=0".to_owned();
    }
    format!(
        "count={} p50={} p99={} p99.9={} max={}",
        histogram.len(),
        micros(histogram.value_at_quantile(0.5)),
        micros(histogram.value_at_quantile(0.99)),
        micros(histogram.value_at_quantile(0.999)),
        micros(histogram.max()),
    )
}

fn micros(nanos: u64) -> String {
    format!("{:.3}us", nanos as f64 / 1_000.0)
}
