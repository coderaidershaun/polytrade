//! Stream liveness: dropped subscription -> WARN; dead socket -> reconnect.

use std::time::{Duration, Instant};

use crate::config::KlineInterval;
use crate::warn;

pub(super) const LIVENESS_POLL: Duration = Duration::from_secs(5);
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(30);
const LIVENESS_MARGIN: Duration = Duration::from_secs(30);
// 10m > futures 3m ping cadence.
const MESSAGE_SILENCE_DEADLINE: Duration = Duration::from_secs(600);

struct StreamLiveness {
    name: String,
    timeout: Duration,
    has_been_seen: bool,
    has_warned: bool,
}

pub(super) struct LivenessMonitor {
    streams: Vec<StreamLiveness>,
    connected_at: Instant,
    last_message_at: Instant,
}

impl LivenessMonitor {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            streams: Vec::new(),
            connected_at: now,
            last_message_at: now,
        }
    }

    pub(super) fn watch_stream(&mut self, name: String) {
        self.watch(name, LIVENESS_TIMEOUT);
    }

    // Kline: interval + margin.
    pub(super) fn watch_kline(&mut self, name: String, interval: KlineInterval) {
        self.watch(name, kline_liveness_timeout(interval));
    }

    fn watch(&mut self, name: String, timeout: Duration) {
        self.streams.push(StreamLiveness {
            name,
            timeout,
            has_been_seen: false,
            has_warned: false,
        });
    }

    // Order matters: builds combined URL.
    pub(super) fn stream_names(&self) -> Vec<String> {
        self.streams
            .iter()
            .map(|stream| stream.name.clone())
            .collect()
    }

    pub(super) fn arm(&mut self) {
        let now = Instant::now();
        self.connected_at = now;
        self.last_message_at = now;
        for stream in &mut self.streams {
            stream.has_been_seen = false;
            stream.has_warned = false;
        }
    }

    pub(super) fn note_message(&mut self) {
        self.last_message_at = Instant::now();
    }

    pub(super) fn mark_seen(&mut self, name: &str) {
        if let Some(stream) = self.streams.iter_mut().find(|stream| stream.name == name) {
            stream.has_been_seen = true;
        }
    }

    /// The gap actually observed, not the deadline it crossed: a REST fetch blocking the read loop
    /// can push the real silence far past the deadline, and an operator reading the reconnect line
    /// needs the measurement.
    pub(super) fn socket_silence(&self) -> Option<Duration> {
        let silent_for = self.last_message_at.elapsed();
        (silent_for > MESSAGE_SILENCE_DEADLINE).then_some(silent_for)
    }

    pub(super) fn warn_silent(&mut self, label: &str) {
        let elapsed = self.connected_at.elapsed();
        for stream in &mut self.streams {
            if !stream.has_been_seen && !stream.has_warned && elapsed >= stream.timeout {
                warn!(
                    "binance adapter {} stream {} silent {}s after connect — possibly dropped",
                    label,
                    stream.name,
                    elapsed.as_secs()
                );
                stream.has_warned = true;
            }
        }
    }
}

// 1M -> 31-day bound; others -> interval.
fn kline_liveness_timeout(interval: KlineInterval) -> Duration {
    const MONTH_BOUND_MINUTES: u64 = 31 * 1_440;
    let minutes = interval.fixed_minutes().unwrap_or(MONTH_BOUND_MINUTES);
    Duration::from_secs(minutes * 60) + LIVENESS_MARGIN
}
