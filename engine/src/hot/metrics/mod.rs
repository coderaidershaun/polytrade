//! Engine self-observability — the machine's health, not the market's data. Per category × stage, a
//! 60-slot ring of ten-second buckets gives a running ten-minute rollup; buckets age out by key, so rollups overlap rather than reset.

mod snapshot;

use crate::ids::QueueId;
use crate::labelled_enum::labelled_enum;
use crate::msg::exec::ExecKind;
use crate::msg::inbound::{BookChunkKind, InboundMessage};
use crate::time::{DurationUs, TsUs};

pub use snapshot::{EngineCounters, MetricsSnapshot};

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Category {
        Trade = "trade",
        BookDelta = "book_delta",
        BookSnapshot = "book_snapshot",
        Kline = "kline",
        Spin = "spin",
        Exec = "exec",
    }
    pub fn label;
    pub const ALL;
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Stage {
        ExchangeToReceived = "exch->recv",
        ReceivedToQueued = "recv->queued",
        QueueWait = "queue_wait",
        Processing = "processing",
        EndToEnd = "end_to_end",
        /// Venue's own match -> send gap; only where the venue publishes both stamps.
        ExchangeInternal = "exch_internal",
        OrderRoundTrip = "round_trip",
    }
    pub fn label;
    pub const ALL;
}

/// Stamps used to derive latency stages; venue-supplied stamps are optional because no venue publishes all of them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MessageMeta {
    pub(crate) category: Category,
    pub(crate) exchange_ts: Option<TsUs>,
    pub(crate) exchange_sent_ts: Option<TsUs>,
    pub(crate) request_sent_ts: Option<TsUs>,
    pub(crate) received_ts: TsUs,
    pub(crate) queued_ts: TsUs,
}

pub(crate) fn message_meta(message: &InboundMessage) -> Option<MessageMeta> {
    match message {
        InboundMessage::Trade(event) => Some(MessageMeta {
            category: Category::Trade,
            exchange_ts: Some(event.exchange_ts_us),
            exchange_sent_ts: event.exchange_sent_ts_us,
            request_sent_ts: None,
            received_ts: event.received_ts_us,
            queued_ts: event.queued_ts_us,
        }),
        InboundMessage::Book(chunk) => Some(MessageMeta {
            category: match chunk.kind {
                BookChunkKind::Snapshot => Category::BookSnapshot,
                BookChunkKind::Delta => Category::BookDelta,
            },
            exchange_ts: chunk.exchange_ts_us,
            exchange_sent_ts: None,
            request_sent_ts: None,
            received_ts: chunk.received_ts_us,
            queued_ts: chunk.queued_ts_us,
        }),
        // A candle's `exchange_ts_us` is the CANDLE's time, not the frame's: on REST backfill it is
        // the close of a row hours old, and measuring against it would record that age as latency.
        // Only the send stamp is a transport fact, so a backfill row measures no wire time at all.
        InboundMessage::Kline(event) => Some(MessageMeta {
            category: Category::Kline,
            exchange_ts: None,
            exchange_sent_ts: event.exchange_sent_ts_us,
            request_sent_ts: None,
            received_ts: event.received_ts_us,
            queued_ts: event.queued_ts_us,
        }),
        InboundMessage::SpinTick(tick) => Some(MessageMeta {
            category: Category::Spin,
            exchange_ts: None,
            exchange_sent_ts: None,
            request_sent_ts: None,
            received_ts: tick.received_ts_us,
            queued_ts: tick.queued_ts_us,
        }),
        InboundMessage::Exec(event) => Some(MessageMeta {
            category: Category::Exec,
            exchange_ts: is_venue_stamped(event.kind).then_some(event.exchange_ts_us),
            exchange_sent_ts: None,
            request_sent_ts: event.request_sent_ts_us,
            received_ts: event.received_ts_us,
            queued_ts: event.queued_ts_us,
        }),
        InboundMessage::BookReset(_)
        | InboundMessage::MarketRotation(_)
        | InboundMessage::Link(_)
        | InboundMessage::RunControl(_)
        | InboundMessage::Account(_) => None,
    }
}

/// Whether `exchange_ts_us` came off the wire rather than our own clock: feeding a locally-stamped
/// kind into the exchange-to-received stage would measure our clock against itself. `AckFailed` is
/// excluded on both of its origins — a synthesised timeout and a venue rejection — because neither
/// carries a transact time.
fn is_venue_stamped(kind: ExecKind) -> bool {
    !matches!(
        kind,
        ExecKind::PlaceNotSent
            | ExecKind::AmendNotSent
            | ExecKind::AckFailed
            | ExecKind::SnapshotEnd
            | ExecKind::StreamReset
            | ExecKind::StreamReady
    )
}

const CATEGORIES: usize = Category::ALL.len();
const STAGES: usize = Stage::ALL.len();
const WINDOW_BUCKETS: usize = 60;
const BUCKET_SPAN_SECS: i64 = 10;
/// Histogram lanes within one bucket — unrelated to [`WINDOW_BUCKETS`].
const BUCKETS: usize = 32;
const MAX_QUEUES: usize = 20;
const BACKLOG_EMA_HALFLIFE: DurationUs = DurationUs::from_secs(60);

#[inline]
fn bucket_key(at: TsUs) -> i64 {
    at.micros().div_euclid(1_000_000 * BUCKET_SPAN_SECS)
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    bucket: i64,
    count: u32,
    sum_us: i64,
    min_us: i64,
    max_us: i64,
    hist: [u32; BUCKETS],
}

impl Slot {
    const EMPTY: Slot = Slot {
        bucket: i64::MIN,
        count: 0,
        sum_us: 0,
        min_us: 0,
        max_us: 0,
        hist: [0; BUCKETS],
    };

    #[cold]
    fn reset_to(&mut self, bucket: i64) {
        self.bucket = bucket;
        self.count = 0;
        self.sum_us = 0;
        self.min_us = i64::MAX;
        self.max_us = i64::MIN;
        self.hist = [0; BUCKETS];
    }
}

#[derive(Debug, Clone, Copy)]
struct BucketRing {
    slots: [Slot; WINDOW_BUCKETS],
}

impl BucketRing {
    const EMPTY: BucketRing = BucketRing {
        slots: [Slot::EMPTY; WINDOW_BUCKETS],
    };

    // saturating: external stamps must never panic the hot thread; real latencies never near saturation
    #[inline]
    fn record(&mut self, bucket: i64, latency_us: i64) {
        let slot = &mut self.slots[bucket.rem_euclid(WINDOW_BUCKETS as i64) as usize];
        if slot.bucket != bucket {
            slot.reset_to(bucket);
        }
        slot.count = slot.count.saturating_add(1);
        slot.sum_us = slot.sum_us.saturating_add(latency_us);
        slot.min_us = slot.min_us.min(latency_us);
        slot.max_us = slot.max_us.max(latency_us);
        let lane = &mut slot.hist[bucket_of(latency_us)];
        *lane = lane.saturating_add(1);
    }

    fn rollup(&self, oldest_bucket: i64, newest_bucket: i64) -> StageStat {
        let mut stat = StageStat::ACCUMULATOR_SEED;
        let mut hist = [0u64; BUCKETS];
        for slot in &self.slots {
            if slot.count == 0 || slot.bucket < oldest_bucket || slot.bucket > newest_bucket {
                continue;
            }
            stat.count += u64::from(slot.count);
            stat.sum_us = stat.sum_us.saturating_add(slot.sum_us);
            stat.min_us = stat.min_us.min(slot.min_us);
            stat.max_us = stat.max_us.max(slot.max_us);
            for (accumulated, &lane) in hist.iter_mut().zip(&slot.hist) {
                *accumulated += u64::from(lane);
            }
        }
        if stat.count == 0 {
            return StageStat::default();
        }
        stat.p50_us = percentile(&hist, stat.count, 0.50).clamp(stat.min_us, stat.max_us);
        stat.p99_us = percentile(&hist, stat.count, 0.99).clamp(stat.min_us, stat.max_us);
        stat
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StageStat {
    pub count: u64,
    pub sum_us: i64,
    pub min_us: i64,
    pub max_us: i64,
    pub p50_us: i64,
    pub p99_us: i64,
}

impl StageStat {
    const ACCUMULATOR_SEED: StageStat = StageStat {
        count: 0,
        sum_us: 0,
        min_us: i64::MAX,
        max_us: i64::MIN,
        p50_us: 0,
        p99_us: 0,
    };

    pub fn mean_us(self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum_us as f64 / self.count as f64 }
    }
}

/// No histogram: queue depth is a count, not a duration — p50/p99 on single-digit values say nothing.
#[derive(Debug, Clone, Copy)]
struct GaugeSlot {
    bucket: i64,
    count: u64,
    sum: u64,
    min: u64,
    max: u64,
}

impl GaugeSlot {
    const EMPTY: GaugeSlot = GaugeSlot {
        bucket: i64::MIN,
        count: 0,
        sum: 0,
        min: 0,
        max: 0,
    };

    #[cold]
    fn reset_to(&mut self, bucket: i64) {
        self.bucket = bucket;
        self.count = 0;
        self.sum = 0;
        self.min = u64::MAX;
        self.max = 0;
    }
}

#[derive(Debug, Clone, Copy)]
struct GaugeRing {
    slots: [GaugeSlot; WINDOW_BUCKETS],
}

impl GaugeRing {
    const EMPTY: GaugeRing = GaugeRing {
        slots: [GaugeSlot::EMPTY; WINDOW_BUCKETS],
    };

    #[inline]
    fn record(&mut self, bucket: i64, depth: u64) {
        let slot = &mut self.slots[bucket.rem_euclid(WINDOW_BUCKETS as i64) as usize];
        if slot.bucket != bucket {
            slot.reset_to(bucket);
        }
        slot.count += 1;
        slot.sum = slot.sum.saturating_add(depth);
        slot.min = slot.min.min(depth);
        slot.max = slot.max.max(depth);
    }

    fn rollup(&self, oldest_bucket: i64, newest_bucket: i64) -> QueueDepthStat {
        let mut stat = QueueDepthStat::ACCUMULATOR_SEED;
        for slot in &self.slots {
            if slot.count == 0 || slot.bucket < oldest_bucket || slot.bucket > newest_bucket {
                continue;
            }
            stat.count += slot.count;
            stat.sum = stat.sum.saturating_add(slot.sum);
            stat.min = stat.min.min(slot.min);
            stat.max = stat.max.max(slot.max);
        }
        if stat.count == 0 {
            return QueueDepthStat::default();
        }
        stat
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueDepthStat {
    pub count: u64,
    pub sum: u64,
    pub min: u64,
    pub max: u64,
}

impl QueueDepthStat {
    const ACCUMULATOR_SEED: QueueDepthStat = QueueDepthStat {
        count: 0,
        sum: 0,
        min: u64::MAX,
        max: 0,
    };

    pub fn mean(self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum as f64 / self.count as f64 }
    }
}

/// Folded on the stamp carried by the message that triggered it rather than on a clock read, so a
/// replayed tape produces the same curve the live run showed.
#[derive(Debug, Clone, Copy)]
struct BacklogEma {
    value: f64,
    sampled_ts_us: TsUs,
}

impl BacklogEma {
    fn folded(self, fresh: f64, at: TsUs) -> Self {
        let elapsed_secs = at.diff(self.sampled_ts_us).to_secs().max(0.0);
        let keep = (-std::f64::consts::LN_2 * elapsed_secs / BACKLOG_EMA_HALFLIFE.to_secs()).exp();
        Self {
            value: self.value + (fresh - self.value) * (1.0 - keep),
            sampled_ts_us: at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HotMetrics {
    /// Boxed: a `CATEGORIES * STAGES` array by value is ~430 KiB, too big for the 2 MiB stacks fitness tests build engines on.
    rings: Box<[BucketRing; CATEGORIES * STAGES]>,
    occupancy: [GaugeRing; MAX_QUEUES],
    backlog_ema: Option<BacklogEma>,
    queue_count: u8,
    counters: EngineCounters,
}

impl HotMetrics {
    pub fn new() -> Self {
        let rings = vec![BucketRing::EMPTY; CATEGORIES * STAGES]
            .into_boxed_slice()
            .try_into()
            .expect("ring vec is built with exactly CATEGORIES * STAGES elements");
        Self {
            rings,
            occupancy: [GaugeRing::EMPTY; MAX_QUEUES],
            backlog_ema: None,
            queue_count: 0,
            counters: EngineCounters::default(),
        }
    }

    #[inline]
    pub fn record(&mut self, category: Category, stage: Stage, at: TsUs, latency: DurationUs) {
        self.rings[category as usize * STAGES + stage as usize]
            .record(bucket_key(at), latency.micros());
    }

    #[inline]
    pub fn record_occupancy(&mut self, queue: QueueId, depth: usize, at: TsUs) {
        let index = usize::from(queue.0);
        debug_assert!(
            index < MAX_QUEUES,
            "occupancy for queue id {} beyond max {MAX_QUEUES}",
            queue.0
        );
        if index >= MAX_QUEUES {
            return;
        }
        self.occupancy[index].record(bucket_key(at), depth as u64);
        self.queue_count = self.queue_count.max(queue.0 + 1);
    }

    #[inline]
    pub fn record_spin_backlog(&mut self, backlog: usize, at: TsUs) {
        let fresh = backlog as f64;
        self.backlog_ema = Some(match self.backlog_ema {
            Some(ema) => ema.folded(fresh, at),
            None => BacklogEma {
                value: fresh,
                sampled_ts_us: at,
            },
        });
    }

    pub fn record_counters(&mut self, counters: EngineCounters) {
        self.counters = counters;
    }

    pub fn snapshot(&self, now: TsUs) -> MetricsSnapshot {
        let newest = bucket_key(now);
        let oldest = newest - WINDOW_BUCKETS as i64 + 1;
        MetricsSnapshot {
            taken_at: now,
            stages: std::array::from_fn(|category| {
                std::array::from_fn(|stage| {
                    self.rings[category * STAGES + stage].rollup(oldest, newest)
                })
            }),
            occupancy: std::array::from_fn(|queue| self.occupancy[queue].rollup(oldest, newest)),
            backlog_ema: self.backlog_ema.map(|ema| ema.value),
            queue_count: self.queue_count,
            counters: self.counters,
        }
    }
}

impl Default for HotMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn bucket_of(latency_us: i64) -> usize {
    if latency_us <= 0 {
        return 0;
    }
    let highest_bit = 63 - (latency_us as u64).leading_zeros();
    (highest_bit as usize).min(BUCKETS - 1)
}

/// Lower edge (`2^bucket`) of the bucket where the cumulative count first reaches `p`.
fn percentile(hist: &[u64; BUCKETS], total: u64, p: f64) -> i64 {
    if total == 0 {
        return 0;
    }
    let target = (p * total as f64).ceil() as u64;
    let mut cumulative = 0u64;
    for (bucket, &count) in hist.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            return 1i64 << bucket;
        }
    }
    1i64 << (BUCKETS - 1)
}
