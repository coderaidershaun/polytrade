//! A hot log call copies raw words on the hot thread, and only the drain turns them into the line
//! an operator reads, with the same words that line has always had.

use std::io::{self, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use engine::hot::exec::HaltReason;
use engine::hot::strategy::Strategy;
use engine::log::{Level, LogDrain, LogOutput, LogValue, LogWriter, open_hot_lane};
use engine::msg::exec::ExecKind;
use engine::msg::ids::{InstrumentId, Side};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{EngineClock, LocalTs};
use engine::runtime::config::KlineInterval;

use crate::engine_support::{
    ONE, engine_without_persistence, instrument_row, last_snapshot_chunk, partial_snapshot_chunk,
    strategy_log_ring, tracker_spec_all, ts,
};

/// A log writer in memory that also counts flushes, so a test sees what reached the output and
/// when it was pushed out.
#[derive(Clone, Default)]
struct MemoryWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    flushes: Arc<AtomicUsize>,
}

impl MemoryWriter {
    fn text(&self) -> String {
        let bytes = self.bytes.lock().expect("no writer panicked");
        String::from_utf8(bytes.clone()).expect("log lines are utf-8")
    }
}

impl LogWriter for MemoryWriter {}

impl Write for MemoryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("no writer panicked")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct Quiet;

impl Strategy for Quiet {}

const BOOK_CAPACITY: usize = 64;
const SNAPSHOT_AT: i64 = 1_000_000;

/// One snapshot whose 80 bid levels arrive in four chunks of 20, best price first, so the 65th
/// level, in the fourth chunk, is the first that the 64-level side has no room for. Each ask sits
/// above the best bid, so the book is not crossed and writes no second line.
fn overfull_bid_snapshot() -> Vec<InboundMessage> {
    let best_bid = 1_000;
    let mut messages: Vec<InboundMessage> = (0..4)
        .map(|chunk| {
            let levels: Vec<(i64, i64)> = (0..20)
                .map(|level| ((best_bid - chunk * 20 - level) * ONE, ONE))
                .collect();
            InboundMessage::new(
                ts(SNAPSHOT_AT),
                MessageBody::Book(partial_snapshot_chunk(0, Side::Buy, &levels, SNAPSHOT_AT)),
            )
        })
        .collect();
    let asks = [((best_bid + 1) * ONE, ONE)];
    messages.push(InboundMessage::new(
        ts(SNAPSHOT_AT),
        MessageBody::Book(last_snapshot_chunk(0, Side::Sell, &asks, SNAPSHOT_AT)),
    ));
    messages
}

#[test]
fn hot_warning_is_formatted_on_the_drain() {
    let hot_lane = open_hot_lane(16);
    let instruments = [instrument_row(0, tracker_spec_all(1), BOOK_CAPACITY)];
    let (log_sink, _strategy_records) = strategy_log_ring(16);
    let mut engine = engine_without_persistence(&instruments, Box::new(Quiet), log_sink);
    let messages = overfull_bid_snapshot();

    let before = crate::alloc_count();
    for message in &messages {
        engine.dispatch(message);
    }
    let allocations = crate::alloc_count() - before;
    assert_eq!(allocations, 0, "the hot thread touched the allocator");

    assert_eq!(hot_lane.len(), 1, "one trim episode, one hot record");
    let record = *hot_lane.peek().expect("the hot lane holds the warning");
    let callsite = record.callsite();
    assert_eq!(callsite.level(), Level::Warn);
    assert_eq!(callsite.module(), "engine::hot::book");
    assert_eq!(
        callsite.template(),
        "book instrument {} bid side at capacity {} — dropping deepest levels"
    );
    assert_eq!(
        record.at(),
        ts(SNAPSHOT_AT),
        "the record carries message time"
    );
    assert_eq!(record.words(), &[0, 64, 0, 0, 0, 0], "raw words, no text");

    let writer = MemoryWriter::default();
    let output = LogOutput {
        open: Box::new({
            let writer = writer.clone();
            move |_at: LocalTs| Ok(Box::new(writer.clone()) as Box<dyn LogWriter>)
        }),
        fallback: Box::new(io::stderr()),
        reopen_limit: 3,
    };
    let mut drain = LogDrain::new(output, EngineClock::start());
    drain.add_hot_lane("hot", hot_lane);
    drain.drain_once();

    let expected = format!(
        "1970-01-01 00:00:01.000000 WARN  [hot] engine::hot::book ({}:{}) book instrument 0 bid \
         side at capacity 64 — dropping deepest levels\n",
        callsite.file(),
        callsite.line()
    );
    assert_eq!(writer.text(), expected, "the drain wrote the line of today");
    assert!(
        writer.flushes.load(Ordering::Relaxed) >= 1,
        "a WARN line is flushed at once"
    );
}

/// Ambiguous, and so a compile error, exactly when `T` is a `LogValue`.
trait AmbiguousIfLogValue<Marker> {
    fn check() {}
}

impl<T> AmbiguousIfLogValue<()> for T {}

struct IsLogValue;

impl<T: LogValue> AmbiguousIfLogValue<IsLogValue> for T {}

macro_rules! assert_not_log_value {
    ($($ty:ty),+) => {
        $(<$ty as AmbiguousIfLogValue<_>>::check();)+
    };
}

fn assert_log_value<T: LogValue>() {}

#[derive(Debug, Clone, Copy)]
struct OnlyDebug;

/// A type test: it holds when the target compiles. Each kind of value the hot call sites carry is a
/// `LogValue`, and text in any form is not, so a hot or strategy log call cannot carry text to the
/// hot thread's ring.
#[test]
fn hot_macro_takes_only_log_values() {
    assert_log_value::<u8>();
    assert_log_value::<u16>();
    assert_log_value::<u32>();
    assert_log_value::<u64>();
    assert_log_value::<usize>();
    assert_log_value::<i32>();
    assert_log_value::<i64>();
    assert_log_value::<f64>();
    assert_log_value::<bool>();
    assert_log_value::<Option<f64>>();
    assert_log_value::<InstrumentId>();
    assert_log_value::<Side>();
    assert_log_value::<HaltReason>();
    assert_log_value::<ExecKind>();
    assert_log_value::<KlineInterval>();
    assert_log_value::<engine::hot::exec::RejectReason>();
    assert_not_log_value!(
        String,
        &str,
        &String,
        char,
        OnlyDebug,
        std::fmt::Arguments<'static>
    );
}
