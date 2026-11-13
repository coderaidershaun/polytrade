//! One lock-free ring per producing unit — an OS thread, a tokio task that opts in, or the hot
//! thread's hot lane — plus registration and the emit entry points. A full lane drops and counts.

use std::backtrace::Backtrace;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, OnceLock};

use rtrb::{Consumer, Producer, RingBuffer};

use super::record::{Callsite, HotRecord, Level, TextRecord, WORDS_PER_RECORD, message_time};
use super::render::LogArgs;
use crate::msg::time::{EngineClock, TsUs, boot_stamp_us};

const UNREGISTERED_TAG: &str = "unregistered";
const HOT_LANE_CAPACITY: usize = 4096;

/// The call site of the marker records that touch each slot of a new lane once.
static MARKER: Callsite = Callsite::new(Level::Info, module_path!(), file!(), line!(), "", &[]);

struct Globals {
    registrations: Sender<Registration>,
    backtraces: Sender<BacktraceMessage>,
    clock: EngineClock,
    ring_capacity: usize,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

pub(super) struct Registration {
    pub(super) tag: &'static str,
    pub(super) reader: AnyLane,
}

/// The read end of one lane, of either record kind.
pub(super) enum AnyLane {
    Text(LaneReader<TextRecord>),
    Hot(LaneReader<HotRecord>),
}

pub(super) struct BacktraceMessage {
    pub(super) ts_us: TsUs,
    pub(super) module: &'static str,
    pub(super) file: &'static str,
    pub(super) line: u32,
    /// Captured on the producer but turned into text only on the drain, where the symbol lookup
    /// costs no thread that matters.
    pub(super) backtrace: Backtrace,
}

pub(super) struct DrainChannels {
    pub(super) registrations: Receiver<Registration>,
    pub(super) backtraces: Receiver<BacktraceMessage>,
}

/// The read end of one lane, with the count of the records its producer had to drop.
pub struct LaneReader<T> {
    consumer: Consumer<T>,
    drops: Arc<AtomicU64>,
}

impl<T> LaneReader<T> {
    /// `drops` is the counter that the producer of this lane bumps when the lane is full.
    pub fn new(consumer: Consumer<T>, drops: Arc<AtomicU64>) -> Self {
        Self { consumer, drops }
    }

    pub fn peek(&self) -> Option<&T> {
        self.consumer.peek().ok()
    }

    pub fn pop(&mut self) -> Option<T> {
        self.consumer.pop().ok()
    }

    /// Records waiting to be read.
    pub fn len(&self) -> usize {
        self.consumer.slots()
    }

    pub fn is_empty(&self) -> bool {
        self.consumer.is_empty()
    }

    /// The producer is gone, so no record can arrive here again.
    pub fn is_abandoned(&self) -> bool {
        self.consumer.is_abandoned()
    }

    pub fn dropped(&self) -> u64 {
        self.drops.load(Ordering::Relaxed)
    }

    pub(super) fn consumer_mut(&mut self) -> &mut Consumer<T> {
        &mut self.consumer
    }
}

/// The write end of one lane. Only its own thread or task pushes, so its drop counter has one
/// writer and needs no locked instruction.
struct LaneWriter<T> {
    producer: Producer<T>,
    drops: Arc<AtomicU64>,
}

impl<T> LaneWriter<T> {
    #[inline]
    fn push(&mut self, record: T) {
        if self.producer.push(record).is_err() {
            count_drop(&self.drops);
        }
    }
}

#[cold]
#[inline(never)]
fn count_drop(drops: &AtomicU64) {
    drops.store(drops.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
}

/// Builds a lane and touches each slot once with `marker`, so no first write to a slot faults a
/// page on its producer later.
fn prefaulted_ring<T: Copy>(capacity: usize, marker: T) -> (Producer<T>, Consumer<T>) {
    let (mut producer, mut consumer) = RingBuffer::<T>::new(capacity);
    while producer.push(marker).is_ok() {}
    while consumer.pop().is_ok() {}
    (producer, consumer)
}

fn lane<T: Copy>(capacity: usize, marker: T) -> (LaneWriter<T>, LaneReader<T>) {
    let (producer, consumer) = prefaulted_ring(capacity, marker);
    let drops = Arc::new(AtomicU64::new(0));
    let writer = LaneWriter {
        producer,
        drops: Arc::clone(&drops),
    };
    (writer, LaneReader { consumer, drops })
}

fn text_lane(capacity: usize) -> (LaneWriter<TextRecord>, LaneReader<TextRecord>) {
    let marker = TextRecord::formatted(&MARKER, TsUs::from_micros(0), format_args!(""));
    lane(capacity, marker)
}

fn hot_lane(capacity: usize) -> (LaneWriter<HotRecord>, LaneReader<HotRecord>) {
    let marker = HotRecord::from_words(&MARKER, TsUs::from_micros(0), [0; WORDS_PER_RECORD]);
    lane(capacity, marker)
}

/// A ring of hot records whose writer another owner keeps, such as the strategy lane. Each slot
/// is touched once here, on the calling thread.
pub(crate) fn hot_ring(capacity: usize) -> (Producer<HotRecord>, Consumer<HotRecord>) {
    let marker = HotRecord::from_words(&MARKER, TsUs::from_micros(0), [0; WORDS_PER_RECORD]);
    prefaulted_ring(capacity, marker)
}

thread_local! {
    static THREAD_LANE: RefCell<Option<LaneWriter<TextRecord>>> = const { RefCell::new(None) };
    static HOT_LANE: RefCell<Option<LaneWriter<HotRecord>>> = const { RefCell::new(None) };
    /// Read before `HOT_LANE`: the first touch of a thread-local that owns a destructor can
    /// allocate, and a thread with no hot lane must not pay that on its first hot log call.
    static HAS_HOT_LANE: Cell<bool> = const { Cell::new(false) };
}

tokio::task_local! {
    static TASK_LANE: RefCell<Option<LaneWriter<TextRecord>>>;
}

pub(super) fn install(clock: &EngineClock, ring_capacity: usize) -> DrainChannels {
    let (registration_tx, registration_rx) = mpsc::channel();
    let (backtrace_tx, backtrace_rx) = mpsc::channel();
    let globals = Globals {
        registrations: registration_tx,
        backtraces: backtrace_tx,
        clock: clock.clone(),
        ring_capacity,
    };
    if GLOBALS.set(globals).is_err() {
        panic!("log::init called more than once");
    }
    DrainChannels {
        registrations: registration_rx,
        backtraces: backtrace_rx,
    }
}

/// Tags the calling OS thread's lane. A tokio task must use [`tag_task`] instead: tasks share the
/// worker pool's threads, so a registration made inside one task is overwritten by the next task to
/// land on that worker. Does nothing if `init` has not run.
pub fn register_thread(tag: &'static str) {
    let lane = open_lane(tag);
    THREAD_LANE.with(|cell| {
        *cell.borrow_mut() = lane;
    });
}

/// Gives `body` a lane of its own for as long as it runs, so its records carry `tag` whichever
/// worker polls it and wherever it migrates at an `.await`.
pub(crate) fn tag_task<F: Future>(tag: &'static str, body: F) -> impl Future<Output = F::Output> {
    TASK_LANE.scope(RefCell::new(open_lane(tag)), body)
}

/// Builds a hot lane on the calling thread and hands back its reader: each hot log call on this
/// thread lands there from now on, as a raw record that the reader's owner renders. A thread with
/// no hot lane renders each hot call into text at once.
pub fn open_hot_lane(capacity: usize) -> LaneReader<HotRecord> {
    let (writer, reader) = hot_lane(capacity);
    HOT_LANE.with(|cell| {
        *cell.borrow_mut() = Some(writer);
    });
    HAS_HOT_LANE.with(|flag| flag.set(true));
    reader
}

/// Builds a text lane on the calling thread and hands back its reader: each text log call on this
/// thread lands there from now on, untagged by any drain until the reader's owner adds it to one.
pub fn open_text_lane(capacity: usize) -> LaneReader<TextRecord> {
    let (writer, reader) = text_lane(capacity);
    THREAD_LANE.with(|cell| {
        *cell.borrow_mut() = Some(writer);
    });
    reader
}

/// The one function that builds the hot lane of a hot thread and hands it to the drain. Call it on
/// that thread after the pin, so each slot is touched on the core that writes it. It does nothing
/// if `init` has not run.
pub(crate) fn register_hot_lane(tag: &'static str) {
    if GLOBALS.get().is_none() {
        return;
    }
    let reader = open_hot_lane(HOT_LANE_CAPACITY);
    register_external_ring(tag, reader);
}

/// A lane of hot records that another owner writes, such as the strategy lane. It does nothing if
/// `init` has not run.
pub(crate) fn register_external_ring(tag: &'static str, reader: LaneReader<HotRecord>) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    let registration = Registration {
        tag,
        reader: AnyLane::Hot(reader),
    };
    // After shutdown the drain is gone, so the ring fills and counts its drops.
    globals.registrations.send(registration).ok();
}

/// Drops the calling thread's text lane, so a drain that waits for each lane to end does not wait
/// for the thread that is waiting on it.
pub(super) fn close_thread_lane() {
    THREAD_LANE.with(|cell| {
        cell.borrow_mut().take();
    });
}

fn open_lane(tag: &'static str) -> Option<LaneWriter<TextRecord>> {
    let globals = GLOBALS.get()?;
    let (writer, reader) = text_lane(globals.ring_capacity);
    let registration = Registration {
        tag,
        reader: AnyLane::Text(reader),
    };
    if globals.registrations.send(registration).is_err() {
        return None;
    }
    Some(writer)
}

/// Engine time from the one process clock, or the boot clock before `init` has run.
#[expect(
    clippy::disallowed_methods,
    reason = "a log line and its file date take wall time, which is never engine state"
)]
fn engine_time() -> TsUs {
    GLOBALS
        .get()
        .map_or_else(boot_stamp_us, |globals| globals.clock.now())
}

/// The entry point of the text macros. Only Level::Error captures a backtrace, and it always does.
#[doc(hidden)]
pub fn text_message(callsite: &'static Callsite, args: core::fmt::Arguments<'_>) {
    let record = TextRecord::formatted(callsite, engine_time(), args);
    push(record);
    if callsite.level() == Level::Error {
        send_backtrace(BacktraceMessage {
            ts_us: record.at(),
            module: callsite.module(),
            file: callsite.file(),
            line: callsite.line(),
            backtrace: Backtrace::force_capture(),
        });
    }
}

/// The entry point of the hot macros. It copies raw words and formats nothing, unless this thread
/// has no hot lane or no message is under fold yet: then the line is rendered here, at once.
#[doc(hidden)]
#[inline]
pub fn hot_message<A: LogArgs>(callsite: &'static Callsite, args: A) {
    let mut words = [0; WORDS_PER_RECORD];
    args.write_words(&mut words);
    push_hot(callsite, words);
}

fn push_hot(callsite: &'static Callsite, words: [u64; WORDS_PER_RECORD]) {
    let Some(at) = message_time().filter(|_| HAS_HOT_LANE.with(Cell::get)) else {
        return push_rendered(callsite, &words);
    };
    let record = HotRecord::from_words(callsite, at, words);
    let has_lane = HOT_LANE.with(|cell| {
        let mut slot = cell.borrow_mut();
        let Some(lane) = slot.as_mut() else {
            return false;
        };
        lane.push(record);
        true
    });
    if !has_lane {
        push_rendered(callsite, &words);
    }
}

#[cold]
fn push_rendered(callsite: &'static Callsite, words: &[u64; WORDS_PER_RECORD]) {
    push(TextRecord::rendered(callsite, engine_time(), words));
}

fn push(record: TextRecord) {
    if TASK_LANE.try_with(|cell| push_into(cell, record)).is_ok() {
        return;
    }
    THREAD_LANE.with(|cell| push_into(cell, record));
}

fn push_into(cell: &RefCell<Option<LaneWriter<TextRecord>>>, record: TextRecord) {
    let mut slot = cell.borrow_mut();
    if slot.is_none() {
        *slot = open_lane(UNREGISTERED_TAG);
    }
    if let Some(lane) = slot.as_mut() {
        lane.push(record);
    }
}

fn send_backtrace(message: BacktraceMessage) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    // After shutdown no drain is left to write a backtrace, so it is lost.
    globals.backtraces.send(message).ok();
}
