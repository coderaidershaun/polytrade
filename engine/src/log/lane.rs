//! One lock-free ring per producing unit — an OS thread, or a tokio task that opts in — plus the
//! hot lane of the hot thread, registration, and the emit entry points. Push = POD copy; error! -> backtrace (heap).

use std::backtrace::Backtrace;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, OnceLock};

use rtrb::{Consumer, Producer, RingBuffer};

use super::record::{Callsite, HotRecord, Level, TextRecord, WORDS_PER_RECORD, message_time};
use super::render::LogArgs;
use crate::msg::time::{TsUs, boot_stamp_us};

const UNREGISTERED_TAG: &str = "unregistered";
const RING_CAPACITY: usize = 8192;
const HOT_LANE_CAPACITY: usize = 4096;

struct Globals {
    registrations: Sender<Registration>,
    backtraces: Sender<BacktraceMessage>,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

pub(super) struct Registration {
    pub(super) tag: &'static str,
    pub(super) reader: LaneReader,
    pub(super) drops: Arc<AtomicU64>,
}

/// The read end of one lane: text made on its producer, or hot records the drain renders.
pub(super) enum LaneReader {
    Text(Consumer<TextRecord>),
    Hot(Consumer<HotRecord>),
}

pub(super) struct BacktraceMessage {
    pub(super) ts_us: TsUs,
    pub(super) module: &'static str,
    pub(super) file: &'static str,
    pub(super) line: u32,
    pub(super) backtrace: String,
}

pub(super) struct DrainChannels {
    pub(super) registrations: Receiver<Registration>,
    pub(super) backtraces: Receiver<BacktraceMessage>,
}

struct Lane {
    producer: Producer<TextRecord>,
    drops: Arc<AtomicU64>,
}

struct HotLane {
    producer: Producer<HotRecord>,
    drops: Arc<AtomicU64>,
}

thread_local! {
    static THREAD_LANE: RefCell<Option<Lane>> = const { RefCell::new(None) };
    static HOT_LANE: RefCell<Option<HotLane>> = const { RefCell::new(None) };
    /// Read before `HOT_LANE`: the first touch of a thread-local that owns a destructor can
    /// allocate, and a thread with no hot lane must not pay that on its first hot log call.
    static HAS_HOT_LANE: Cell<bool> = const { Cell::new(false) };
}

tokio::task_local! {
    static TASK_LANE: RefCell<Option<Lane>>;
}

pub(super) fn install() -> DrainChannels {
    let (registration_tx, registration_rx) = mpsc::channel();
    let (backtrace_tx, backtrace_rx) = mpsc::channel();
    let globals = Globals {
        registrations: registration_tx,
        backtraces: backtrace_tx,
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
pub fn open_hot_lane(capacity: usize) -> Consumer<HotRecord> {
    install_hot_lane(capacity, Arc::new(AtomicU64::new(0)))
}

/// The hot lane of a hot thread, read by the drain. No-op if init() not run.
pub(crate) fn register_hot_lane(tag: &'static str) {
    if GLOBALS.get().is_none() {
        return;
    }
    let drops = Arc::new(AtomicU64::new(0));
    let reader = install_hot_lane(HOT_LANE_CAPACITY, Arc::clone(&drops));
    register_external_ring(tag, reader, drops);
}

fn install_hot_lane(capacity: usize, drops: Arc<AtomicU64>) -> Consumer<HotRecord> {
    let (producer, consumer) = RingBuffer::<HotRecord>::new(capacity);
    HOT_LANE.with(|cell| {
        *cell.borrow_mut() = Some(HotLane { producer, drops });
    });
    HAS_HOT_LANE.with(|flag| flag.set(true));
    consumer
}

/// A lane of hot records that another owner writes, such as the strategy lane. `drops` = shared
/// full-ring counter. No-op if init() not run.
pub(crate) fn register_external_ring(
    tag: &'static str,
    consumer: Consumer<HotRecord>,
    drops: Arc<AtomicU64>,
) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    let registration = Registration {
        tag,
        reader: LaneReader::Hot(consumer),
        drops,
    };
    // Post-shutdown: drain gone -> ring fills, counts drops.
    globals.registrations.send(registration).ok();
}

fn open_lane(tag: &'static str) -> Option<Lane> {
    let globals = GLOBALS.get()?;
    let (producer, consumer) = RingBuffer::<TextRecord>::new(RING_CAPACITY);
    let drops = Arc::new(AtomicU64::new(0));
    let registration = Registration {
        tag,
        reader: LaneReader::Text(consumer),
        drops: Arc::clone(&drops),
    };
    if globals.registrations.send(registration).is_err() {
        return None;
    }
    Some(Lane { producer, drops })
}

/// The entry point of the text macros. Only Level::Error captures a backtrace, and it always does.
#[doc(hidden)]
pub fn text_message(callsite: &'static Callsite, args: core::fmt::Arguments<'_>) {
    let record = TextRecord::formatted(callsite, boot_stamp_us(), args);
    push(record);
    if callsite.level() == Level::Error {
        send_backtrace(BacktraceMessage {
            ts_us: record.at(),
            module: callsite.module(),
            file: callsite.file(),
            line: callsite.line(),
            backtrace: Backtrace::force_capture().to_string(),
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
        if lane.producer.push(record).is_err() {
            lane.drops.fetch_add(1, Ordering::Relaxed);
        }
        true
    });
    if !has_lane {
        push_rendered(callsite, &words);
    }
}

#[cold]
fn push_rendered(callsite: &'static Callsite, words: &[u64; WORDS_PER_RECORD]) {
    push(TextRecord::rendered(callsite, boot_stamp_us(), words));
}

fn push(record: TextRecord) {
    if TASK_LANE.try_with(|cell| push_into(cell, record)).is_ok() {
        return;
    }
    THREAD_LANE.with(|cell| push_into(cell, record));
}

fn push_into(cell: &RefCell<Option<Lane>>, record: TextRecord) {
    let mut slot = cell.borrow_mut();
    if slot.is_none() {
        *slot = open_lane(UNREGISTERED_TAG);
    }
    if let Some(lane) = slot.as_mut()
        && lane.producer.push(record).is_err()
    {
        lane.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn send_backtrace(message: BacktraceMessage) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    // Post-shutdown: nowhere to report lost backtrace.
    globals.backtraces.send(message).ok();
}
