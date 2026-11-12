//! One lock-free ring per producing unit — an OS thread, or a tokio task that opts in — plus
//! registration and the emit entry points. Push = POD copy; error! -> backtrace (heap).

use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, OnceLock};

use rtrb::{Consumer, Producer, RingBuffer};

use super::record::{Level, LogRecord};
use crate::time::TsUs;

const UNREGISTERED_TAG: &str = "unregistered";
const RING_CAPACITY: usize = 8192;

struct Globals {
    registrations: Sender<Registration>,
    backtraces: Sender<BacktraceMessage>,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

pub(super) struct Registration {
    pub(super) tag: &'static str,
    pub(super) consumer: Consumer<LogRecord>,
    pub(super) drops: Arc<AtomicU64>,
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
    producer: Producer<LogRecord>,
    drops: Arc<AtomicU64>,
}

thread_local! {
    static THREAD_LANE: RefCell<Option<Lane>> = const { RefCell::new(None) };
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

/// `drops` = shared full-ring counter. No-op if init() not run.
pub(crate) fn register_external_ring(
    tag: &'static str,
    consumer: Consumer<LogRecord>,
    drops: Arc<AtomicU64>,
) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    let registration = Registration {
        tag,
        consumer,
        drops,
    };
    // Post-shutdown: drain gone -> ring fills, counts drops.
    globals.registrations.send(registration).ok();
}

fn open_lane(tag: &'static str) -> Option<Lane> {
    let globals = GLOBALS.get()?;
    let (producer, consumer) = RingBuffer::<LogRecord>::new(RING_CAPACITY);
    let drops = Arc::new(AtomicU64::new(0));
    let registration = Registration {
        tag,
        consumer,
        drops: Arc::clone(&drops),
    };
    if globals.registrations.send(registration).is_err() {
        return None;
    }
    Some(Lane { producer, drops })
}

#[doc(hidden)]
pub fn info_message(
    module: &'static str,
    file: &'static str,
    line: u32,
    args: core::fmt::Arguments<'_>,
) {
    push(LogRecord::new(Level::Info, module, file, line, args));
}

#[doc(hidden)]
pub fn warn_message(
    module: &'static str,
    file: &'static str,
    line: u32,
    args: core::fmt::Arguments<'_>,
) {
    push(LogRecord::new(Level::Warn, module, file, line, args));
}

/// Only path for Level::Error (always captures backtrace).
#[doc(hidden)]
pub fn error_message(
    module: &'static str,
    file: &'static str,
    line: u32,
    args: core::fmt::Arguments<'_>,
) {
    let record = LogRecord::new(Level::Error, module, file, line, args);
    let ts_us = record.ts_us;
    push(record);
    send_backtrace(BacktraceMessage {
        ts_us,
        module,
        file,
        line,
        backtrace: Backtrace::force_capture().to_string(),
    });
}

fn push(record: LogRecord) {
    if TASK_LANE.try_with(|cell| push_into(cell, record)).is_ok() {
        return;
    }
    THREAD_LANE.with(|cell| push_into(cell, record));
}

fn push_into(cell: &RefCell<Option<Lane>>, record: LogRecord) {
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
