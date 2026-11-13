//! Logging that never blocks a producer: a lock-free ring per thread feeds one drain thread, and
//! the hot thread hands over raw records that only the drain turns into text. In the browser build
//! the same macros print straight to the console instead.

#[cfg(target_arch = "wasm32")]
mod console;
mod drain;
mod lane;
mod output;
mod record;
mod render;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(target_arch = "wasm32")]
pub use console::register_thread;
pub use drain::{DrainReport, LogDrain};
#[cfg(not(target_arch = "wasm32"))]
pub use lane::register_thread;
pub use lane::{LaneReader, open_hot_lane, open_text_lane};
pub use output::{LogOutput, LogWriter, OpenWriter};
pub use record::{Callsite, HotRecord, Level, TextRecord, WORDS_PER_RECORD};
pub use render::{LogValue, Spec};

#[cfg(target_arch = "wasm32")]
pub(crate) use console::register_hot_lane;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use lane::{hot_ring, register_external_ring, register_hot_lane, tag_task};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use record::TEXT_CAPACITY;
pub(crate) use record::set_message_time;
pub(crate) use render::fieldless_log_value;

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub use console::{hot_message, text_message};
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub use lane::{hot_message, text_message};
#[doc(hidden)]
pub use render::{LogArgs, Renderer};

use crate::msg::time::EngineClock;
use crate::runtime::pin::spawn_pinned;
use crate::runtime::{PinCall, PinRefused};

const DEFAULT_FILE_STEM: &str = "polysim";

#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Where the log file goes in a release build. A debug build logs to stderr instead.
    pub dir: PathBuf,
    /// The log file is `{file_stem}-YYYY-MM-DD.log`, one for each UTC day of record time. The
    /// engine sets it from its strategy and engine ids, with `-sim` added for a simulated run.
    pub file_stem: Box<str>,
    /// Records each thread's lane holds before it drops.
    pub ring_capacity: usize,
    /// How long the drain keeps writing at shutdown before it counts the rest as lost.
    pub drain_deadline: Duration,
    /// Reopens of a failing log file in a row before the drain falls back to stderr.
    pub reopen_limit: u32,
    /// The core the drain thread pins itself to before it drains. Absent, the OS places it.
    pub core_id: Option<usize>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("logs"),
            file_stem: DEFAULT_FILE_STEM.into(),
            ring_capacity: 8_192,
            drain_deadline: Duration::from_secs(2),
            reopen_limit: 3,
            core_id: None,
        }
    }
}

/// Call `drain` to flush and stop the log thread. Dropping the handle does neither.
#[derive(Debug)]
pub struct LogHandle {
    stop: Arc<AtomicBool>,
    join: JoinHandle<DrainReport>,
}

impl LogHandle {
    /// Closes the calling thread's lane, drains every lane under the configured deadline, prints
    /// the counts to stderr, and returns them.
    pub fn drain(self) -> DrainReport {
        lane::close_thread_lane();
        self.stop.store(true, Ordering::Release);
        let Ok(report) = self.join.join() else {
            eprintln!("polysim: logging drain thread panicked during shutdown");
            return DrainReport::default();
        };
        eprintln!("polysim: {report}");
        report
    }
}

/// Call at startup, before any thread logs. Text lines take their time from `clock`, the one clock
/// of the process, and the drain thread pins itself to `config.core_id` through `pin`.
///
/// # Errors
/// [`PinRefused`] when the drain thread cannot pin itself to `config.core_id` on Linux. The drain
/// then never runs, and the log is not installed: lines stay dropped as before the call.
///
/// # Panics
/// Panics if called more than once in a process after a call that returned a handle, and in the
/// browser build, which can start no drain thread. The browser needs no init: there the macros
/// print straight to the console.
pub fn init(
    config: &LogConfig,
    clock: &EngineClock,
    pin: PinCall,
) -> Result<LogHandle, PinRefused> {
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let deadline = config.drain_deadline;
    let (hand_over, handed) = mpsc::sync_channel::<LogDrain>(1);
    let join = spawn_pinned("polysim-log", config.core_id, pin, move || {
        handed.recv().map_or_else(
            |_| DrainReport::default(),
            |log_drain| drain::run(log_drain, thread_stop, deadline),
        )
    })?;
    let channels = lane::install(clock, config.ring_capacity);
    let output = LogOutput::from_config(config);
    hand_over
        .send(LogDrain::with_channels(output, clock.clone(), channels))
        .expect("the drain thread waits for its drain once its pin holds");
    Ok(LogHandle { stop, join })
}

/// The call site of a text macro, made once at compile time.
#[doc(hidden)]
#[macro_export]
macro_rules! __text_callsite {
    ($level:ident) => {{
        static CALLSITE: $crate::log::Callsite = $crate::log::Callsite::new(
            $crate::log::Level::$level,
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
            "",
            &[],
        );
        &CALLSITE
    }};
}

/// Logs at INFO. It takes the same arguments as `format!`.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::log::text_message(
            $crate::__text_callsite!(Info),
            ::core::format_args!($($arg)*),
        )
    };
}

/// Logs at WARN, flushed at once.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::log::text_message(
            $crate::__text_callsite!(Warn),
            ::core::format_args!($($arg)*),
        )
    };
}

/// Counts a repeating fault and logs at WARN on the 1st, 2nd, 4th, 8th … occurrence, so a fault
/// that recurs on every datagram cannot flood the log. It is a macro rather than a function so the
/// file and line logged are the caller's.
#[macro_export]
macro_rules! warn_repeating {
    ($counter:expr, $($arg:tt)*) => {{
        $counter += 1;
        if $counter.is_power_of_two() {
            $crate::warn!($($arg)*);
        }
    }};
}

/// Logs at ERROR with a backtrace, flushed at once. The browser build prints no backtrace.
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::log::text_message(
            $crate::__text_callsite!(Error),
            ::core::format_args!($($arg)*),
        )
    };
}

/// The call site and the argument tuple of one hot or strategy log call. The call site is a
/// constant of the argument types, so a template that does not fit its arguments, or arguments
/// that are not all `LogValue`s, fail the build.
#[doc(hidden)]
#[macro_export]
macro_rules! __hot_parts {
    ($level:ident, $template:literal $(, $arg:expr)* $(,)?) => {{
        fn __polysim_callsite<A: $crate::log::LogArgs>(_: &A) -> &'static $crate::log::Callsite {
            const {
                &$crate::log::Callsite::new(
                    $crate::log::Level::$level,
                    ::core::module_path!(),
                    ::core::file!(),
                    ::core::line!(),
                    $template,
                    A::RENDERERS,
                )
            }
        }
        let args = ($($arg,)*);
        (__polysim_callsite(&args), args)
    }};
}

/// Logs at INFO from code below `hot/`. The template takes `{}`, `{:?}`, `{:x}` and `{:.N}`, and
/// each argument is a `LogValue`: the hot thread copies raw words, and the drain makes the text.
macro_rules! hot_info {
    ($($arg:tt)*) => {{
        let (callsite, args) = $crate::__hot_parts!(Info, $($arg)*);
        $crate::log::hot_message(callsite, args)
    }};
}

/// Logs at WARN from code below `hot/`, flushed at once on the drain. See `hot_info!`.
macro_rules! hot_warn {
    ($($arg:tt)*) => {{
        let (callsite, args) = $crate::__hot_parts!(Warn, $($arg)*);
        $crate::log::hot_message(callsite, args)
    }};
}

/// Logs at ERROR from code below `hot/`, with no backtrace: its call site already names the file
/// and line. See `hot_info!`.
macro_rules! hot_error {
    ($($arg:tt)*) => {{
        let (callsite, args) = $crate::__hot_parts!(Error, $($arg)*);
        $crate::log::hot_message(callsite, args)
    }};
}

pub(crate) use {hot_error, hot_info, hot_warn};
