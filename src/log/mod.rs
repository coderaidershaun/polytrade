//! Logging that never blocks a producer: a lock-free ring per thread feeds one drain thread. In the
//! browser build the same macros print straight to the console instead.

#[cfg(target_arch = "wasm32")]
mod console;
mod drain;
mod output;
mod producer;
mod record;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

#[cfg(target_arch = "wasm32")]
pub use console::register_thread;
#[cfg(not(target_arch = "wasm32"))]
pub use producer::register_thread;
pub use record::{Level, LogRecord};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use producer::{register_external_ring, tag_task};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use record::MSG_CAPACITY;

#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub use console::{error_message, info_message, warn_message};
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub use producer::{error_message, info_message, warn_message};

const DEFAULT_FILE_STEM: &str = "polysim";

#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Where the log file goes in a release build. A debug build logs to stderr instead.
    pub dir: PathBuf,
    /// The log file is `{file_stem}-YYYY-MM-DD.log`. The engine sets it from its strategy and
    /// engine ids, with `-sim` added for a simulated run.
    pub file_stem: Box<str>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("logs"),
            file_stem: DEFAULT_FILE_STEM.into(),
        }
    }
}

/// Call `drain` to flush and stop the log thread. Dropping the handle does neither.
#[derive(Debug)]
pub struct LogHandle {
    drain: Arc<AtomicBool>,
    join: JoinHandle<()>,
}

impl LogHandle {
    pub fn drain(self) {
        self.drain.store(true, Ordering::Release);
        if self.join.join().is_err() {
            eprintln!("polysim: logging drain thread panicked during shutdown");
        }
    }
}

/// Call at startup, before any thread logs.
///
/// # Panics
/// Panics if called more than once in a process, and in the browser build, which can start no
/// drain thread. The browser needs no init: there the macros print straight to the console.
pub fn init(config: &LogConfig) -> LogHandle {
    let channels = producer::install();
    let output = output::Output::open(config);
    let drain = Arc::new(AtomicBool::new(false));
    let thread_drain = Arc::clone(&drain);
    let join = thread::Builder::new()
        .name("polysim-log".to_owned())
        .spawn(move || drain::run(channels, output, thread_drain))
        .expect("failed to spawn logging drain thread");
    LogHandle { drain, join }
}

/// Logs at INFO. It takes the same arguments as `format!`.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::log::info_message(
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
            ::core::format_args!($($arg)*),
        )
    };
}

/// Logs at WARN, flushed at once.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::log::warn_message(
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
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
        $crate::log::error_message(
            ::core::module_path!(),
            ::core::file!(),
            ::core::line!(),
            ::core::format_args!($($arg)*),
        )
    };
}
