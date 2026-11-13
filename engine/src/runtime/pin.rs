//! The one place that pins a thread to a core, so each thread that needs a pin gets the same rule.

use std::sync::mpsc;
use std::thread::{self, JoinHandle};

/// A thread that could not pin itself to its core on Linux, where a pin is a promise.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[error("a thread could not pin to core {core_id} — pinning is mandatory on linux")]
pub struct PinRefused {
    pub core_id: usize,
}

/// The call that pins the calling thread to a core. A run passes [`pin_to_core`], and a test passes
/// a stand-in, since no host refuses a pin on demand.
pub type PinCall = fn(usize) -> Result<(), PinRefused>;

/// Only Linux refuses. Elsewhere no pin holds for sure, so the attempt is best effort and never an
/// error.
///
/// # Errors
/// [`PinRefused`] when Linux does not let the calling thread run on `core_id`.
pub fn pin_to_core(core_id: usize) -> Result<(), PinRefused> {
    let pinned = core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
    if pinned || !cfg!(target_os = "linux") { Ok(()) } else { Err(PinRefused { core_id }) }
}

/// Starts a thread that pins itself to `core_id` through `pin` first and runs `body` only once the
/// pin holds. The caller learns of a refused pin before this returns, and the thread then ends
/// without running.
///
/// # Panics
/// If the OS cannot start a thread at all.
pub(crate) fn spawn_pinned<T: Default + Send + 'static>(
    name: &str,
    core_id: Option<usize>,
    pin: PinCall,
    body: impl FnOnce() -> T + Send + 'static,
) -> Result<JoinHandle<T>, PinRefused> {
    let (pin_report, pin_outcome) = mpsc::sync_channel(1);
    let join = thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let pinned = core_id.map_or(Ok(()), pin);
            let is_pinned = pinned.is_ok();
            if pin_report.send(pinned).is_err() || !is_pinned {
                return T::default();
            }
            body()
        })
        .unwrap_or_else(|error| panic!("failed to spawn the {name} thread: {error}"));
    let outcome = pin_outcome
        .recv()
        .expect("a pinned thread reports its pin before it does anything else");
    if let Err(refused) = outcome {
        assert!(
            join.join().is_ok(),
            "a thread whose pin was refused returns before it runs anything that can panic"
        );
        return Err(refused);
    }
    Ok(join)
}
