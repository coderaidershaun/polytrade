//! The one place that pins a thread to a core, so each thread that needs a pin gets the same rule.

use crate::runtime::shutdown::FatalSignal;
use crate::{error, warn};

/// Linux: pinning mandatory (failure trips fatal). Other OS: best-effort, single WARN.
pub(crate) fn pin_current_thread(core_id: usize, fatal: &FatalSignal) {
    let pinned = core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
    if !cfg!(target_os = "linux") {
        warn!(
            "cpu pinning is best-effort on {} — hot thread may float",
            std::env::consts::OS
        );
        return;
    }
    if !pinned {
        error!("hot thread failed to pin to core {core_id} — pinning is mandatory on linux");
        fatal.trip(format!(
            "hot thread failed to pin to core {core_id} — pinning is mandatory on linux"
        ));
    }
}
