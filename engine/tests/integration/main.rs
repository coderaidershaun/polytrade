//! Integration suite: real venue, real time, real serialisation — never CI. Every test is `#[ignore]`.
//! Only the live-runner role of a job runs a test that signs, by its runbook and the live policy. An
//! agent runs a public read only where a plan names it, by its exact name and with an empty
//! environment. Grouped as one target, the same way the fitness tests share one target. None of
//! these tests places an order.

mod binance_scale;
mod binance_signed;
mod observer;
mod poly_rotation;
mod rest_check;
mod rotations_parquet;

use std::time::{SystemTime, UNIX_EPOCH};

/// The venue's fixed 5-minute grid; window starts and rotation parity both derive from it.
pub(crate) const WINDOW_SECS: i64 = 300;

/// Microseconds per second, for converting `_us` stamps into reported seconds.
pub(crate) const US: f64 = 1_000_000.0;

/// Wall-clock seconds since epoch — the venue's grid is UTC, so alignment and mid-window checks key
/// off real time (this is edge-side test orchestration, never hot-path state).
pub(crate) fn unix_now_s() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs() as i64
}
