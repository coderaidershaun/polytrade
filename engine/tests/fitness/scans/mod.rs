//! Tests that read the repository as data: manifests, source folders, and log call sites that no
//! type can pin.

mod cargo_bins;
mod clock_sites;
mod exec_no_clock;
mod log_lane_ownership;
mod suite_integrity;

pub(crate) use exec_no_clock::rust_sources;
