//! Tests of the operator side: the workstation views, the engine fleet behind its tool server, and
//! the web relay.

/// These drive `engine::operator::relay` and its binary, which exist only under feature `web`.
#[cfg(feature = "web")]
mod accept;
mod fleet;
#[cfg(feature = "web")]
mod relay;
#[cfg(feature = "web")]
mod tab_to_fold;
#[cfg(feature = "web")]
mod web_http;
mod workstation;

pub(crate) use workstation::rust_sources;
