//! Tests of the operator side: the workstation views, the engine fleet behind its tool server, and
//! the web relay.

mod fleet;
/// These drive `engine::operator::relay` and its binary, which exist only under feature `web`.
#[cfg(feature = "web")]
mod web_http;
mod workstation;

pub(crate) use workstation::rust_sources;
