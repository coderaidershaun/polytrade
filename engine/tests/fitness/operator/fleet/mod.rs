//! Tests of the engine fleet and its tool server, which build only with the feature that turns the
//! server on.

/// These drive `engine::operator::fleet` and its binary, which exist only under feature `mcp`.
#[cfg(feature = "mcp")]
mod mcp_fleet;
#[cfg(feature = "mcp")]
mod mcp_http;
#[cfg(feature = "mcp")]
mod mcp_support;
#[cfg(feature = "mcp")]
mod mcp_tools;
