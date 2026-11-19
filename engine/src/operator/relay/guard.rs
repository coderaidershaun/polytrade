//! The check that each request passes before any route: it must come from this machine's own
//! address and from a page that this server served.

use hyper::header::{HOST, ORIGIN};
use hyper::{HeaderMap, StatusCode};

pub(super) struct Refusal {
    pub(super) status: StatusCode,
    pub(super) reason: String,
}

impl Refusal {
    fn new(status: StatusCode, reason: impl Into<String>) -> Refusal {
        Refusal {
            status,
            reason: reason.into(),
        }
    }
}

/// The relay carries run control with no login, so a request must come from this machine's own
/// address and from a page this server served. Checking `Host` stops a rebinding attack, where a
/// foreign name resolves to loopback, and checking `Origin` stops a page on another site from
/// opening the relay. A browser cannot forge either header, and it always sends `Origin` when a
/// page opens a WebSocket. A frame load or a plain link from another site sends none, which is
/// why every response also refuses a frame on another site.
pub(super) fn guard(headers: &HeaderMap, port: u16) -> Result<(), Refusal> {
    let mut hosts = headers.get_all(HOST).iter();
    let (Some(host), None) = (hosts.next(), hosts.next()) else {
        return Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            "the request needs exactly one Host header",
        ));
    };
    let host = host.to_str().unwrap_or_default();
    if !is_loopback_authority(host, port) {
        return Err(Refusal::new(
            StatusCode::FORBIDDEN,
            format!("this server answers only to 127.0.0.1, localhost and [::1] on port {port}"),
        ));
    }
    let mut origins = headers.get_all(ORIGIN).iter();
    match (origins.next(), origins.next()) {
        (None, _) => Ok(()),
        (Some(origin), None) if *origin == format!("http://{host}") => Ok(()),
        _ => Err(Refusal::new(
            StatusCode::FORBIDDEN,
            "this server refuses requests from a web page it did not serve",
        )),
    }
}

// SMELL: a browser leaves `:80` off both `Host` and `Origin`, so a server started with
// `--port 80` refuses every page. Accepting a bare loopback name when bound to port 80 would fix
// it.
fn is_loopback_authority(authority: &str, port: u16) -> bool {
    let Some((name, number)) = authority.rsplit_once(':') else {
        return false;
    };
    let is_loopback_name = ["127.0.0.1", "localhost", "[::1]"]
        .iter()
        .any(|loopback| name.eq_ignore_ascii_case(loopback));
    is_loopback_name && number == port.to_string()
}
