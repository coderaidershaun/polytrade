//! Contract tests of the relay: a tab's own bytes cross the real relay into the real link guard of
//! the engine, and the web guard answers before any route.

use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::protocol::Message;

use engine::link::{
    FrameGuard, LINK_MAX_DATAGRAM, LinkBody, LinkDatagram, LinkDecodeError, LinkHash,
    schema_hash_of_fields,
};
use engine::msg::time::LocalTs;
use engine::operator::workstation::link::session::{LinkSession, LinkTarget};
use engine::runtime::config::StrategyId;

use super::web_http::{
    DEADLINE, TOKEN, Tab, UPGRADE_HEADERS, WebServer, bundle, bytes_of_length, command, exchange,
    expect_hello, expect_silence, fake_engine, host, listening, next_message, open_tab, receive,
    within,
};

const STRATEGY: &str = "web-test";

/// The guard that the engine's link builds from its config: the token, the strategy, and the
/// fields that its strategy declares.
fn engine_guard(token: &str) -> FrameGuard {
    FrameGuard {
        token_hash: LinkHash::of_name(token),
        strategy_hash: LinkHash::of_name(STRATEGY),
        schema_hash: schema_hash_of_fields(&["mid"]),
    }
}

/// The session that the browser builds from the hello, as the workstation does.
fn session_from(hello: &Value, now: LocalTs) -> LinkSession {
    let peers = hello["peers"]
        .as_array()
        .expect("the hello lists its peers")
        .iter()
        .map(|peer| {
            peer.as_str()
                .and_then(|text| text.parse::<SocketAddr>().ok())
                .expect("each peer is an address")
        })
        .collect();
    let target = LinkTarget {
        strategy: StrategyId::new(hello["strategy"].as_str().expect("a strategy"))
            .expect("the hello names a valid strategy"),
        peers,
        token_hash: LinkHash(hello["tokenHash"].as_u64().expect("a token hash")),
        sender_nonce: 7,
    };
    LinkSession::new(target, now).expect("the hello names at least one peer")
}

/// The hello comes first, and the subscribe that a tab builds from it is a frame that the
/// engine's own guard admits. The relay adds and drops nothing: one datagram crosses each way, a
/// frame longer than a datagram closes the tab and reaches no engine, and a tab outlasts an
/// engine that is down.
#[tokio::test(flavor = "multi_thread")]
async fn tab_subscribe_is_admitted_by_the_real_link_guard() {
    let (first_engine, first_address) = fake_engine();
    let (second_engine, second_address) = fake_engine();
    let (down_engine, down_address) = fake_engine();
    drop(down_engine);
    let site = bundle("relay-guard");
    let mut serve = command(site.path(), &[first_address, second_address, down_address]);
    serve.args(["--token", TOKEN]);
    let (server, port) = listening(serve);

    let mut tab = open_tab(port, 1).await;
    let hello = expect_hello(&mut tab).await;
    assert_eq!(
        hello,
        json!({
            "strategy": STRATEGY,
            "peers": [first_address.to_string(), second_address.to_string(), down_address.to_string()],
            "tokenHash": engine_guard(TOKEN).token_hash.0,
        })
    );

    let now = LocalTs::from_micros(1_700_000_000_000_000);
    let mut session = session_from(&hello, now);
    let subscribe = session
        .subscribe_due(now)
        .expect("a subscribe is due at start")
        .to_vec();
    within(tab.send(Message::binary(subscribe.clone())))
        .await
        .expect("the subscribe is sent");
    let (arrived, relay_source) = receive(&second_engine);
    assert_eq!(arrived, subscribe, "the subscribe crosses untouched");
    let admitted = LinkDatagram::decode(&arrived, &engine_guard(TOKEN))
        .expect("the engine's guard admits the tab's subscribe");
    assert!(
        matches!(admitted.body, LinkBody::Subscribe(_)),
        "{admitted:?}"
    );
    assert!(
        matches!(
            LinkDatagram::decode(&arrived, &engine_guard("another-token")),
            Err(LinkDecodeError::TokenMismatch { .. })
        ),
        "a guard of another token refuses the same bytes"
    );

    datagrams_cross_untouched_both_ways(&mut tab, &first_engine, &second_engine, relay_source)
        .await;
    within(tab.close(None)).await.expect("the tab closes");
    let closed = server.wait_for_line_containing("closed");
    assert_eq!(
        closed,
        format!(
            "polysim-web: relay {second_address} closed (forwarded 2 to engine, 2 to browser, \
             refused 2)"
        )
    );
    assert!(!closed.contains(TOKEN), "the token is never printed");

    a_frame_longer_than_a_datagram_closes_the_tab(&server, port, &first_engine).await;
    a_tab_outlasts_an_engine_that_is_down(&server, port, down_address).await;
}

async fn datagrams_cross_untouched_both_ways(
    tab: &mut Tab,
    first_engine: &UdpSocket,
    second_engine: &UdpSocket,
    relay_source: SocketAddr,
) {
    let outbound = bytes_of_length(LINK_MAX_DATAGRAM);
    within(tab.send(Message::binary(outbound.clone())))
        .await
        .expect("the frame is sent");
    let (arrived, _) = receive(second_engine);
    assert_eq!(arrived, outbound, "a full-size datagram crosses untouched");
    expect_silence(first_engine);

    within(tab.send(Message::text("not a datagram")))
        .await
        .expect("the frame is sent");

    let inbound = bytes_of_length(LINK_MAX_DATAGRAM);
    second_engine
        .send_to(&inbound, relay_source)
        .expect("the engine replies to the relay");
    assert_eq!(
        next_message(tab).await,
        Message::binary(inbound),
        "a full-size datagram reaches the tab untouched"
    );

    let stranger = UdpSocket::bind(("127.0.0.1", 0)).expect("a free udp port");
    stranger
        .send_to(b"spoof", relay_source)
        .expect("the stranger writes to the relay");
    second_engine
        .send_to(&bytes_of_length(LINK_MAX_DATAGRAM + 1), relay_source)
        .expect("the engine sends too much");
    second_engine
        .send_to(&[9, 9, 9, 9, 9], relay_source)
        .expect("the engine sends a short one");
    assert_eq!(
        next_message(tab).await,
        Message::binary(vec![9_u8; 5]),
        "only the engine's datagrams, and only link-sized ones, reach the tab"
    );
}

async fn a_frame_longer_than_a_datagram_closes_the_tab(
    server: &WebServer,
    port: u16,
    engine: &UdpSocket,
) {
    let mut tab = open_tab(port, 0).await;
    expect_hello(&mut tab).await;
    // The send itself can fail once the relay has dropped the socket, so only the outcome counts.
    tab.send(Message::binary(vec![7_u8; LINK_MAX_DATAGRAM + 1]))
        .await
        .ok();
    let outcome = within(tab.next()).await;
    assert!(
        matches!(outcome, None | Some(Err(_) | Ok(Message::Close(_)))),
        "the relay answered an oversized frame with {outcome:?}"
    );
    expect_silence(engine);
    let closed = server.wait_for_line_containing("closed");
    assert!(
        closed.contains("closed (forwarded 0 to engine, 0 to browser, refused 0): "),
        "an abnormal end names its cause on the same line: {closed}"
    );
}

async fn a_tab_outlasts_an_engine_that_is_down(
    server: &WebServer,
    port: u16,
    down_address: SocketAddr,
) {
    let mut tab = open_tab(port, 2).await;
    expect_hello(&mut tab).await;
    for _ in 0..3 {
        within(tab.send(Message::binary(vec![1_u8, 2, 3])))
            .await
            .expect("the relay keeps the tab while the engine is down");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let revived = UdpSocket::bind(down_address).expect("the engine's port is free again");
    revived
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("a read timeout");
    let mut buffer = [0_u8; 64];
    let deadline = Instant::now() + DEADLINE;
    loop {
        within(tab.send(Message::binary(b"after".to_vec())))
            .await
            .expect("the relay still holds the tab");
        if let Ok((length, _)) = revived.recv_from(&mut buffer) {
            assert_eq!(&buffer[..length], b"after");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the revived engine never heard from the tab"
        );
    }

    drop(tab);
    let closed = server.wait_for_line_containing("closed");
    assert!(
        closed.ends_with(')'),
        "a tab that vanishes without a close frame is a quiet end: {closed}"
    );
}

/// One route of the server, with the status that it gives a request that the guard passes.
struct Route {
    what: &'static str,
    method: &'static str,
    path: &'static str,
    is_upgrade: bool,
    passed: u16,
}

const ROUTES: [Route; 7] = [
    Route {
        what: "the descriptor",
        method: "GET",
        path: "/jet-app.json",
        is_upgrade: false,
        passed: 200,
    },
    Route {
        what: "the page",
        method: "GET",
        path: "/",
        is_upgrade: false,
        passed: 200,
    },
    Route {
        what: "a file of the bundle",
        method: "GET",
        path: "/index.html",
        is_upgrade: false,
        passed: 200,
    },
    Route {
        what: "a file outside the bundle",
        method: "GET",
        path: "/missing",
        is_upgrade: false,
        passed: 404,
    },
    Route {
        what: "a method the bundle does not take",
        method: "POST",
        path: "/",
        is_upgrade: false,
        passed: 405,
    },
    Route {
        what: "a peer the relay does not carry",
        method: "GET",
        path: "/api/link?peer=9",
        is_upgrade: true,
        passed: 404,
    },
    Route {
        what: "the relay of a known peer",
        method: "GET",
        path: "/api/link?peer=0",
        is_upgrade: true,
        passed: 101,
    },
];

struct Shape {
    what: &'static str,
    headers: Vec<(&'static str, String)>,
    status: u16,
}

/// A page on any website can open a socket to a loopback port, and the browser adds that page's
/// origin, so these are the shapes that must never reach a route.
fn refused_shapes(port: u16) -> Vec<Shape> {
    let own = host(port);
    let other_port = host(port.wrapping_add(1));
    let shape = |what, headers: &[(&'static str, &str)], status| Shape {
        what,
        headers: headers
            .iter()
            .map(|(name, value)| (*name, (*value).to_owned()))
            .collect(),
        status,
    };
    vec![
        shape(
            "a foreign origin",
            &[("Host", &own), ("Origin", "http://evil.example")],
            403,
        ),
        shape(
            "an origin of null",
            &[("Host", &own), ("Origin", "null")],
            403,
        ),
        shape(
            "an origin on another port of this machine",
            &[("Host", &own), ("Origin", &format!("http://{other_port}"))],
            403,
        ),
        shape(
            "two origins",
            &[
                ("Host", &own),
                ("Origin", &format!("http://{own}")),
                ("Origin", &format!("http://{own}")),
            ],
            403,
        ),
        shape(
            "a host that resolves to loopback by rebinding",
            &[("Host", &format!("evil.example:{port}"))],
            403,
        ),
        shape(
            "a loopback host on another port",
            &[("Host", &other_port)],
            403,
        ),
        shape(
            "a loopback host with no port",
            &[("Host", "127.0.0.1")],
            403,
        ),
        shape("no host", &[], 400),
        shape("two hosts", &[("Host", &own), ("Host", &own)], 400),
    ]
}

fn ask(port: u16, route: &Route, headers: &[(&'static str, String)]) -> (u16, Vec<String>) {
    let mut sent: Vec<(&str, &str)> = headers
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    if route.is_upgrade {
        sent.extend(UPGRADE_HEADERS);
    }
    let reply = exchange(port, route.method, route.path, &sent);
    let guarded_headers = ["cache-control", "content-security-policy"]
        .iter()
        .map(|name| reply.header(name).unwrap_or_default().to_owned())
        .collect();
    (reply.status, guarded_headers)
}

/// The guard answers each refused shape with its own status on every route, so no route ever sees
/// such a request. Each loopback name, with or without this page's own origin, reaches the route.
/// Every answer refuses a frame on another site, because a frame load sends no `Origin` and a
/// framing site could lay a decoy over START or STOP.
#[tokio::test(flavor = "multi_thread")]
async fn web_guard_runs_before_each_route() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("relay-routes");
    let (_server, port) = listening(command(site.path(), &[engine_address]));
    let guarded = vec!["no-store".to_owned(), "frame-ancestors 'self'".to_owned()];

    for route in &ROUTES {
        for shape in refused_shapes(port) {
            assert_eq!(
                ask(port, route, &shape.headers),
                (shape.status, guarded.clone()),
                "{} on {}",
                shape.what,
                route.what
            );
        }
    }

    let passing_routes = ROUTES.iter().filter(|route| route.passed != 101);
    for route in passing_routes {
        for authority in [
            host(port),
            format!("localhost:{port}"),
            format!("LocalHost:{port}"),
            format!("[::1]:{port}"),
        ] {
            let own_origin = format!("http://{authority}");
            let without_origin = vec![("Host", authority.clone())];
            let with_origin = vec![("Host", authority.clone()), ("Origin", own_origin)];
            for headers in [without_origin, with_origin] {
                assert_eq!(
                    ask(port, route, &headers),
                    (route.passed, guarded.clone()),
                    "{} through {headers:?}",
                    route.what
                );
            }
        }
    }
    let mut tab = open_tab(port, 0).await;
    expect_hello(&mut tab).await;
}
