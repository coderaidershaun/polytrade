//! The three request lanes of the Polymarket edge against a stand-in venue on this host, and the
//! heartbeat that keeps the venue from cancelling the book.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use engine::msg::exec::{ExecCommand, ExecKind, OrderStyle};
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use engine::msg::time::{DurationUs, EngineClock, LocalTs};
use engine::runtime::secrets::Secret;
use engine::venues::exec::{ExecCore, ExecRequest, ExecSession, PlaceNotSentReason, SessionSetup};
use engine::venues::polymarket::exec::codec::{EncodedRequest, cancel_venue_order, heartbeat};
use engine::venues::polymarket::exec::lanes::{
    Lane, LaneSetup, RestJob, RestLanes, RestOutcome, Submitted, spawn_lanes,
};
use engine::venues::polymarket::exec::rest::ClobHttp;
use engine::venues::polymarket::exec::sign::address::Address;
use engine::venues::polymarket::exec::sign::l2::{ApiCredentials, HttpMethod, RequestSigner};
use tokio::sync::mpsc;

use crate::exec_support::SentEvents;
use crate::venues::exec::obligations::{Session, send};

/// A request as the stand-in venue read it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    method: String,
    target: String,
    body: String,
}

impl Request {
    fn names(&self, method: &str, path: &str) -> bool {
        self.method == method && self.target.split('?').next() == Some(path)
    }
}

/// What the stand-in venue does with one request.
enum Reply {
    Answer {
        status: u16,
        body: String,
    },
    /// The answer waits until the test opens the gate.
    AfterRelease {
        status: u16,
        body: String,
    },
    /// The connection closes with no answer, so the client sees a failure in transport.
    Lost,
}

fn ok(body: &str) -> Reply {
    Reply::Answer {
        status: 200,
        body: body.to_owned(),
    }
}

type Handler = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;

#[derive(Default)]
struct Gate {
    is_open: Mutex<bool>,
    opened: Condvar,
}

impl Gate {
    fn open(&self) {
        *self.is_open.lock().expect("no holder of the gate panics") = true;
        self.opened.notify_all();
    }

    fn wait(&self) {
        let mut is_open = self.is_open.lock().expect("no holder of the gate panics");
        while !*is_open {
            is_open = self
                .opened
                .wait(is_open)
                .expect("no holder of the gate panics");
        }
    }
}

/// A venue on this host. It reads each request, keeps it, and replies as its handler says, one
/// connection for each request.
struct StandInVenue {
    port: u16,
    seen: Arc<Mutex<Vec<Request>>>,
    gate: Arc<Gate>,
}

impl StandInVenue {
    fn start(handler: Handler) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("the test binds a loopback port");
        let port = listener.local_addr().expect("a bound address").port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(Gate::default());
        let (venue_seen, venue_gate) = (Arc::clone(&seen), Arc::clone(&gate));
        thread::spawn(move || {
            for stream in listener.incoming().map_while(Result::ok) {
                let (seen, gate, handler) = (
                    Arc::clone(&venue_seen),
                    Arc::clone(&venue_gate),
                    Arc::clone(&handler),
                );
                thread::spawn(move || serve(stream, &seen, &gate, &handler));
            }
        });
        Self { port, seen, gate }
    }

    fn seen(&self) -> Vec<Request> {
        self.seen
            .lock()
            .expect("no reader panics while it holds the list")
            .clone()
    }

    fn count(&self, method: &str, path: &str) -> usize {
        self.seen()
            .iter()
            .filter(|request| request.names(method, path))
            .count()
    }

    async fn wait_for(&self, method: &str, path: &str, count: usize) {
        for _ in 0..5_000 {
            if self.count(method, path) >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!(
            "the venue never saw {count} {method} {path}: {:?}",
            self.seen()
        );
    }
}

fn serve(stream: TcpStream, seen: &Mutex<Vec<Request>>, gate: &Gate, handler: &Handler) {
    let mut reader = BufReader::new(stream);
    let Some(request) = read_request(&mut reader) else {
        return;
    };
    seen.lock()
        .expect("no reader panics while it holds the list")
        .push(request.clone());
    match handler(&request) {
        Reply::Answer { status, body } => write_answer(reader.get_mut(), status, &body),
        Reply::AfterRelease { status, body } => {
            gate.wait();
            write_answer(reader.get_mut(), status, &body);
        }
        Reply::Lost => {}
    }
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let (method, target) = (parts.next()?.to_owned(), parts.next()?.to_owned());
    let mut length = 0;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).ok()?;
        if header.trim().is_empty() {
            break;
        }
        let lower = header.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            length = value.trim().parse().ok()?;
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    Some(Request {
        method,
        target,
        body: String::from_utf8(body).ok()?,
    })
}

fn write_answer(stream: &mut TcpStream, status: u16, body: &str) {
    let answer = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(answer.as_bytes())
        .expect("the answer is written");
}

/// Signed with a key that is no key: the stand-in reads no signature.
fn lane_setup(venue: &StandInVenue) -> LaneSetup {
    let credentials = ApiCredentials::new(
        "no-key".to_owned(),
        Secret::new("c2VjcmV0"),
        Secret::new("no-passphrase"),
    );
    LaneSetup {
        http: Arc::new(
            ClobHttp::on_loopback(venue.port, Duration::from_secs(1), Duration::from_secs(10))
                .expect("the loopback client builds"),
        ),
        signer: Arc::new(
            RequestSigner::new(&credentials, Address::ZERO).expect("the secret is base64url"),
        ),
        clock: EngineClock::start(),
        venue_clock_offset: DurationUs::ZERO,
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test builds a runtime")
}

async fn next<P>(outcomes: &mut mpsc::Receiver<RestOutcome<P>>) -> RestOutcome<P> {
    tokio::time::timeout(Duration::from_secs(10), outcomes.recv())
        .await
        .expect("an outcome comes within ten seconds")
        .expect("the lanes are alive")
}

fn status<P>(outcome: &RestOutcome<P>) -> u16 {
    outcome
        .answer
        .as_ref()
        .map_or(0, |response| response.status)
}

const UP: InstrumentId = InstrumentId(0);
const OURS: ClientOrderId = ClientOrderId(0x6a64_f040_000c_0001);
const VENUE_ORDER: &str = "0x3f1c9b4a72e5d08a1c6b3fd2094e7a5b81c02d6e9fa473b5c8e10d92a4f67b3c";
const NOW: LocalTs = LocalTs::from_micros(1_786_046_862_000_000);

/// What a test asked a lane for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Asked {
    Place,
    Cancel,
    Beat,
}

/// A placement the stand-in holds. The signed order body of the venue is the encoder's job and no
/// concern of the lanes, so the body here is a placeholder.
fn place() -> RestJob<Asked> {
    let request = EncodedRequest {
        method: HttpMethod::Post,
        path: "/order".to_owned(),
        query: String::new(),
        body: r#"{"order":{}}"#.to_owned(),
    };
    RestJob::signed(Asked::Place, request)
}

fn cancel() -> RestJob<Asked> {
    RestJob::signed(
        Asked::Cancel,
        cancel_venue_order(VENUE_ORDER).expect("a venue id encodes"),
    )
}

fn beat() -> RestJob<Asked> {
    RestJob::signed(Asked::Beat, heartbeat("").expect("a heartbeat encodes"))
}

/// Quoting, with the placement of [`OURS`] decided by the core.
fn quoting_session() -> Session {
    let mut session = ExecSession::new(SessionSetup {
        venue: "polymarket",
        core: ExecCore::with_limits(8, 32),
        instruments: vec![UP],
        events: SentEvents::default(),
        inflight_timeout: DurationUs::from_millis(5_000),
    });
    assert_eq!(
        session.connect().len(),
        1,
        "a connect subscribes the stream"
    );
    let resync_seq = session.begin_pass(1);
    assert!(session.resync.on_read(resync_seq));
    session.finish_pass();
    assert!(session.arm_readiness(NOW));
    session.events.clear();
    session
}

const PRICE: Price = Price::from_mantissa(52 * FIXED_SCALE / 100);
const SIZE: Qty = Qty::from_mantissa(10 * FIXED_SCALE);

fn place_command() -> ExecCommand {
    ExecCommand::Place {
        instrument: UP,
        client_id: OURS,
        side: Side::Buy,
        price: PRICE,
        qty: SIZE,
        style: OrderStyle::PostOnly,
    }
}

fn our_placement() -> ExecRequest {
    ExecRequest::Place {
        instrument: UP,
        client_id: OURS,
        side: Side::Buy,
        price: PRICE,
        qty: SIZE,
        style: OrderStyle::PostOnly,
    }
}

/// CONTRACT: the lanes of the edge and the venue meet at the network. A marketable placement
/// blocks its request for the venue's taker hold, so a cancel or a heartbeat queued behind it would
/// wait out that hold, and ten seconds without a heartbeat cancel the whole book. So each has its
/// own lane: the cancel and the beat finish while the placement is held. A full lane refuses its
/// job and leaves the other lanes free, and a placement that its lane refused is answered as not
/// sent, so its slot never waits for an answer that cannot come.
#[test]
fn cancel_and_heartbeat_finish_while_a_place_blocks() {
    let venue = StandInVenue::start(Arc::new(|request: &Request| {
        if request.names("POST", "/order") {
            return Reply::AfterRelease {
                status: 200,
                body: r#"{"success":true,"orderID":"0xplaced","status":"live"}"#.to_owned(),
            };
        }
        if request.names("DELETE", "/order") {
            return ok(r#"{"canceled":[],"not_canceled":{}}"#);
        }
        if request.names("POST", "/v1/heartbeats") {
            return ok(r#"{"heartbeat_id":"1a2b"}"#);
        }
        Reply::Lost
    }));
    let runtime = runtime();
    runtime.block_on(async {
        let (lanes, mut outcomes): (RestLanes<Asked>, _) =
            spawn_lanes(lane_setup(&venue), &tokio::runtime::Handle::current());

        assert!(lanes.submit(Lane::Place, place()).is_queued());
        venue.wait_for("POST", "/order", 1).await;
        assert!(lanes.submit(Lane::Control, cancel()).is_queued());
        assert!(lanes.submit(Lane::Heartbeat, beat()).is_queued());
        let mut answered = [next(&mut outcomes).await, next(&mut outcomes).await];
        answered.sort_by_key(|outcome| outcome.purpose);
        assert_eq!(
            answered
                .each_ref()
                .map(|outcome| (outcome.purpose, status(outcome))),
            [(Asked::Cancel, 200), (Asked::Beat, 200)],
            "the cancel and the beat are answered while the placement is held"
        );
        assert!(
            outcomes.try_recv().is_err(),
            "the placement has no answer yet"
        );

        let mut queued = 0;
        let mut session = quoting_session();
        loop {
            match lanes.submit(Lane::Place, place()) {
                Submitted::Queued => queued += 1,
                Submitted::LaneFull => {
                    // As the actor does with a placement that its lane refused.
                    assert_eq!(send(&mut session, place_command(), NOW), [our_placement()]);
                    session
                        .refuse(our_placement(), PlaceNotSentReason::NoTransport, NOW)
                        .expect("the session raises no fault here");
                    break;
                }
            }
            assert!(queued < 100_000, "the place lane never refused a job");
        }
        assert!(queued > 0, "a held placement leaves room on its lane");
        let not_sent: Vec<ExecKind> = session.events.iter().map(|event| event.kind).collect();
        assert_eq!(
            not_sent,
            [ExecKind::PlaceNotSent],
            "the refused placement is not sent"
        );
        assert!(lanes.submit(Lane::Control, cancel()).is_queued());
        assert!(lanes.submit(Lane::Heartbeat, beat()).is_queued());
        let mut answered = [next(&mut outcomes).await, next(&mut outcomes).await];
        answered.sort_by_key(|outcome| outcome.purpose);
        assert_eq!(
            answered.each_ref().map(|outcome| outcome.purpose),
            [Asked::Cancel, Asked::Beat],
            "a full place lane leaves the other two lanes free"
        );

        venue.gate.open();
        for _ in 0..=queued {
            let outcome = next(&mut outcomes).await;
            assert_eq!((outcome.purpose, status(&outcome)), (Asked::Place, 200));
        }
        assert_eq!(venue.count("POST", "/order"), queued + 1);
    });
}
