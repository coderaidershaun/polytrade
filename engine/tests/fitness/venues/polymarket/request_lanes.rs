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
use engine::venues::polymarket::exec::codec::{
    DecodeContext, EncodedRequest, OrderIndex, OrdersRead, TokenTable, VenueAnswer,
    cancel_venue_order, decode_orders_page, heartbeat, open_orders_page,
};
use engine::venues::polymarket::exec::facts::POLYMARKET_FACTS;
use engine::venues::polymarket::exec::heartbeat::{BeatAnswer, HeartbeatSchedule, HeartbeatStep};
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
    Read,
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

/// The book of the venue before its own cancel: two open orders of the account.
const BOOK: &str = include_str!("../../../../fixtures/polymarket/exec/data_orders_wrapped.json");
const EMPTY_BOOK: &str = r#"{"limit":100,"next_cursor":"LTE=","count":0,"data":[]}"#;

const SECOND: i64 = 1_000_000;

fn at(micros: i64) -> LocalTs {
    LocalTs::from_micros(micros)
}

/// What the stand-in venue does with beats and reads, which the test changes as it goes.
#[derive(Default)]
struct VenueScript {
    are_beats_lost: bool,
    /// Answers for the next beats, oldest first. After them each beat is accepted with a new id.
    beat_answers: std::collections::VecDeque<(u16, &'static str)>,
    accepted: u32,
    has_cancelled_book: bool,
}

fn scripted_venue() -> (StandInVenue, Arc<Mutex<VenueScript>>) {
    let script = Arc::new(Mutex::new(VenueScript::default()));
    let venue_script = Arc::clone(&script);
    let venue = StandInVenue::start(Arc::new(move |request: &Request| {
        let mut script = venue_script.lock().expect("no holder of the script panics");
        if request.names("POST", "/v1/heartbeats") {
            if script.are_beats_lost {
                return Reply::Lost;
            }
            if let Some((status, body)) = script.beat_answers.pop_front() {
                return Reply::Answer {
                    status,
                    body: body.to_owned(),
                };
            }
            script.accepted += 1;
            return ok(&format!(r#"{{"heartbeat_id":"h{}"}}"#, script.accepted));
        }
        if request.names("GET", "/data/orders") {
            return ok(if script.has_cancelled_book { EMPTY_BOOK } else { BOOK });
        }
        Reply::Lost
    }));
    (venue, script)
}

/// What one poll led to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Polled {
    Waited,
    Beat(BeatAnswer),
    /// The mirror was reset, and the read of open orders found this many orders at the venue.
    MirrorReset {
        venue_holds: usize,
    },
    ReadAfterVenueCancel {
        venue_holds: usize,
    },
}

/// The heartbeat of the edge, with the real lanes, client, and codec, driven as the actor drives
/// it, at the times the test passes in.
struct BeatingEdge {
    schedule: HeartbeatSchedule,
    lanes: RestLanes<Asked>,
    outcomes: mpsc::Receiver<RestOutcome<Asked>>,
}

impl BeatingEdge {
    fn start(venue: &StandInVenue) -> Self {
        let (lanes, outcomes) = spawn_lanes(lane_setup(venue), &tokio::runtime::Handle::current());
        Self {
            schedule: HeartbeatSchedule::new(),
            lanes,
            outcomes,
        }
    }

    async fn poll(&mut self, now: LocalTs) -> Polled {
        match self.schedule.on_poll(now) {
            HeartbeatStep::Wait => Polled::Waited,
            HeartbeatStep::Send { id } => {
                let request = heartbeat(&id).expect("a heartbeat encodes");
                let job = RestJob::signed(Asked::Beat, request);
                assert!(self.lanes.submit(Lane::Heartbeat, job).is_queued());
                self.schedule.on_sent(now);
                let outcome = next(&mut self.outcomes).await;
                assert_eq!(outcome.purpose, Asked::Beat);
                Polled::Beat(self.schedule.on_answer(&outcome.answer))
            }
            HeartbeatStep::ResetMirror => Polled::MirrorReset {
                venue_holds: self.read_open_orders(now).await,
            },
            HeartbeatStep::ReadAfterVenueCancel => Polled::ReadAfterVenueCancel {
                venue_holds: self.read_open_orders(now).await,
            },
        }
    }

    /// The orders that the venue holds. No token is bound, so each order of the read is one that
    /// the codec cannot attribute, which is enough to count them.
    async fn read_open_orders(&mut self, now: LocalTs) -> usize {
        let job = RestJob::signed(Asked::Read, open_orders_page(None));
        assert!(self.lanes.submit(Lane::Control, job).is_queued());
        let outcome = next(&mut self.outcomes).await;
        assert_eq!(outcome.purpose, Asked::Read);
        let response = outcome.answer.expect("the venue answers its reads");
        let (tokens, orders) = (
            TokenTable::with_retired_capacity(1),
            OrderIndex::with_capacity(1),
        );
        let context = DecodeContext {
            tokens: &tokens,
            orders: &orders,
            api_key: "no-key",
            received_ts_us: now,
        };
        let read = OrdersRead {
            instrument: UP,
            recon_seq: 0,
        };
        let VenueAnswer::Answered(page) =
            decode_orders_page(response.answer(), read, &context).expect("the page decodes")
        else {
            panic!("a page of open orders is answered, not refused as venue state");
        };
        page.unattributable.len()
    }
}

/// CONTRACT: the venue cancels each order of the account when it hears no heartbeat for its limit,
/// and the edge learns that only from reports. So a beat that fails is due again at the next poll,
/// which gives several tries before the deadline, the venue limit less a margin. Past the deadline
/// the edge rebuilds its order mirror from a read and holds placing; past the venue limit it reads
/// again, because the venue has cancelled the book; a beat answered again lets placing resume. The
/// id of the next beat comes from a refusal as from a success, and an answer with no id keeps the
/// last one.
#[test]
fn lost_heartbeat_past_deadline_resets_order_mirror() {
    let facts = POLYMARKET_FACTS;
    assert_eq!(
        (
            facts.heartbeat_period.micros(),
            facts.heartbeat_limit.micros(),
            facts.heartbeat_margin.micros()
        ),
        (5 * SECOND, 10 * SECOND, 2 * SECOND),
        "the facts of the venue keep their values, so the deadline is 8 s after the last good beat"
    );
    let (venue, script) = scripted_venue();
    let runtime = runtime();
    runtime.block_on(async {
        let mut edge = BeatingEdge::start(&venue);
        assert_eq!(edge.poll(at(0)).await, Polled::Beat(BeatAnswer::Accepted));
        for second in 1..5 {
            assert_eq!(edge.poll(at(second * SECOND)).await, Polled::Waited);
        }
        script.lock().expect("the script is free").are_beats_lost = true;

        let mut failed = 0;
        for (second, step) in [(5, "step 1"), (6, "step 2"), (7, "step 2")] {
            assert_eq!(
                edge.poll(at(second * SECOND)).await,
                Polled::Beat(BeatAnswer::Lost),
                "{step}: a beat goes out at {second} s, and the one after a failure is due at once"
            );
            failed += 1;
            assert!(
                !edge.schedule.is_placing_held(),
                "before the deadline placing goes on"
            );
        }
        assert_eq!(failed, 3);

        assert_eq!(
            edge.poll(at(8 * SECOND)).await,
            Polled::MirrorReset { venue_holds: 2 },
            "step 3: the deadline passed, so the mirror is read again from the venue"
        );
        assert!(edge.schedule.is_placing_held(), "step 3: placing stops");
        assert_eq!(
            edge.poll(at(9 * SECOND)).await,
            Polled::Beat(BeatAnswer::Lost)
        );
        assert!(edge.schedule.is_placing_held());

        script
            .lock()
            .expect("the script is free")
            .has_cancelled_book = true;
        assert_eq!(
            edge.poll(at(10 * SECOND)).await,
            Polled::ReadAfterVenueCancel { venue_holds: 0 },
            "step 4: past the venue limit the venue cancelled the book, and a second read shows it"
        );
        assert_eq!(
            edge.poll(at(10 * SECOND + SECOND / 2)).await,
            Polled::Beat(BeatAnswer::Lost),
            "the read after the venue limit comes once"
        );
        assert!(
            edge.schedule.is_placing_held(),
            "no placement before a beat is answered"
        );

        script.lock().expect("the script is free").are_beats_lost = false;
        assert_eq!(
            edge.poll(at(11 * SECOND)).await,
            Polled::Beat(BeatAnswer::Accepted),
            "step 5: the venue answers the next beat"
        );
        assert!(!edge.schedule.is_placing_held(), "step 5: placing goes on");
    });
    assert_eq!(
        (
            venue.count("POST", "/v1/heartbeats"),
            venue.count("GET", "/data/orders")
        ),
        (7, 2),
        "a beat at 0, 5, 6, 7, 9, 10.5, and 11 s, and the two reads"
    );

    let (venue, script) = scripted_venue();
    script
        .lock()
        .expect("the script is free")
        .beat_answers
        .extend([
            (401, r#"{"error_msg":"Unauthorized"}"#),
            (
                400,
                r#"{"error_msg":"Invalid Heartbeat ID","heartbeat_id":"3c4d"}"#,
            ),
            (200, r#"{"heartbeat_id":"1a2b"}"#),
        ]);
    runtime.block_on(async {
        let mut edge = BeatingEdge::start(&venue);
        let answers = [
            edge.poll(at(0)).await,
            edge.poll(at(SECOND)).await,
            edge.poll(at(2 * SECOND)).await,
            edge.poll(at(7 * SECOND)).await,
        ];
        assert_eq!(
            answers,
            [
                Polled::Beat(BeatAnswer::Lost),
                Polled::Beat(BeatAnswer::Refused),
                Polled::Beat(BeatAnswer::Accepted),
                Polled::Beat(BeatAnswer::Accepted),
            ],
            "an answer with no id and a refusal are each tried again at once"
        );
    });
    let bodies: Vec<String> = venue
        .seen()
        .into_iter()
        .filter(|request| request.names("POST", "/v1/heartbeats"))
        .map(|request| request.body)
        .collect();
    assert_eq!(
        bodies,
        [
            r#"{"heartbeat_id":""}"#,
            r#"{"heartbeat_id":""}"#,
            r#"{"heartbeat_id":"3c4d"}"#,
            r#"{"heartbeat_id":"1a2b"}"#,
        ],
        "a refusal names the id of the next beat as a success does, and no id keeps the last one"
    );
}
