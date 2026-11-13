//! Message time comes from one clock, or a replay folds an order the live run never saw. So each
//! clock start, each file that names a command's latency tick, and each allowed clock read is here.

use std::collections::BTreeMap;
use std::path::Path;

use super::log_lane_ownership::without_comments_and_literals;
use crate::operator::rust_sources;

const CLOCK_START: &str = "EngineClock::start(";

/// Each file of the library that starts an engine clock one time, and why.
const CLOCK_STARTS: [(&str, &str); 4] = [
    (
        "src/runtime/assembly/mod.rs",
        "the one clock of the process, which every producer of message stamps reads",
    ),
    (
        "src/venues/binance/rest/signed.rs",
        "the signing clock, which learns the venue's offset and stamps signed requests, not messages",
    ),
    (
        "src/venues/polymarket/preflight.rs",
        "one read at start to find the current market window, before any message exists",
    ),
    (
        "src/operator/workstation/browser/app.rs",
        "the display clock of the browser workstation, which runs in another process",
    ),
];

#[test]
fn one_engine_clock_stamps_every_producer() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut starts: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for path in rust_sources(&root.join("src")) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let relative = path
            .strip_prefix(root)
            .expect("each source sits below the package")
            .to_string_lossy()
            .into_owned();
        for (number, line) in source.lines().enumerate() {
            let code = line.split("//").next().unwrap_or(line);
            if code.contains(CLOCK_START) {
                starts.entry(relative.clone()).or_default().push(number + 1);
            }
        }
    }

    let mut problems = Vec::new();
    for (file, lines) in &starts {
        let is_listed = CLOCK_STARTS.iter().any(|(listed, _)| listed == file);
        match (is_listed, lines.as_slice()) {
            (true, [_]) => {}
            (true, _) => problems.push(format!("{file} starts {} clocks: {lines:?}", lines.len())),
            (false, _) => problems.push(format!("{file} starts a clock at lines {lines:?}")),
        }
    }
    for (listed, reason) in CLOCK_STARTS {
        if !starts.contains_key(listed) {
            problems.push(format!("{listed} no longer starts a clock ({reason})"));
        }
    }
    assert!(
        problems.is_empty(),
        "each producer of message stamps must read the one clock that the engine start builds and \
         passes on, or two time bases stamp the same moment apart. Pass that clock in, or name the \
         new site in the list with its reason:\n{}",
        problems.join("\n")
    );
}

const ISSUE_TICK: &str = "issue_tick";

/// Each file of the production code that names the issue tick, and what it does with it. The tick
/// is a latency stamp: a reader of it outside a sender could order or time a command by a counter
/// reading, and replay would then send other orders than the live run sent.
const ISSUE_TICK_SITES: [(&str, &str); 6] = [
    (
        "engine/src/msg/exec.rs",
        "declares the tick on the lane item that carries a command to its edge",
    ),
    (
        "engine/src/venues/edge/sender.rs",
        "copies the tick of each command it sends into that command's wire sample",
    ),
    (
        "engine/src/hot/sink/exec.rs",
        "reads the counter when a command enters the bank and hands the tick to the lane",
    ),
    (
        "engine/src/msg/latency.rs",
        "declares the tick on the wire sample that a sender writes",
    ),
    (
        "engine/src/runtime/assembly/metrics/stage.rs",
        "subtracts the ticks of a wire sample on the metrics thread",
    ),
    (
        "tools/latency-probe/sender.rs",
        "the timing harness stamps each command it puts on the lane, as the command sink does",
    ),
];

#[test]
fn issue_tick_is_read_only_by_senders() {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = package.parent().expect("the package sits in the workspace");
    let roots = ["engine/src", "strategies", "tools", "lab/src"];
    let mut naming: Vec<String> = Vec::new();
    for root in roots {
        for path in rust_sources(&workspace.join(root)) {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            let names_it = source
                .lines()
                .map(|line| line.split("//").next().unwrap_or(line))
                .any(|code| code.contains(ISSUE_TICK));
            if names_it {
                let relative = path
                    .strip_prefix(workspace)
                    .expect("each source sits below the workspace")
                    .to_string_lossy()
                    .into_owned();
                naming.push(relative);
            }
        }
    }
    naming.sort();
    let mut listed: Vec<String> = ISSUE_TICK_SITES
        .iter()
        .map(|(file, _)| (*file).to_owned())
        .collect();
    listed.sort();
    assert_eq!(
        naming, listed,
        "the issue tick of a command is a latency stamp for the sender alone. The fold, a strategy \
         and the simulator must never read it; a new file that does needs its reason in the list"
    );
}

/// Each function of the production code that may read a clock, with each banned call that it makes,
/// one entry for each call. Each one says why in the reason of its
/// `#[expect(clippy::disallowed_methods, ...)]`. A function that only reads keeps a read apart from
/// the logic beside it, so a read added to that logic fails the lint.
const PERMITTED_CLOCK_SITES: [(&str, &str, &[&str]); 115] = [
    (
        "engine/src/hot/metrics.rs",
        "stamp",
        &["engine::msg::time::Stamper::stamp"],
    ),
    (
        "engine/src/hot/sink/exec.rs",
        "issue_tick",
        &["engine::msg::time::Stamper::stamp"],
    ),
    (
        "engine/src/link/actor/mod.rs",
        "run",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/link/actor/mod.rs",
        "on_payload",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/log/drain.rs",
        "drain_once",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/log/drain.rs",
        "finish",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/log/drain.rs",
        "write_records",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/log/drain.rs",
        "write_drops",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/log/lane.rs",
        "engine_time",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::wall_clock_now",
        ],
    ),
    (
        "engine/src/msg/time.rs",
        "start",
        &["std::time::Instant::now", "std::time::SystemTime::now"],
    ),
    (
        "engine/src/msg/time.rs",
        "now",
        &["std::time::Instant::elapsed"],
    ),
    ("engine/src/msg/time.rs", "stamp", &["quanta::Clock::raw"]),
    (
        "engine/src/msg/time.rs",
        "wall_clock_now",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/operator/fleet/child.rs",
        "running",
        &["engine::msg::time::wall_clock_now"],
    ),
    (
        "engine/src/operator/fleet/supervisor.rs",
        "await_start_grace",
        &["std::time::Instant::now", "std::time::Instant::now"],
    ),
    (
        "engine/src/operator/fleet/supervisor.rs",
        "stop_together",
        &["std::time::Instant::now", "std::time::Instant::now"],
    ),
    (
        "engine/src/operator/fleet/supervisor.rs",
        "stopped",
        &["std::time::Instant::elapsed"],
    ),
    (
        "engine/src/operator/workstation/browser/app.rs",
        "new",
        &["engine::msg::time::EngineClock::start"],
    ),
    (
        "engine/src/operator/workstation/browser/app.rs",
        "pump",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/operator/workstation/browser/app.rs",
        "on_frame",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/operator/workstation/browser/app.rs",
        "act_on",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/runtime/assembly/link.rs",
        "link_bring_up",
        &["engine::msg::time::wall_clock_now"],
    ),
    (
        "engine/src/runtime/assembly/metrics/mod.rs",
        "take_anchor",
        &["engine::msg::time::TickScale::nanos"],
    ),
    (
        "engine/src/runtime/assembly/metrics/mod.rs",
        "read_anchor",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::Stamper::stamp",
            "engine::msg::time::Stamper::stamp",
        ],
    ),
    (
        "engine/src/runtime/assembly/metrics/mod.rs",
        "run",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/runtime/assembly/metrics/stage.rs",
        "stage_nanos",
        &["engine::msg::time::TickScale::nanos"],
    ),
    (
        "engine/src/runtime/assembly/metrics/stage.rs",
        "stamp_to_tick",
        &[
            "engine::msg::time::TickScale::nanos_since",
            "engine::msg::time::TickScale::nanos_since",
        ],
    ),
    (
        "engine/src/runtime/assembly/metrics/stage.rs",
        "wire_nanos",
        &[
            "engine::msg::time::TickScale::nanos",
            "engine::msg::time::TickScale::nanos",
            "engine::msg::time::TickScale::nanos",
        ],
    ),
    (
        "engine/src/runtime/assembly/mod.rs",
        "start",
        &[
            "engine::msg::time::EngineClock::start",
            "quanta::Clock::new",
        ],
    ),
    (
        "engine/src/runtime/assembly/timer.rs",
        "spawn_timer",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/runtime/exposure/writer.rs",
        "report",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/runtime/exposure/writer.rs",
        "document",
        &["engine::msg::time::wall_clock_now"],
    ),
    (
        "engine/src/tape/drain.rs",
        "spawn",
        &["engine::msg::time::wall_clock_now"],
    ),
    (
        "engine/src/tape/drain.rs",
        "pump",
        &[
            "std::time::Instant::elapsed",
            "std::time::Instant::now",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/tape/retention.rs",
        "prune",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/binance/actor/depth_resync.rs",
        "fetch_depth_snapshot",
        &["std::time::Instant::now", "std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/kline_backfill.rs",
        "fill_kline_gap",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/kline_backfill.rs",
        "backfill_klines",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/liveness.rs",
        "new",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/liveness.rs",
        "arm",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/liveness.rs",
        "note_message",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/liveness.rs",
        "socket_silence",
        &["std::time::Instant::elapsed"],
    ),
    (
        "engine/src/venues/binance/actor/liveness.rs",
        "warn_silent",
        &["std::time::Instant::elapsed"],
    ),
    (
        "engine/src/venues/binance/actor/mod.rs",
        "flush",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/actor/mod.rs",
        "on_rest_error",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/mod.rs",
        "session",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/binance/actor/tap.rs",
        "flush_tapped",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/actor/tap.rs",
        "read_rest",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/binance/exec/actor/apply.rs",
        "dispatch",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/binance/exec/actor/apply.rs",
        "feed",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/binance/exec/actor/apply.rs",
        "submit_over_rest",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/inbound.rs",
        "on_response",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/inbound.rs",
        "on_decoded",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/inbound.rs",
        "on_stream_frame",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/binance/exec/actor/mod.rs",
        "start",
        &["tokio::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/mod.rs",
        "serve",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "tokio::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/binance/exec/actor/poll.rs",
        "on_housekeeping",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/reconcile.rs",
        "on_account",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/reconcile.rs",
        "on_order_record",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/reconcile.rs",
        "decode_order",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/reconcile.rs",
        "on_trade_owner",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/reconcile.rs",
        "on_order_read_failure",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/resync.rs",
        "finish_resync",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/rest/signed.rs",
        "new",
        &["engine::msg::time::EngineClock::start"],
    ),
    (
        "engine/src/venues/binance/rest/signed.rs",
        "sync_clock",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/rest/signed.rs",
        "send_signed",
        &[
            "engine::msg::time::EngineClock::now",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/binance/rest/signed.rs",
        "wait_before_retry",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/rest/weight.rs",
        "new",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/rest/weight.rs",
        "charge",
        &["std::time::Instant::elapsed", "std::time::Instant::now"],
    ),
    (
        "engine/src/venues/edge/backoff.rs",
        "delay",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        "open_exit",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        "claim_sweep_retry",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        "next_attempt_at",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        "send",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        "run_edge",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        "settle_offline",
        &["tokio::time::Instant::now"],
    ),
    (
        "engine/src/venues/edge/sender.rs",
        "drain",
        &[
            "engine::msg::time::Stamper::stamp",
            "engine::msg::time::Stamper::stamp",
            "engine::msg::time::Stamper::stamp",
        ],
    ),
    (
        "engine/src/venues/polymarket/actor/effects.rs",
        "handle_effect",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/actor/effects.rs",
        "withhold_while_rest_quiet",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/polymarket/actor/mod.rs",
        "bootstrap",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/polymarket/actor/mod.rs",
        "prime_connection",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/actor/mod.rs",
        "on_socket_message",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/actor/mod.rs",
        "on_core_event",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/polymarket/actor/mod.rs",
        "session",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/answer.rs",
        "on_place_answer",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/answer.rs",
        "on_transport_failure",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/answer.rs",
        "decode_context",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "dispatch",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "send",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::now",
        ],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "subscribe",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "decide_send",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "cancel_hold",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "release_withheld_cancels",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "on_unavailable",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        "re_arm_after_sweep",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/inbound.rs",
        "on_text",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/inbound.rs",
        "abandon_expired_frames",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/mod.rs",
        "on_disconnected",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/poll.rs",
        "on_housekeeping",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/poll.rs",
        "sweep_expiring_windows",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/reads.rs",
        "on_balance",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/rest.rs",
        "venue_seconds",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/resync.rs",
        "adopted_event",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/resync.rs",
        "finish_readiness_if_clean",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/preflight.rs",
        "local_unix_secs",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/probe/mod.rs",
        "local_unix_secs",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/polymarket/preflight.rs",
        "preflight_poly",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::start",
        ],
    ),
    (
        "engine/src/venues/sim/actor.rs",
        "publish",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "new",
        &["quanta::Clock::new"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "read",
        &["quanta::Clock::raw"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "nanos",
        &["quanta::Clock::delta_as_nanos"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "start_engine_clock",
        &["engine::msg::time::EngineClock::start"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "now",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "stamp",
        &["engine::msg::time::Stamper::stamp"],
    ),
    (
        "tools/latency-probe/clock.rs",
        "counter_stamper",
        &["quanta::Clock::new"],
    ),
];

const PRODUCTION_ROOTS: [&str; 4] = ["engine/src", "strategies", "tools", "lab/src"];
const LINT: &str = "clippy::disallowed_methods";
const ENGINE_PATH: &str = "engine::";

/// Banned calls that take no argument. A method of another type with the same name takes one, so
/// the empty call is what tells them apart in the text.
const CALLS_WITHOUT_ARGUMENTS: [&str; 5] = ["now", "elapsed", "raw", "recent", "stamp"];

/// Banned names that other types use too. They count only where the code names their type.
const CALLS_BY_TYPE: [&str; 2] = ["new", "start"];

/// FITNESS: the clock lint bans each clock read, and the functions that may read one, with the
/// calls of each, are exactly these. A clock read anywhere else would make hot state a function of
/// how fast the process ran, and a replay of the tape would part from the live run.
///
/// The lint lets any read through inside a function that expects it, so the calls of each such
/// function are counted here against its row. A banned path that no longer resolves is only a
/// warning, and its ban is then gone, so each banned engine path must keep a site that calls it.
#[test]
fn permitted_clock_sites_equal_the_list() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the package sits in the workspace");
    let banned = banned_paths(workspace);
    let mut problems = Vec::new();
    let mut found = Vec::new();
    for root in PRODUCTION_ROOTS {
        for path in rust_sources(&workspace.join(root)) {
            let relative = relative_to(&path, workspace);
            found.extend(sites_of(&read(&path), &relative, &mut problems));
        }
    }
    for site in &found {
        let row = PERMITTED_CLOCK_SITES
            .iter()
            .find(|(file, function, _)| *file == site.file && *function == site.function);
        match row {
            Some((_, _, calls)) => problems.extend(site.unlisted_calls(calls, &banned)),
            None => problems.push(format!(
                "{}:{} `fn {}` may read a clock, but the list holds no row for it",
                site.file, site.attribute_line, site.function
            )),
        }
    }
    for (file, function, calls) in PERMITTED_CLOCK_SITES {
        if !found
            .iter()
            .any(|site| site.file == file && site.function == function)
        {
            problems.push(format!(
                "{file}: the list holds `fn {function}`, which no longer expects the clock lint"
            ));
        }
        for call in calls {
            if !banned.iter().any(|path| path == call) {
                problems.push(format!(
                    "{file} `fn {function}` lists {call}, which the lint does not ban"
                ));
            }
        }
    }
    for path in banned.iter().filter(|path| path.starts_with(ENGINE_PATH)) {
        let is_called = PERMITTED_CLOCK_SITES
            .iter()
            .any(|(_, _, calls)| calls.contains(&path.as_str()));
        if !is_called {
            problems.push(format!(
                "the lint bans {path}, but no permitted site calls it, so a rename of it would \
                 drop its ban with every gate green"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "the clock reads of the production code no longer match the list of permitted sites. A new \
         site needs a reason in its expect attribute and a row in PERMITTED_CLOCK_SITES:\n{}",
        problems.join("\n")
    );
}

/// One function that expects the clock lint.
struct Site {
    file: String,
    function: String,
    attribute_line: usize,
    first_line: usize,
    body: String,
}

/// One banned call in a body, with the type that the text names before `::`, if it names one.
struct Call {
    line: usize,
    owner: Option<String>,
}

impl Site {
    /// Each difference between the banned calls of the body and the row: a name called a different
    /// number of times, or a call that names a type that the row does not list for that name, such
    /// as `SystemTime::now` where the row lists `EngineClock::now`.
    fn unlisted_calls(&self, listed: &[&str], banned: &[String]) -> Vec<String> {
        let mut names: Vec<&str> = banned.iter().map(|path| last_segment(path)).collect();
        names.sort_unstable();
        names.dedup();
        let mut problems = Vec::new();
        for name in names {
            let owners: Vec<&str> = banned
                .iter()
                .filter(|path| last_segment(path) == name)
                .map(|path| owner_of(path))
                .collect();
            let calls = self.calls_of(name, &owners);
            let rows: Vec<&str> = listed
                .iter()
                .copied()
                .filter(|path| last_segment(path) == name)
                .collect();
            if calls.len() != rows.len() {
                let lines: Vec<usize> = calls.iter().map(|call| call.line).collect();
                problems.push(format!(
                    "{}:{} `fn {}` calls {name} {} times, at lines {lines:?}, and its row lists {}",
                    self.file,
                    self.attribute_line,
                    self.function,
                    calls.len(),
                    rows.len()
                ));
            }
            let mut typed: Vec<&str> = calls
                .iter()
                .filter_map(|call| call.owner.as_deref())
                .collect();
            typed.sort_unstable();
            typed.dedup();
            for owner in typed {
                let lines: Vec<usize> = calls
                    .iter()
                    .filter(|call| call.owner.as_deref() == Some(owner))
                    .map(|call| call.line)
                    .collect();
                let allowed = rows.iter().filter(|path| owner_of(path) == owner).count();
                if lines.len() > allowed {
                    problems.push(format!(
                        "{}:{} `fn {}` calls {owner}::{name} at lines {lines:?}, but its row lists \
                         {allowed} such call: {rows:?}",
                        self.file, lines[0], self.function
                    ));
                }
            }
        }
        problems
    }

    /// The calls of `name` in the body. A call written with its type (`SystemTime::now()`) keeps
    /// the type. A method call (`clock.now()`) names no type, so only its name counts.
    fn calls_of(&self, name: &str, owners: &[&str]) -> Vec<Call> {
        let mut calls: Vec<(usize, Option<String>)> = Vec::new();
        if name == FREE_CALL {
            for (at, _) in self.body.match_indices(name) {
                if is_free_call(&self.body, at, name) {
                    calls.push((at, None));
                }
            }
        } else {
            let empty = CALLS_WITHOUT_ARGUMENTS.contains(&name);
            let suffix = if empty { "()" } else { "(" };
            for (at, _) in self.body.match_indices(&format!("::{name}{suffix}")) {
                let owner = owner_before(&self.body, at);
                if CALLS_BY_TYPE.contains(&name) && !owners.contains(&owner.as_str()) {
                    continue;
                }
                calls.push((at, Some(owner)));
            }
            if !CALLS_BY_TYPE.contains(&name) {
                for (at, _) in self.body.match_indices(&format!(".{name}{suffix}")) {
                    calls.push((at, None));
                }
            }
        }
        calls.sort_by_key(|(at, _)| *at);
        calls
            .into_iter()
            .map(|(at, owner)| Call {
                line: self.first_line + self.body[..at].matches('\n').count(),
                owner,
            })
            .collect()
    }
}

/// The one banned free function. It counts as a word wherever it stands, also passed by name, but
/// never in its own `fn` line.
const FREE_CALL: &str = "wall_clock_now";

fn is_free_call(body: &str, at: usize, name: &str) -> bool {
    let before = body[..at].chars().next_back();
    let after = body[at + name.len()..].chars().next();
    !before.is_some_and(is_ident)
        && !after.is_some_and(is_ident)
        && !body[..at].trim_end().ends_with("fn")
}

/// The identifier that ends where `::` starts at `at`.
fn owner_before(body: &str, at: usize) -> String {
    let start = body[..at]
        .char_indices()
        .rev()
        .take_while(|&(_, character)| is_ident(character))
        .last()
        .map_or(at, |(index, _)| index);
    body[start..at].to_owned()
}

fn is_ident(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn last_segment(path: &str) -> &str {
    path.rsplit("::").next().expect("a path has a last segment")
}

fn owner_of(path: &str) -> &str {
    path.rsplit("::").nth(1).unwrap_or_default()
}

/// The clock lint, the clippy groups that hold it, and the group of every warning: an `allow` or an
/// `expect` of any of them lifts the clock lint where it stands.
const LINT_COVERS: [&str; 4] = [LINT, "clippy::style", "clippy::all", "warnings"];

/// Each function of a source that expects the clock lint. Every attribute is read, inner and outer,
/// on any item. One that allows or expects the lint, or a group that holds it, is a problem unless
/// it is a function's own outer expectation of the lint alone: only that reports a site that has
/// stopped reading a clock, and only a listed function may carry it.
fn sites_of(source: &str, file: &str, problems: &mut Vec<String>) -> Vec<Site> {
    let code = without_comments_and_literals(source);
    let source_lines: Vec<&str> = source.lines().collect();
    let code_lines: Vec<&str> = code.lines().collect();
    let mut openings: Vec<usize> = code
        .match_indices("#[")
        .chain(code.match_indices("#!["))
        .map(|(at, _)| at)
        .collect();
    openings.sort_unstable();
    let mut sites = Vec::new();
    for at in openings {
        let attribute = &code[at..attribute_close(&code, at)];
        let covers: Vec<&str> = LINT_COVERS
            .iter()
            .copied()
            .filter(|name| names_lint(attribute, name))
            .collect();
        let lifts_a_level = attribute.contains("allow(") || attribute.contains("expect(");
        if covers.is_empty() || !lifts_a_level {
            continue;
        }
        let line = code[..at].matches('\n').count();
        let is_inner = code[at..].starts_with("#![");
        let is_site = !is_inner
            && attribute.contains("expect(")
            && !attribute.contains("allow(")
            && covers == [LINT];
        if !is_site {
            problems.push(format!(
                "{file}:{} `{}` lifts the clock lint here: only a listed function's own \
                 #[expect(clippy::disallowed_methods)] may",
                line + 1,
                source_lines[line].trim()
            ));
            continue;
        }
        let end_line = line + attribute.matches('\n').count();
        match function_after(&code_lines, end_line + 1) {
            Some(first) => {
                let last = function_end(&code_lines, first);
                sites.push(Site {
                    file: file.to_owned(),
                    function: function_name(code_lines[first]),
                    attribute_line: line + 1,
                    first_line: first + 1,
                    body: code_lines[first..=last].join("\n"),
                });
            }
            None => problems.push(format!(
                "{file}:{} expects the clock lint on something that is not a function",
                line + 1
            )),
        }
    }
    sites
}

/// The byte just past the `]` that closes the attribute opened at `at`.
fn attribute_close(code: &str, at: usize) -> usize {
    let mut depth = 0i32;
    for (offset, character) in code[at..].char_indices() {
        match character {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return at + offset + 1;
                }
            }
            _ => {}
        }
    }
    code.len()
}

/// `name` as a whole lint path inside the attribute, not a piece of a longer one.
fn names_lint(attribute: &str, name: &str) -> bool {
    attribute.match_indices(name).any(|(at, _)| {
        let before = attribute[..at].chars().next_back();
        let after = attribute[at + name.len()..].chars().next();
        let is_path = |character: char| is_ident(character) || character == ':';
        !before.is_some_and(is_path) && !after.is_some_and(is_path)
    })
}

/// The line that closes the attribute opened at `start`.
fn attribute_end(code_lines: &[&str], start: usize) -> usize {
    let mut depth = 0i32;
    for (offset, line) in code_lines[start..].iter().enumerate() {
        depth += line.matches('[').count() as i32 - line.matches(']').count() as i32;
        if depth <= 0 {
            return start + offset;
        }
    }
    code_lines.len() - 1
}

/// The `fn` line that the attributes from `start` decorate, past other attributes and blank lines.
fn function_after(code_lines: &[&str], start: usize) -> Option<usize> {
    let mut index = start;
    while index < code_lines.len() {
        let line = code_lines[index].trim_start();
        if is_function_line(line) {
            return Some(index);
        }
        if line.starts_with("#[") {
            index = attribute_end(code_lines, index) + 1;
            continue;
        }
        if !line.is_empty() {
            return None;
        }
        index += 1;
    }
    None
}

fn is_function_line(line: &str) -> bool {
    let mut rest = line;
    for qualifier in [
        "pub(crate) ",
        "pub(super) ",
        "pub ",
        "const ",
        "async ",
        "unsafe ",
    ] {
        rest = rest.strip_prefix(qualifier).unwrap_or(rest);
    }
    rest.starts_with("fn ")
}

fn function_name(line: &str) -> String {
    let (_, rest) = line
        .split_once("fn ")
        .expect("a function line names its function");
    rest.chars()
        .take_while(|character| character.is_alphanumeric() || *character == '_')
        .collect()
}

/// A function on one line ends there. Else it ends at the first `}` at its own indent.
fn function_end(code_lines: &[&str], first: usize) -> usize {
    let line = code_lines[first];
    if line.contains('{') && line.trim_end().ends_with('}') {
        return first;
    }
    let indent = line.len() - line.trim_start().len();
    let closing = format!("{}}}", " ".repeat(indent));
    (first + 1..code_lines.len())
        .find(|&index| code_lines[index].trim_end() == closing)
        .unwrap_or(code_lines.len() - 1)
}

fn banned_paths(workspace: &Path) -> Vec<String> {
    read(&workspace.join("clippy.toml"))
        .lines()
        .filter_map(|line| line.split_once("path = \"")?.1.split_once('"'))
        .map(|(path, _)| path.to_owned())
        .collect()
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

fn relative_to(path: &Path, workspace: &Path) -> String {
    path.strip_prefix(workspace)
        .expect("each source sits below the workspace")
        .to_string_lossy()
        .into_owned()
}
