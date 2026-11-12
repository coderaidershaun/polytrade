//! FITNESS: no clock reads in the execution engine, the shared edge machinery it drives, or the
//! simulated venue that answers it.
//!
//! Hot state must be a pure function of the ordered input sequence, which is what lets a recorded
//! tape replay to identical state — the basis of every fitness fixture and of backtesting. The
//! execution engine is where that is easiest to break, because it is full of DEADLINES: an in-flight
//! command times out, a closed record is reaped, a book goes stale, a silent stream is detected.
//! Every one of those is currently a difference of two message stamps, and every one of them reads
//! more naturally as "how long since now". A single `Instant::now()` would make hot state a function
//! of how fast the process happened to run, and the divergence would appear as a replay that
//! cancelled one order more or fewer — not as a failure anyone could attribute.
//!
//! The same reasoning reaches two neighbours. The shared edge machinery takes the current time as a
//! PARAMETER everywhere, so its answers stay reproducible from the message stream; a clock read
//! inside it would quietly outrank the argument its callers pass. The simulated venue synthesises
//! exchange behaviour instead of reaching one, and the only reason that is safe is that its output
//! is ordinary inbound traffic — reading a clock to decide what to emit would relocate the
//! nondeterminism into the venue rather than remove it, and a replayed tape would fill differently.
//!
//! Actor directories are deliberately out of scope, and so is `src/adapters/edge.rs`. An actor
//! legitimately reads the engine clock to stamp when it queued a message, and the session chassis in
//! that file owns a clock rather than taking one as a parameter, because pacing a reconnect and
//! measuring how long an edge has been blind are wall-clock questions no message can answer. A
//! word-scan over either would flag correct code by design.
//!
//! A source scan rather than a type-level ban because the ban has to cover code that does not exist
//! yet, which no signature can. `cargo_bins.rs` is the precedent for reading the repo as data.

use std::path::Path;

/// Spellings that reach a clock. `now` alone covers `Instant::now`, `SystemTime::now`,
/// `EngineClock::now` and any method a future type calls the same thing, which is the point: the
/// scan is over the WORD, so a new clock type is caught before it is named here.
pub const CLOCK_SPELLINGS: [&str; 5] =
    ["Instant", "SystemTime", "EngineClock", "now()", "elapsed()"];

/// Each directory carries the fewest rust files it can honestly hold, so a rename that leaves the
/// old path present but empty fails here instead of passing a scan of nothing.
const SCANNED: [(&str, usize); 4] = [
    ("src/hot/exec", 10),
    ("src/adapters/exec", 6),
    ("src/adapters/exchange_sim/core", 8),
    ("src/adapters/exchange_sim/driver", 3),
];

#[test]
fn the_execution_engine_reads_no_clock() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offences = Vec::new();
    for (relative, floor) in SCANNED {
        let sources = rust_sources(&root.join(relative));
        assert!(
            sources.len() >= floor,
            "{} rust files under {relative} — the scan is reading the wrong directory",
            sources.len()
        );

        for path in &sources {
            let source = std::fs::read_to_string(path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            for (number, line) in source.lines().enumerate() {
                let code = line.split("//").next().unwrap_or(line);
                for spelling in CLOCK_SPELLINGS {
                    if code.contains(spelling) {
                        let name = path.file_name().unwrap_or_default().to_string_lossy();
                        offences.push(format!("{relative}/{name}:{} reads {spelling}", number + 1));
                    }
                }
            }
        }
    }
    assert!(
        offences.is_empty(),
        "execution code reads a clock, so hot state is no longer a pure function of the message \
         sequence and replaying a recorded tape will diverge from the live run — every deadline in \
         the engine must be a difference of two message stamps, the shared edge machinery must take \
         the time it needs from its caller, and the simulated venue must decide what to emit from \
         the stamps on its input alone: {offences:?}"
    );
}

pub fn rust_sources(directory: &Path) -> Vec<std::path::PathBuf> {
    let entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", directory.display()));
    let mut sources = Vec::new();
    for path in entries.filter_map(|entry| entry.ok().map(|entry| entry.path())) {
        match path.is_dir() {
            true => sources.extend(rust_sources(&path)),
            false if path.extension().is_some_and(|extension| extension == "rs") => {
                sources.push(path);
            }
            false => {}
        }
    }
    sources.sort();
    sources
}
