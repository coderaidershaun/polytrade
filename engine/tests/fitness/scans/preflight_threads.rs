//! The start checks start no thread: preflight and its venue reads name no thread spawn, except the
//! one current-thread runtime of `NetworkPort`, which ends with it. They also write no log line,
//! because the log starts after them: what a venue read reports, the start logs once the log runs.

use std::path::Path;

use super::log_lane_ownership::without_comments_and_literals;

const CHECKED: [&str; 3] = [
    "src/runtime/assembly/preflight.rs",
    "src/runtime/otlp/plan.rs",
    "src/venues/port.rs",
];

const SPAWNS: [&str; 7] = [
    "thread::Builder",
    "thread::spawn",
    "spawn_pinned",
    "WriterThread::spawn",
    "tokio::spawn",
    "spawn_blocking",
    "new_multi_thread",
];

#[test]
fn preflight_names_no_thread_but_its_one_runtime() {
    let engine = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut runtimes = 0;
    for file in CHECKED {
        let source = std::fs::read_to_string(engine.join(file)).expect("the checked file reads");
        let code = without_comments_and_literals(&source);
        for spawn in SPAWNS {
            assert!(
                !code.contains(spawn),
                "{file} names {spawn}: a start check must start no thread"
            );
        }
        runtimes += code.matches("new_current_thread").count();
    }
    assert_eq!(
        runtimes, 1,
        "the network port builds one current-thread runtime for the venue reads, and no other"
    );
}

/// The files whose code runs only inside the venue reads of preflight.
const VENUE_READS: [&str; 5] = [
    "src/venues/port.rs",
    "src/venues/binance/preflight.rs",
    "src/venues/binance/exec/probe.rs",
    "src/venues/polymarket/preflight.rs",
    "src/venues/polymarket/exec/preflight.rs",
];

const LOG_MACROS: [&str; 4] = ["info!", "warn!", "error!", "warn_repeating!"];

/// CONTRACT: a venue read of preflight, the permission probe of the venue that the run trades on
/// among them, writes its report into the plan, and the start writes that report once the log
/// runs. A line logged during preflight is lost, since no log exists yet.
#[test]
fn venue_reads_of_preflight_write_no_log_line() {
    let engine = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut lost = Vec::new();
    for file in VENUE_READS {
        let source = std::fs::read_to_string(engine.join(file)).expect("the checked file reads");
        let code = without_comments_and_literals(&source);
        for (number, line) in code.lines().enumerate() {
            let words = line.split(|character: char| {
                !(character.is_alphanumeric() || character == '_' || character == '!')
            });
            if let Some(name) = words.into_iter().find(|word| LOG_MACROS.contains(word)) {
                lost.push(format!("{file}:{} logs with {name}", number + 1));
            }
        }
    }
    assert!(
        lost.is_empty(),
        "preflight runs before the log starts, so each of these lines is lost. Write it into the \
         report of the venue read instead:\n{}",
        lost.join("\n")
    );
}
