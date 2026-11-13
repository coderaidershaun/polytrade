//! The start checks start no thread: preflight and its venue reads name no thread spawn, except the
//! one current-thread runtime of `NetworkPort`, which ends with it.

use std::path::Path;

use super::log_lane_ownership::without_comments_and_literals;

const CHECKED: [&str; 2] = ["src/runtime/assembly/preflight.rs", "src/venues/port.rs"];

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
