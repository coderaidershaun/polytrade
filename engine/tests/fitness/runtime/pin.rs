//! A thread that must pin to its core and cannot refuses the start before anything runs on it.
//! Only linux refuses: elsewhere pinning is best-effort, so no pin can fail there.

/// The last core a cpu set can name. No test host has it, and a higher id does not fit the set.
#[cfg(target_os = "linux")]
const UNUSABLE_CORE: usize = 1_023;

#[cfg(target_os = "linux")]
#[test]
fn sender_pin_failure_refuses_start() {
    use engine::runtime::{EngineError, PinnedSenders};

    let refusal = PinnedSenders::spawn(Some(UNUSABLE_CORE)).err();
    assert!(
        matches!(refusal, Some(EngineError::SenderPin { core_id }) if core_id == UNUSABLE_CORE),
        "a sender thread that cannot pin refuses the start and names its core: {refusal:?}"
    );
}
