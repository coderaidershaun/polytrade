//! Shutdown exit-status truth table: a fatal trip — the trigger OR one that occurs while the
//! queues drain (a hot-thread panic caught by the panic hook) — must never be reported as a
//! graceful (exit-0) shutdown, and a fatal dominates a drain error. `decide_exit` is the pure
//! decision `run_until_shutdown` wires through; one case per row of its truth table. It sees a
//! graceful trigger only as a name, so the last case passes a name that is not a signal's.

use polysim::runtime::decide_exit;

struct Case {
    signal: Option<&'static str>,
    fatal: Option<&'static str>,
    drain_error: Option<&'static str>,
    graceful: bool,
    reason: &'static str,
}

#[test]
fn fatal_is_never_reported_graceful() {
    let cases = [
        Case {
            signal: Some("SIGTERM"),
            fatal: None,
            drain_error: None,
            graceful: true,
            reason: "received SIGTERM",
        },
        // SIGTERM triggered the shutdown, but the hot thread panicked while draining.
        Case {
            signal: Some("SIGTERM"),
            fatal: Some("panic at book.rs:42: crossed"),
            drain_error: None,
            graceful: false,
            reason: "panic at book.rs:42: crossed",
        },
        Case {
            signal: None,
            fatal: Some("input queue 0 full"),
            drain_error: None,
            graceful: false,
            reason: "input queue 0 full",
        },
        Case {
            signal: Some("SIGINT"),
            fatal: None,
            drain_error: Some("persistence drain failed: disk full"),
            graceful: false,
            reason: "persistence drain failed: disk full",
        },
        // A fatal dominates a concurrent drain error.
        Case {
            signal: None,
            fatal: Some("panic X"),
            drain_error: Some("persistence drain failed: Y"),
            graceful: false,
            reason: "panic X",
        },
        // A trigger reaches `decide_exit` as its name and nothing else, so any name is reported
        // graceful, not only a signal's. The fatal and drain-error rows above already cover what
        // happens when one lands on top of it.
        Case {
            signal: Some("shutdown request"),
            fatal: None,
            drain_error: None,
            graceful: true,
            reason: "received shutdown request",
        },
    ];

    for case in cases {
        let report = decide_exit(
            case.signal,
            case.fatal.map(Into::into),
            case.drain_error.map(Into::into),
        );
        assert_eq!(
            report.graceful, case.graceful,
            "graceful mismatch for signal={:?} fatal={:?} drain={:?}",
            case.signal, case.fatal, case.drain_error
        );
        assert_eq!(report.reason.as_ref(), case.reason);
    }
}
