//! The execution lease's host lock and on-disk nonce history must be keyed correctly. A wrong key
//! lets a second process arm the same account, or re-mint client order ids a venue has already seen.

use std::sync::atomic::{AtomicU64, Ordering};

use polysim::adapters::binance::exec as binance_exec;
use polysim::adapters::exchange_sim;
use polysim::adapters::exec::TeTag;
use polysim::adapters::polymarket::exec as polymarket_exec;
use polysim::config::{BinanceEnv, RunIdentity};
use polysim::runtime::{EngineError, ExecutionLease};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
static NEXT_TE: AtomicU64 = AtomicU64::new(0);

fn fresh_directory() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "polysim-exec-lease-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Avoids collisions with concurrent test processes.
fn fresh_te_tag() -> TeTag {
    let identity = RunIdentity::new(
        "strategy",
        &format!(
            "engine-{}-{}",
            std::process::id(),
            NEXT_TE.fetch_add(1, Ordering::Relaxed)
        ),
    )
    .expect("generated ids are well formed");
    TeTag::of(&identity)
}

fn account(label: &str) -> Vec<u8> {
    format!("{label}-{}", std::process::id()).into_bytes()
}

/// Restates how an account is hashed into a file name, so the shipped-names pin below fails on a
/// change to either the hash or the truncation.
fn fingerprint(credential: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, credential)
        .as_ref()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The nonce file name is a permanent on-disk contract, not a formatting choice: a run that finds
/// no history under its own name starts again at nonce 1 and re-mints client order ids the venue has
/// already seen. The namespaces come from the venue modules a live run uses, so renaming one there
/// fails here; only the expected names are spelled out, and a rename has to be argued for twice.
#[test]
fn each_venue_keeps_the_nonce_file_name_it_shipped_with() {
    let directory = fresh_directory();
    let te_tag = fresh_te_tag();
    let te = te_tag.get();
    let api_key = account("shipped-names");
    let signer = "0x0d09aEC2D10F396fB59482644708CBd353798b87";

    for (namespace, expected) in [
        (
            binance_exec::lease_namespace(BinanceEnv::Production, &api_key),
            format!(".exec-production-{te:08x}-{}.nonce", fingerprint(&api_key)),
        ),
        (
            binance_exec::lease_namespace(BinanceEnv::Testnet, &api_key),
            format!(".exec-testnet-{te:08x}-{}.nonce", fingerprint(&api_key)),
        ),
        (
            polymarket_exec::lease_namespace(signer),
            format!(
                ".exec-poly-{te:08x}-{}.nonce",
                fingerprint(signer.as_bytes())
            ),
        ),
        (
            exchange_sim::lease_namespace(),
            format!(".exec-sim-{te:08x}.nonce"),
        ),
    ] {
        // Released before the next acquisition: one host lock covers all four.
        drop(
            ExecutionLease::acquire(&directory, te_tag, &namespace)
                .expect("each namespace acquires in turn"),
        );
        assert!(
            directory.join(&expected).exists(),
            "expected the nonce history at {expected}, found {:?}",
            std::fs::read_dir(&directory)
                .expect("the lease created the directory")
                .filter_map(|entry| Some(entry.ok()?.file_name()))
                .collect::<Vec<_>>()
        );
    }
}

/// FITNESS: the host lock is scoped to the TE identity alone, and the nonce history to whatever axis
/// actually distinguishes one credential's traffic from another's — never to the state directory,
/// which is a deployment detail with no bearing on which venue account is armed.
///
/// The three go together deliberately: each names a different thing the lock must NOT be keyed by.
/// Two credentials on the same venue under one TE identity still contend for the one host lock, and
/// each starts its own nonce history rather than inheriting the other's. Two different venues under
/// one TE identity contend the same way — the lock is keyed by TE identity alone, so it is the same
/// lock whichever venue a run trades, while the nonce namespace is keyed by venue and account, so
/// sharing it would mint client order ids under a nonce the other venue already used. And a different
/// state directory must not be a way to evade the host lock entirely — the lease is about which
/// PROCESS is armed, not where it happens to keep its files.
#[test]
fn the_host_lock_and_nonce_history_are_scoped_by_credential_and_venue_never_by_directory() {
    let directory = fresh_directory();
    let te_tag = fresh_te_tag();
    let account_a = ExecutionLease::acquire(
        &directory,
        te_tag,
        &binance_exec::lease_namespace(BinanceEnv::Testnet, &account("distinct-a")),
    )
    .expect("first account acquires");
    assert_eq!(account_a.run_nonce(), 1);

    let contended = ExecutionLease::acquire(
        &directory,
        te_tag,
        &binance_exec::lease_namespace(BinanceEnv::Testnet, &account("distinct-b")),
    )
    .err()
    .expect("a second credential under one TE identity is still a second armed process");
    assert!(matches!(
        contended,
        EngineError::ExecutionIdentityInUse { .. }
    ));

    drop(account_a);
    let account_b = ExecutionLease::acquire(
        &directory,
        te_tag,
        &binance_exec::lease_namespace(BinanceEnv::Testnet, &account("distinct-b")),
    )
    .expect("the second account acquires once the first has released");
    assert_eq!(
        account_b.run_nonce(),
        1,
        "a credential that has never run starts its own nonce history, not the other account's"
    );
    drop(account_b);

    let signer = "0x0d09aEC2D10F396fB59482644708CBd353798b87";
    let binance = ExecutionLease::acquire(
        &directory,
        te_tag,
        &binance_exec::lease_namespace(BinanceEnv::Testnet, &account("two-venues")),
    )
    .expect("the binance edge acquires");
    assert_eq!(binance.run_nonce(), 1);

    let contended = ExecutionLease::acquire(
        &directory,
        te_tag,
        &polymarket_exec::lease_namespace(signer),
    )
    .err()
    .expect("one TE identity is one armed process per host, whichever venue it trades");
    assert!(matches!(
        contended,
        EngineError::ExecutionIdentityInUse { .. }
    ));

    drop(binance);
    let polymarket = ExecutionLease::acquire(
        &directory,
        te_tag,
        &polymarket_exec::lease_namespace(signer),
    )
    .expect("the polymarket edge acquires once the binance one released");
    assert_eq!(
        polymarket.run_nonce(),
        1,
        "a venue that has never run starts its own nonce history — sharing one would mint client order ids under a nonce the other venue already used"
    );

    drop(polymarket);
    let restarted = ExecutionLease::acquire(
        &directory,
        te_tag,
        &polymarket_exec::lease_namespace(signer),
    )
    .expect("the next polymarket process acquires");
    assert_eq!(restarted.run_nonce(), 2);
    drop(restarted);

    let first_directory = fresh_directory();
    let second_directory = fresh_directory();
    let evasion_tag = fresh_te_tag();
    let api_key = account("directory-evasion");
    let namespace = binance_exec::lease_namespace(BinanceEnv::Testnet, &api_key);
    let first = ExecutionLease::acquire(&first_directory, evasion_tag, &namespace)
        .expect("first configuration acquires");
    let second = ExecutionLease::acquire(&second_directory, evasion_tag, &namespace)
        .err()
        .expect("a different state directory cannot evade the host lock");
    assert!(matches!(second, EngineError::ExecutionIdentityInUse { .. }));
    drop(first);
}
