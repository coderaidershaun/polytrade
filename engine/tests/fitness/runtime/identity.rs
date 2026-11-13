//! The run nonce file of each venue and account, and the host lock of each engine identity. A wrong
//! name or a wrong key lets a run mint client order ids that a venue has already seen.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use engine::msg::failure::{FailureClass, Halt};
use engine::runtime::config::{BinanceEnv, Config, NoParams, RunIdentity};
use engine::runtime::{ExecutionLease, Preflight, StartError, StartPlan};
use engine::venues::binance::exec as binance_exec;
use engine::venues::exec::{LeaseNamespace, TeTag};
use engine::venues::polymarket::exec as polymarket_exec;
use engine::venues::sim;

use super::preflight::{Scales, StandInVenue, sim_document};
use crate::engine_support::Idle;
use crate::parquet_readback::TempDir;

const STRATEGY: &str = "strat-identity";
const SIGNER: &str = "0x0d09aEC2D10F396fB59482644708CBd353798b87";

/// Each test of this module has its own engine identity, so tests that run at the same time never
/// share a host lock.
fn identity(te_id: &str) -> RunIdentity {
    RunIdentity::new(STRATEGY, te_id).expect("a valid identity")
}

fn te_tag(te_id: &str) -> TeTag {
    TeTag::of(&identity(te_id))
}

/// The run nonce that one start gets. The lease is released again before this returns.
fn run_nonce(state: &Path, tag: TeTag, namespace: &LeaseNamespace<'_>) -> u32 {
    ExecutionLease::acquire(state, tag, namespace)
        .expect("no other lease holds this engine identity")
        .run_nonce()
}

fn is_refused_as_held(state: &Path, tag: TeTag, namespace: &LeaseNamespace<'_>) -> bool {
    matches!(
        ExecutionLease::acquire(state, tag, namespace),
        Err(StartError::ExecutionIdentityInUse { .. })
    )
}

/// Preflight of a simulated engine whose state folder is `state`. The venue stand-in passes each
/// check before the lease.
fn preflight(state: &Path) -> Result<StartPlan, StartError> {
    let config = Config::<NoParams>::from_yaml(&sim_document(state, ""))
        .expect("the document passes config");
    Preflight {
        identity: &identity("te-empty-nonce"),
        config: &config,
        strategy: &Idle,
    }
    .run_with(&mut StandInVenue {
        scales: Scales::Stamped {
            max_num_orders: 200,
        },
    })
}

fn file_names(state: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(state)
        .expect("the lease made the state folder")
        .map(|entry| {
            entry
                .expect("a readable folder entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// FITNESS: the nonce file name is a promise to the next run on this host, not a format. A run that
/// finds no history under its own name starts again at nonce 1 and mints client order ids that the
/// venue has seen. The names are the ones that the code of commit `e4f3f0d` wrote for this engine
/// identity and these accounts, and they must never change.
///
/// The keys go with the names. The host lock is the engine identity alone: a second account, a
/// second venue, or another state folder is still a second armed process. The nonce history is the
/// venue and the account: a history that two of them shared would mint ids under a nonce that the
/// other already spent.
#[test]
fn nonce_file_names_and_keys_are_exact() {
    let root = TempDir::new("nonce-names");
    let state = root.path();
    let tag = te_tag("te-nonce-names");
    let production = binance_exec::lease_namespace(BinanceEnv::Production, b"account-a");
    let testnet = binance_exec::lease_namespace(BinanceEnv::Testnet, b"account-a");
    let second_account = binance_exec::lease_namespace(BinanceEnv::Testnet, b"account-b");
    let polymarket = polymarket_exec::lease_namespace(SIGNER);
    let simulated = sim::lease_namespace();

    let named = [
        (
            &production,
            ".exec-production-501f07ee-fc164f8250803ea8.nonce",
        ),
        (&testnet, ".exec-testnet-501f07ee-fc164f8250803ea8.nonce"),
        (&polymarket, ".exec-poly-501f07ee-0f8fa6fb1d5a0539.nonce"),
        (&simulated, ".exec-sim-501f07ee.nonce"),
    ];
    for (namespace, name) in named {
        assert_eq!(
            run_nonce(state, tag, namespace),
            1,
            "{name} starts a history of its own"
        );
        assert_eq!(
            fs::read_to_string(state.join(name)).expect("the lease wrote its nonce file"),
            "1\n"
        );
    }
    let mut expected: Vec<String> = named.iter().map(|(_, name)| (*name).to_owned()).collect();
    expected.sort();
    assert_eq!(file_names(state), expected);

    let held = ExecutionLease::acquire(state, tag, &testnet).expect("the lease is free");
    assert_eq!(held.run_nonce(), 2, "a restart advances its own history");
    assert!(
        is_refused_as_held(state, tag, &second_account),
        "a second account under one engine identity is a second armed process"
    );
    assert!(
        is_refused_as_held(state, tag, &polymarket),
        "one engine identity is one armed process on a host, whichever venue it trades"
    );
    let elsewhere = TempDir::new("nonce-names-elsewhere");
    assert!(
        is_refused_as_held(elsewhere.path(), tag, &testnet),
        "another state folder cannot evade the host lock"
    );
    drop(held);

    assert_eq!(
        run_nonce(state, tag, &second_account),
        1,
        "an account that never ran starts its own history, not the history of the other account"
    );
    assert_eq!(run_nonce(state, tag, &polymarket), 2);
    assert_eq!(run_nonce(state, tag, &testnet), 3);
}

/// FITNESS: a run that stopped while it wrote its nonce file must not let the next run count from 0
/// again. That run would mint client order ids that the venue has already seen, and the answers for
/// an order of the earlier run would land on an order of this one. So an empty file refuses the
/// start and stays as it is, for a person to fill in. A missing file is a first run, and each new
/// nonce replaces the file by a rename, so a stop in the middle leaves the old or the new nonce.
#[test]
fn empty_nonce_file_refuses_start() {
    let root = TempDir::new("empty-nonce");
    let state = root.path().join("state");
    let nonce = state.join(".exec-sim-0ad70317.nonce");
    fs::create_dir_all(&state).expect("the test makes the state folder");
    fs::write(&nonce, "").expect("the test writes an empty nonce file");

    let refused = preflight(&state)
        .err()
        .expect("an empty nonce file refuses the start");
    let StartError::ExecutionNonceEmpty { path } = &refused else {
        panic!("an empty nonce file refuses with its own variant, got {refused}");
    };
    assert_eq!(path, &nonce);
    assert!(
        refused.to_string().contains(&nonce.display().to_string()),
        "the refusal names the file: {refused}"
    );
    assert_eq!(refused.failure_class(), FailureClass::Halt(Halt::Start));
    assert_eq!(
        fs::read(&nonce).expect("the nonce file stays").len(),
        0,
        "a refused start leaves the file as it found it"
    );

    fs::remove_file(&nonce).expect("the test removes the nonce file");
    drop(preflight(&state).expect("no nonce file is the first run"));
    assert_eq!(
        fs::read_to_string(&nonce).expect("the first run wrote its nonce"),
        "1\n"
    );
    assert_eq!(
        file_names(&state),
        [".exec-sim-0ad70317.nonce"],
        "no temporary file stays"
    );

    fs::write(&nonce, "41\n").expect("the test writes the last nonce");
    let replaced = fs::metadata(&nonce).expect("the nonce file exists").ino();
    drop(preflight(&state).expect("the run after nonce 41 starts"));
    assert_eq!(
        fs::read_to_string(&nonce).expect("the run wrote its nonce"),
        "42\n"
    );
    assert_ne!(
        fs::metadata(&nonce).expect("the nonce file exists").ino(),
        replaced,
        "the new nonce replaces the file by a rename, never in place"
    );
}
