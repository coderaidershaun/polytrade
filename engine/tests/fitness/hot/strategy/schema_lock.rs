//! Each strategy's recorded columns and link schema stay what its `schema.lock` says, because a file
//! on disk names a column by its position and a renamed or moved column relabels data with no error.

use std::fs;
use std::path::{Path, PathBuf};

use engine::hot::strategy::{FeatureId, Strategy, StrategyConfig};
use engine::link::schema_hash_of_fields;

use crate::engine_support::{NOMINAL_SPIN, engine_view, recorder_spec};
use crate::micro_strategy::MicroRecorder;
use crate::micro_strategy::features::{Features, QuoteSide};
use crate::poly_strategy::PolyUpPublisher;

const LOCK_FILE: &str = "schema.lock";

/// The recorder records each link field it receives as a column of its own, and this is where that
/// block starts.
const RECORDER_LINK_COLUMNS_START: usize = 57;

/// The claims a strategy makes beyond its lock, given its columns, its link fields and its lock.
type OwnClaims = fn(&[&str], &[&str], &SchemaLock);

/// One shipped strategy: its folder below `strategies/`, the strategy as the engine builds it, and
/// its own claims, if it has any.
struct Shipped {
    folder: &'static str,
    strategy: Box<dyn Strategy>,
    own_claims: Option<OwnClaims>,
}

fn shipped_strategies() -> Vec<Shipped> {
    let engine = engine_view(NOMINAL_SPIN);
    vec![
        Shipped {
            folder: "strat-micro-recorder/te-binance-spot-btcusdt",
            strategy: Box::new(MicroRecorder::from_spec(&recorder_spec(Vec::new()), engine)),
            own_claims: Some(holds_the_recorder_claims),
        },
        Shipped {
            folder: "strat-micro-recorder/te-polymarket-btc-updown-5m",
            strategy: Box::new(PolyUpPublisher::from_spec(
                &recorder_spec(Vec::new()),
                engine,
            )),
            own_claims: None,
        },
    ]
}

/// What a `schema.lock` holds. The test only reads it: a person writes each line by hand.
struct SchemaLock {
    columns: Vec<String>,
    link_schema_digest: u64,
}

impl SchemaLock {
    fn read(path: &Path) -> Self {
        let text = fs::read_to_string(path).unwrap_or_else(|error| {
            panic!(
                "cannot read {}: {error}. Each strategy folder holds a {LOCK_FILE} with its \
                 columns in order and the digest of its link schema",
                path.display()
            )
        });
        let mut columns = Vec::new();
        let mut link_schema_digest = None;
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let at = format!("{}:{}", path.display(), index + 1);
            match line.split_once(' ') {
                Some(("column", name)) => columns.push(name.to_owned()),
                Some(("link_schema_digest", hex)) => {
                    let digits = hex.strip_prefix("0x").unwrap_or_else(|| {
                        panic!("{at}: the digest {hex:?} is not written as 0x and hex digits")
                    });
                    let digest = u64::from_str_radix(digits, 16)
                        .unwrap_or_else(|error| panic!("{at}: the digest {hex:?}: {error}"));
                    assert!(
                        link_schema_digest.replace(digest).is_none(),
                        "{at}: a second link_schema_digest line"
                    );
                }
                _ => panic!(
                    "{at}: {line:?} is neither `column <name>` nor `link_schema_digest 0x<hex>`"
                ),
            }
        }
        let link_schema_digest = link_schema_digest
            .unwrap_or_else(|| panic!("{} has no link_schema_digest line", path.display()));
        Self {
            columns,
            link_schema_digest,
        }
    }

    fn id_of(&self, name: &str) -> FeatureId {
        let position = self
            .columns
            .iter()
            .position(|column| column == name)
            .unwrap_or_else(|| panic!("the lock holds no column named {name:?}"));
        FeatureId(u16::try_from(position).expect("a lock holds fewer than 65536 columns"))
    }
}

fn strategies_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../strategies")
}

/// Each folder below `strategies/` that holds a `strategy.rs`, as a path relative to that root.
fn strategy_folders(root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = fs::read_dir(&dir)
            .unwrap_or_else(|error| panic!("cannot list {}: {error}", dir.display()));
        for entry in entries {
            let path = entry.expect("read a directory entry").path();
            let is_hidden = path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with('.'));
            if path.is_dir() && !is_hidden {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == "strategy.rs") {
                let folder = dir
                    .strip_prefix(root)
                    .expect("the walk stays below its root");
                found.push(folder.to_string_lossy().into_owned());
            }
        }
    }
    found.sort();
    found
}

/// The first place where the declared columns leave the locked ones, in words a reader can act on.
fn column_drift(declared: &[&str], locked: &[String]) -> Option<String> {
    for (index, locked_name) in locked.iter().enumerate() {
        match declared.get(index) {
            None => {
                return Some(format!(
                    "column {index} {locked_name:?} is locked but no longer declared: a column \
                     is never removed"
                ));
            }
            Some(name) if name != locked_name => {
                return Some(format!(
                    "column {index} is declared as {name:?} but locked as {locked_name:?}: a \
                     column is never renamed or moved, because every file on disk names it by \
                     this position"
                ));
            }
            Some(_) => {}
        }
    }
    declared.get(locked.len()).map(|name| {
        format!(
            "column {} {name:?} is declared but not locked: add `column {name}` at the end of the \
             lock by hand",
            locked.len()
        )
    })
}

/// Each field of each group accessor, with the column it claims, written out for each side so the
/// accessor under test is not its own expectation. A swapped pair inside an accessor leaves the
/// names and their order alone, and only puts a number under a heading that plausibly fits it.
fn accessor_claims(features: &Features, side: QuoteSide) -> [(FeatureId, &'static str); 16] {
    let markout = features.markout(side);
    let hawkes = features.hawkes(side);
    let gueant = features.gueant(side);
    let intensity = features.intensity(side);
    match side {
        QuoteSide::Bid => [
            (markout.forward_1s, "markout_bid_1s_bps"),
            (markout.forward_3s, "markout_bid_3s_bps"),
            (markout.forward_5s, "markout_bid_5s_bps"),
            (markout.reverse_1s, "markout_bid_rev_1s_bps"),
            (markout.reverse_5s, "markout_bid_rev_5s_bps"),
            (markout.fills, "markout_bid_fills"),
            (hawkes.lambda, "hawkes_lambda_bid_per_sec"),
            (hawkes.mu, "hawkes_mu_bid_per_sec"),
            (hawkes.alpha, "hawkes_alpha_bid_per_sec"),
            (hawkes.beta, "hawkes_beta_bid_per_sec"),
            (hawkes.branching, "hawkes_branching_bid"),
            (gueant.half_spread_bps, "gueant_bid_half_spread_bps"),
            (gueant.skew_bps, "gueant_bid_skew_bps"),
            (gueant.price, "gueant_bid_price"),
            (intensity.a, "intensity_a_bid_per_sec"),
            (intensity.k, "intensity_k_bid_per_bps"),
        ],
        QuoteSide::Ask => [
            (markout.forward_1s, "markout_ask_1s_bps"),
            (markout.forward_3s, "markout_ask_3s_bps"),
            (markout.forward_5s, "markout_ask_5s_bps"),
            (markout.reverse_1s, "markout_ask_rev_1s_bps"),
            (markout.reverse_5s, "markout_ask_rev_5s_bps"),
            (markout.fills, "markout_ask_fills"),
            (hawkes.lambda, "hawkes_lambda_ask_per_sec"),
            (hawkes.mu, "hawkes_mu_ask_per_sec"),
            (hawkes.alpha, "hawkes_alpha_ask_per_sec"),
            (hawkes.beta, "hawkes_beta_ask_per_sec"),
            (hawkes.branching, "hawkes_branching_ask"),
            (gueant.half_spread_bps, "gueant_ask_half_spread_bps"),
            (gueant.skew_bps, "gueant_ask_skew_bps"),
            (gueant.price, "gueant_ask_price"),
            (intensity.a, "intensity_a_ask_per_sec"),
            (intensity.k, "intensity_k_ask_per_bps"),
        ],
    }
}

/// The recorder's own claims beyond its lock: the link block sits in place in wire order, and each
/// accessor field resolves, through the ids the engine hands out, to the locked column it names.
fn holds_the_recorder_claims(columns: &[&str], link_fields: &[&str], lock: &SchemaLock) {
    let block_end = RECORDER_LINK_COLUMNS_START + link_fields.len();
    assert_eq!(
        &columns[RECORDER_LINK_COLUMNS_START..block_end],
        link_fields,
        "recorder columns {RECORDER_LINK_COLUMNS_START}..{block_end} must be the link fields in \
         wire order"
    );
    let dense_ids: Vec<FeatureId> = (0..columns.len())
        .map(|index| FeatureId(u16::try_from(index).expect("fewer than 65536 columns")))
        .collect();
    let features = Features::from_ids(&dense_ids);
    for side in [QuoteSide::Bid, QuoteSide::Ask] {
        for (actual, claimed) in accessor_claims(&features, side) {
            assert_eq!(
                actual,
                lock.id_of(claimed),
                "the {side:?} accessor field that claims {claimed:?} resolves to column {:?}",
                columns[usize::from(actual.0)]
            );
        }
    }
}

/// Fitness: a renamed, removed or moved column relabels every file already on disk, and a changed
/// link schema makes each peer drop the other's frames, and neither fails anything at run time.
#[test]
fn schema_lock_of_each_strategy_holds() {
    let root = strategies_root();
    let shipped = shipped_strategies();
    let mut cased: Vec<String> = shipped.iter().map(|each| each.folder.to_owned()).collect();
    cased.sort();
    assert_eq!(
        strategy_folders(&root),
        cased,
        "each strategy folder below {} has a case in this test and a {LOCK_FILE}",
        root.display()
    );
    for Shipped {
        folder,
        strategy,
        own_claims,
    } in &shipped
    {
        let lock = SchemaLock::read(&root.join(folder).join(LOCK_FILE));
        let columns = strategy.features();
        if let Some(drift) = column_drift(columns, &lock.columns) {
            panic!("{folder}: {drift}");
        }
        let digest = schema_hash_of_fields(strategy.link_fields()).0;
        assert_eq!(
            digest, lock.link_schema_digest,
            "{folder}: the link schema digests to {digest:#018x}, locked as {:#018x}: a renamed, \
             removed or moved link field makes each peer of another build drop every frame",
            lock.link_schema_digest
        );
        if let Some(own_claims) = own_claims {
            own_claims(columns, strategy.link_fields(), &lock);
        }
    }
}
