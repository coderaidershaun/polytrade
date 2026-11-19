//! A recorded column is its name: each strategy's columns and link schema stay what its
//! `schema.lock` says, and a file decodes by its own footer whatever order its build declared.

use std::fs;
use std::path::{Path, PathBuf};

use engine::hot::strategy::{
    FeatureId, FeatureKind, Registration, Strategy, StrategyConfig, StrategyCtx,
};
use engine::link::schema_hash_of_fields;
use engine::msg::ids::InstrumentId;
use engine::msg::inbound::SpinTick;
use engine::msg::persist::ResearchRecord;

use crate::engine_support::{
    NOMINAL_SPIN, engine_view, engine_without_warmup, instrument_row, persist_ring, recorder_spec,
    spin, strategy_log_ring, tracker_spec_all,
};
use crate::micro_strategy::MicroRecorder;
use crate::micro_strategy::features::{Features, QuoteSide};
use crate::parquet_readback::{
    Cell, TempDir, drain_blocking, named_feature_rows, parquet_files, read_parquet_file,
    spawn_feature_writer,
};
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
    kinds: Vec<String>,
    link_schema_digest: u64,
}

impl SchemaLock {
    fn read(path: &Path) -> Self {
        let text = fs::read_to_string(path).unwrap_or_else(|error| {
            panic!(
                "cannot read {}: {error}. Each strategy folder holds a {LOCK_FILE} with its \
                 columns and their kinds in order and the digest of its link schema",
                path.display()
            )
        });
        let mut columns = Vec::new();
        let mut kinds = Vec::new();
        let mut link_schema_digest = None;
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let at = format!("{}:{}", path.display(), index + 1);
            match line.split_once(' ') {
                Some(("column", column)) => {
                    let Some((name, kind)) = column.split_once(' ') else {
                        panic!("{at}: {line:?} has no kind: write `column <name> <kind>`")
                    };
                    columns.push(name.to_owned());
                    kinds.push(kind.to_owned());
                }
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
                    "{at}: {line:?} is neither `column <name> <kind>` nor `link_schema_digest 0x<hex>`"
                ),
            }
        }
        let link_schema_digest = link_schema_digest
            .unwrap_or_else(|| panic!("{} has no link_schema_digest line", path.display()));
        Self {
            columns,
            kinds,
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

/// The first locked column whose kind the strategy now declares otherwise. A column keeps its kind:
/// a reader decides by it whether a replay under another strategy gives the column again.
fn kind_drift(declared: &[FeatureKind], locked: &[String]) -> Option<String> {
    locked
        .iter()
        .zip(declared)
        .enumerate()
        .find(|(_, (locked, declared))| declared.as_str() != locked.as_str())
        .map(|(index, (locked, declared))| {
            format!(
                "column {index} is declared {} but locked {locked}: a column never changes its kind",
                declared.as_str()
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
        assert_eq!(
            strategy.feature_kinds().len(),
            columns.len(),
            "{folder}: each declared column has one kind"
        );
        if let Some(drift) = kind_drift(strategy.feature_kinds(), &lock.kinds) {
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

engine::features! {
    struct FirstOrder {
        alpha: market_only,
        beta: market_only,
        gamma: execution_aware,
    }
    const FIRST_ORDER;
}

engine::features! {
    struct SecondOrder {
        gamma: execution_aware,
        alpha: market_only,
        beta: market_only,
    }
    const SECOND_ORDER;
}

/// The three columns of either declaration, named by field, so each build emits a value under the
/// name it means whatever id that name received.
#[derive(Clone, Copy)]
struct Columns {
    alpha: FeatureId,
    beta: FeatureId,
    gamma: FeatureId,
}

fn first_order(ids: &[FeatureId]) -> Columns {
    let declared = FirstOrder::from_ids(ids);
    Columns {
        alpha: declared.alpha,
        beta: declared.beta,
        gamma: declared.gamma,
    }
}

fn second_order(ids: &[FeatureId]) -> Columns {
    let declared = SecondOrder::from_ids(ids);
    Columns {
        alpha: declared.alpha,
        beta: declared.beta,
        gamma: declared.gamma,
    }
}

/// A strategy that records the same three values each spin, under one of the two declarations.
struct OrderProbe {
    declared: &'static [&'static str],
    resolve: fn(&[FeatureId]) -> Columns,
    columns: Option<Columns>,
}

impl Strategy for OrderProbe {
    fn features(&self) -> &'static [&'static str] {
        self.declared
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.columns = Some((self.resolve)(registration.features));
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let columns = self.columns.expect("registered before the first callback");
        ctx.emit(columns.alpha, InstrumentId(0), 1.5);
        ctx.emit(columns.beta, InstrumentId(0), -2.25);
        ctx.emit(columns.gamma, InstrumentId(0), 4096.0);
    }
}

/// Runs one build of the probe for three spins and writes its rows through the real tape writer.
fn record_a_run(
    tape: &Path,
    declared: &'static [&'static str],
    resolve: fn(&[FeatureId]) -> Columns,
) {
    let (persistence, rings) = persist_ring(64);
    let mut consumer = rings.research;
    let (log_sink, _log_consumer) = strategy_log_ring(16);
    let probe = OrderProbe {
        declared,
        resolve,
        columns: None,
    };
    let mut engine = engine_without_warmup(
        &[instrument_row(0, tracker_spec_all(1), 8)],
        Box::new(probe),
        persistence,
        log_sink,
    );
    for seq in 1..=3 {
        engine.dispatch(&spin(
            seq,
            1_000_000 * i64::try_from(seq).expect("a small seq"),
        ));
    }
    let mut records = Vec::new();
    while let Ok(record) = consumer.pop() {
        if matches!(record, ResearchRecord::Feature(_)) {
            records.push(record.into());
        }
    }
    assert_eq!(records.len(), 9, "three spins of three columns");
    let names = engine.feature_names().to_vec();
    drain_blocking(spawn_feature_writer(tape, &names, records)).expect("the tape drains clean");
}

fn feature_ids(path: &Path) -> Vec<Cell> {
    let file = read_parquet_file(path);
    let column = file
        .field_names
        .iter()
        .position(|field| field == "feature_id")
        .expect("a features file has a feature_id column");
    file.rows.iter().map(|row| row[column].clone()).collect()
}

/// Fitness: a column is its name. Two builds that declare the same columns in two orders give the
/// same name two ids, and a reader that names one file's ids through another file's footer puts
/// every value under the wrong heading with no error.
#[test]
fn permuted_declaration_decodes_to_equal_tables() {
    let tape = TempDir::new("permuted-declaration");
    record_a_run(tape.path(), FIRST_ORDER, first_order);
    record_a_run(tape.path(), SECOND_ORDER, second_order);
    let files = parquet_files(tape.path());
    assert_eq!(files.len(), 2, "one features file for each run: {files:?}");
    assert_ne!(
        feature_ids(&files[0]),
        feature_ids(&files[1]),
        "the two declarations give each name another id"
    );
    let tables: Vec<Vec<_>> = files
        .iter()
        .map(|path| {
            let mut rows = named_feature_rows(path);
            rows.sort();
            rows
        })
        .collect();
    assert_eq!(
        tables[0], tables[1],
        "each file, named through its own footer, holds the same table"
    );
}
