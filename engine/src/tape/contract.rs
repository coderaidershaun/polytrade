//! The one definition of each table that a reader outside the engine reads: `contract/tables.json`,
//! made from the column lists, the label sets, the footer keys and the manifest fields of this
//! build, so a reader holds no column list of its own.

use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};

use crate::msg::failure::FailureClass;
use crate::msg::ids::FIXED_SCALE;
use crate::msg::persist::{
    BookEventRow, FeatureRow, FillRow, IncidentRow, KlineRow, LedgerEntry, LinkFrameRow, MidRow,
    NoQuoteRow, OrderRow, RotationRow, TradeRow,
};
use crate::runtime::manifest::{CONTRACT_VERSION, MANIFEST_FIELDS, MANIFEST_FILE};
use crate::runtime::metrics::{BacklogRow, DropRow, LatencyRow, SenderDropRow};

use super::TapeTable;
use super::payload::TAPE_KINDS;
use super::schema::{ColumnInfo, FooterKey, LABEL_SETS, LabelSet, TableRow, TapeRow};
use super::table::UNFINISHED_EXTENSION;

/// The folder of the runs that no research may read, relative to the folder that holds
/// `contract/`. A reader resolves each path it is given and refuses one below it.
pub const HOLDOUT_ROOT: &str = "data/holdout";

const PATH_FORM: &str = "{persistence_dir}/{strategy_id}/{te_id}/{kind}/{run_id}/{table}/\
                         date=YYYY-MM-DD/{HH}-{run_id}-{seq:06}.parquet";

/// One measure of the summary of a run: each reader computes these names and no other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricName {
    pub name: &'static str,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub unit: &'static str,
}

const fn metric(name: &'static str, kind: &'static str, unit: &'static str) -> MetricName {
    MetricName { name, kind, unit }
}

/// The measures of the summary of a run, in the order of the contract.
pub const METRICS: [MetricName; 27] = [
    metric("orders.placed", "integer", "count"),
    metric("orders.canceled", "integer", "count"),
    metric("orders.rejected", "integer", "count"),
    metric("orders.filled", "integer", "count"),
    metric("orders.ack_round_trip_us", "object", "us"),
    metric("fills.count", "integer", "count"),
    metric("fills.base_bought", "mantissa", "base"),
    metric("fills.base_sold", "mantissa", "base"),
    metric("fills.fill_rate", "float", "ratio"),
    metric("fees.quote", "mantissa", "quote"),
    metric("fees.other_asset_fills", "integer", "count"),
    metric("pnl.session_quote", "mantissa", "quote"),
    metric("pnl.spread_quote", "mantissa", "quote"),
    metric("pnl.unpriced_fills", "integer", "count"),
    metric("pnl.inventory_quote", "mantissa", "quote"),
    metric("pnl.fees_quote", "mantissa", "quote"),
    metric("pnl.remainder_quote", "mantissa", "quote"),
    metric("risk.max_abs_position_base", "mantissa", "base"),
    metric("risk.max_drawdown_quote", "mantissa", "quote"),
    metric("markout.1s.mean_bps", "float", "bps"),
    metric("markout.5s.mean_bps", "float", "bps"),
    metric("markout.30s.mean_bps", "float", "bps"),
    metric("markout.60s.mean_bps", "float", "bps"),
    metric("markout.dropped", "integer", "count"),
    metric("quoting.quoting_side_spins", "integer", "count"),
    metric("quoting.not_quoting_spins", "object", "count"),
    metric("latency", "object", "ns"),
];

#[derive(Serialize)]
struct Contract {
    contract_version: u32,
    fixed_scale: i64,
    notional_rounding: &'static str,
    holdout_root: &'static str,
    path_form: &'static str,
    unfinished_suffix: String,
    manifest: Manifest,
    footer_keys: Vec<FooterEntry>,
    tables: Vec<TableEntry>,
    label_sets: LabelSets,
    tape_kinds: Vec<TapeKindEntry>,
    metrics: &'static [MetricName],
}

#[derive(Serialize)]
struct Manifest {
    file: &'static str,
    fields: Vec<ManifestField>,
}

#[derive(Serialize)]
struct ManifestField {
    name: &'static str,
    #[serde(rename = "type")]
    kind: &'static str,
    nullable: bool,
}

#[derive(Serialize)]
struct TapeKindEntry {
    kind: u16,
    name: &'static str,
    digest: String,
}

#[derive(Serialize)]
struct FooterEntry {
    key: &'static str,
    form: &'static str,
}

#[derive(Serialize)]
struct TableEntry {
    name: &'static str,
    ring: &'static str,
    fault_class: &'static str,
    switch: Option<&'static str>,
    partition_column: &'static str,
    seals: &'static str,
    columns: Vec<ColumnEntry>,
}

#[derive(Serialize)]
struct ColumnEntry {
    name: &'static str,
    kind: &'static str,
    arrow: &'static str,
    nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<&'static str>,
}

/// An object whose keys keep the order of the contract, not the order of the alphabet.
struct LabelSets(&'static [&'static LabelSet]);

impl Serialize for LabelSets {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for set in self.0 {
            map.serialize_entry(set.name, set.values)?;
        }
        map.end()
    }
}

/// The text of `contract/tables.json` for this build: two-space indent, one trailing newline.
pub fn contract_json() -> String {
    let contract = Contract {
        contract_version: CONTRACT_VERSION,
        fixed_scale: FIXED_SCALE,
        notional_rounding: "truncate_toward_zero",
        holdout_root: HOLDOUT_ROOT,
        path_form: PATH_FORM,
        unfinished_suffix: format!(".{UNFINISHED_EXTENSION}"),
        manifest: Manifest {
            file: MANIFEST_FILE,
            fields: MANIFEST_FIELDS
                .iter()
                .map(|&(name, kind, nullable)| ManifestField {
                    name,
                    kind,
                    nullable,
                })
                .collect(),
        },
        footer_keys: FooterKey::ALL
            .into_iter()
            .map(|key| FooterEntry {
                key: key.as_str(),
                form: key.form(),
            })
            .collect(),
        tables: TapeTable::ALL.into_iter().map(table_entry).collect(),
        label_sets: LabelSets(&LABEL_SETS),
        tape_kinds: TAPE_KINDS
            .iter()
            .map(|kind| TapeKindEntry {
                kind: kind.id,
                name: kind.name,
                digest: kind.digest_text(),
            })
            .collect(),
        metrics: &METRICS,
    };
    let mut text = serde_json::to_string_pretty(&contract)
        .expect("a contract of strings, integers and lists always serialises to json");
    text.push('\n');
    text
}

fn table_entry(table: TapeTable) -> TableEntry {
    let (partition_column, seals, columns) = match table {
        TapeTable::Orders => described::<OrderRow>(),
        TapeTable::Fills => described::<FillRow>(),
        TapeTable::Incidents => described::<IncidentRow>(),
        TapeTable::Ledger => described::<LedgerEntry>(),
        TapeTable::Rotations => described::<RotationRow>(),
        TapeTable::Features => described::<FeatureRow>(),
        TapeTable::Trades => described::<TradeRow>(),
        TapeTable::BookEvents => described::<BookEventRow>(),
        TapeTable::Klines => described::<KlineRow>(),
        TapeTable::LinkFrames => described::<LinkFrameRow>(),
        TapeTable::Mids => described::<MidRow>(),
        TapeTable::NoQuotes => described::<NoQuoteRow>(),
        TapeTable::Tape => described::<TapeRow>(),
        TapeTable::MetricsLatency => described::<LatencyRow>(),
        TapeTable::MetricsDrops => described::<DropRow>(),
        TapeTable::MetricsSenderDrops => described::<SenderDropRow>(),
        TapeTable::MetricsBacklog => described::<BacklogRow>(),
    };
    TableEntry {
        name: table.as_str(),
        ring: table.ring().as_str(),
        fault_class: match table.fault_class() {
            FailureClass::Reject => "reject",
            FailureClass::Halt(_) | FailureClass::Retry | FailureClass::Reset => "halt_quoting",
        },
        switch: table.switch().map(|switch| switch.as_str()),
        partition_column,
        seals,
        columns,
    }
}

/// # Panics
/// When the partition column of a row type is none of its columns: a reader would partition by a
/// column that no file holds.
fn described<R: TableRow>() -> (&'static str, &'static str, Vec<ColumnEntry>) {
    let columns: Vec<ColumnInfo> = R::columns();
    assert!(
        columns
            .iter()
            .any(|column| column.name == R::PARTITION_COLUMN),
        "table {} partitions by {}, which is none of its columns",
        R::TABLE,
        R::PARTITION_COLUMN
    );
    let entries = columns
        .into_iter()
        .map(|column| ColumnEntry {
            name: column.name,
            kind: column.kind,
            arrow: column.arrow,
            nullable: column.is_nullable,
            labels: column.labels,
        })
        .collect();
    (R::PARTITION_COLUMN, R::SEALS.as_str(), entries)
}
