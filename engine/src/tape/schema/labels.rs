//! The named sets of words that a label column, a footer key or the manifest may hold. A value is
//! appended to a set, never renamed: a rename relabels history.

use crate::msg::exec::{Liquidity, OrderStyle, Provenance, RejectClass};
use crate::msg::ids::Side;
use crate::msg::kinds::{ExecutionMode, FeatureKind, KlineInterval};
use crate::msg::latency::{Category, Stage};
use crate::msg::persist::{
    BookEventKind, IncidentKind, LedgerKind, LinkRowKind, NoQuoteReason, OrderLifecycle,
    OrderTransition,
};
use crate::msg::time::ClockSource;
use crate::runtime::manifest::{EndState, RunKind};
use crate::runtime::metrics::SinkKind;

pub(in crate::tape) struct LabelSet {
    pub(in crate::tape) name: &'static str,
    pub(in crate::tape) values: &'static [&'static str],
}

/// The words of `first`, then those of `second`. The length is given at the use, so a word added
/// to an enum fails the build here until the set is sized again.
const fn joined<const N: usize>(
    first: &[&'static str],
    second: &[&'static str],
) -> [&'static str; N] {
    assert!(first.len() + second.len() == N);
    let mut words = [""; N];
    let mut index = 0;
    while index < first.len() {
        words[index] = first[index];
        index += 1;
    }
    while index < N {
        words[index] = second[index - first.len()];
        index += 1;
    }
    words
}

const ORDER_STYLE_WORDS: [&str; 3] = joined(OrderStyle::LABELS, &["unknown"]);
const REJECT_CLASS_WORDS: [&str; 6] = joined(RejectClass::LABELS, &["none"]);
const LIQUIDITY_WORDS: [&str; 3] = joined(Liquidity::LABELS, &["none"]);
const BOOK_SIDE_WORDS: [&str; 3] = joined(Side::LABELS, &["none"]);
const EXECUTION_MODE_WORDS: [&str; 4] = joined(&["absent"], ExecutionMode::LABELS);

pub(in crate::tape) const ORDER_TRANSITION: LabelSet = LabelSet {
    name: "order_transition",
    values: OrderTransition::LABELS,
};
pub(in crate::tape) const ORDER_LIFECYCLE: LabelSet = LabelSet {
    name: "order_lifecycle",
    values: OrderLifecycle::LABELS,
};
pub(in crate::tape) const INCIDENT_KIND: LabelSet = LabelSet {
    name: "incident_kind",
    values: IncidentKind::LABELS,
};
pub(in crate::tape) const LINK_ROW_KIND: LabelSet = LabelSet {
    name: "link_row_kind",
    values: LinkRowKind::LABELS,
};
pub(in crate::tape) const SIDE: LabelSet = LabelSet {
    name: "side",
    values: Side::LABELS,
};
pub(in crate::tape) const PROVENANCE: LabelSet = LabelSet {
    name: "provenance",
    values: Provenance::LABELS,
};
pub(in crate::tape) const ORDER_STYLE: LabelSet = LabelSet {
    name: "order_style",
    values: &ORDER_STYLE_WORDS,
};
pub(in crate::tape) const REJECT_CLASS: LabelSet = LabelSet {
    name: "reject_class",
    values: &REJECT_CLASS_WORDS,
};
pub(in crate::tape) const LIQUIDITY: LabelSet = LabelSet {
    name: "liquidity",
    values: &LIQUIDITY_WORDS,
};
pub(in crate::tape) const KLINE_INTERVAL: LabelSet = LabelSet {
    name: "kline_interval",
    values: KlineInterval::LABELS,
};
pub(in crate::tape) const BOOK_EVENT_KIND: LabelSet = LabelSet {
    name: "book_event_kind",
    values: BookEventKind::LABELS,
};
pub(in crate::tape) const BOOK_SIDE: LabelSet = LabelSet {
    name: "book_side",
    values: &BOOK_SIDE_WORDS,
};
pub(in crate::tape) const LEDGER_KIND: LabelSet = LabelSet {
    name: "ledger_kind",
    values: LedgerKind::LABELS,
};
pub(in crate::tape) const NO_QUOTE_REASON: LabelSet = LabelSet {
    name: "no_quote_reason",
    values: NoQuoteReason::LABELS,
};
pub(in crate::tape) const LATENCY_CATEGORY: LabelSet = LabelSet {
    name: "latency_category",
    values: Category::LABELS,
};
pub(in crate::tape) const LATENCY_STAGE: LabelSet = LabelSet {
    name: "latency_stage",
    values: Stage::LABELS,
};
pub(in crate::tape) const CLOCK_SOURCE: LabelSet = LabelSet {
    name: "clock_source",
    values: ClockSource::LABELS,
};
pub(in crate::tape) const SINK_KIND: LabelSet = LabelSet {
    name: "sink_kind",
    values: SinkKind::LABELS,
};
pub(in crate::tape) const EXECUTION_MODE: LabelSet = LabelSet {
    name: "execution_mode",
    values: &EXECUTION_MODE_WORDS,
};
pub(in crate::tape) const RUN_KIND: LabelSet = LabelSet {
    name: "run_kind",
    values: RunKind::LABELS,
};
pub(in crate::tape) const END_STATE: LabelSet = LabelSet {
    name: "end_state",
    values: EndState::LABELS,
};
pub(in crate::tape) const FEATURE_KIND: LabelSet = LabelSet {
    name: "feature_kind",
    values: FeatureKind::LABELS,
};

/// Each set, in the order of the contract.
pub(in crate::tape) const LABEL_SETS: [&LabelSet; 22] = [
    &ORDER_TRANSITION,
    &ORDER_LIFECYCLE,
    &INCIDENT_KIND,
    &LINK_ROW_KIND,
    &SIDE,
    &PROVENANCE,
    &ORDER_STYLE,
    &REJECT_CLASS,
    &LIQUIDITY,
    &KLINE_INTERVAL,
    &BOOK_EVENT_KIND,
    &BOOK_SIDE,
    &LEDGER_KIND,
    &NO_QUOTE_REASON,
    &LATENCY_CATEGORY,
    &LATENCY_STAGE,
    &CLOCK_SOURCE,
    &SINK_KIND,
    &EXECUTION_MODE,
    &RUN_KIND,
    &END_STATE,
    &FEATURE_KIND,
];
