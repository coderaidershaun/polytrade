//! The two published contracts of the recorder, frozen against a copy no refactor can reach.
//!
//! `FeatureId` is a position in `FEATURE_NAMES`, so the list IS the Parquet footer dictionary: a
//! reordering renames every column of every file already on disk while the engine, the pins and the
//! UI catalog all stay green, because they read the same list they wrote. The list's 57..77 block
//! is the link schema, whose digest gates every frame — a drift there is drop + count SILENT: the
//! receiver rejects the lot as `SchemaMismatch` and the engine keeps running.
//!
//! So the expectations here are hand-copied literals, never derived from the source of truth. That
//! duplication is the entire mechanism; a "cleanup" that reads the constant back proves nothing.

use polysim::hot::strategy::FeatureId;
use polysim::link::{LinkHash, schema_hash_of_fields};

use crate::engine_support::recorder_feature_id;
use crate::micro_strategy::features::{FEATURE_NAMES, Features, QuoteSide};
use crate::poly_strategy::common::LINK_FIELDS;

/// Copied by hand from the `features![…]` list, in order. Append-only: a new column is added at the
/// END here and nowhere else, and no line ever moves or changes.
const FROZEN_FEATURE_NAMES: [&str; 88] = [
    "mid",
    "microprice",
    "imbalance",
    "egarch_vol_lt",
    "realised_vol_st",
    "resilience_median_1m",
    "resilience_mean_1m",
    "intensity_a_bid_per_sec",
    "intensity_k_bid_per_tick",
    "intensity_a_ask_per_sec",
    "intensity_k_ask_per_tick",
    "gueant_bid_half_spread_ticks",
    "gueant_ask_half_spread_ticks",
    "gueant_bid_skew_ticks",
    "gueant_ask_skew_ticks",
    "gueant_bid_price",
    "gueant_ask_price",
    "gueant_sigma_ticks",
    "volume_bar_imbalance",
    "volume_bar_duration_secs",
    "markout_bid_1s_bps",
    "markout_bid_3s_bps",
    "markout_bid_5s_bps",
    "markout_ask_1s_bps",
    "markout_ask_3s_bps",
    "markout_ask_5s_bps",
    "markout_bid_rev_1s_bps",
    "markout_bid_rev_5s_bps",
    "markout_ask_rev_1s_bps",
    "markout_ask_rev_5s_bps",
    "markout_bid_fills",
    "markout_ask_fills",
    "kyle_lambda_per_notional",
    "kyle_lambda_ticks_per_notional",
    "kyle_one_tick_notional",
    "vpin_st",
    "vpin_lt",
    "vpin_signed_flow_st",
    "vpin_signed_flow_lt",
    "hawkes_lambda_bid_per_sec",
    "hawkes_mu_bid_per_sec",
    "hawkes_alpha_bid_per_sec",
    "hawkes_beta_bid_per_sec",
    "hawkes_branching_bid",
    "hawkes_lambda_ask_per_sec",
    "hawkes_mu_ask_per_sec",
    "hawkes_alpha_ask_per_sec",
    "hawkes_beta_ask_per_sec",
    "hawkes_branching_ask",
    "egarch_omega",
    "egarch_gamma",
    "egarch_theta",
    "egarch_beta",
    "egarch_uncond_vol_lt",
    "ewma_vol_per_event",
    "kyle_intercept",
    "inventory_quote",
    "poly_cur_up_bid",
    "poly_cur_up_ask",
    "poly_cur_up_bid_qty",
    "poly_cur_up_ask_qty",
    "poly_cur_up_intensity_a_bid",
    "poly_cur_up_intensity_k_bid",
    "poly_cur_up_intensity_a_ask",
    "poly_cur_up_intensity_k_ask",
    "poly_cur_up_buy_vol",
    "poly_cur_up_sell_vol",
    "poly_next_up_bid",
    "poly_next_up_ask",
    "poly_next_up_bid_qty",
    "poly_next_up_ask_qty",
    "poly_next_up_intensity_a_bid",
    "poly_next_up_intensity_k_bid",
    "poly_next_up_intensity_a_ask",
    "poly_next_up_intensity_k_ask",
    "poly_next_up_buy_vol",
    "poly_next_up_sell_vol",
    "obi_half_bp",
    "realised_vol_st_bps",
    "intensity_k_bid_per_bps",
    "intensity_k_ask_per_bps",
    "gueant_bid_half_spread_bps",
    "gueant_ask_half_spread_bps",
    "gueant_bid_skew_bps",
    "gueant_ask_skew_bps",
    "gueant_sigma_bps",
    "kyle_lambda_bps_per_notional",
    "kyle_one_bp_notional",
];

/// The digest every `poly_up` frame carries. A change here means every Parquet file written by a
/// peer on the old build decodes its `poly_*` columns from a schema this build rejects.
const FROZEN_LINK_SCHEMA_HASH: LinkHash = LinkHash(0x277c_c7cf_6091_fcd5);

#[test]
fn the_recorder_feature_names_are_frozen() {
    assert_eq!(
        FEATURE_NAMES.len(),
        FROZEN_FEATURE_NAMES.len(),
        "the recorder declares {} feature columns, {} are frozen — a column may be APPENDED (extend \
         FROZEN_FEATURE_NAMES by the same name at the same end), never inserted or removed",
        FEATURE_NAMES.len(),
        FROZEN_FEATURE_NAMES.len()
    );
    for (index, expected) in FROZEN_FEATURE_NAMES.iter().enumerate() {
        assert_eq!(
            &FEATURE_NAMES[index], expected,
            "FeatureId({index}) is the Parquet column name {:?}, frozen as {expected:?} — every \
             file already written names this column by its position",
            FEATURE_NAMES[index]
        );
    }
}

#[test]
fn the_link_fields_digest_to_the_recorded_schema_hash() {
    assert_eq!(
        schema_hash_of_fields(&LINK_FIELDS),
        FROZEN_LINK_SCHEMA_HASH,
        "the link schema hash moved: a sender on this build and a receiver on the old one agree on \
         nothing, and the mismatch is counted rather than raised, so both processes stay up with \
         every poly_* column silently null"
    );
}

const POLY_COLUMNS_START: usize = 57;

#[test]
fn the_link_fields_mirror_the_poly_feature_block() {
    let block = &FEATURE_NAMES[POLY_COLUMNS_START..POLY_COLUMNS_START + LINK_FIELDS.len()];
    assert_eq!(
        block,
        LINK_FIELDS,
        "feature columns {POLY_COLUMNS_START}..{} must be the link fields in wire order — the \
         receiver resolves wire names at boot, so this pins LAYOUT, not decoding: the block is \
         frozen in place, columns exist past it, so a new link field can no longer ride the list's \
         tail and is appended to both lists by decision",
        POLY_COLUMNS_START + LINK_FIELDS.len()
    );
}

/// The ids a registration hands the recorder: dense and in declaration order, because a `FeatureId`
/// IS the position of its name in the list.
fn resolved_features() -> Features {
    let ids: Vec<FeatureId> = (0..FROZEN_FEATURE_NAMES.len() as u16)
        .map(FeatureId)
        .collect();
    Features::from_ids(&ids)
}

/// Every field of every per-side group accessor, paired with the column name that field claims.
/// Written out per side rather than derived: the accessor under test cannot be its own expectation.
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

/// The accessors exist so an emit site names a column once instead of matching a side against a
/// tuple, which only pays if the field a caller reads is the column its name promises. A transposed
/// pair inside an accessor is invisible to every other pin here — the names and their order are
/// untouched, and the wrong number simply lands under a heading that plausibly fits it.
#[test]
fn every_group_accessor_field_resolves_to_the_column_it_names() {
    let features = resolved_features();
    for side in [QuoteSide::Bid, QuoteSide::Ask] {
        for (actual, claimed) in accessor_claims(&features, side) {
            assert_eq!(
                actual,
                recorder_feature_id(claimed),
                "{side:?} accessor field claiming {claimed:?} resolves to {:?}, the column an emit \
                 site would record under that heading instead",
                FEATURE_NAMES[usize::from(actual.0)]
            );
        }
    }
}
