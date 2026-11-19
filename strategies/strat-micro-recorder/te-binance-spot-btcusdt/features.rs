//! Feature column registry. One list drives names, kinds, resolved struct, id assignment. Append-only on-disk.

use engine::hot::strategy::FeatureId;

// A column is execution_aware when it reads the run's own position or the quotes priced from it:
// the inventory, the quoted Guéant prices, and the markouts armed on those quotes.
engine::features! {
    pub(crate) struct Features {
        mid: market_only,
        microprice: market_only,
        imbalance: market_only,
        egarch_vol_lt: market_only,
        realised_vol_st: market_only,
        resilience_median_1m: market_only,
        resilience_mean_1m: market_only,
        intensity_a_bid_per_sec: market_only,
        intensity_k_bid_per_tick: market_only,
        intensity_a_ask_per_sec: market_only,
        intensity_k_ask_per_tick: market_only,
        gueant_bid_half_spread_ticks: market_only,
        gueant_ask_half_spread_ticks: market_only,
        gueant_bid_skew_ticks: market_only,
        gueant_ask_skew_ticks: market_only,
        gueant_bid_price: execution_aware,
        gueant_ask_price: execution_aware,
        gueant_sigma_ticks: market_only,
        volume_bar_imbalance: market_only,
        volume_bar_duration_secs: market_only,
        markout_bid_1s_bps: execution_aware,
        markout_bid_3s_bps: execution_aware,
        markout_bid_5s_bps: execution_aware,
        markout_ask_1s_bps: execution_aware,
        markout_ask_3s_bps: execution_aware,
        markout_ask_5s_bps: execution_aware,
        markout_bid_rev_1s_bps: execution_aware,
        markout_bid_rev_5s_bps: execution_aware,
        markout_ask_rev_1s_bps: execution_aware,
        markout_ask_rev_5s_bps: execution_aware,
        markout_bid_fills: execution_aware,
        markout_ask_fills: execution_aware,
        kyle_lambda_per_notional: market_only,
        kyle_lambda_ticks_per_notional: market_only,
        kyle_one_tick_notional: market_only,
        vpin_st: market_only,
        vpin_lt: market_only,
        vpin_signed_flow_st: market_only,
        vpin_signed_flow_lt: market_only,
        hawkes_lambda_bid_per_sec: market_only,
        hawkes_mu_bid_per_sec: market_only,
        hawkes_alpha_bid_per_sec: market_only,
        hawkes_beta_bid_per_sec: market_only,
        hawkes_branching_bid: market_only,
        hawkes_lambda_ask_per_sec: market_only,
        hawkes_mu_ask_per_sec: market_only,
        hawkes_alpha_ask_per_sec: market_only,
        hawkes_beta_ask_per_sec: market_only,
        hawkes_branching_ask: market_only,
        egarch_omega: market_only,
        egarch_gamma: market_only,
        egarch_theta: market_only,
        egarch_beta: market_only,
        egarch_uncond_vol_lt: market_only,
        ewma_vol_per_event: market_only,
        kyle_intercept: market_only,
        inventory_quote: execution_aware,
        // Polymarket up legs from the other TE over link, recorded on own instrument. Names and order
        // mirror link_schema::LINK_FIELDS; `cur` hosts the open window, `next` the one not yet open.
        poly_cur_up_bid: market_only,
        poly_cur_up_ask: market_only,
        poly_cur_up_bid_qty: market_only,
        poly_cur_up_ask_qty: market_only,
        poly_cur_up_intensity_a_bid: market_only,
        poly_cur_up_intensity_k_bid: market_only,
        poly_cur_up_intensity_a_ask: market_only,
        poly_cur_up_intensity_k_ask: market_only,
        poly_cur_up_buy_vol: market_only,
        poly_cur_up_sell_vol: market_only,
        poly_next_up_bid: market_only,
        poly_next_up_ask: market_only,
        poly_next_up_bid_qty: market_only,
        poly_next_up_ask_qty: market_only,
        poly_next_up_intensity_a_bid: market_only,
        poly_next_up_intensity_k_bid: market_only,
        poly_next_up_intensity_a_ask: market_only,
        poly_next_up_intensity_k_ask: market_only,
        poly_next_up_buy_vol: market_only,
        poly_next_up_sell_vol: market_only,
        obi_half_bp: market_only,
        realised_vol_st_bps: market_only,
        intensity_k_bid_per_bps: market_only,
        intensity_k_ask_per_bps: market_only,
        gueant_bid_half_spread_bps: market_only,
        gueant_ask_half_spread_bps: market_only,
        gueant_bid_skew_bps: market_only,
        gueant_ask_skew_bps: market_only,
        gueant_sigma_bps: market_only,
        kyle_lambda_bps_per_notional: market_only,
        kyle_one_bp_notional: market_only,
    }
    pub(crate) const FEATURE_NAMES;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QuoteSide {
    Bid,
    Ask,
}

// A per-side family is one concept spread over a flat append-only list that cannot group it. Naming
// the family's columns once here beats re-matching bid-vs-ask column tuples at every emit site,
// where a transposed pair reads as a plausible number under the wrong name.

#[derive(Debug, Clone, Copy)]
pub(crate) struct MarkoutColumns {
    pub(crate) forward_1s: FeatureId,
    pub(crate) forward_3s: FeatureId,
    pub(crate) forward_5s: FeatureId,
    pub(crate) reverse_1s: FeatureId,
    pub(crate) reverse_5s: FeatureId,
    pub(crate) fills: FeatureId,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HawkesColumns {
    pub(crate) lambda: FeatureId,
    pub(crate) mu: FeatureId,
    pub(crate) alpha: FeatureId,
    pub(crate) beta: FeatureId,
    pub(crate) branching: FeatureId,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GueantSideColumns {
    pub(crate) half_spread_bps: FeatureId,
    pub(crate) skew_bps: FeatureId,
    pub(crate) price: FeatureId,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntensityColumns {
    pub(crate) a: FeatureId,
    pub(crate) k: FeatureId,
}

impl Features {
    pub(crate) fn markout(&self, side: QuoteSide) -> MarkoutColumns {
        match side {
            QuoteSide::Bid => MarkoutColumns {
                forward_1s: self.markout_bid_1s_bps,
                forward_3s: self.markout_bid_3s_bps,
                forward_5s: self.markout_bid_5s_bps,
                reverse_1s: self.markout_bid_rev_1s_bps,
                reverse_5s: self.markout_bid_rev_5s_bps,
                fills: self.markout_bid_fills,
            },
            QuoteSide::Ask => MarkoutColumns {
                forward_1s: self.markout_ask_1s_bps,
                forward_3s: self.markout_ask_3s_bps,
                forward_5s: self.markout_ask_5s_bps,
                reverse_1s: self.markout_ask_rev_1s_bps,
                reverse_5s: self.markout_ask_rev_5s_bps,
                fills: self.markout_ask_fills,
            },
        }
    }

    pub(crate) fn hawkes(&self, side: QuoteSide) -> HawkesColumns {
        match side {
            QuoteSide::Bid => HawkesColumns {
                lambda: self.hawkes_lambda_bid_per_sec,
                mu: self.hawkes_mu_bid_per_sec,
                alpha: self.hawkes_alpha_bid_per_sec,
                beta: self.hawkes_beta_bid_per_sec,
                branching: self.hawkes_branching_bid,
            },
            QuoteSide::Ask => HawkesColumns {
                lambda: self.hawkes_lambda_ask_per_sec,
                mu: self.hawkes_mu_ask_per_sec,
                alpha: self.hawkes_alpha_ask_per_sec,
                beta: self.hawkes_beta_ask_per_sec,
                branching: self.hawkes_branching_ask,
            },
        }
    }

    pub(crate) fn gueant(&self, side: QuoteSide) -> GueantSideColumns {
        match side {
            QuoteSide::Bid => GueantSideColumns {
                half_spread_bps: self.gueant_bid_half_spread_bps,
                skew_bps: self.gueant_bid_skew_bps,
                price: self.gueant_bid_price,
            },
            QuoteSide::Ask => GueantSideColumns {
                half_spread_bps: self.gueant_ask_half_spread_bps,
                skew_bps: self.gueant_ask_skew_bps,
                price: self.gueant_ask_price,
            },
        }
    }

    pub(crate) fn intensity(&self, side: QuoteSide) -> IntensityColumns {
        match side {
            QuoteSide::Bid => IntensityColumns {
                a: self.intensity_a_bid_per_sec,
                k: self.intensity_k_bid_per_bps,
            },
            QuoteSide::Ask => IntensityColumns {
                a: self.intensity_a_ask_per_sec,
                k: self.intensity_k_ask_per_bps,
            },
        }
    }
}
