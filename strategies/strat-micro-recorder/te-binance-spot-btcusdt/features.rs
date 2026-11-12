//! Feature column registry. One ident list drives names, resolved struct, id assignment. Append-only on-disk.

use polysim::hot::strategy::FeatureId;

polysim::features! {
    pub(crate) struct Features {
        mid,
        microprice,
        imbalance,
        egarch_vol_lt,
        realised_vol_st,
        resilience_median_1m,
        resilience_mean_1m,
        intensity_a_bid_per_sec,
        intensity_k_bid_per_tick,
        intensity_a_ask_per_sec,
        intensity_k_ask_per_tick,
        gueant_bid_half_spread_ticks,
        gueant_ask_half_spread_ticks,
        gueant_bid_skew_ticks,
        gueant_ask_skew_ticks,
        gueant_bid_price,
        gueant_ask_price,
        gueant_sigma_ticks,
        volume_bar_imbalance,
        volume_bar_duration_secs,
        markout_bid_1s_bps,
        markout_bid_3s_bps,
        markout_bid_5s_bps,
        markout_ask_1s_bps,
        markout_ask_3s_bps,
        markout_ask_5s_bps,
        markout_bid_rev_1s_bps,
        markout_bid_rev_5s_bps,
        markout_ask_rev_1s_bps,
        markout_ask_rev_5s_bps,
        markout_bid_fills,
        markout_ask_fills,
        kyle_lambda_per_notional,
        kyle_lambda_ticks_per_notional,
        kyle_one_tick_notional,
        vpin_st,
        vpin_lt,
        vpin_signed_flow_st,
        vpin_signed_flow_lt,
        hawkes_lambda_bid_per_sec,
        hawkes_mu_bid_per_sec,
        hawkes_alpha_bid_per_sec,
        hawkes_beta_bid_per_sec,
        hawkes_branching_bid,
        hawkes_lambda_ask_per_sec,
        hawkes_mu_ask_per_sec,
        hawkes_alpha_ask_per_sec,
        hawkes_beta_ask_per_sec,
        hawkes_branching_ask,
        egarch_omega,
        egarch_gamma,
        egarch_theta,
        egarch_beta,
        egarch_uncond_vol_lt,
        ewma_vol_per_event,
        kyle_intercept,
        inventory_quote,
        // Polymarket up legs from the other TE over link, recorded on own instrument. Names and order
        // mirror common::LINK_FIELDS; `cur` hosts the open window, `next` the one not yet open.
        poly_cur_up_bid,
        poly_cur_up_ask,
        poly_cur_up_bid_qty,
        poly_cur_up_ask_qty,
        poly_cur_up_intensity_a_bid,
        poly_cur_up_intensity_k_bid,
        poly_cur_up_intensity_a_ask,
        poly_cur_up_intensity_k_ask,
        poly_cur_up_buy_vol,
        poly_cur_up_sell_vol,
        poly_next_up_bid,
        poly_next_up_ask,
        poly_next_up_bid_qty,
        poly_next_up_ask_qty,
        poly_next_up_intensity_a_bid,
        poly_next_up_intensity_k_bid,
        poly_next_up_intensity_a_ask,
        poly_next_up_intensity_k_ask,
        poly_next_up_buy_vol,
        poly_next_up_sell_vol,
        obi_half_bp,
        realised_vol_st_bps,
        intensity_k_bid_per_bps,
        intensity_k_ask_per_bps,
        gueant_bid_half_spread_bps,
        gueant_ask_half_spread_bps,
        gueant_bid_skew_bps,
        gueant_ask_skew_bps,
        gueant_sigma_bps,
        kyle_lambda_bps_per_notional,
        kyle_one_bp_notional,
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
