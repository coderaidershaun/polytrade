//! What both trading engines must agree on: the link schema. Each engine's strategy.rs includes
//! this file by path.
//! The LINK_FIELDS digest is the schema_hash carried on every frame, so any drift between the two
//! sides makes the receiver reject every frame.
//!
//! Absent = `f64::NAN` — no book side, book not yet Valid, no or stale (A, k) fit, unfilled role.
//! The receiver emits only finite slots, and NaN survives the codec bit-exactly.
//!
//! Volumes are CUMULATIVE traded qty since that leg's rotation, in outcome shares, zeroed at
//! rotation. Link topics must carry STATE rather than deltas, because a frame may be dropped: a
//! dropped delta would lose its spin's volume forever, whereas a dropped cumulative frame is
//! superseded by the next. Per-spin volume differences out offline, and a decrease marks a rotation.
//!
//! `_bid`/`_ask` on the intensity fields follow the engine's convention — a sell aggressor hits the
//! bid, so bid intensity is fitted from sells.

polysim::link_schema! {
    /// One up leg's published slots, in the order they ride the wire within their block.
    pub(crate) struct UpRole {
        bid,
        ask,
        bid_qty,
        ask_qty,
        intensity_a_bid,
        intensity_k_bid,
        intensity_a_ask,
        intensity_k_ask,
        buy_vol,
        sell_vol,
    }

    /// Keyed by ROLE, not by slot: the series alternates its two window slots, so which slot is which
    /// changes every rotation. `cur` is the slot hosting the open window, `next` the one not yet open;
    /// at most one slot holds each role, and a role no slot fills stays `ABSENT`.
    pub(crate) struct UpFrame {
        cur: {
            poly_cur_up_bid => bid,
            poly_cur_up_ask => ask,
            poly_cur_up_bid_qty => bid_qty,
            poly_cur_up_ask_qty => ask_qty,
            poly_cur_up_intensity_a_bid => intensity_a_bid,
            poly_cur_up_intensity_k_bid => intensity_k_bid,
            poly_cur_up_intensity_a_ask => intensity_a_ask,
            poly_cur_up_intensity_k_ask => intensity_k_ask,
            poly_cur_up_buy_vol => buy_vol,
            poly_cur_up_sell_vol => sell_vol,
        },
        next: {
            poly_next_up_bid => bid,
            poly_next_up_ask => ask,
            poly_next_up_bid_qty => bid_qty,
            poly_next_up_ask_qty => ask_qty,
            poly_next_up_intensity_a_bid => intensity_a_bid,
            poly_next_up_intensity_k_bid => intensity_k_bid,
            poly_next_up_intensity_a_ask => intensity_a_ask,
            poly_next_up_intensity_k_ask => intensity_k_ask,
            poly_next_up_buy_vol => buy_vol,
            poly_next_up_sell_vol => sell_vol,
        },
    }

    pub(crate) const LINK_FIELDS;
}

/// Topic names in id-assignment order.
pub(crate) const LINK_TOPICS: &[&str] = &["poly_up"];
