//! The two money rules of the order table: a passive quote never snaps into aggression, and a
//! duplicated, reordered or late event stream never books a fill twice or loses one.
//!
//! Both failures are silent by construction. An inverted snap still compiles, still makes a legal
//! order, and each layer after it agrees with it. A fill folded twice makes a ledger that is wrong
//! in a way that no later reader can detect.

use engine::hot::exec::{OrderClaim, OrderTable, QuoteLevel};
use engine::msg::exec::{ExecEvent, ExecKind, OrderStyle, Provenance};
use engine::msg::ids::{
    AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, Side, VenueOrderId,
};
use engine::msg::time::{LocalTs, VenueTs};
use proptest::prelude::*;

use crate::engine_support::exec_capacity;

const RUN_NONCE: u32 = 0x5EED_1234;
const TICK: i64 = 1_000_000;

/// The receipt stamp of every venue answer built here.
const RECEIVED: LocalTs = LocalTs::from_micros(2);

fn event(kind: ExecKind, client_id: ClientOrderId) -> ExecEvent {
    ExecEvent {
        instrument: Some(InstrumentId(0)),
        client_id,
        venue_order_id: Some(VenueOrderId(77)),
        trade_id: None,
        kind,
        status: None,
        reject: None,
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: None,
        price: Price::from_mantissa(100 * TICK),
        qty: Qty::from_mantissa(10),
        last_price: Price::from_mantissa(0),
        last_qty: Qty::from_mantissa(0),
        cumulative_qty: Qty::from_mantissa(0),
        cumulative_quote: Notional::from_mantissa(0),
        commission: 0,
        commission_asset: AssetId(1),
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: VenueTs::from_micros(1),
        request_sent_ts_us: None,
    }
}

fn buy_claim(level: QuoteLevel, price: i64, qty: i64) -> OrderClaim {
    OrderClaim {
        instrument: InstrumentId(0),
        side: Side::Buy,
        level,
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        style: OrderStyle::PostOnly,
        claimed_ts_us: LocalTs::from_micros(1),
        recon_seq: 0,
    }
}

proptest! {
    /// FITNESS: snapping is passive on both sides, at every price and grid. A buy that snapped UP or
    /// a sell that snapped DOWN would cross a spread the strategy meant to rest inside — and it
    /// would look like a working system, because the order is legal and every layer agrees.
    #[test]
    fn snapping_never_manufactures_aggression(
        mantissa in -1_000_000_000_000i64..1_000_000_000_000,
        increment in 1i64..1_000_000,
    ) {
        let raw = Price::from_mantissa(mantissa);
        let bid = Side::Buy.snap_passive(raw, increment);
        let ask = Side::Sell.snap_passive(raw, increment);

        prop_assert!(bid.mantissa() <= raw.mantissa(), "a buy snapped UP, from {} to {}", raw.mantissa(), bid.mantissa());
        prop_assert!(ask.mantissa() >= raw.mantissa(), "a sell snapped DOWN, from {} to {}", raw.mantissa(), ask.mantissa());
        prop_assert_eq!(bid.mantissa().rem_euclid(increment), 0, "a buy left the grid");
        prop_assert_eq!(ask.mantissa().rem_euclid(increment), 0, "a sell left the grid");
        // Never further than one increment: rounding past a whole tick is a different price, not a
        // snap, and on a wide grid it would silently reprice the quote.
        prop_assert!(raw.mantissa() - bid.mantissa() < increment);
        prop_assert!(ask.mantissa() - raw.mantissa() < increment);
    }

    /// FITNESS: folding the venue's CUMULATIVE totals is idempotent under any redelivery order.
    /// Replaying the same reports in any order, with duplicates, must leave exactly the totals the
    /// highest report carried — which is what lets the engine hold no seen-set and allocate nothing.
    #[test]
    fn redelivered_fills_fold_once(
        steps in prop::collection::vec(1u8..20, 1..12),
        duplicates in prop::collection::vec(0usize..11, 0..12),
    ) {
        let mut cumulative = 0i64;
        let mut reports = Vec::new();
        for step in &steps {
            cumulative += i64::from(*step);
            let mut trade = event(ExecKind::ReportTrade, ClientOrderId(1));
            trade.cumulative_qty = Qty::from_mantissa(cumulative);
            trade.cumulative_quote = Notional::from_mantissa(cumulative * 100);
            reports.push(trade);
        }
        let highest = cumulative;
        for index in duplicates {
            if let Some(report) = reports.get(index % reports.len().max(1)).copied() {
                reports.push(report);
            }
        }

        let mut orders = OrderTable::new(&exec_capacity(1), RUN_NONCE);
        let (index, client_id) = orders
            .claim(buy_claim(QuoteLevel::ZERO, 100 * TICK, highest))
            .expect("a fresh table has a free slot");
        orders.apply(index, &event(ExecKind::AckPlaced, client_id), RECEIVED);
        let mut applied_base = 0i64;
        for report in &reports {
            applied_base += orders.apply(index, report, RECEIVED).fill.base.mantissa();
        }

        prop_assert_eq!(orders.slot(index).filled_base().mantissa(), highest, "the slot's total is not the venue's");
        prop_assert_eq!(applied_base, highest, "the deltas handed out do not sum to the total");
    }
}
