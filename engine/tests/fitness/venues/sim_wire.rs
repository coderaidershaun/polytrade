//! The simulator's Binance-shaped payloads, all minted through its stamped wire, where the check of
//! book against wallet stops a fill that invents money.

use engine::msg::ids::{
    AssetId, ClientOrderId, FIXED_SCALE, Price, Qty, Side, TradeId, VenueOrderId,
};
use engine::msg::time::LocalTs;
use engine::runtime::config::RunIdentity;
use engine::venues::exec::{EngineIdentity, TeTag};
use engine::venues::sim::core::orders::SimOrder;
use engine::venues::sim::core::wallet::FillSettlement;
use engine::venues::sim::wire::{SimFill, VenueWire};

const REPORTED_AT: LocalTs = LocalTs::from_micros(1_785_000_123_456_789);
const TRADE_ID: TradeId = TradeId(778_291);

#[test]
fn a_settlement_agreeing_with_the_order_mints_a_partial_fill_report() {
    let order = half_filled_order();
    let settlement = settlement_of(&order);
    let report = wire().trade_report(
        &order,
        SimFill {
            trade_id: TRADE_ID,
            settlement: &settlement,
            fee_asset: "",
        },
        REPORTED_AT,
    );

    assert!(report.contains("PARTIALLY_FILLED"), "{report}");
    assert!(
        report.contains(&format!("\"t\":{}", TRADE_ID.0)),
        "the report names its trade: {report}"
    );
}

#[test]
#[should_panic(expected = "the venue's book and its wallet disagree")]
fn a_settlement_disagreeing_with_the_order_stops_the_trade_report() {
    let order = half_filled_order();
    let mut settlement = settlement_of(&order);
    settlement.cumulative_qty = Qty::from_mantissa(settlement.cumulative_qty.mantissa() + 1);

    wire().trade_report(
        &order,
        SimFill {
            trade_id: TRADE_ID,
            settlement: &settlement,
            fee_asset: "",
        },
        REPORTED_AT,
    );
}

fn wire() -> VenueWire {
    let identity = RunIdentity::new("strat-micro-recorder", "te-binance-spot-btcusdt")
        .expect("the fixture strategy and trading engine ids are well formed");
    VenueWire::new(EngineIdentity {
        te_tag: TeTag::of(&identity),
        run_nonce: 1_785_000_000,
    })
}

fn half_filled_order() -> SimOrder {
    let price = Price::from_mantissa(117_999 * FIXED_SCALE);
    let filled = Qty::from_mantissa(5_000);
    SimOrder {
        client_id: ClientOrderId(41),
        venue_order_id: VenueOrderId(12_510_053_280),
        side: Side::Buy,
        price,
        qty: Qty::from_mantissa(10_000),
        filled,
        filled_quote: price.notional(filled),
    }
}

fn settlement_of(order: &SimOrder) -> FillSettlement {
    FillSettlement {
        last_qty: order.filled,
        last_quote: order.filled_quote,
        cumulative_qty: order.filled,
        cumulative_quote: order.filled_quote,
        debit: order.filled_quote.mantissa(),
        received_gross: order.filled.mantissa(),
        received_net: order.filled.mantissa(),
        fee: 0,
        fee_asset: AssetId::UNKNOWN,
    }
}
