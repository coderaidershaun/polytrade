//! The simulated venue's Binance-shaped payloads. Production mints every one of them through the
//! timestamped wire, so that is the only path on which the book-versus-wallet check can protect a
//! run: a fill report whose settlement totals disagree with the order it names is money invented
//! between the matching model and the wallet.

use polysim::adapters::exchange_sim::core::orders::SimOrder;
use polysim::adapters::exchange_sim::core::wallet::{
    FeeBps, FillSettlement, ReservationRequest, ReserveOutcome, SimWallet, SimWalletSetup,
};
use polysim::adapters::exchange_sim::wire::{SimFill, VenueWire};
use polysim::adapters::exec::{EngineIdentity, TeTag};
use polysim::config::RunIdentity;
use polysim::ids::{AssetId, ClientOrderId, FIXED_SCALE, Price, Qty, Side, TradeId, VenueOrderId};
use polysim::time::TsUs;

const REPORTED_AT: TsUs = TsUs::from_micros(1_785_000_123_456_789);
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
    settlement.cumulative_qty = Qty(settlement.cumulative_qty.0 + 1);

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

struct FeeRow {
    name: &'static str,
    side: Side,
    rate_bps_e8: i64,
    slices: &'static [i64],
    last_fee: i64,
    fee_asset: &'static str,
    last_received_net: i64,
    base_free: i64,
    quote_free: i64,
    wire_fee: &'static str,
}

#[test]
fn a_rebate_is_paid_in_quote_and_a_charge_in_the_asset_received() {
    const BASE: AssetId = AssetId(0);
    const QUOTE: AssetId = AssetId(1);
    let rows = [
        FeeRow {
            name: "rebated sell",
            side: Side::Sell,
            rate_bps_e8: -4 * FIXED_SCALE / 10,
            slices: &[10_000],
            last_fee: -47_199,
            fee_asset: "USDT",
            last_received_net: 1_179_990_000 + 47_199,
            base_free: 0,
            quote_free: 1_179_990_000 + 47_199,
            wire_fee: "-0.00047199",
        },
        FeeRow {
            name: "charged buy",
            side: Side::Buy,
            rate_bps_e8: 75 * FIXED_SCALE / 10,
            slices: &[10_000],
            last_fee: 7,
            fee_asset: "BTC",
            last_received_net: 9_993,
            base_free: 9_993,
            quote_free: 0,
            wire_fee: "0.00000007",
        },
        FeeRow {
            name: "rebated buy",
            side: Side::Buy,
            rate_bps_e8: -4 * FIXED_SCALE / 10,
            slices: &[5_000, 5_000],
            last_fee: -23_599,
            fee_asset: "USDT",
            last_received_net: 5_000,
            base_free: 10_000,
            quote_free: 2 * 23_599,
            wire_fee: "-0.00023599",
        },
    ];
    let price = Price(117_999 * FIXED_SCALE);
    let qty = Qty(10_000);

    for row in rows {
        let name = row.name;
        let (opening_base, opening_quote) = match row.side {
            Side::Buy => (0, price.notional(qty)),
            Side::Sell => (qty.0, 0),
        };
        let mut wallet = SimWallet::new(SimWalletSetup {
            base_asset: BASE,
            quote_asset: QUOTE,
            opening_base,
            opening_quote,
            maker_fee_bps: FeeBps::new(row.rate_bps_e8).expect("the row's rate is in range"),
        })
        .expect("two distinct assets and non-negative balances");
        let request = ReservationRequest {
            side: row.side,
            price,
            qty,
        };
        let ReserveOutcome::Reserved(mut reservation) = wallet.reserve(request) else {
            panic!("{name}: the opening balance covers the whole order");
        };

        let settlement = row
            .slices
            .iter()
            .map(|&slice| wallet.fill(&mut reservation, Qty(slice)))
            .last()
            .expect("every row fills at least once");

        let fee_asset = match settlement.fee_asset {
            BASE => "BTC",
            QUOTE => "USDT",
            other => panic!(
                "{name}: the fee was paid in asset {} of neither slot",
                other.0
            ),
        };
        assert_eq!(
            (settlement.fee, fee_asset, settlement.received_net),
            (row.last_fee, row.fee_asset, row.last_received_net),
            "{name}: the last fill's fee, the asset it is paid in, and what the received asset rose by"
        );
        let [base, quote] = wallet.settle(0).balances;
        assert_eq!(
            (base.free, quote.free),
            (row.base_free, row.quote_free),
            "{name}: free balances after the order is fully filled"
        );

        let order = SimOrder {
            client_id: ClientOrderId(42),
            venue_order_id: VenueOrderId(12_510_053_281),
            side: row.side,
            price,
            qty,
            filled: settlement.cumulative_qty,
            filled_quote: settlement.cumulative_quote,
        };
        let report = wire().trade_report(
            &order,
            SimFill {
                trade_id: TRADE_ID,
                settlement: &settlement,
                fee_asset,
            },
            REPORTED_AT,
        );
        assert!(
            report.contains(&format!("\"n\":\"{}\"", row.wire_fee)),
            "{name}: the wire commission amount: {report}"
        );
        assert!(
            report.contains(&format!("\"N\":\"{}\"", row.fee_asset)),
            "{name}: the wire commission asset: {report}"
        );
    }
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
    let price = Price(117_999 * FIXED_SCALE);
    let filled = Qty(5_000);
    SimOrder {
        client_id: ClientOrderId(41),
        venue_order_id: VenueOrderId(12_510_053_280),
        side: Side::Buy,
        price,
        qty: Qty(10_000),
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
        debit: order.filled_quote,
        received_gross: order.filled.0,
        received_net: order.filled.0,
        fee: 0,
        fee_asset: AssetId::UNKNOWN,
    }
}
