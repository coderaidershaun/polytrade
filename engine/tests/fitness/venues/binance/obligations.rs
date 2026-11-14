//! The written obligations of a venue execution edge, held for Binance: the deterministic fake venue
//! answers behind the real Binance codec.

use engine::msg::exec::{ExecEvent, ExecKind};
use engine::msg::ids::{FIXED_SCALE, InstrumentId, Price, Qty};
use engine::msg::inbound::InboundMessage;
use engine::msg::time::LocalTs;
use engine::venues::binance::exec::{
    ResponseContext, StreamEvent, account_snapshot_chunks, decode_order_record, decode_response,
    decode_stream_event, format_client_order_id,
};
use engine::venues::binance::rest::{Balance, OrderRecord};
use engine::venues::exec::ExecRequest;

use super::fake_venue::{FakeVenue, RUN_NONCE};
use crate::venues::exec::obligations::{
    BalanceReport, EdgeHarness, venue_edge_keeps_each_written_obligation,
};

pub(crate) struct BinanceEdge {
    venue: FakeVenue,
}

/// An answer that the codec of this build knows no code for.
const UNMAPPED_REFUSAL: &str = r#"{"id":"0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0","status":400,
    "error":{"code":-9999,"msg":"A refusal this build has never seen."}}"#;

fn order_record(client_order_id: &str, price: &str, status: &str) -> OrderRecord {
    serde_json::from_str(&format!(
        r#"{{"symbol":"BTCUSDT","orderId":12510053279,"clientOrderId":"{client_order_id}",
            "price":"{price}","origQty":"0.00010000","executedQty":"0.00000000",
            "cummulativeQuoteQty":"0.00000000","status":"{status}","timeInForce":"GTC",
            "type":"LIMIT_MAKER","side":"BUY","time":1785000000600,"updateTime":1785000000657}}"#
    ))
    .expect("the record parses")
}

fn balance(asset: &str, free: &str) -> Balance {
    serde_json::from_str(&format!(
        r#"{{"asset":"{asset}","free":"{free}","locked":"0.00000000"}}"#
    ))
    .expect("the balance parses")
}

impl EdgeHarness for BinanceEdge {
    const VENUE: &'static str = "binance";
    const INSTRUMENT: InstrumentId = InstrumentId(0);
    const RUN_NONCE: u32 = RUN_NONCE;
    const RESTING_BID: Price = Price::from_mantissa(117_000 * FIXED_SCALE);
    const QTY: Qty = Qty::from_mantissa(10_000);

    fn new() -> Self {
        Self {
            venue: FakeVenue::new(),
        }
    }

    fn answer(&mut self, request: ExecRequest, at: LocalTs) -> Vec<InboundMessage> {
        self.venue.submit(request, at)
    }

    fn fill(
        &mut self,
        client_id: engine::msg::ids::ClientOrderId,
        qty: Qty,
        at: LocalTs,
    ) -> Vec<InboundMessage> {
        self.venue.fill(client_id, qty, at)
    }

    fn balance_reports(&mut self) -> Vec<BalanceReport> {
        let context = self
            .venue
            .decode_context(super::super::exec::obligations::NOW);
        let read = account_snapshot_chunks(
            &[
                balance("BTC", "0.00135871"),
                balance("USDT", "171.14535000"),
            ],
            1_785_000_000_600,
            &context,
        )
        .expect("the read decodes");
        let mut reports = vec![BalanceReport::Chunks(read)];
        for payload in [
            include_str!("../../../../fixtures/binance/exec/account_position.json"),
            include_str!("../../../../fixtures/binance/exec/balance_update_negative.json"),
        ] {
            reports.push(
                match decode_stream_event(payload, &context).expect("the payload decodes") {
                    StreamEvent::Account(chunks) => BalanceReport::Chunks(chunks),
                    StreamEvent::BalanceChanged => BalanceReport::Delta,
                    other => panic!("a balance payload decoded as {other:?}"),
                },
            );
        }
        reports
    }

    fn unmapped_refusal(&mut self, request: ExecRequest, at: LocalTs) -> Vec<ExecEvent> {
        decode_response(
            UNMAPPED_REFUSAL,
            &ResponseContext {
                decode: self.venue.decode_context(at),
                request,
                recon_seq: 0,
            },
        )
        .expect("an error answer decodes")
        .events
    }

    fn pending_cancel(
        &mut self,
        client_id: engine::msg::ids::ClientOrderId,
        at: LocalTs,
    ) -> ExecEvent {
        let text = format_client_order_id(self.venue.identity().te_tag, client_id);
        decode_order_record(
            &order_record(&text, "117000.00000000", "PENDING_CANCEL"),
            ExecKind::SnapshotOrder,
            0,
            &self.venue.decode_context(at),
        )
        .expect("the record decodes")
        .expect("the symbol is tracked")
    }

    fn undecodable_open_order(
        &mut self,
        client_id: engine::msg::ids::ClientOrderId,
    ) -> (i64, bool) {
        let text = format_client_order_id(self.venue.identity().te_tag, client_id);
        let record = order_record(&text, "not-a-price", "NEW");
        let decoded = decode_order_record(
            &record,
            ExecKind::SnapshotOrder,
            0,
            &self
                .venue
                .decode_context(super::super::exec::obligations::NOW),
        );
        (record.order_id, decoded.is_ok())
    }
}

venue_edge_keeps_each_written_obligation!(BinanceEdge);
