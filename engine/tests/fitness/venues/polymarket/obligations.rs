//! The written obligations of a venue execution edge, held for Polymarket: a stand-in venue answers
//! with the committed payloads of the venue, behind the real Polymarket codec.

use engine::msg::exec::{AccountChunkKind, ExecEvent};
use engine::msg::ids::{AssetId, ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{LocalTs, VenueTs};
use engine::venues::exec::ExecRequest;
use engine::venues::polymarket::exec::codec::{
    AccountStamps, DecodeContext, HttpAnswer, KnownOrder, OrderIndex, OrdersRead,
    PlaceRequestContext, SettlementWatermark, StreamEvent, TokenBinding, TokenTable, VenueAnswer,
    WireError, account_snapshot, decode_balance, decode_cancel, decode_orders_page, decode_place,
    decode_stream_frame, venue_order_id_digest,
};
use serde_json::Value;

use crate::venues::exec::obligations::{
    BalanceReport, EdgeHarness, NOW, venue_edge_keeps_each_written_obligation,
};

macro_rules! payload {
    ($name:literal) => {
        include_str!(concat!(
            "../../../../fixtures/polymarket/exec/",
            $name,
            ".json"
        ))
    };
}

/// The token of the up outcome and the api key that each committed payload names, which is no key.
const UP_TOKEN: &str =
    "112440684578249703625547365250874526064395690274269051885028145282946008600017";
const API_KEY: &str = "b1f2c3d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d";

/// An answer whose message the classifier of this build has never seen.
const UNMAPPED_REFUSAL: &str = r#"{"error":"A refusal this build has never seen."}"#;

/// A stand-in Polymarket: it names each order in its placement answer, states running filled
/// totals, and answers each read with the committed payloads, fields set for the order at hand.
pub(crate) struct PolymarketEdge {
    tokens: TokenTable,
    orders: OrderIndex,
    /// The filled total of each order, as the venue states it.
    matched: Vec<(ClientOrderId, Qty)>,
}

/// The venue mints the id of an order. This stand-in mints one from the client id, so that each
/// order of a test has its own.
fn venue_order_of(client_id: ClientOrderId) -> String {
    format!("0x{:064x}", client_id.0)
}

fn amount(qty: Qty) -> String {
    let mantissa = qty.mantissa();
    format!("{}.{:08}", mantissa / FIXED_SCALE, mantissa % FIXED_SCALE)
}

/// A committed payload with fields set.
fn with_fields(payload: &str, fields: &[(&str, &str)]) -> String {
    let mut document: Value = serde_json::from_str(payload).expect("the committed payload parses");
    for (field, value) in fields {
        document[*field] = Value::from(*value);
    }
    document.to_string()
}

impl PolymarketEdge {
    fn context(&self, at: LocalTs) -> DecodeContext<'_> {
        DecodeContext {
            tokens: &self.tokens,
            orders: &self.orders,
            api_key: API_KEY,
            received_ts_us: at,
        }
    }

    fn stream_order(&self, frame: &str, at: LocalTs) -> InboundMessage {
        match decode_stream_frame(frame, &self.context(at)).expect("the frame decodes") {
            StreamEvent::Order(event) => InboundMessage::new(at, MessageBody::Exec(event)),
            other => panic!("an order frame of a known order is an order event, got {other:?}"),
        }
    }

    fn place(&mut self, request: PlaceRequestContext, at: LocalTs) -> Vec<InboundMessage> {
        let venue_order = venue_order_of(request.client_id);
        let answer = with_fields(payload!("place_live"), &[("orderID", &venue_order)]);
        let answered = decode_place(
            HttpAnswer {
                status: 200,
                body: &answer,
            },
            &request,
            &self.context(at),
        )
        .expect("the answer decodes");
        let VenueAnswer::Answered(outcome) = answered else {
            panic!("a placement that rests is answered, not refused as venue state");
        };
        // As the actor does: the answer is the only place that names the order.
        let known = KnownOrder {
            client_id: request.client_id,
            instrument: request.instrument,
        };
        self.orders
            .record(&venue_order, known)
            .expect("the index has room");
        let placed = with_fields(payload!("ws_order_placement"), &[("id", &venue_order)]);
        vec![
            InboundMessage::new(at, MessageBody::Exec(outcome.event)),
            self.stream_order(&placed, at),
        ]
    }

    fn cancel(&mut self, client_id: ClientOrderId, at: LocalTs) -> Vec<InboundMessage> {
        let venue_order = venue_order_of(client_id);
        let answer = format!(r#"{{"canceled":["{venue_order}"],"not_canceled":{{}}}}"#);
        let answered = decode_cancel(
            HttpAnswer {
                status: 200,
                body: &answer,
            },
            &self.context(at),
        )
        .expect("the answer decodes");
        let VenueAnswer::Answered(events) = answered else {
            panic!("a cancel is answered, not refused as venue state");
        };
        events
            .into_iter()
            .map(|event| InboundMessage::new(at, MessageBody::Exec(event)))
            .collect()
    }
}

fn place_context(request: ExecRequest) -> PlaceRequestContext {
    let ExecRequest::Place {
        instrument,
        client_id,
        side,
        price,
        qty,
        ..
    } = request
    else {
        panic!("the stand-in refuses only placements in this suite, got {request:?}");
    };
    PlaceRequestContext {
        instrument,
        client_id,
        side,
        price,
        qty,
    }
}

impl EdgeHarness for PolymarketEdge {
    const VENUE: &'static str = "polymarket";
    const INSTRUMENT: InstrumentId = InstrumentId(0);
    /// The venue echoes no client id, so the nonce of a run never reaches its wire.
    const RUN_NONCE: u32 = 1_786_000_000;
    /// The price and the size that each committed payload is written around.
    const RESTING_BID: Price = Price::from_mantissa(52 * FIXED_SCALE / 100);
    const QTY: Qty = Qty::from_mantissa(10 * FIXED_SCALE);
    /// The venue answers a balance read with an absolute balance, and its stream reports no change
    /// of balance at all.
    const SENDS_BALANCE_DELTAS: bool = false;
    const QUEUES_THROUGH_FUNNEL: bool = true;

    fn new() -> Self {
        let mut tokens = TokenTable::with_retired_capacity(4);
        tokens.bind(TokenBinding {
            instrument: Self::INSTRUMENT,
            token_id: UP_TOKEN.into(),
            tick: Price::from_mantissa(FIXED_SCALE / 100),
            is_neg_risk: false,
        });
        Self {
            tokens,
            orders: OrderIndex::with_capacity(16),
            matched: Vec::new(),
        }
    }

    fn answer(&mut self, request: ExecRequest, at: LocalTs) -> Vec<InboundMessage> {
        match request {
            ExecRequest::Place { .. } => self.place(place_context(request), at),
            ExecRequest::Cancel { client_id, .. } => self.cancel(client_id, at),
            other => {
                panic!("the suite asks the stand-in only to place and to cancel, got {other:?}")
            }
        }
    }

    fn fill(&mut self, client_id: ClientOrderId, qty: Qty, at: LocalTs) -> Vec<InboundMessage> {
        let total = match self.matched.iter_mut().find(|(id, _)| *id == client_id) {
            Some((_, total)) => {
                *total = Qty::from_mantissa(total.mantissa() + qty.mantissa());
                *total
            }
            None => {
                self.matched.push((client_id, qty));
                qty
            }
        };
        let update = with_fields(
            payload!("ws_order_update_partial"),
            &[
                ("id", &venue_order_of(client_id)),
                ("size_matched", &amount(total)),
            ],
        );
        let report = self.stream_order(&update, at);
        // As the actor does after it folds an event: the next report states only its own step.
        if let MessageBody::Exec(event) = report.body {
            self.orders
                .record_matched(event.client_id, event.cumulative_qty);
        }
        vec![report]
    }

    fn balance_reports(&mut self) -> Vec<BalanceReport> {
        let read = |edge: &Self, payload: &str, asset: AssetId| {
            let answer = HttpAnswer {
                status: 200,
                body: payload,
            };
            match decode_balance(answer, asset, &edge.context(NOW)).expect("the read decodes") {
                VenueAnswer::Answered(balance) => balance,
                VenueAnswer::Unavailable(state) => panic!("a balance read answered {state:?}"),
            }
        };
        let balances = [
            read(self, payload!("balance_allowance_collateral"), AssetId(1)),
            read(self, payload!("balance_allowance_conditional"), AssetId(0)),
        ];
        let mut settled_through = SettlementWatermark::NONE;
        [1_786_046_865_257_000, 1_786_046_866_000_000]
            .into_iter()
            .map(|settled_us| {
                settled_through.advance_to(VenueTs::from_micros(settled_us));
                let stamps = AccountStamps {
                    settled_through,
                    received_ts_us: NOW,
                };
                BalanceReport::Chunks(account_snapshot(
                    &balances,
                    AccountChunkKind::Snapshot,
                    stamps,
                ))
            })
            .collect()
    }

    fn unmapped_refusal(&mut self, request: ExecRequest, at: LocalTs) -> Vec<ExecEvent> {
        let answer = HttpAnswer {
            status: 400,
            body: UNMAPPED_REFUSAL,
        };
        match decode_place(answer, &place_context(request), &self.context(at))
            .expect("an error answer decodes")
        {
            VenueAnswer::Answered(outcome) => vec![outcome.event],
            VenueAnswer::Unavailable(state) => {
                panic!("a refusal of the order read as venue state {state:?}")
            }
        }
    }

    fn pending_cancel(&mut self, _client_id: ClientOrderId, _at: LocalTs) -> Option<ExecEvent> {
        None
    }

    /// The venue order id that the codec names for the record of the page that it cannot decode.
    fn undecodable_open_order(&mut self, client_id: ClientOrderId) -> (i64, bool) {
        let venue_order = venue_order_of(client_id);
        let known = KnownOrder {
            client_id,
            instrument: Self::INSTRUMENT,
        };
        self.orders
            .record(&venue_order, known)
            .expect("the index has room");
        let mut page: Value =
            serde_json::from_str(payload!("data_orders_wrapped")).expect("the page parses");
        let mut record = page["data"][0].take();
        record["id"] = Value::from(venue_order.as_str());
        record["price"] = Value::from("not-a-price");
        page["data"] = Value::from(vec![record]);
        let body = page.to_string();
        let read = OrdersRead {
            instrument: Self::INSTRUMENT,
            recon_seq: 0,
        };
        let answer = HttpAnswer {
            status: 200,
            body: &body,
        };
        match decode_orders_page(answer, read, &self.context(NOW)) {
            Ok(_) => (venue_order_id_digest(&venue_order).0, true),
            Err(WireError::UndecodableOpenOrder { venue_order_id, .. }) => {
                assert_eq!(
                    &*venue_order_id, venue_order,
                    "the codec names the record it cannot read"
                );
                (venue_order_id_digest(&venue_order_id).0, false)
            }
            Err(error) => {
                panic!("the codec cannot read the page and names none of its orders: {error}")
            }
        }
    }
}

venue_edge_keeps_each_written_obligation!(PolymarketEdge);
