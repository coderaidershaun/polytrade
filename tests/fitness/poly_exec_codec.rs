//! Polymarket execution codec goldens. With no testnet, these committed venue payloads are the only
//! place the normalisation layer is exercised before real money moves through it.
//!
//! Four judgements are pinned, because each is silent and expensive when wrong: the fill law (which
//! payload may be folded and which may not), the message-string → [`RejectClass`] table, the split
//! between an order rejection and a venue-wide outage, and the byte-exact place body — whose
//! signature is checked against a vector the official SDK minted, not against our own output.
//!
//! A fifth is the startup gate's protocol-version read, which must accept the real venue's JSON
//! shape and fail closed on anything else, because nothing else stands between it and a live run.
//!
//! A sixth is the settlement reading, which decides two things money depends on: when a reservation
//! may be released, and whether a failed settlement stops the run. Both are asked long after the
//! order behind the trade stopped being nameable, so both rest on what the payload itself says.

use std::collections::BTreeSet;

use polysim::adapters::decode::DecimalFault;
use polysim::adapters::exec::ExecRequest;
use polysim::adapters::polymarket::exec::codec::{
    AccountStamps, DecodeContext, EncodeContext, HttpAnswer, IgnoredReason, KnownOrder, OrderIndex,
    OrderSigner, OrderSignerSetup, OrdersRead, PROTOCOL_VERSION, PlaceRequestContext,
    PlacementStatus, RejectSubject, RejectVerdict, SettlementWatermark, StreamEvent, TokenBinding,
    TokenTable, TradeSettlement, VenueAnswer, VenueAvailability, VenueFailure, WireError,
    account_snapshot, cancel_market_orders, classify_error, decode_balance, decode_cancel,
    decode_clob_market, decode_closed_only, decode_orders_page, decode_place,
    decode_protocol_version, decode_single_order, decode_stream_frame, decode_trades_page,
    encode_request,
};
use polysim::adapters::polymarket::exec::sign::address::Address;
use polysim::adapters::polymarket::exec::sign::order::SignatureType;
use polysim::ids::{AssetId, ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{
    AccountChunkKind, ExecEvent, ExecKind, Liquidity, OrderStyle, RejectClass, VenueOrderStatus,
};
use polysim::time::TsUs;
use serde_json::Value;

use crate::poly_sign::{text, wallet_key};

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!("../../fixtures/polymarket/exec/", $name, ".json"))
    };
}

const SIGN_VECTORS: &str = include_str!("../../fixtures/polymarket/sign_vectors.json");

const NOW: TsUs = TsUs::from_micros(1_786_046_870_000_000);

const UP_TOKEN: &str =
    "112440684578249703625547365250874526064395690274269051885028145282946008600017";
const DOWN_TOKEN: &str =
    "36887535789881101410843950967599110889290657643254699143262364696546631284186";
const CONDITION: &str = "0x5df7ba83dd01a6010279e777ad80fb4ba1a3092cd5b5b119e2a387bc5b4c37c0";

const ORDER_A: &str = "0x3f1c9b4a72e5d08a1c6b3fd2094e7a5b81c02d6e9fa473b5c8e10d92a4f67b3c";
const ORDER_B: &str = "0x7d2e8a015c93b64fe0a17d3b28c95f4610ed7b3a92c4085fd61b73e2a908c54d";
const ORDER_FOREIGN: &str = "0xc4a90b7e13d582f6a08b4c17e29d360f5ba81c7d049e3b62f18a05c937de4b21";

const API_KEY: &str = "b1f2c3d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d";

const UP: InstrumentId = InstrumentId(3);
const DOWN: InstrumentId = InstrumentId(4);

const CLIENT_A: ClientOrderId = ClientOrderId(0x6a64_f040_000c_0001);
const CLIENT_B: ClientOrderId = ClientOrderId(0x6a64_f040_000c_0002);

/// `0.52` and `10` shares, the size every fixture is written around.
const PRICE: Price = Price(52_000_000);
const SIZE: Qty = Qty(10 * FIXED_SCALE);
const TICK: Price = Price(1_000_000);

fn tokens() -> TokenTable {
    let mut table = TokenTable::with_retired_capacity(4);
    table.bind(TokenBinding {
        instrument: UP,
        token_id: UP_TOKEN.into(),
        tick: TICK,
        is_neg_risk: false,
    });
    table.bind(TokenBinding {
        instrument: DOWN,
        token_id: DOWN_TOKEN.into(),
        tick: TICK,
        is_neg_risk: false,
    });
    table
}

/// The index the driver would hold once both placements had answered.
fn index() -> OrderIndex {
    let mut index = OrderIndex::with_capacity(16);
    for (venue_order_id, client_id) in [(ORDER_A, CLIENT_A), (ORDER_B, CLIENT_B)] {
        index
            .record(
                venue_order_id,
                KnownOrder {
                    client_id,
                    instrument: UP,
                },
            )
            .expect("index has room");
    }
    index
}

struct Wiring {
    tokens: TokenTable,
    orders: OrderIndex,
}

impl Wiring {
    fn new() -> Self {
        Self {
            tokens: tokens(),
            orders: index(),
        }
    }

    fn decode(&self) -> DecodeContext<'_> {
        DecodeContext {
            tokens: &self.tokens,
            orders: &self.orders,
            api_key: API_KEY,
            received_ts_us: NOW,
        }
    }
}

fn ok(body: &str) -> HttpAnswer<'_> {
    HttpAnswer { status: 200, body }
}

fn buy_request(client_id: ClientOrderId) -> PlaceRequestContext {
    PlaceRequestContext {
        instrument: UP,
        client_id,
        side: Side::Buy,
        price: PRICE,
        qty: SIZE,
    }
}

fn answered<T>(answer: VenueAnswer<T>) -> T {
    match answer {
        VenueAnswer::Answered(value) => value,
        VenueAnswer::Unavailable(availability) => {
            panic!("expected an order answer, got venue state {availability:?}")
        }
    }
}

fn stream(fixture: &str, wiring: &Wiring) -> StreamEvent {
    decode_stream_frame(fixture, &wiring.decode()).expect("committed fixture decodes")
}

fn stream_order(fixture: &str, wiring: &Wiring) -> ExecEvent {
    match stream(fixture, wiring) {
        StreamEvent::Order(event) => event,
        other => panic!("expected an order event, got {other:?}"),
    }
}

/// The fill law: which payload may be folded and which may not, pinned per response shape. The
/// live venue answers a matched taker with DECIMAL-DOLLAR amounts (`"2.549999"`), not the
/// doc-shaped 6-decimal integer the write surface signs — a taker that reads $2.55 as $0.0000255
/// spends real money against a phantom quote. A gateway 502/504 with an HTML body no message table
/// matches may have left the order LIVE, so it must class Ambiguous rather than Fatal: closing the
/// slot on a wrong Fatal orphans a resting order past every sweep.
#[test]
fn place_responses_decode_to_the_outcome_each_shape_demands() {
    let wiring = Wiring::new();

    // A taker fill folds from the place response amounts.
    {
        let outcome = answered(
            decode_place(
                ok(fixture!("place_matched")),
                &buy_request(CLIENT_B),
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(outcome.event.cumulative_qty, SIZE);
        assert_eq!(outcome.event.cumulative_quote, 520_000_000);
        assert_eq!(outcome.event.status, Some(VenueOrderStatus::Filled));
        assert_eq!(outcome.event.kind, ExecKind::AckPlaced);
        let placed = outcome.placed.expect("a matched placement still has an id");
        assert_eq!(&*placed.venue_order_id, ORDER_B);
        assert_eq!(placed.status, PlacementStatus::Matched);
    }

    // A matched taker folds the venue's decimal-dollar amounts, never a scaled integer.
    {
        let outcome = answered(
            decode_place(
                ok(fixture!("place_matched_live")),
                &buy_request(CLIENT_B),
                &wiring.decode(),
            )
            .expect("the live decimal-dollar amounts decode"),
        );
        assert_eq!(outcome.event.cumulative_qty, Qty(542_553_000));
        assert_eq!(outcome.event.cumulative_quote, 254_999_900);
        assert_eq!(outcome.event.status, Some(VenueOrderStatus::Filled));
        assert_eq!(outcome.event.kind, ExecKind::AckPlaced);
        assert_eq!(
            outcome
                .placed
                .expect("a matched placement still has an id")
                .status,
            PlacementStatus::Matched
        );
    }

    // A delayed placement is pending and not a fill.
    {
        let outcome = answered(
            decode_place(
                ok(fixture!("place_delayed")),
                &buy_request(CLIENT_B),
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(
            outcome.placed.expect("delayed is accepted").status,
            PlacementStatus::Delayed
        );
        assert_eq!(outcome.event.cumulative_qty, Qty(0));
        assert_eq!(outcome.event.status, Some(VenueOrderStatus::New));
        assert_eq!(outcome.event.reject, None);
    }

    // A resting order answers `live`, the commonest answer a maker ever gets: accepted and on the
    // book with nothing filled.
    {
        let outcome = answered(
            decode_place(
                ok(fixture!("place_live")),
                &buy_request(CLIENT_A),
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(
            outcome.placed.expect("a live placement rests").status,
            PlacementStatus::Live
        );
        assert_eq!(outcome.event.kind, ExecKind::AckPlaced);
        assert_eq!(outcome.event.status, Some(VenueOrderStatus::New));
        assert_eq!(outcome.event.cumulative_qty, Qty(0));
        assert_eq!(outcome.event.reject, None);
    }

    // An unmatched placement is live with empty amounts — the venue leaves these EMPTY rather
    // than "0"; a strict integer parse would fail the frame.
    {
        let outcome = answered(
            decode_place(
                ok(fixture!("place_unmatched")),
                &buy_request(CLIENT_A),
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(
            outcome.placed.expect("unmatched rests").status,
            PlacementStatus::Unmatched
        );
        assert_eq!(outcome.event.cumulative_qty, Qty(0));
        assert_eq!(outcome.event.cumulative_quote, 0);
    }

    // HTTP 200 with success:false is a rejection, not an accepted order.
    {
        let outcome = answered(
            decode_place(
                ok(fixture!("place_failure_balance")),
                &buy_request(CLIENT_A),
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(
            outcome.placed, None,
            "a refused placement has no venue id to record"
        );
        assert_eq!(outcome.event.kind, ExecKind::AckFailed);
        assert_eq!(
            outcome.event.reject,
            Some(RejectClass::Fatal),
            "branching on the http status alone reads this body as an accepted order"
        );
        assert_eq!(outcome.event.client_id, CLIENT_A);
    }

    // An unknown gateway 5xx on a place is ambiguous, not fatal — it may have left the order LIVE.
    {
        let bad_gateway = HttpAnswer {
            status: 502,
            body: "<html><body>502 Bad Gateway</body></html>",
        };
        let outcome = answered(
            decode_place(bad_gateway, &buy_request(CLIENT_A), &wiring.decode())
                .expect("a non-json 5xx body still decodes to a verdict"),
        );
        assert_eq!(outcome.placed, None);
        assert_eq!(outcome.event.reject, Some(RejectClass::Ambiguous));
    }
}

/// Two edge cases in `classify_error` outside the documented-error table below: the STATUS-only
/// fallback for a gateway failure no message table matches, and the one message ("order not
/// found") whose verdict depends entirely on which request asked.
#[test]
fn gateway_status_fallback_and_cancel_not_found_classify_correctly() {
    use RejectClass::{Ambiguous, Fatal};
    for (status, want) in [
        (502, Ambiguous),
        (504, Ambiguous),
        (500, Ambiguous),
        (418, Fatal),
        (400, Fatal),
    ] {
        assert_eq!(
            classify_error(
                VenueFailure::new(status, "<html>gateway</html>"),
                RejectSubject::Placement
            ),
            RejectVerdict::Order(want)
        );
    }

    // A cancel that finds no order is Gone only on the cancel path; anywhere else "not found" is
    // a claim about the request, not about the order.
    let message = "order not found";
    assert_eq!(
        classify_error(VenueFailure::new(400, message), RejectSubject::Cancellation),
        RejectVerdict::Order(RejectClass::Gone)
    );
    assert_eq!(
        classify_error(VenueFailure::new(400, message), RejectSubject::Read),
        RejectVerdict::Order(RejectClass::Ambiguous)
    );
}

#[test]
fn a_maker_fill_folds_from_the_cumulative_size_matched() {
    let wiring = Wiring::new();
    let partial = stream_order(fixture!("ws_order_update_partial"), &wiring);
    let filled = stream_order(fixture!("ws_order_update_filled"), &wiring);

    assert_eq!(partial.kind, ExecKind::ReportTrade);
    assert_eq!(partial.liquidity, Some(Liquidity::Maker));
    assert_eq!(partial.cumulative_qty, Qty(4 * FIXED_SCALE));
    assert_eq!(partial.cumulative_quote, 208_000_000);
    // The venue answers LIVE for a partly filled resting order and never says "partially filled".
    assert_eq!(partial.status, Some(VenueOrderStatus::PartiallyFilled));

    assert_eq!(filled.cumulative_qty, SIZE);
    assert_eq!(filled.status, Some(VenueOrderStatus::Filled));
    assert!(
        filled.cumulative_qty.0 > partial.cumulative_qty.0,
        "size_matched must be cumulative or the fold is a delta fold in disguise"
    );
}

/// The two order frames that report no fill of their own. A cancellation still carries the quantity
/// matched before it, because the engine's totals are cumulative and a cancel that zeroed them
/// would hand back a fill the venue had already settled.
#[test]
fn placement_and_cancellation_frames_report_their_own_kind() {
    let wiring = Wiring::new();

    let placed = stream_order(fixture!("ws_order_placement"), &wiring);
    assert_eq!(placed.kind, ExecKind::ReportNew);
    assert_eq!(placed.client_id, CLIENT_A);
    assert_eq!(placed.status, Some(VenueOrderStatus::New));
    assert_eq!(placed.liquidity, None);
    assert_eq!(placed.cumulative_qty, Qty(0));

    let canceled = stream_order(fixture!("ws_order_cancellation"), &wiring);
    assert_eq!(canceled.kind, ExecKind::ReportCanceled);
    assert_eq!(canceled.client_id, CLIENT_A);
    assert_eq!(canceled.status, Some(VenueOrderStatus::Canceled));
    assert_eq!(canceled.liquidity, None);
    assert_eq!(canceled.qty, SIZE);
    assert_eq!(canceled.cumulative_qty, Qty(4 * FIXED_SCALE));
    assert_eq!(canceled.cumulative_quote, 208_000_000);
}

/// Everything downstream of a trade frame that is NOT the failed↔matched settlement transition
/// pinned separately below: owner filtering, on-chain evidence, and ownership surviving past the
/// point the order behind a trade can no longer be named.
#[test]
fn trade_frames_decode_owner_filtering_and_settlement_evidence() {
    let wiring = Wiring::new();

    // Our own maker fill is owner-filtered and never read from the taker order.
    {
        let StreamEvent::Trade(made) = stream(fixture!("ws_trade_maker"), &wiring) else {
            panic!("maker trade fixture is a trade frame");
        };
        assert_eq!(made.role, Some(Liquidity::Maker));
        assert_eq!(made.maker_fills.len(), 1);
        assert_eq!(made.maker_fills[0].client_id, CLIENT_A);
        assert_eq!(made.maker_fills[0].matched, SIZE);
        assert_eq!(
            made.taker_order, None,
            "the taker order id on a maker trade belongs to the counterparty"
        );

        let StreamEvent::Trade(took) = stream(fixture!("ws_trade_taker"), &wiring) else {
            panic!("taker trade fixture is a trade frame");
        };
        assert_eq!(took.role, Some(Liquidity::Taker));
        assert_eq!(took.taker_order, Some(CLIENT_B));
        assert!(
            took.maker_fills.is_empty(),
            "maker_orders on a taker trade holds the counterparty; owner filtering is what \
             excludes it"
        );
    }

    // Only a trade the venue put ON CHAIN is evidence the money moved — a match is an off-chain
    // promise, and reading it as settled money frees a reservation against collateral the venue
    // is still holding.
    {
        let StreamEvent::Trade(mined) = stream(fixture!("ws_trade_mined"), &wiring) else {
            panic!("mined trade fixture is a trade frame");
        };
        assert_eq!(mined.settlement, TradeSettlement::Mined);
        assert!(mined.settlement.is_on_chain());

        let StreamEvent::Trade(matched) = stream(fixture!("ws_trade_maker"), &wiring) else {
            panic!("maker trade fixture is a trade frame");
        };
        assert!(
            !matched.settlement.is_on_chain(),
            "a match was read as settled money"
        );

        let StreamEvent::Trade(failed) = stream(fixture!("ws_trade_failed"), &wiring) else {
            panic!("failed trade fixture is a trade frame");
        };
        assert!(
            !failed.settlement.is_on_chain(),
            "a settlement that never happened was read as settled money"
        );
    }

    // Ownership survives once the order behind a trade can no longer be named — the
    // venue-stamped credential on each maker leg is all that is left by then.
    {
        let forgotten = Wiring {
            tokens: tokens(),
            orders: OrderIndex::with_capacity(16),
        };
        let StreamEvent::Trade(ours) = stream(fixture!("ws_trade_failed"), &forgotten) else {
            panic!("failed trade fixture is a trade frame");
        };
        assert!(
            ours.maker_fills.is_empty() && ours.taker_order.is_none(),
            "the index this pin exists for must name nothing"
        );
        assert!(
            ours.is_ours,
            "our own failed settlement read as a stranger's"
        );

        let StreamEvent::Trade(theirs) = stream(fixture!("ws_trade_failed_foreign"), &forgotten)
        else {
            panic!("foreign failed trade fixture is a trade frame");
        };
        assert!(
            !theirs.is_ours,
            "a failure the venue attributes to another credential would halt a healthy run"
        );
    }
}

/// A trade-id dedup applied naively drops this transition: the same `venue_trade_id` walks every
/// settlement step (MATCHED -> MINED -> CONFIRMED/FAILED), so a trade first seen MATCHED is
/// already deduped, and a later FAILED update on the same id would vanish — settlement failure
/// never detected. This case stays its own named test so the specific regression stays legible.
#[test]
fn a_failed_settlement_is_terminal_and_distinguishable() {
    let wiring = Wiring::new();
    let StreamEvent::Trade(failed) = stream(fixture!("ws_trade_failed"), &wiring) else {
        panic!("failed trade fixture is a trade frame");
    };
    assert_eq!(failed.settlement, TradeSettlement::Failed);
    assert!(failed.settlement.is_terminal());

    let StreamEvent::Trade(matched) = stream(fixture!("ws_trade_maker"), &wiring) else {
        panic!("maker trade fixture is a trade frame");
    };
    assert_eq!(matched.settlement, TradeSettlement::Matched);
    assert!(!matched.settlement.is_terminal());
}

#[test]
fn a_partial_cancel_reports_both_halves() {
    let wiring = Wiring::new();
    let events = answered(
        decode_cancel(ok(fixture!("cancel_partial")), &wiring.decode())
            .expect("committed fixture decodes"),
    );
    assert_eq!(events.len(), 2);

    let cancelled = events
        .iter()
        .find(|event| event.client_id == CLIENT_A)
        .expect("the cancelled order");
    assert_eq!(cancelled.kind, ExecKind::AckCanceled);
    assert_eq!(cancelled.status, Some(VenueOrderStatus::Canceled));

    let refused = events
        .iter()
        .find(|event| event.client_id == CLIENT_B)
        .expect("the declined order");
    assert_eq!(refused.reject, Some(RejectClass::Ambiguous));
}

/// Page decoding: which orders a page can name, and where the list truly ends. `LTE=` is this
/// venue's end-of-list marker, so a decoder that reports it as a cursor tells the resync there is
/// another page and the resync answers by walking forever.
#[test]
fn open_orders_and_trades_pages_paginate_and_separate_unmapped_orders() {
    let wiring = Wiring::new();

    // One mapped order plus the end marker; the unmapped one is NOT an event, because
    // classifying it is a decision only the driver can make.
    {
        let decoded = answered(
            decode_orders_page(
                ok(fixture!("data_orders_wrapped")),
                OrdersRead {
                    instrument: UP,
                    recon_seq: 7,
                },
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(decoded.events.len(), 2);
        assert_eq!(decoded.events[0].client_id, CLIENT_A);
        assert_eq!(decoded.events[0].kind, ExecKind::SnapshotOrder);
        assert_eq!(decoded.events[0].cumulative_qty, Qty(4 * FIXED_SCALE));
        assert_eq!(decoded.events[0].recon_seq, 7);
        assert_eq!(decoded.events[1].kind, ExecKind::SnapshotEnd);
        assert_eq!(decoded.unmapped.len(), 1);
        assert_eq!(&*decoded.unmapped[0].venue_order_id, ORDER_FOREIGN);
        assert_eq!(decoded.unmapped[0].instrument, DOWN);
        assert_eq!(decoded.unmapped[0].side, Side::Sell);
        assert_eq!(decoded.next_cursor, None);
    }

    // Anything other than `LTE=` is a real page the pass has not read yet, and finishing without
    // it claims a completeness it never had.
    {
        let more = r#"{"next_cursor":"MTAw","data":[]}"#;
        let decoded = answered(
            decode_orders_page(
                ok(more),
                OrdersRead {
                    instrument: UP,
                    recon_seq: 1,
                },
                &wiring.decode(),
            )
            .expect("a well-formed page decodes"),
        );
        assert_eq!(decoded.next_cursor.as_deref(), Some("MTAw"));

        let trades = answered(
            decode_trades_page(ok(r#"{"next_cursor":"LTE=","data":[]}"#), &wiring.decode())
                .expect("a well-formed page decodes"),
        );
        assert_eq!(trades.next_cursor, None);
    }
}

/// Single-fixture reads through four different decoders — a bare-object order, a two-role trades
/// page, six-decimal balance rescaling, and the tick read off market metadata.
#[test]
fn single_fixture_reads_decode_their_documented_shape() {
    let wiring = Wiring::new();

    // The single order read answers a bare object.
    {
        let event = answered(
            decode_single_order(ok(fixture!("data_order_single")), 3, &wiring.decode())
                .expect("committed fixture decodes"),
        )
        .expect("the fixture names an order this run placed");
        assert_eq!(event.client_id, CLIENT_A);
        assert_eq!(event.status, Some(VenueOrderStatus::PartiallyFilled));
        assert_eq!(event.qty, SIZE);
    }

    // A status spelling this engine does not know is refused rather than mapped to a near match.
    // The refusal names the field, carries the venue's own spelling rather than the normalised one
    // an operator would never find in the payload, and is not fatal.
    {
        let body = fixture!("data_order_single").replace(
            "\"status\": \"LIVE\"",
            "\"status\": \"ORDER_STATUS_Paused\"",
        );
        let refused = decode_single_order(ok(&body), 3, &wiring.decode())
            .expect_err("an unknown order status is refused");
        match &refused {
            WireError::Decode(DecimalFault::UnknownEnum { field, value }) => {
                assert_eq!(*field, "status");
                assert_eq!(&**value, "ORDER_STATUS_Paused");
            }
            other => panic!("expected an unknown-status refusal, got {other:?}"),
        }
        assert!(!refused.is_fatal());
    }

    // The trades page decodes both roles.
    {
        let page = answered(
            decode_trades_page(ok(fixture!("data_trades")), &wiring.decode())
                .expect("committed fixture decodes"),
        );
        let trades = page.trades;
        assert_eq!(trades.len(), 2);
        assert_eq!(page.next_cursor, None);
        assert_eq!(trades[0].role, Some(Liquidity::Maker));
        assert_eq!(trades[1].role, Some(Liquidity::Taker));
        assert_ne!(trades[0].venue_trade_id, trades[1].venue_trade_id);
        assert_ne!(trades[0].trade_id, trades[1].trade_id);
    }

    // Balances rescale from the venue's six decimals.
    {
        let quote = AssetId(0);
        let balance = answered(
            decode_balance(
                ok(fixture!("balance_allowance_collateral")),
                quote,
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(balance.free, 7_436_809_900);
        assert_eq!(balance.asset, quote);

        let shares = answered(
            decode_balance(
                ok(fixture!("balance_allowance_conditional")),
                AssetId(1),
                &wiring.decode(),
            )
            .expect("committed fixture decodes"),
        );
        assert_eq!(shares.free, 10 * FIXED_SCALE);
    }

    // Market metadata reads the tick without a float.
    {
        let market =
            decode_clob_market(fixture!("clob_market")).expect("committed fixture decodes");
        assert_eq!(market.condition_id.as_ref(), CONDITION);
        assert_eq!(market.tick_size, TICK);
        assert_eq!(market.min_order_size, Qty(5 * FIXED_SCALE));
        assert_eq!(market.tokens.len(), 2);
        assert_eq!(market.tokens[0].token_id.as_ref(), UP_TOKEN);
        assert_eq!(market.tokens[0].outcome.as_ref(), "Up");
        assert!(market.is_accepting_orders);
        assert!(market.has_taker_delay);
    }
}

const NONE: SettlementWatermark = SettlementWatermark::NONE;

fn stamps(settled_through: SettlementWatermark) -> AccountStamps {
    AccountStamps {
        settled_through,
        received_ts_us: NOW,
    }
}

/// Chunking and stamping policy for `account_snapshot`: only the last chunk arms readiness, and a
/// chunk is stamped by settlement, never by our own clock. This venue publishes no account clock
/// for that stamp to come from — reading ours in makes every chunk look newer than the last, and on
/// this venue a balance read taken right after a fill still answers the pre-fill number.
#[test]
fn account_snapshot_chunks_arm_readiness_and_stamp_from_settlement() {
    // Only the last chunk of a balance sweep arms readiness.
    {
        let balances: Vec<_> = (0..3)
            .map(|index| polysim::msg::exec::AssetBalance {
                asset: AssetId(index),
                free: 100,
                locked: 0,
            })
            .collect();
        let chunks = account_snapshot(&balances, AccountChunkKind::Snapshot, stamps(NONE));
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_last_chunk);
        assert_eq!(chunks[0].len, 3);
        let empty = account_snapshot(&[], AccountChunkKind::Snapshot, stamps(NONE));
        assert_eq!(empty.len(), 1);
        assert!(empty[0].is_last_chunk);
        assert_eq!(empty[0].len, 0);
    }

    // A balance chunk is stamped by settlement, not by our clock.
    {
        let balances = [polysim::msg::exec::AssetBalance {
            asset: AssetId(1),
            free: 100,
            locked: 0,
        }];
        let chunks = account_snapshot(&balances, AccountChunkKind::Snapshot, stamps(NONE));
        assert_eq!(
            chunks[0].venue_update_ts_ms, 0,
            "a run that has watched nothing settle has no evidence to stamp"
        );

        let wiring = Wiring::new();
        let StreamEvent::Trade(mined) = stream(fixture!("ws_trade_mined"), &wiring) else {
            panic!("mined trade fixture is a trade frame");
        };
        let mut settled = NONE;
        assert!(settled.advance_to(mined.exchange_ts_us), "first settlement");
        assert!(
            !settled.advance_to(mined.exchange_ts_us),
            "the same trade re-sends once per settlement step, and only the first is new evidence"
        );
        let chunks = account_snapshot(&balances, AccountChunkKind::Update, stamps(settled));
        assert_eq!(
            chunks[0].venue_update_ts_ms, 1_786_046_865_000,
            "the chunk carries something other than the settled trade's own venue stamp"
        );
    }
}

/// The startup gate's own reads, both of which must FAIL CLOSED. The protocol version is read as a
/// number: a comparison of the raw body to `"2"` never matches the live venue's `{"version":2}`, so
/// the engine would refuse to arm on the real venue every time. The closed-only flag is read
/// strictly: a defaulted `bool` would read an error envelope or `{}` as "may trade" and arm a
/// closed-only account into quoting one side straight into rejection.
#[test]
fn the_startup_gates_read_fail_closed() {
    // The protocol version is read as a number from both shapes.
    {
        let live = decode_protocol_version(fixture!("version"));
        assert_eq!(live, Some(2));
        assert_eq!(live, Some(PROTOCOL_VERSION));
        assert_eq!(decode_protocol_version("2"), Some(PROTOCOL_VERSION));
    }

    // A body carrying no closed-only flag refuses the arm. Absent field, error envelope, empty
    // object, non-json — every one is "cannot prove it".
    {
        assert_eq!(decode_closed_only(r#"{"closed_only":true}"#), Some(true));
        assert_eq!(decode_closed_only(r#"{"closed_only":false}"#), Some(false));
        assert_eq!(decode_closed_only("{}"), None);
        assert_eq!(decode_closed_only(r#"{"error":"unauthorized"}"#), None);
        assert_eq!(
            decode_closed_only("<html>500 Internal Server Error</html>"),
            None
        );
        assert_eq!(decode_closed_only(""), None);
    }
}

fn expected_verdicts() -> Vec<(&'static str, RejectVerdict)> {
    use RejectClass::{Ambiguous, Fatal, Gone, Refused, StillLive};
    let order = RejectVerdict::Order;
    vec![
        ("unauthorized", order(Fatal)),
        ("invalid_l1_headers", order(Fatal)),
        ("invalid_order_payload", order(Fatal)),
        ("owner_mismatch", order(Fatal)),
        ("signer_mismatch", order(Fatal)),
        ("address_banned", order(Fatal)),
        ("closed_only_mode", order(Fatal)),
        ("not_enough_balance", order(Fatal)),
        ("tick_size_rule", order(Fatal)),
        ("below_minimum_size", order(Fatal)),
        ("duplicated", order(Ambiguous)),
        ("post_only_crosses_book", order(Refused)),
        ("invalid_expiration", order(Fatal)),
        ("fok_not_fully_filled", order(Refused)),
        ("fak_no_match", order(Refused)),
        ("market_not_ready", order(Refused)),
        ("order_match_delayed", order(StillLive)),
        ("ctf_contract_cancel", order(Gone)),
        ("order_timed_out", order(Refused)),
        ("no_matching_orders", order(Refused)),
        ("rounding_issues", order(Refused)),
        (
            "matching_engine_restart",
            RejectVerdict::Venue(VenueAvailability::Restarting),
        ),
        (
            "rate_limited",
            RejectVerdict::Venue(VenueAvailability::RateLimited {
                retry_after_secs: Some(3),
            }),
        ),
        (
            "trading_disabled",
            RejectVerdict::Venue(VenueAvailability::TradingDisabled),
        ),
        (
            "cancel_only",
            RejectVerdict::Venue(VenueAvailability::CancelOnly),
        ),
        (
            "post_only_mode",
            RejectVerdict::Venue(VenueAvailability::PostOnlyMode {
                retry_after_secs: Some(120),
            }),
        ),
    ]
}

struct ErrorCase {
    name: String,
    status: u16,
    message: String,
    code: String,
    retry_after_secs: Option<i64>,
}

fn error_cases() -> Vec<ErrorCase> {
    let document: Value =
        serde_json::from_str(fixture!("errors")).expect("error fixture is valid json");
    document["cases"]
        .as_array()
        .expect("cases is an array")
        .iter()
        .map(|case| {
            let body = &case["body"];
            ErrorCase {
                name: case["name"].as_str().expect("case name").to_owned(),
                status: case["status"].as_u64().expect("case status") as u16,
                // A string body is the venue's bytes verbatim, which is how the empty 425 is
                // expressed; an object body carries the message under `error`.
                message: body
                    .get("error")
                    .and_then(Value::as_str)
                    .or_else(|| body.as_str())
                    .unwrap_or_default()
                    .to_owned(),
                code: body["code"].as_str().unwrap_or_default().to_owned(),
                retry_after_secs: body["retry_after_seconds"].as_i64(),
            }
        })
        .collect()
}

/// Every documented error message classifies as written, and no venue-wide outage (425/429/503)
/// ever reaches a reject counter that would fire precisely while the engine is offline rather than
/// wrong.
#[test]
fn every_documented_error_classifies_as_written() {
    let cases = error_cases();
    let expected = expected_verdicts();

    let fixture_names: BTreeSet<&str> = cases.iter().map(|case| case.name.as_str()).collect();
    let pinned_names: BTreeSet<&str> = expected.iter().map(|(name, _)| *name).collect();
    assert_eq!(fixture_names, pinned_names);

    for (name, want) in expected {
        let case = cases
            .iter()
            .find(|case| case.name == name)
            .expect("names agree");
        let got = classify_error(
            VenueFailure {
                status: case.status,
                message: &case.message,
                code: &case.code,
                retry_after_secs: case.retry_after_secs,
            },
            RejectSubject::Placement,
        );
        assert_eq!(
            got, want,
            "message {name:?} classified {got:?}, expected {want:?}"
        );
        let is_outage = matches!(case.status, 425 | 429 | 503);
        assert_eq!(
            matches!(got, RejectVerdict::Venue(_)),
            is_outage,
            "outage classification for {name:?}"
        );
    }
}

#[test]
fn a_full_pause_is_distinguishable_from_a_pause_that_still_takes_cancels() {
    assert!(!VenueAvailability::TradingDisabled.allows_cancel());
    for state in [
        VenueAvailability::Restarting,
        VenueAvailability::CancelOnly,
        VenueAvailability::PostOnlyMode {
            retry_after_secs: None,
        },
        VenueAvailability::RateLimited {
            retry_after_secs: None,
        },
    ] {
        assert!(state.allows_cancel());
    }
}

fn sign_vector() -> Value {
    let vectors: Value =
        serde_json::from_str(SIGN_VECTORS).expect("sign vector fixture is valid json");
    vectors["orders"]
        .as_array()
        .expect("order vectors")
        .iter()
        .find(|vector| vector["signature_type"] == 2)
        .cloned()
        .expect("a gnosis safe order vector")
}

fn vector_signature_type(vector: &Value) -> u8 {
    vector["signature_type"]
        .as_u64()
        .expect("vector signature type") as u8
}

fn vector_signer(vector: &Value) -> OrderSigner {
    OrderSigner::new(OrderSignerSetup {
        key: wallet_key(vector),
        maker: Address::parse(text(vector, "maker")).expect("vector maker parses"),
        signer: Address::parse(text(vector, "signer")).expect("vector signer parses"),
        signature_type: SignatureType::from_code(vector_signature_type(vector))
            .expect("vector names a wallet type this engine knows"),
        api_key: API_KEY.to_owned(),
    })
}

#[test]
fn the_place_body_is_byte_exact_against_the_sdk_signature() {
    let vector = sign_vector();
    let signature = text(&vector, "expected_signature");
    let token = text(&vector, "token_id");
    let salt: u64 = text(&vector, "salt").parse().expect("vector salt");
    let timestamp_millis: i64 = text(&vector, "timestamp_millis")
        .parse()
        .expect("vector timestamp");
    let signer = vector_signer(&vector);

    let mut table = TokenTable::with_retired_capacity(2);
    table.bind(TokenBinding {
        instrument: UP,
        token_id: token.into(),
        tick: TICK,
        is_neg_risk: vector["neg_risk"].as_bool().expect("vector neg_risk"),
    });
    let orders = OrderIndex::with_capacity(4);
    let sent_ts_us = TsUs::from_micros(timestamp_millis * 1_000);
    let context = EncodeContext {
        tokens: &table,
        orders: &orders,
        signer: &signer,
        sent_ts_us,
    };

    // The salt is derived from the send stamp and the client id rather than drawn at random, so a
    // client id exists that lands on the vector's salt exactly. Solving for it is what lets an
    // SDK-minted signature be reproduced here at all.
    let client_id = ClientOrderId(sent_ts_us.micros() as u64 ^ salt);

    let encoded = encode_request(
        ExecRequest::Place {
            instrument: UP,
            client_id,
            side: Side::Buy,
            price: PRICE,
            qty: SIZE,
            style: OrderStyle::PostOnly,
        },
        &context,
    )
    .expect("the binding, price and size are all on the venue's grid");

    let expected = format!(
        concat!(
            r#"{{"order":{{"salt":{salt},"maker":"{maker}","signer":"{signer}","#,
            r#""tokenId":"{token}","makerAmount":"{maker_amount}","takerAmount":"{taker_amount}","#,
            r#""side":"BUY","expiration":"0","timestamp":"{timestamp}","#,
            r#""signatureType":{signature_type},"#,
            r#""signature":"{signature}","#,
            r#""metadata":"0x0000000000000000000000000000000000000000000000000000000000000000","#,
            r#""builder":"0x0000000000000000000000000000000000000000000000000000000000000000"}},"#,
            r#""owner":"{api_key}","orderType":"GTC","deferExec":false,"postOnly":true}}"#
        ),
        salt = salt,
        maker = text(&vector, "maker"),
        signer = text(&vector, "signer"),
        token = token,
        maker_amount = text(&vector, "maker_amount"),
        taker_amount = text(&vector, "taker_amount"),
        timestamp = timestamp_millis,
        signature_type = vector_signature_type(&vector),
        signature = signature,
        api_key = API_KEY,
    );
    assert_eq!(encoded.body, expected);
    assert_eq!(encoded.path, "/order");
    assert!(encoded.query.is_empty());
}

/// Encoding through the real signer: order-type/postOnly selection, cancel addressability
/// (mapped vs never-acknowledged), and read/rotation requests scoped by token — plus the token
/// table's own rebind policy, which every encoded request ultimately depends on.
#[test]
fn requests_encode_by_token_and_the_token_table_rebinds_correctly() {
    let vector = sign_vector();
    let signer = vector_signer(&vector);
    let table = tokens();
    let orders = index();
    let context = EncodeContext {
        tokens: &table,
        orders: &orders,
        signer: &signer,
        sent_ts_us: NOW,
    };

    // Post-only and immediate choose different order types.
    {
        let place = |style| {
            encode_request(
                ExecRequest::Place {
                    instrument: UP,
                    client_id: CLIENT_A,
                    side: Side::Buy,
                    price: PRICE,
                    qty: SIZE,
                    style,
                },
                &context,
            )
            .expect("both styles encode")
            .body
        };
        let resting = place(OrderStyle::PostOnly);
        assert!(resting.contains(r#""orderType":"GTC""#));
        assert!(resting.contains(r#""postOnly":true"#));

        let taking = place(OrderStyle::Immediate);
        assert!(taking.contains(r#""orderType":"FAK""#));
        assert!(taking.contains(r#""postOnly":false"#));
    }

    // An order the venue never acknowledged cannot be addressed.
    {
        let known = encode_request(
            ExecRequest::Cancel {
                instrument: UP,
                client_id: CLIENT_A,
            },
            &context,
        )
        .expect("a mapped order can be cancelled");
        assert!(known.body.contains(ORDER_A));
        assert_eq!(known.path, "/order");

        let unmapped = encode_request(
            ExecRequest::Cancel {
                instrument: UP,
                client_id: ClientOrderId(0xdead_beef),
            },
            &context,
        );
        assert!(unmapped.is_err());
    }

    // The open orders read scopes to a token outside the signature.
    {
        let encoded = encode_request(ExecRequest::OpenOrders { instrument: UP }, &context)
            .expect("bound token");
        assert_eq!(encoded.path, "/data/orders");
        assert_eq!(encoded.query, format!("asset_id={UP_TOKEN}"));
        assert!(encoded.body.is_empty());
    }

    // The rotation sweep cancels by token.
    {
        let encoded = cancel_market_orders(UP_TOKEN).expect("body serialises");
        assert_eq!(encoded.path, "/cancel-market-orders");
        assert!(encoded.body.contains(UP_TOKEN));
    }

    // A retired token still routes its late fills home.
    {
        let mut table = tokens();
        let stale_token = UP_TOKEN.to_owned();
        table.bind(TokenBinding {
            instrument: UP,
            token_id: "999888777666555444333222111".into(),
            tick: TICK,
            is_neg_risk: false,
        });
        assert_eq!(table.instrument(&stale_token), Some(UP));
        assert_eq!(
            table
                .live_binding(UP)
                .expect("the new binding is live")
                .token_id
                .as_ref(),
            "999888777666555444333222111"
        );
    }
}

/// A stream frame decodes to an `Ignored` verdict, and never to an event for the wrong order, when
/// it names a token this run does not track or an order whose placement answer has not landed.
#[test]
fn untracked_and_unmapped_stream_frames_decode_to_ignored() {
    let mut untracked = TokenTable::with_retired_capacity(2);
    untracked.bind(TokenBinding {
        instrument: DOWN,
        token_id: DOWN_TOKEN.into(),
        tick: TICK,
        is_neg_risk: false,
    });
    let wiring = Wiring {
        tokens: untracked,
        orders: index(),
    };
    let event = decode_stream_frame(fixture!("ws_order_placement"), &wiring.decode())
        .expect("committed fixture decodes");
    assert!(matches!(
        event,
        StreamEvent::Ignored(IgnoredReason::UntrackedToken)
    ));

    let wiring = Wiring {
        tokens: tokens(),
        orders: OrderIndex::with_capacity(4),
    };
    let event = decode_stream_frame(fixture!("ws_order_placement"), &wiring.decode())
        .expect("committed fixture decodes");
    assert!(matches!(
        event,
        StreamEvent::Ignored(IgnoredReason::UnknownOrder)
    ));
}

#[test]
fn adopting_an_order_repoints_it_rather_than_duplicating_it() {
    let mut index = OrderIndex::with_capacity(2);
    let first = KnownOrder {
        client_id: CLIENT_A,
        instrument: UP,
    };
    index.record(ORDER_A, first).expect("room");
    index
        .record(
            ORDER_A,
            KnownOrder {
                client_id: CLIENT_B,
                instrument: UP,
            },
        )
        .expect("re-recording replaces");
    assert_eq!(index.len(), 1);
    let resolved = index.resolve(ORDER_A).expect("still mapped");
    assert_eq!(resolved.client_id, CLIENT_B);
    assert!(index.forget(CLIENT_B));
    assert!(index.is_empty());
}
