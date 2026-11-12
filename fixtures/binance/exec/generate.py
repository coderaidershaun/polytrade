#!/usr/bin/env python3
"""Regenerates every fixture in this directory.

It exists because the client order id format is a decision that will move at least once, and the ids
are embedded in 30 files across four different payload shapes. Editing them by hand guarantees that
some subset drifts — and the ones most likely to be missed are the hand-written edge cases, which
are exactly the ones that matter.

It is Python, and porting it to Rust would gut the suite: these fixtures test our DECODER, so a
generator built on our own wire types would only prove our decoder agrees with our encoder. The
question is whether it agrees with Binance. No Rust type flows in here, and that is deliberate.

To change the id format, change `client_order_id` below and re-run. Nothing else knows the format.

    python3 fixtures/binance/exec/generate.py

Field names and enum spellings come from binance-spot-api-docs (user-data-stream.md, enums.md,
web-socket-api.md, errors.md), verified 2026-07-27. Nothing here is derived from a real account.
"""

import collections
import json
import pathlib

OUT = pathlib.Path(__file__).parent

E = 1785000000658  # event time, ms
T = 1785000000657  # transact time, ms

# THE one statement of the wire form. Ruled 2026-07-27: `pd-{te_tag:08x}-{client_id:016x}`, 28 chars,
# inside Binance's newClientOrderId charset `[\.A-Z\:/a-z0-9_-]` and its 36-char limit. It must
# round-trip a ClientOrderId(u64), because slot addressing and the generation check decode straight
# out of it — which is why a human-readable sequence number is not good enough.
#
# The u64's layout, which is what makes these fixtures test anything at all:
#   bits 63..32  run_nonce   the process run that minted the order
#   bits 31..16  slot_index  which order slot it addresses
#   bits 15..0   generation  bumped on slot reuse, so a late report for a reaped order is detectable
STRATEGY_ID = "strat-micro-recorder"
TE_ID = "te-binance-spot-btcusdt"


def te_tag(strategy_id: str, te_id: str) -> int:
    """FNV-1a/32 over `strategy_id`, a NUL, then `te_id` — byte for byte what `TeTag::of` computes,
    so these fixtures classify through production's own arithmetic rather than a chosen constant."""
    digest = 0x811C9DC5
    for byte in strategy_id.encode() + b"\x00" + te_id.encode():
        digest = ((digest ^ byte) * 0x01000193) & 0xFFFFFFFF
    return digest


TE_TAG = te_tag(STRATEGY_ID, TE_ID)
RUN_NONCE = 0x6A64F040        # this run
PRIOR_RUN_NONCE = 0x6A64E230  # an EARLIER run of the SAME engine: swept at startup, never quoted


def client_order_id(slot: int, generation: int = 1, run_nonce: int = RUN_NONCE) -> str:
    """Every id this engine mints carries THIS engine's tag — there is deliberately no parameter to
    vary it.

    A differing tag is precisely what `Foreign` MEANS, and `Foreign` is the one provenance the
    engine must never cancel. An earlier version of this generator took a `te_tag` argument and used
    it to build the "prior run" fixture, which therefore encoded a HUMAN's order while being named
    for ours. A decoder test written against it would have asserted `PriorRun` on a payload meaning
    `Foreign`, passed, and cemented the engine cancelling somebody's manual order.

    `PriorRun` is the same tag with a different RUN_NONCE, in bits 63..32 — so that is the only knob
    here, and the mistake is now unrepresentable rather than merely fixed."""
    return f"pd-{TE_TAG:08x}-{(run_nonce << 32) | (slot << 16) | generation:016x}"


# Slot 12 is the resting quote every fixture is about.
MINE = client_order_id(12)
PRIOR = client_order_id(4, run_nonce=PRIOR_RUN_NONCE)

# Ids the VENUE mints, not us. Binance generates one whenever the caller supplies no replacement id,
# which this engine deliberately does not for cancels — so these address no slot at all. They appear
# as `clientOrderId` on a cancel or amend response and as `c` on a cancel report, with the order's
# real id alongside in `origClientOrderId` / `C`. A decoder keying on the obvious field addresses
# nothing, silently, which is the single trap this corpus exists to catch.
CANCEL_REQUEST = "4zR9HFcEq8gM1tWUqPEUHc"
AMENDED = "xbxXh5SSwaHS7oUEOCI88B"
FOREIGN = "web_a1b2c3d4e5f6"  # a human's order from the Binance UI: no tag, never ours to cancel


def write(name, payload):
    (OUT / f"{name}.json").write_text(json.dumps(payload, indent=2) + "\n")


def report(**over):
    """One executionReport, defaulted to a resting BUY. Key order follows the docs so a reader can
    diff a fixture against them line by line."""
    base = collections.OrderedDict([
        ("e", "executionReport"), ("E", E), ("s", "BTCUSDT"), ("c", MINE), ("S", "BUY"),
        ("o", "LIMIT_MAKER"), ("f", "GTC"), ("q", "0.00010000"), ("p", "118000.00000000"),
        ("P", "0.00000000"), ("F", "0.00000000"), ("g", -1), ("C", ""), ("x", "NEW"), ("X", "NEW"),
        ("r", "NONE"), ("i", 12510053279), ("l", "0.00000000"), ("z", "0.00000000"),
        ("L", "0.00000000"), ("n", "0"), ("N", None), ("T", T), ("t", -1), ("I", 8641984),
        ("w", True), ("m", False), ("M", False), ("O", T), ("Z", "0.00000000"),
        ("Y", "0.00000000"), ("Q", "0.00000000"), ("W", T), ("V", "NONE"),
    ])
    base.update(over)
    # userDataStream.subscribe wraps every event. The old listen-key stream did not, and a decoder
    # written to the bare shape reads null for every field and reports nothing at all.
    return {"subscriptionId": 0, "event": base}


# --- user stream: order lifecycle ---------------------------------------------------------------
write("report_new", report())

# l/L are THIS execution; z/Z are the running totals. Commission is charged in the asset RECEIVED,
# so a BUY pays in BTC, at the 10 bps the live account reports.
write("report_trade_partially_filled", report(
    x="TRADE", X="PARTIALLY_FILLED", l="0.00004000", z="0.00004000", L="118000.00000000",
    Y="4.72000000", Z="4.72000000", n="0.00000004", N="BTC", t=778291, m=True, w=True))

write("report_trade_filled", report(
    x="TRADE", X="FILLED", l="0.00006000", z="0.00010000", L="118000.00000000",
    Y="7.08000000", Z="11.80000000", n="0.00000006", N="BTC", t=778292, m=True, w=False))

# The BNB fee discount pays commission in an asset no configured instrument names -> AssetId::UNKNOWN.
write("report_trade_filled_unknown_commission_asset", report(
    x="TRADE", X="FILLED", l="0.00010000", z="0.00010000", L="118000.00000000",
    Y="11.80000000", Z="11.80000000", n="0.00013500", N="BNB", t=778293, m=True, w=False))

# C is the id of the order being cancelled; c is the cancel request's own id.
write("report_canceled", report(c=CANCEL_REQUEST, C=MINE, x="CANCELED", X="CANCELED", w=False))

# EXPIRED is the venue removing the order for a reason WE DID NOT CHOOSE — not the post-only case.
write("report_expired", report(x="EXPIRED", X="EXPIRED", w=False, eR="EXCHANGE_CANCELED"))

# THE post-only outcome on spot. WOULD_MATCH_IMMEDIATELY is an Order REJECT Reason and appears
# nowhere in the Order Expiry Reason table. Routine for a maker: the venue enforced what we asked.
write("report_rejected_would_match_immediately", report(
    x="REJECTED", X="REJECTED", r="WOULD_MATCH_IMMEDIATELY", i=-1, w=False))

# Same -2010 code as the post-only cross, and NOT routine — the account is out of money.
write("report_rejected_insufficient_balance", report(
    x="REJECTED", X="REJECTED", r="INSUFFICIENT_BALANCES", i=-1, w=False))

# An amend keeping queue priority reports as REPLACED. There is no AMENDMENT execution type.
write("report_replaced_by_amend", report(
    c=AMENDED, C=MINE, x="REPLACED", X="NEW", q="0.00006000"))

# Self-trade prevention killed it mid-match; v appears only here.
write("report_trade_prevention", report(
    x="TRADE_PREVENTION", X="EXPIRED_IN_MATCH", v=3, w=False, V="EXPIRE_MAKER"))

write("report_foreign_order", report(c=FOREIGN, x="NEW", X="NEW"))
write("report_prior_run_order", report(c=PRIOR, x="NEW", X="NEW"))

# The account stream is account-wide, so events arrive for symbols this engine does not track.
write("report_untracked_symbol", report(s="ETHUSDT", c=FOREIGN, x="NEW", X="NEW"))

# --- user stream: balances ----------------------------------------------------------------------
write("account_position", {"subscriptionId": 0, "event": collections.OrderedDict([
    ("e", "outboundAccountPosition"), ("E", E), ("u", T),
    ("B", [
        {"a": "BTC", "f": "0.00135871", "l": "0.00010000"},
        {"a": "USDT", "f": "171.14535000", "l": "11.80000000"},
        {"a": "BNB", "f": "0.00000000", "l": "0.00000000"},
        {"a": "EDG", "f": "0.00000123", "l": "0.00000000"},
    ]),
])})

# A NEGATIVE delta. balanceUpdate is a DELTA, which is why it may never become an AccountChunk —
# the edge answers it by asking for a fresh ABSOLUTE snapshot.
write("balance_update_negative", {"subscriptionId": 0, "event": collections.OrderedDict([
    ("e", "balanceUpdate"), ("E", E), ("a", "USDT"), ("d", "-25.50000000"), ("T", T),
])})

# --- ws api ---------------------------------------------------------------------------------------
RATE_LIMITS = [
    {"rateLimitType": "ORDERS", "interval": "SECOND", "intervalNum": 10, "limit": 100, "count": 12},
    {"rateLimitType": "ORDERS", "interval": "DAY", "intervalNum": 1, "limit": 200000, "count": 4043},
    {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 6000,
     "count": 321},
]


def order_result(**over):
    base = collections.OrderedDict([
        ("symbol", "BTCUSDT"), ("orderId", 12510053279), ("orderListId", -1),
        ("clientOrderId", MINE), ("transactTime", T), ("price", "118000.00000000"),
        ("origQty", "0.00010000"), ("executedQty", "0.00000000"),
        ("origQuoteOrderQty", "0.00000000"), ("cummulativeQuoteQty", "0.00000000"),
        ("status", "NEW"), ("timeInForce", "GTC"), ("type", "LIMIT_MAKER"), ("side", "BUY"),
        ("workingTime", T), ("selfTradePreventionMode", "NONE"),
    ])
    base.update(over)
    return base


write("ack_order_place", {
    "id": "e2a85d9f-07a5-4f94-8d5f-789dc3deb097", "status": 200,
    "result": order_result(), "rateLimits": RATE_LIMITS})

write("ack_order_cancel", {
    "id": "5633b6a2-90a9-4192-83e7-925c90b6a2fd", "status": 200,
    "result": order_result(
        origClientOrderId=MINE, clientOrderId=CANCEL_REQUEST, status="CANCELED"),
    "rateLimits": RATE_LIMITS})

# The venue mints a NEW clientOrderId here and returns the old as origClientOrderId — and the
# quantity key is `qty`, not `origQty` as everywhere else. A reused place-response reader gets null.
write("ack_order_amend_keep_priority", {
    "id": "56374a46-3061-486b-a311-89ee972eb648", "status": 200,
    "result": collections.OrderedDict([
        ("transactTime", T), ("executionId", 16),
        ("amendedOrder", collections.OrderedDict([
            ("symbol", "BTCUSDT"), ("orderId", 12510053279), ("orderListId", -1),
            ("origClientOrderId", MINE), ("clientOrderId", AMENDED),
            ("price", "118000.00000000"), ("qty", "0.00006000"),
            ("executedQty", "0.00000000"), ("preventedQty", "0.00000000"),
            ("quoteOrderQty", "0.00000000"), ("cumulativeQuoteQty", "0.00000000"),
            ("status", "NEW"), ("timeInForce", "GTC"), ("type", "LIMIT_MAKER"), ("side", "BUY"),
            ("workingTime", T), ("selfTradePreventionMode", "NONE"),
        ])),
    ]),
    "rateLimits": RATE_LIMITS})

# Named for what the engine must DO about each, because the number alone does not say: -2010 covers
# both a routine post-only cross and an account with no money left.
ERRORS = [
    ("error_2010_would_match_immediately", 400, -2010, "Order would immediately match and take."),
    ("error_2010_insufficient_balance", 400, -2010,
     "Account has insufficient balance for requested action."),
    ("error_2011_cancel_rejected", 400, -2011, "Unknown order sent."),
    ("error_2013_no_such_order", 400, -2013, "Order does not exist."),
    ("error_1021_timestamp_outside_recv_window", 400, -1021,
     "Timestamp for this request is outside of the recvWindow."),
    ("error_1013_filter_failure", 400, -1013, "Filter failure: MIN_NOTIONAL"),
    # -2038 ORDER_AMEND_REJECTED is a MANY-messages-one-code error, documented in the same table as
    # -2010 ("Messages for -1010, -2010, -2011 and -2038") with no standalone entry. These two are
    # the pair that proves it: only the first states the amend LIMIT — the one thing the venue ever
    # says about an order's amend budget — while the second is an ordinary refusal that must not be
    # read as one, because "budget spent" retires the amend primitive for that order for good.
    ("error_2038_amend_budget_spent", 400, -2038, "Filter failure: MAX_NUM_ORDER_AMENDS"),
    ("error_2038_amend_quantity_increase", 400, -2038,
     "Order amend (quantity increase) is not supported."),
    # Filter-failure MESSAGES are a table of their own and are not owned by any one code, so the same
    # string can arrive under -1013 FILTER_FAILURE. Only a rejection OF AN AMEND may speak about an
    # amend budget; this one is fatal and speaks about nothing.
    ("error_1013_amend_filter_failure", 400, -1013, "Filter failure: MAX_NUM_ORDER_AMENDS"),
]
for name, status, code, msg in ERRORS:
    write(name, {"id": "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0", "status": status,
                 "error": {"code": code, "msg": msg}, "rateLimits": RATE_LIMITS})

print(f"generated fixtures refreshed; {len(list(OUT.glob('*.json')))} fixtures present in {OUT}")
