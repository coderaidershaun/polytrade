"""The label sets of the data contract, written by `python -m posttrade.generate`."""

from enum import StrEnum

__all__ = [
    "BookEventKind",
    "BookSide",
    "ClockSource",
    "EndState",
    "ExecutionMode",
    "FeatureKind",
    "IncidentKind",
    "KlineInterval",
    "LatencyCategory",
    "LatencyStage",
    "LedgerKind",
    "LinkRowKind",
    "Liquidity",
    "NoQuoteReason",
    "OrderLifecycle",
    "OrderStyle",
    "OrderTransition",
    "Provenance",
    "RejectClass",
    "RunKind",
    "Side",
    "SinkKind",
]


class OrderTransition(StrEnum):
    PLACED = "placed"
    CANCEL_SENT = "cancel_sent"
    AMEND_SENT = "amend_sent"
    SEND_ABANDONED = "send_abandoned"
    TIMEOUT = "timeout"
    SWEEP_CLOSED = "sweep_closed"
    STREAM_RESET = "stream_reset"
    ACK_PLACED = "ack_placed"
    ACK_CANCELED = "ack_canceled"
    ACK_AMENDED = "ack_amended"
    ACK_FAILED = "ack_failed"
    REPORT_NEW = "report_new"
    REPORT_TRADE = "report_trade"
    REPORT_CANCELED = "report_canceled"
    REPORT_EXPIRED = "report_expired"
    REPORT_REJECTED = "report_rejected"
    REPORT_AMENDED = "report_amended"
    SNAPSHOT_ORDER = "snapshot_order"


class OrderLifecycle(StrEnum):
    FREE = "free"
    PENDING_NEW = "pending_new"
    LIVE = "live"
    CANCEL_IN_FLIGHT = "cancel_in_flight"
    AMEND_IN_FLIGHT = "amend_in_flight"
    UNKNOWN = "unknown"
    CLOSED_FILLED = "closed_filled"
    CLOSED_CANCELED = "closed_canceled"
    CLOSED_REJECTED = "closed_rejected"
    CLOSED_EXPIRED = "closed_expired"
    CLOSED_RECONCILED_GONE = "closed_reconciled_gone"


class IncidentKind(StrEnum):
    FEE_GAP = "fee_gap"
    STRANDED_POSITION = "stranded_position"
    ORPHAN_FILL = "orphan_fill"
    UNBOOKED_FEE = "unbooked_fee"


class LinkRowKind(StrEnum):
    PAYLOAD = "payload"
    RUN_RUNNING = "run_running"
    RUN_IDLE = "run_idle"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Provenance(StrEnum):
    MINE = "mine"
    PRIOR_RUN = "prior_run"
    FOREIGN = "foreign"


class OrderStyle(StrEnum):
    POST_ONLY = "post_only"
    IMMEDIATE = "immediate"
    UNKNOWN = "unknown"


class RejectClass(StrEnum):
    STILL_LIVE = "still_live"
    REFUSED = "refused"
    GONE = "gone"
    AMBIGUOUS = "ambiguous"
    FATAL = "fatal"
    NONE = "none"


class Liquidity(StrEnum):
    MAKER = "maker"
    TAKER = "taker"
    NONE = "none"


class KlineInterval(StrEnum):
    V_1m = "1m"
    V_3m = "3m"
    V_5m = "5m"
    V_15m = "15m"
    V_30m = "30m"
    V_1h = "1h"
    V_2h = "2h"
    V_4h = "4h"
    V_6h = "6h"
    V_8h = "8h"
    V_12h = "12h"
    V_1d = "1d"
    V_3d = "3d"
    V_1w = "1w"
    V_1M = "1M"


class BookEventKind(StrEnum):
    DELTA = "delta"
    SNAPSHOT = "snapshot"
    RESET = "reset"


class BookSide(StrEnum):
    BUY = "buy"
    SELL = "sell"
    NONE = "none"


class LedgerKind(StrEnum):
    BOOT = "boot"
    FILL = "fill"
    INTERVAL = "interval"
    ROTATION = "rotation"


class NoQuoteReason(StrEnum):
    QTY_BELOW_MIN = "qty_below_min"
    NOTIONAL_BELOW_MIN = "notional_below_min"
    NOTIONAL_ABOVE_MAX = "notional_above_max"
    WOULD_CROSS = "would_cross"
    OUTSIDE_BAND = "outside_band"
    UNDERFUNDED = "underfunded"
    NOT_READY_STREAM = "not_ready_stream"
    NOT_READY_BALANCES = "not_ready_balances"
    NOT_READY_OPEN_ORDERS = "not_ready_open_orders"
    HALTED = "halted"
    SESSION_REDUCING_ONLY = "session_reducing_only"
    EXPOSURE_CEILING = "exposure_ceiling"
    NO_QUOTE_DECLARED = "no_quote_declared"
    BOOK_NOT_QUOTABLE = "book_not_quotable"
    DUPLICATE_PRICE = "duplicate_price"
    ORDER_LIMIT = "order_limit"
    RATE_BUDGET = "rate_budget"
    OUTSIDE_WINDOW = "outside_window"
    WARMING = "warming"
    IDS_SPENT = "ids_spent"


class LatencyCategory(StrEnum):
    TRADE = "trade"
    BOOK_DELTA = "book_delta"
    BOOK_SNAPSHOT = "book_snapshot"
    KLINE = "kline"
    SPIN = "spin"
    EXEC = "exec"
    COMMAND = "command"


class LatencyStage(StrEnum):
    VENUE_TO_ENGINE = "venue_to_engine"
    VENUE_INTERNAL = "venue_internal"
    WIRE_TO_MESSAGE = "wire_to_message"
    MESSAGE_TO_DECISION = "message_to_decision"
    QUEUE_WAIT = "queue_wait"
    PROCESSING = "processing"
    DECISION_TO_WIRE = "decision_to_wire"
    BANK_AND_LANE_WAIT = "bank_and_lane_wait"
    SEND = "send"
    ORDER_ROUND_TRIP = "order_round_trip"


class ClockSource(StrEnum):
    TSC = "tsc"
    CNTVCT_EL0 = "cntvct_el0"
    MONOTONIC = "monotonic"


class SinkKind(StrEnum):
    STAGE_SAMPLES = "stage_samples"
    COUNTER_SAMPLES = "counter_samples"
    WIRE_SAMPLES = "wire_samples"
    LATENCY_REPORTS = "latency_reports"
    UI_BOOKS = "ui_books"
    UI_EVENTS = "ui_events"
    LINK = "link"
    BANK_FEATURES = "bank_features"
    BANK_PERSIST = "bank_persist"
    BANK_LOGS = "bank_logs"
    BANK_LINK = "bank_link"
    EXPOSURE_SUPERSEDED = "exposure_superseded"
    RESEARCH_FEATURES = "research_features"
    RESEARCH_TRADES = "research_trades"
    RESEARCH_BOOK_EVENTS = "research_book_events"
    RESEARCH_KLINES = "research_klines"
    RESEARCH_LINK_FRAMES = "research_link_frames"
    RESEARCH_MIDS = "research_mids"
    RESEARCH_NO_QUOTES = "research_no_quotes"
    AUDIT_LOST = "audit_lost"
    TAPE_LOST = "tape_lost"
    ROLLUP_ROWS = "rollup_rows"


class ExecutionMode(StrEnum):
    ABSENT = "absent"
    OFF = "off"
    SIM = "sim"
    LIVE = "live"


class RunKind(StrEnum):
    LIVE = "live"
    SIM = "sim"
    REPLAY = "replay"
    BACKTEST = "backtest"


class EndState(StrEnum):
    OPEN = "open"
    GRACEFUL = "graceful"
    HALTED = "halted"
    DRAIN_FAILED = "drain_failed"
    DEADLINE_PASSED = "deadline_passed"
    START_REFUSED = "start_refused"
    REFUSED = "refused"


class FeatureKind(StrEnum):
    MARKET_ONLY = "market_only"
    EXECUTION_AWARE = "execution_aware"
