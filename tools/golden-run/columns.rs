//! The columns of each table in the order of the contract, written out by hand, so a reader that
//! checks a file against `expected.json` also checks the names and the order of its columns.

pub const ORDERS: &[&str] = &[
    "exchange_ts_us",
    "received_ts_us",
    "instrument_id",
    "client_order_id",
    "quote_level",
    "venue_order_id",
    "transition",
    "state",
    "previous_state",
    "provenance",
    "side",
    "style",
    "price",
    "qty",
    "filled_qty",
    "filled_quote",
    "reject_class",
    "reject_code",
    "depth_ahead",
];
pub const FILLS: &[&str] = &[
    "exchange_ts_us",
    "received_ts_us",
    "instrument_id",
    "trade_id",
    "venue_order_id",
    "client_order_id",
    "quote_level",
    "provenance",
    "side",
    "liquidity",
    "last_price",
    "last_qty",
    "booked_qty",
    "booked_quote",
    "commission",
    "commission_asset_id",
];
pub const INCIDENTS: &[&str] = &[
    "exchange_ts_us",
    "received_ts_us",
    "kind",
    "instrument_id",
    "client_order_id",
    "trade_id",
    "qty",
    "amount",
    "window_close_ts_us",
];
pub const LEDGER: &[&str] = &[
    "received_ts_us",
    "kind",
    "instrument_id",
    "position_base",
    "cash_quote",
    "basis_quote",
    "realised_quote",
    "mark",
    "exposure_quote",
    "pnl_quote",
    "client_order_id",
    "trade_id",
];
pub const ROTATIONS: &[&str] = &[
    "received_ts_us",
    "instrument_id",
    "window_open_ts_us",
    "window_close_ts_us",
    "token_id_up",
    "token_id_down",
    "condition_id",
];
pub const FEATURES: &[&str] = &["event_ts_us", "instrument_id", "feature_id", "value"];
pub const TRADES: &[&str] = &[
    "exchange_ts_us",
    "received_ts_us",
    "instrument_id",
    "price",
    "qty",
    "side",
];
pub const BOOK_EVENTS: &[&str] = &[
    "received_ts_us",
    "instrument_id",
    "kind",
    "side",
    "price",
    "qty",
    "update_id",
];
pub const KLINES: &[&str] = &[
    "exchange_ts_us",
    "received_ts_us",
    "instrument_id",
    "interval",
    "open_ts_us",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume",
    "trade_count",
    "is_closed",
];
pub const LINK_FRAMES: &[&str] = &[
    "received_ts_us",
    "event_ts_us",
    "kind",
    "sender_te_hash",
    "topic",
    "seq",
    "slot",
    "count",
    "value",
];
pub const MIDS: &[&str] = &[
    "event_ts_us",
    "spin_seq",
    "instrument_id",
    "bid",
    "ask",
    "mid",
];
pub const NO_QUOTES: &[&str] = &["event_ts_us", "spin_seq", "instrument_id", "side", "reason"];
pub const METRICS_LATENCY: &[&str] = &[
    "interval_start_ts_us",
    "interval_end_ts_us",
    "category",
    "stage",
    "count",
    "dropped",
    "backward",
    "negative",
    "p50_ns",
    "p99_ns",
    "p999_ns",
    "max_ns",
    "budget_p99_ns",
    "budget_p999_ns",
    "is_over_budget",
    "clock_source",
];
pub const METRICS_DROPS: &[&str] = &["interval_end_ts_us", "sink", "dropped_total"];
pub const METRICS_SENDER_DROPS: &[&str] = &[
    "interval_end_ts_us",
    "sender",
    "queue_full_total",
    "over_rate_total",
];
pub const METRICS_BACKLOG: &[&str] = &["interval_end_ts_us", "queue_id", "max", "ema"];
