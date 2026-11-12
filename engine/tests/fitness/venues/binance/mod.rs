//! Tests of the Binance venue: parsing, sequencing, signing, the order codec, and its fake venue.

mod binance_depth_sequencing;
mod binance_kline_sequencing;
mod binance_parse;
mod binance_rest_plan;
mod binance_sign;
mod exec_codec;
/// The deterministic Binance Spot matching model. It is where every fill, partial fill,
/// post-only rejection and ack/report race is exercised, because production is the only other
/// venue this engine will ever reach.
mod fake_venue;
