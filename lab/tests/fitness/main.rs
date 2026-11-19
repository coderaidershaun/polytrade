//! The fitness suite of `lab`: properties that a replay must keep for its verdict to be about the
//! run that traded.

mod draws;
mod fixtures;
mod messages;
/// The shipped Binance strategy, included from its folder so a re-fold folds the real one.
#[path = "../../../strategies/strat-micro-recorder/te-binance-spot-btcusdt/strategy.rs"]
mod micro_strategy;
/// The shipped Polymarket strategy, included for the same reason. Both include the strategy-level
/// `link_schema.rs`, which is the link schema the two engines share, so it loads twice here.
#[allow(clippy::duplicate_mod)]
#[path = "../../../strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/strategy.rs"]
mod poly_strategy;
mod refold_refusals;
mod refold_runs;
mod replay_driver;
mod runs;
mod scans;
mod venue;
