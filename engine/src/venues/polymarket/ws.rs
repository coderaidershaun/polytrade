//! Polymarket CLOB market-channel addressing: one unauthenticated connection carries every live
//! slot token. Where to dial, and the wire messages the actor sends — the subscribe/unsubscribe ops
//! that let the 5-min rotation add the next window's tokens without a reconnect. Dialling is shared
//! (`adapters::socket`); reading, the client-driven `PING` keepalive, sequencing and parse all live
//! downstream in the actor.

use std::time::Duration;

use serde::Serialize;

use super::rotation::WindowTokens;

pub const MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Application-level PING→PONG (client-driven, not WS control).
pub const PING_INTERVAL: Duration = Duration::from_secs(10);

pub const PING: &str = "PING";

/// The venue's answer to [`PING`], and the only text on this socket that is not JSON.
pub const PONG: &str = "PONG";

const CHANNEL: &str = "market";

/// Zero-reconnect rotation via subscribe/unsubscribe on live socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsCommand {
    Subscribe(WindowTokens),
    Unsubscribe(WindowTokens),
}

pub fn subscribe_message(asset_ids: &[impl AsRef<str>]) -> String {
    let ids: Vec<&str> = asset_ids.iter().map(AsRef::as_ref).collect();
    to_json(&SubscribeMsg {
        assets_ids: &ids,
        channel: CHANNEL,
    })
}

pub fn operation_message(command: &WsCommand) -> String {
    let (operation, tokens) = match command {
        WsCommand::Subscribe(tokens) => ("subscribe", tokens),
        WsCommand::Unsubscribe(tokens) => ("unsubscribe", tokens),
    };
    to_json(&OperationMsg {
        operation,
        assets_ids: &[tokens.up.as_str(), tokens.down.as_str()],
        channel: CHANNEL,
    })
}

fn to_json<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serialize message")
}

#[derive(Serialize)]
struct SubscribeMsg<'a> {
    assets_ids: &'a [&'a str],
    #[serde(rename = "type")]
    channel: &'static str,
}

#[derive(Serialize)]
struct OperationMsg<'a> {
    operation: &'static str,
    assets_ids: &'a [&'a str],
    #[serde(rename = "type")]
    channel: &'static str,
}
