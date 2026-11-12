//! Client order id round trip: engine id <-> venue 28-char string.
//! Not display format; bits address slot + generation. Account stream: only ownership evidence.

use crate::adapters::exec::{EngineIdentity, OrderOwnership, TeTag};
use crate::ids::ClientOrderId;

const PREFIX: &str = "pd-";
const TAG_HEX: usize = 8;
const ID_HEX: usize = 16;

/// 28 characters, inside Binance's 36-character `^[\.A-Z:/a-z0-9_-]{1,36}$`.
pub const CLIENT_ORDER_ID_LEN: usize = PREFIX.len() + TAG_HEX + 1 + ID_HEX;

const TAG_START: usize = PREFIX.len();
const TAG_END: usize = TAG_START + TAG_HEX;

pub fn format_client_order_id(tag: TeTag, client_id: ClientOrderId) -> String {
    format!("{PREFIX}{:08x}-{:016x}", tag.get(), client_id.0)
}

pub fn parse_client_order_id(text: &str) -> Option<(TeTag, ClientOrderId)> {
    if text.len() != CLIENT_ORDER_ID_LEN || !text.starts_with(PREFIX) {
        return None;
    }
    let (tag, rest) = (&text[TAG_START..TAG_END], &text[TAG_END..]);
    let id = rest.strip_prefix('-')?;
    if !is_hex(tag) || !is_hex(id) {
        return None;
    }
    let tag = u32::from_str_radix(tag, 16).ok()?;
    let id = u64::from_str_radix(id, 16).ok()?;
    Some((TeTag::from_bits(tag), ClientOrderId(id)))
}

pub fn classify_client_order_id(text: &str, identity: EngineIdentity) -> OrderOwnership {
    let Some((tag, client_id)) = parse_client_order_id(text) else {
        return OrderOwnership::FOREIGN;
    };
    OrderOwnership::of(tag, client_id, identity)
}

// `from_str_radix` accepts leading `+` -> `pd-+1234567-…` aliases tag. Engine must reject.
fn is_hex(text: &str) -> bool {
    text.bytes().all(|byte| byte.is_ascii_hexdigit())
}
