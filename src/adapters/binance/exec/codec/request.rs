//! Turns an engine request into a WebSocket API method and its parameters. The hot side names what
//! it wants and this picks the method. Only the signer adds `apiKey`, `recvWindow`, `timestamp` and
//! `signature`, so nothing here sets them.

use crate::adapters::exec::ExecRequest;
use crate::ids::{ClientOrderId, FIXED_SCALE, InstrumentId};
use crate::msg::exec::OrderStyle;

use super::super::sign::RequestParams;
use super::client_id::format_client_order_id;
use super::{EncodeContext, WireError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRequest {
    pub method: &'static str,
    pub params: RequestParams,
}

/// # Errors
/// The request names an instrument the symbol table does not hold, which is a wiring fault.
pub fn encode_request(
    request: ExecRequest,
    context: &EncodeContext<'_>,
) -> Result<EncodedRequest, WireError> {
    Ok(match request {
        ExecRequest::Place {
            instrument,
            client_id,
            side,
            price,
            qty,
            style,
        } => EncodedRequest {
            method: "order.place",
            params: style_params(
                RequestParams::new()
                    .set("symbol", symbol(instrument, context)?)
                    .set("side", side.wire_name())
                    .set("price", decimal(price.0))
                    .set("quantity", decimal(qty.0))
                    .set("newClientOrderId", client_order_id(client_id, context)),
                style,
            ),
        },
        ExecRequest::Cancel {
            instrument,
            client_id,
        } => EncodedRequest {
            method: "order.cancel",
            params: RequestParams::new()
                .set("symbol", symbol(instrument, context)?)
                .set("origClientOrderId", client_order_id(client_id, context)),
        },
        ExecRequest::AmendQty {
            instrument,
            client_id,
            qty,
        } => EncodedRequest {
            method: "order.amend.keepPriority",
            params: RequestParams::new()
                .set("symbol", symbol(instrument, context)?)
                .set("origClientOrderId", client_order_id(client_id, context))
                // Echoing the id back is what keeps the order addressable. Omitting this parameter
                // makes the venue mint a RANDOM replacement id, and the order would then be known
                // to the venue by a name no slot holds — every later cancel by client id would
                // answer -2011, which reads identically to "it filled".
                .set("newClientOrderId", client_order_id(client_id, context))
                .set("newQty", decimal(qty.0)),
        },
        ExecRequest::OrderStatus {
            instrument,
            client_id,
        } => EncodedRequest {
            method: "order.status",
            params: RequestParams::new()
                .set("symbol", symbol(instrument, context)?)
                .set("origClientOrderId", client_order_id(client_id, context)),
        },
        // Scoped to one symbol: the account-wide form costs weight 80 instead of 6 and returns
        // symbols this engine does not trade.
        ExecRequest::OpenOrders { instrument } => EncodedRequest {
            method: "openOrders.status",
            params: RequestParams::new().set("symbol", symbol(instrument, context)?),
        },
        // The unsigned `userDataStream.subscribe` needs an Ed25519 session, so this uses the
        // signature form, signed with HMAC.
        ExecRequest::SubscribeUserStream => EncodedRequest {
            method: "userDataStream.subscribe.signature",
            params: RequestParams::new(),
        },
    })
}

/// Pins `newOrderRespType=RESULT`, because the default differs by order type. `LIMIT_MAKER` takes
/// no `timeInForce`.
fn style_params(params: RequestParams, style: OrderStyle) -> RequestParams {
    let params = params.set("newOrderRespType", "RESULT");
    match style {
        OrderStyle::PostOnly => params.set("type", "LIMIT_MAKER"),
        OrderStyle::Immediate => params.set("type", "LIMIT").set("timeInForce", "GTC"),
    }
}

fn symbol(instrument: InstrumentId, context: &EncodeContext<'_>) -> Result<String, WireError> {
    context
        .symbols
        .symbol(instrument)
        .map(str::to_owned)
        .ok_or(WireError::UnknownInstrument {
            instrument: instrument.0,
        })
}

fn client_order_id(client_id: ClientOrderId, context: &EncodeContext<'_>) -> String {
    format_client_order_id(context.identity.te_tag, client_id)
}

/// Decimal places in an engine mantissa, i.e. `FIXED_SCALE == 10^FIXED_DECIMALS`.
const FIXED_DECIMALS: u32 = 8;

const _: () = assert!(FIXED_SCALE == 10_i64.pow(FIXED_DECIMALS));

/// Renders a mantissa as a decimal with trailing zeros trimmed, never passing through `f64`.
fn decimal(mantissa: i64) -> String {
    let scale = FIXED_SCALE as u64;
    // `unsigned_abs` avoids the overflow of negating `i64::MIN`.
    let magnitude = mantissa.unsigned_abs();
    let sign = match mantissa < 0 {
        true => "-",
        false => "",
    };
    let (whole, fraction) = (magnitude / scale, magnitude % scale);
    if fraction == 0 {
        return format!("{sign}{whole}");
    }
    let places = format!("{fraction:0width$}", width = FIXED_DECIMALS as usize);
    format!("{sign}{whole}.{}", places.trim_end_matches('0'))
}
