//! Turns an engine request into an HTTP call. The hot side names WHAT it wants; this picks the
//! endpoint, builds the body and signs the order inside it.
//!
//! Two rules bind every function here. The body string that comes back is the exact byte sequence
//! the socket must send, because the L2 HMAC covers it and re-serialising invalidates the header.
//! And the L2 preimage takes `path` WITHOUT `query` — signing `/data/orders?asset_id=x` where the
//! venue signs `/data/orders` fails auth with no clue as to why.

use crate::adapters::exec::ExecRequest;
use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::OrderStyle;
use crate::secrets::Secret;

use super::super::sign::address::Address;
use super::super::sign::amount::{AmountRequest, order_amounts};
use super::super::sign::eip712::ZERO_WORD;
use super::super::sign::key::SigningKey;
use super::super::sign::l2::HttpMethod;
use super::super::sign::order::{
    Exchange, ExchangeDomain, OrderSide, OrderSignError, SignatureType, SignedOrderFields, TokenId,
    salt, sign_order,
};
use super::wire::{
    CancelMarketOrdersBody, CancelOrderBody, HeartbeatBody, PlaceOrderBody, SignedOrderBody,
    SubscribeAuth, SubscribeBody,
};
use super::{EncodeContext, WireError};

const ZERO_WORD_HEX: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

/// GTC/FOK/FAK all require this; only GTD would carry a real value, and a 60-second security offset
/// plus a three-minute floor rules GTD out of a five-minute market entirely.
const NO_EXPIRATION: &str = "0";

/// The path is what gets signed. The query is appended to the URL after signing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRequest {
    pub method: HttpMethod,
    pub path: String,
    pub query: String,
    pub body: String,
}

impl EncodedRequest {
    fn new(method: HttpMethod, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
            query: String::new(),
            body: String::new(),
        }
    }

    fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = query.into();
        self
    }

    fn with_cursor(mut self, cursor: Option<&str>) -> Self {
        if let Some(cursor) = cursor {
            let separator = if self.query.is_empty() { "" } else { "&" };
            self.query = format!("{}{separator}next_cursor={cursor}", self.query);
        }
        self
    }

    fn with_body<T: serde::Serialize>(
        mut self,
        what: &'static str,
        body: &T,
    ) -> Result<Self, WireError> {
        self.body =
            serde_json::to_string(body).map_err(|source| WireError::Encoding { what, source })?;
        Ok(self)
    }
}

#[derive(Debug)]
pub struct OrderSignerSetup {
    pub key: SigningKey,
    /// Account wallet. The CLOB validates it against the api key's account.
    pub maker: Address,
    pub signer: Address,
    pub signature_type: SignatureType,
    pub api_key: String,
}

pub struct OrderSigner {
    setup: OrderSignerSetup,
    standard: ExchangeDomain,
    neg_risk: ExchangeDomain,
}

impl OrderSigner {
    pub fn new(setup: OrderSignerSetup) -> Self {
        Self {
            setup,
            standard: ExchangeDomain::new(Exchange::Standard),
            neg_risk: ExchangeDomain::new(Exchange::NegRisk),
        }
    }

    pub fn api_key(&self) -> &str {
        &self.setup.api_key
    }

    fn domain(&self, is_neg_risk: bool) -> &ExchangeDomain {
        match is_neg_risk {
            true => &self.neg_risk,
            false => &self.standard,
        }
    }
}

/// Hand-written: the derived form would render the signing key.
impl std::fmt::Debug for OrderSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrderSigner")
            .field("maker", &self.setup.maker)
            .field("signer", &self.setup.signer)
            .field("signature_type", &self.setup.signature_type)
            .finish_non_exhaustive()
    }
}

/// # Errors
/// The instrument has no live token binding, an order this venue never acknowledged is being
/// addressed, the price or size is off the venue's grid, or the request is one this venue does not
/// offer.
pub fn encode_request(
    request: ExecRequest,
    context: &EncodeContext<'_>,
) -> Result<EncodedRequest, WireError> {
    match request {
        ExecRequest::Place {
            instrument,
            client_id,
            side,
            price,
            qty,
            style,
        } => place(
            &PlaceRequest {
                instrument,
                client_id,
                side,
                price,
                qty,
                style,
            },
            context,
        ),
        ExecRequest::Cancel { client_id, .. } => {
            cancel_venue_order(venue_order_id(context, client_id)?)
        }
        ExecRequest::OpenOrders { instrument } => {
            // Token-scoped so other windows' markets don't leak in.
            let Some(binding) = context.tokens.live_binding(instrument) else {
                return Err(WireError::UnboundInstrument {
                    instrument: instrument.0,
                });
            };
            Ok(open_orders_for_token(&binding.token_id, None))
        }
        ExecRequest::OrderStatus { client_id, .. } => {
            let order_id = venue_order_id(context, client_id)?;
            Ok(EncodedRequest::new(
                HttpMethod::Get,
                format!("/data/order/{order_id}"),
            ))
        }
        ExecRequest::SubscribeUserStream => Err(WireError::UnsupportedRequest {
            request: "user stream subscription",
        }),
        // No amend on this venue. Preflight sets amend budget to zero, so every shrink degrades to
        // cancel and this arm never executes.
        ExecRequest::AmendQty { .. } => Err(WireError::UnsupportedRequest {
            request: "order amendment",
        }),
    }
}

fn venue_order_id<'a>(
    context: &EncodeContext<'a>,
    client_id: ClientOrderId,
) -> Result<&'a str, WireError> {
    context
        .orders
        .venue_order_id(client_id)
        .ok_or(WireError::UnknownOrder {
            client_id: client_id.0,
        })
}

struct PlaceRequest {
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    price: Price,
    qty: Qty,
    style: OrderStyle,
}

fn place(request: &PlaceRequest, context: &EncodeContext<'_>) -> Result<EncodedRequest, WireError> {
    let Some(binding) = context.tokens.live_binding(request.instrument) else {
        return Err(WireError::UnboundInstrument {
            instrument: request.instrument.0,
        });
    };
    let side = match request.side {
        Side::Buy => OrderSide::Buy,
        Side::Sell => OrderSide::Sell,
    };
    let amounts = order_amounts(&AmountRequest {
        side,
        price: request.price,
        size: request.qty,
        tick: binding.tick,
    })?;

    let signer = context.signer;
    let timestamp_millis = context.sent_ts_us.micros() / 1_000;
    let fields = SignedOrderFields {
        salt: salt(context.sent_ts_us, request.client_id.0),
        maker: signer.setup.maker,
        signer: signer.setup.signer,
        token_id: TokenId::parse(&binding.token_id)
            .map_err(|source| OrderSignError::TokenId { source })?,
        maker_amount: amounts.maker,
        taker_amount: amounts.taker,
        side,
        signature_type: signer.setup.signature_type,
        timestamp_millis,
        metadata: ZERO_WORD,
        builder: ZERO_WORD,
    };
    let signature = sign_order(
        &signer.setup.key,
        signer.domain(binding.is_neg_risk),
        &fields,
    )?;

    // Post-only is native to this venue; it rejects crossing orders. Immediate orders use FAK
    // instead of FOK so partial fills still reduce the position.
    let (order_type, post_only) = match request.style {
        OrderStyle::PostOnly => ("GTC", true),
        OrderStyle::Immediate => ("FAK", false),
    };

    let maker_hex = signer.setup.maker.to_checksum_hex();
    let signer_hex = signer.setup.signer.to_checksum_hex();
    EncodedRequest::new(HttpMethod::Post, "/order").with_body(
        "place order",
        &PlaceOrderBody {
            order: SignedOrderBody {
                salt: fields.salt,
                maker: &maker_hex,
                signer: &signer_hex,
                token_id: &binding.token_id,
                maker_amount: amounts.maker.to_string(),
                taker_amount: amounts.taker.to_string(),
                side: request.side.wire_name(),
                expiration: NO_EXPIRATION,
                timestamp: timestamp_millis.to_string(),
                signature_type: signer.setup.signature_type.code(),
                signature: signature.as_str(),
                metadata: ZERO_WORD_HEX,
                builder: ZERO_WORD_HEX,
            },
            owner: signer.api_key(),
            order_type,
            defer_exec: false,
            post_only,
        },
    )
}

/// # Errors
/// The body cannot be serialised.
pub fn cancel_venue_order(venue_order_id: &str) -> Result<EncodedRequest, WireError> {
    EncodedRequest::new(HttpMethod::Delete, "/order").with_body(
        "cancel order",
        &CancelOrderBody {
            order_id: venue_order_id,
        },
    )
}

/// Cancels everything resting on one token. Ordinary reconciliation cancels one order at a time.
///
/// # Errors
/// The body cannot be serialised.
pub fn cancel_market_orders(token_id: &str) -> Result<EncodedRequest, WireError> {
    EncodedRequest::new(HttpMethod::Delete, "/cancel-market-orders").with_body(
        "cancel market orders",
        &CancelMarketOrdersBody { asset_id: token_id },
    )
}

/// Public and unauthenticated: tick size, minimum size, fee schedule, both token ids and the taker
/// delay flag, in the one call a rotation binding needs.
pub fn clob_market_request(condition_id: &str) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, format!("/clob-markets/{condition_id}"))
}

/// The one fact [`clob_market_request`] omits. Public, and per token rather than per market.
pub fn neg_risk_request(token_id: &str) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/neg-risk").with_query(format!("token_id={token_id}"))
}

/// The venue's allowance cache must be warmed before a sell: an unrefreshed cache rejects the sell
/// as if the wallet were empty, and every rotation brings a token that has never been refreshed.
pub fn conditional_allowance_refresh(
    token_id: &str,
    signature_type: SignatureType,
) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/balance-allowance/update")
        .with_query(conditional_query(token_id, signature_type))
}

/// Collateral (pUSD) and per-token balances use the same endpoint but specify different asset
/// types.
pub fn collateral_balance(signature_type: SignatureType) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/balance-allowance").with_query(format!(
        "asset_type=COLLATERAL&signature_type={}",
        signature_type.code()
    ))
}

pub fn conditional_balance(token_id: &str, signature_type: SignatureType) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/balance-allowance")
        .with_query(conditional_query(token_id, signature_type))
}

fn conditional_query(token_id: &str, signature_type: SignatureType) -> String {
    format!(
        "asset_type=CONDITIONAL&token_id={token_id}&signature_type={}",
        signature_type.code()
    )
}

/// The venue's dead man's switch. A stale id is answered with the id the venue expects next.
///
/// # Errors
/// The body cannot be serialised.
pub fn heartbeat(heartbeat_id: &str) -> Result<EncodedRequest, WireError> {
    EncodedRequest::new(HttpMethod::Post, "/v1/heartbeats")
        .with_body("heartbeat", &HeartbeatBody { heartbeat_id })
}

/// Account-wide read to see orders from prior runs before token bindings land.
pub fn open_orders_page(cursor: Option<&str>) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/data/orders").with_cursor(cursor)
}

/// One token's open orders — the check a freshly bound window gets.
pub fn open_orders_for_token(token_id: &str, cursor: Option<&str>) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/data/orders")
        .with_query(format!("asset_id={token_id}"))
        .with_cursor(cursor)
}

/// The cursor rides in the query, which is not signed, so every page of the walk signs the same
/// path.
pub fn trades_page(cursor: Option<&str>) -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/data/trades").with_cursor(cursor)
}

pub fn protocol_version() -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/version")
}

pub fn server_time() -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/time")
}

pub fn closed_only_status() -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/auth/ban-status/closed-only")
}

/// The wallet key signs these L1 calls directly, because they are what mint the L2 credentials.
pub fn derive_api_key() -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Get, "/auth/derive-api-key")
}

pub fn create_api_key() -> EncodedRequest {
    EncodedRequest::new(HttpMethod::Post, "/auth/api-key")
}

/// The raw api secret crosses this channel, and there is no HMAC here. We subscribe unfiltered
/// to avoid gaps between unsubscribe and resubscribe.
///
/// # Errors
/// The secret or passphrase is not UTF-8, or the frame cannot be serialised.
pub fn subscribe_user_stream(
    api_key: &str,
    secret: &Secret,
    passphrase: &Secret,
) -> Result<String, WireError> {
    let frame = SubscribeBody {
        auth: SubscribeAuth {
            api_key,
            secret: secret_text("api secret", secret)?,
            passphrase: secret_text("api passphrase", passphrase)?,
        },
        channel: "user",
    };
    serde_json::to_string(&frame).map_err(|source| WireError::Encoding {
        what: "user stream subscription",
        source,
    })
}

fn secret_text<'a>(what: &'static str, secret: &'a Secret) -> Result<&'a str, WireError> {
    str::from_utf8(secret.expose_bytes()).map_err(|_| WireError::CredentialNotUtf8 { what })
}
