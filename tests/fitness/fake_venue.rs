//! Deterministic Binance Spot scenarios over the production simulator.

use polysim::adapters::binance::exec::{
    DecodeContext, EncodeContext, ResponseContext, SymbolTable, encode_request,
};
use polysim::adapters::exchange_sim::core::orders::{CORPUS_VENUE_ORDER_ID, SimOrder};
use polysim::adapters::exchange_sim::core::wallet::FillSettlement;
use polysim::adapters::exchange_sim::wire::{
    SimFill, VenueWire, response_messages, stream_messages,
};
use polysim::adapters::exec::{EngineIdentity, ExecRequest, TeTag};
use polysim::config::RunIdentity;
use polysim::ids::{AssetId, ClientOrderId, Price, Qty, Side, TradeId, VenueOrderId};
use polysim::msg::exec::{ExecEvent, OrderStyle, VenueOrderStatus};
use polysim::msg::inbound::InboundMessage;
use polysim::registry::{AssetDictionary, Registry};
use polysim::time::TsUs;

pub const STRATEGY_ID: &str = "strat-micro-recorder";
pub const TE_ID: &str = "te-binance-spot-btcusdt";
/// The run nonce the committed fixtures were minted under. A different one would make every fixture
/// order `PriorRun`, which is itself worth a test but is not the default scenario.
pub const RUN_NONCE: u32 = 1_785_000_000;

const FIRST_TRADE_ID: TradeId = TradeId(778_291);

#[derive(Debug, Clone)]
struct ScenarioOrders {
    resting: Vec<SimOrder>,
    latest_venue_order_id: VenueOrderId,
}

impl ScenarioOrders {
    fn new() -> Self {
        Self {
            resting: Vec::new(),
            latest_venue_order_id: CORPUS_VENUE_ORDER_ID,
        }
    }

    fn resting(&self) -> &[SimOrder] {
        &self.resting
    }

    fn get(&self, client_id: ClientOrderId) -> Option<&SimOrder> {
        self.resting
            .iter()
            .find(|order| order.client_id == client_id)
    }

    fn get_mut(&mut self, client_id: ClientOrderId) -> Option<&mut SimOrder> {
        self.resting
            .iter_mut()
            .find(|order| order.client_id == client_id)
    }

    fn insert(&mut self, order: SimOrder) {
        self.resting.push(order);
    }

    fn remove(&mut self, client_id: ClientOrderId) -> Option<SimOrder> {
        let at = self
            .resting
            .iter()
            .position(|order| order.client_id == client_id)?;
        Some(self.resting.remove(at))
    }

    fn mint_venue_order_id(&mut self) -> VenueOrderId {
        let next = self
            .latest_venue_order_id
            .0
            .checked_add(1)
            .expect("scenario venue order ids exhausted");
        self.latest_venue_order_id = VenueOrderId(next);
        self.latest_venue_order_id
    }

    fn latest_venue_order_id(&self) -> VenueOrderId {
        self.latest_venue_order_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Placement {
    client_id: ClientOrderId,
    side: Side,
    price: Price,
    qty: Qty,
    style: OrderStyle,
}

/// How the venue's two answers to one request are delivered. Real networks do all of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Delivery {
    /// The request's own answer first, then the account stream's.
    AckFirst,
    /// The account stream beat the response — routine, and the case a naive state machine breaks on.
    ReportFirst,
    /// The stream is down or lagging: only the request was answered.
    AckOnly,
    /// The response was lost; only the stream said anything.
    ReportOnly,
    /// Neither arrived. The order may or may not exist, which is the whole reason `Unknown` is a
    /// state rather than an error.
    Ambiguous,
    /// Binance redelivers. Every event arrives twice, and the fold must move the ledger once.
    Duplicated,
}

pub struct FakeVenue {
    registry: Registry,
    symbols: SymbolTable,
    identity: EngineIdentity,
    wire: VenueWire,
    orders: ScenarioOrders,
    best_bid: Price,
    best_ask: Price,
    delivery: Delivery,
    next_trade_id: TradeId,
}

impl FakeVenue {
    pub fn new() -> Self {
        let registry = spot_registry();
        let symbols = SymbolTable::from_registry(&registry);
        let identity = EngineIdentity {
            te_tag: TeTag::of(
                &RunIdentity::new(STRATEGY_ID, TE_ID).expect("fixture ids are well formed"),
            ),
            run_nonce: RUN_NONCE,
        };
        Self {
            registry,
            symbols,
            identity,
            wire: VenueWire::new(identity),
            orders: ScenarioOrders::new(),
            best_bid: Price(117_999 * polysim::ids::FIXED_SCALE),
            best_ask: Price(118_001 * polysim::ids::FIXED_SCALE),
            delivery: Delivery::AckFirst,
            next_trade_id: FIRST_TRADE_ID,
        }
    }

    pub fn identity(&self) -> EngineIdentity {
        self.identity
    }

    pub fn set_delivery(&mut self, delivery: Delivery) {
        self.delivery = delivery;
    }

    pub fn is_resting(&self, client_id: ClientOrderId) -> bool {
        self.orders.get(client_id).is_some()
    }

    pub fn decode_context(&self, at: TsUs) -> DecodeContext<'_> {
        DecodeContext {
            symbols: &self.symbols,
            assets: self.registry.assets(),
            identity: self.identity,
            received_ts_us: at,
        }
    }

    pub fn encode_context(&self) -> EncodeContext<'_> {
        EncodeContext {
            symbols: &self.symbols,
            identity: self.identity,
        }
    }

    pub fn assets(&self) -> &AssetDictionary {
        self.registry.assets()
    }

    /// Drive one request through the model. The returned messages are what the hot thread would
    /// receive, in the order the configured [`Delivery`] puts them.
    ///
    /// The request is ENCODED first and the encoding thrown away, so a scenario cannot reach the
    /// matching model with a request the encoder would have refused to build.
    pub fn submit(&mut self, request: ExecRequest, at: TsUs) -> Vec<InboundMessage> {
        encode_request(request, &self.encode_context()).expect("the encoder builds every request");

        let (ack, reports) = match request {
            ExecRequest::Place {
                client_id,
                side,
                price,
                qty,
                style,
                ..
            } => self.place(
                Placement {
                    client_id,
                    side,
                    price,
                    qty,
                    style,
                },
                at,
            ),
            ExecRequest::Cancel { client_id, .. } => self.cancel(client_id, at),
            ExecRequest::AmendQty { client_id, qty, .. } => self.amend(client_id, qty, at),
            ExecRequest::OrderStatus { client_id, .. } => (self.status(client_id, at), Vec::new()),
            ExecRequest::OpenOrders { .. } => (
                vec![self.wire.open_orders(self.orders.resting(), at)],
                Vec::new(),
            ),
            ExecRequest::SubscribeUserStream => (Vec::new(), Vec::new()),
        };
        self.deliver(request, ack, reports, at)
    }

    /// Fill `qty` of a resting order at its own price — the maker case, which is the only one a
    /// post-only strategy can reach.
    pub fn fill(&mut self, client_id: ClientOrderId, qty: Qty, at: TsUs) -> Vec<InboundMessage> {
        let Some(order) = self.orders.get_mut(client_id) else {
            return Vec::new();
        };
        let quote_before = order.filled_quote;
        let taken = order.take(qty);
        let order = *order;
        let trade_id = self.mint_trade_id();
        if order.is_complete() {
            self.orders.remove(client_id);
        }
        let settlement = settlement_of(&order, taken, quote_before);
        let report = self.wire.trade_report(
            &order,
            SimFill {
                trade_id,
                settlement: &settlement,
                fee_asset: "",
            },
            at,
        );
        self.stream_messages(&[report], at)
    }

    fn place(&mut self, placement: Placement, at: TsUs) -> (Vec<String>, Vec<String>) {
        let Placement {
            client_id,
            side,
            price,
            qty,
            style,
        } = placement;
        if self.would_cross(side, price, style) {
            let rejected = SimOrder {
                client_id,
                venue_order_id: VenueOrderId(-1),
                side,
                price,
                qty,
                filled: Qty(0),
                filled_quote: 0,
            };
            return (
                vec![self.wire.would_match_error()],
                vec![self.wire.rejected_report(&rejected, at)],
            );
        }
        let order = SimOrder {
            client_id,
            venue_order_id: self.orders.mint_venue_order_id(),
            side,
            price,
            qty,
            filled: Qty(0),
            filled_quote: 0,
        };
        self.orders.insert(order);
        (
            vec![self.wire.place_ack(&order, at)],
            vec![self.wire.new_report(&order, at)],
        )
    }

    fn cancel(&mut self, client_id: ClientOrderId, at: TsUs) -> (Vec<String>, Vec<String>) {
        let Some(order) = self.orders.remove(client_id) else {
            return (vec![self.wire.unknown_order_error()], Vec::new());
        };
        (
            vec![self.wire.cancel_ack(&order, at)],
            vec![self.wire.cancel_report(&order, at)],
        )
    }

    fn amend(
        &mut self,
        client_id: ClientOrderId,
        qty: Qty,
        at: TsUs,
    ) -> (Vec<String>, Vec<String>) {
        let Some(order) = self.orders.get_mut(client_id) else {
            return (vec![self.wire.unknown_order_error()], Vec::new());
        };
        order.qty = qty;
        let order = *order;
        (vec![self.wire.amend_ack(&order, at)], Vec::new())
    }

    fn status(&self, client_id: ClientOrderId, at: TsUs) -> Vec<String> {
        let wire = &self.wire;
        match self.orders.get(client_id) {
            Some(order) => {
                let status = match order.filled.0 > 0 {
                    true => VenueOrderStatus::PartiallyFilled,
                    false => VenueOrderStatus::New,
                };
                vec![wire.order_status_as(order, status, at)]
            }
            // Not resting: the model's answer to "what happened" is that it filled, which is the
            // reading a -2011 has to be reconciled against rather than assumed away.
            None => vec![wire.order_status_as(
                &SimOrder {
                    client_id,
                    venue_order_id: self.orders.latest_venue_order_id(),
                    side: Side::Buy,
                    price: self.best_bid,
                    qty: Qty(10_000),
                    filled: Qty(10_000),
                    filled_quote: self.best_bid.notional(Qty(10_000)),
                },
                VenueOrderStatus::Filled,
                at,
            )],
        }
    }

    fn would_cross(&self, side: Side, price: Price, style: OrderStyle) -> bool {
        if !matches!(style, OrderStyle::PostOnly) {
            return false;
        }
        match side {
            Side::Buy => price >= self.best_ask,
            Side::Sell => price <= self.best_bid,
        }
    }

    fn mint_trade_id(&mut self) -> TradeId {
        let minted = self.next_trade_id;
        self.next_trade_id = TradeId(minted.0 + 1);
        minted
    }

    fn deliver(
        &self,
        request: ExecRequest,
        ack: Vec<String>,
        reports: Vec<String>,
        at: TsUs,
    ) -> Vec<InboundMessage> {
        let acks = self.response_messages(request, &ack, at);
        let streamed = self.stream_messages(&reports, at);
        match self.delivery {
            Delivery::AckFirst => [acks, streamed].concat(),
            Delivery::ReportFirst => [streamed, acks].concat(),
            Delivery::AckOnly => acks,
            Delivery::ReportOnly => streamed,
            Delivery::Ambiguous => Vec::new(),
            Delivery::Duplicated => [acks.clone(), streamed.clone(), acks, streamed].concat(),
        }
    }

    fn response_messages(
        &self,
        request: ExecRequest,
        payloads: &[String],
        at: TsUs,
    ) -> Vec<InboundMessage> {
        response_messages(
            payloads,
            &ResponseContext {
                decode: self.decode_context(at),
                request,
                recon_seq: 0,
            },
        )
    }

    fn stream_messages(&self, payloads: &[String], at: TsUs) -> Vec<InboundMessage> {
        stream_messages(payloads, self.decode_context(at))
    }
}

/// Builds a fee-free settlement.
fn settlement_of(order: &SimOrder, last_qty: Qty, quote_before: i64) -> FillSettlement {
    let last_quote = order.filled_quote - quote_before;
    FillSettlement {
        last_qty,
        last_quote,
        cumulative_qty: order.filled,
        cumulative_quote: order.filled_quote,
        debit: last_quote,
        received_gross: last_qty.0,
        received_net: last_qty.0,
        fee: 0,
        fee_asset: AssetId::UNKNOWN,
    }
}

pub fn exec_events(messages: &[InboundMessage]) -> Vec<ExecEvent> {
    messages
        .iter()
        .filter_map(|message| match message {
            InboundMessage::Exec(event) => Some(*event),
            _ => None,
        })
        .collect()
}

/// One binance spot BTCUSDT source, built through the real config path so the asset dictionary is
/// the one production would hold: BTC and USDT interned, BNB and the dust assets unknown.
fn spot_registry() -> Registry {
    const YAML: &str = "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
source:
  exchange: binance
  max_exposure_quote: 500
  market: spot
  base: BTC
  quote: USDT
  tracker: {}
strategy:
  instruments: all
persistence:
  dir: ./data
logging:
  dir: ./logs
";
    let config: polysim::config::Config =
        polysim::config::Config::from_yaml(YAML).expect("the fake venue's config parses");
    Registry::build(&config).expect("the fake venue's registry builds")
}
