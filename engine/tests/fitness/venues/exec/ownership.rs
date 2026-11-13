//! An order that this engine did not place is never mirrored and never cancelled, whatever client
//! order id the venue shows for it.

use engine::msg::exec::{CancelReason, ExecKind, Provenance};
use engine::msg::ids::InstrumentId;
use engine::msg::time::LocalTs;
use engine::runtime::config::RunIdentity;
use engine::runtime::registry::AssetDictionary;
use engine::venues::binance::exec::{
    DecodeContext, SymbolTable, classify_client_order_id, decode_order_record,
    parse_client_order_id,
};
use engine::venues::binance::rest::OrderRecord;
use engine::venues::exec::{EngineIdentity, ExecCore, ExecEffect, MirroredOrder, Phase, TeTag};
use proptest::prelude::*;

const INSTRUMENT: InstrumentId = InstrumentId(0);
const RECEIVED: LocalTs = LocalTs::from_micros(1_785_000_000_700_000);

fn engine_identity(run_nonce: u32) -> EngineIdentity {
    EngineIdentity {
        te_tag: TeTag::of(
            &RunIdentity::new("strat-identity", "te-foreign").expect("a valid identity"),
        ),
        run_nonce,
    }
}

/// A client order id that this engine never minted: the id of another engine, a near miss of an id
/// of this engine, or any text a person can type into the venue.
fn foreign_client_order_id(ours: TeTag) -> impl Strategy<Value = String> {
    let ours = ours.get();
    let near_miss = (any::<u64>(), 0..4_u8).prop_map(move |(id, miss)| {
        let own = format!("pd-{ours:08x}-{id:016x}");
        match miss {
            0 => own[..own.len() - 1].to_owned(),
            1 => format!("{own}0"),
            2 => own.replacen("pd-", "PD-", 1),
            _ => format!("pd-{ours:08x}_{id:016x}"),
        }
    });
    prop_oneof![
        (any::<u32>(), any::<u64>())
            .prop_filter("another engine", move |(tag, _)| *tag != ours)
            .prop_map(|(tag, id)| format!("pd-{tag:08x}-{id:016x}")),
        near_miss,
        "[.A-Z:/a-z0-9_-]{0,36}",
    ]
}

/// One open order on the venue, as the read of open orders reports it.
fn resting_order(client_order_id: &str) -> OrderRecord {
    serde_json::from_str(&format!(
        r#"{{"symbol":"BTCUSDT","orderId":12510053279,"clientOrderId":"{client_order_id}",
            "price":"118000.00000000","origQty":"0.00010000","executedQty":"0.00004000",
            "cummulativeQuoteQty":"4.72000000","status":"PARTIALLY_FILLED","timeInForce":"GTC",
            "type":"LIMIT_MAKER","side":"BUY","time":1785000000600,"updateTime":1785000000657}}"#
    ))
    .expect("the record parses")
}

fn quoting_core() -> ExecCore {
    let mut core = ExecCore::with_limits(8, 32);
    core.on_connected(&mut |_| {});
    core.on_stream_ready();
    assert_eq!(core.phase(), Phase::Quoting);
    core
}

proptest! {
    /// FITNESS: an order that this engine did not place belongs to a person or to another engine on
    /// the same account. A verdict that read one of them as this engine's would let the exit sweep
    /// cancel it, and no later check could tell. So for each client order id that this engine never
    /// minted, the codec says foreign, the open-orders read gives an event of a foreign order, the
    /// core keeps it out of the mirror, and a sweep sends no request.
    #[test]
    fn foreign_order_is_never_mirrored_or_cancelled(
        (identity, text) in any::<u32>().prop_flat_map(|run_nonce| {
            let identity = engine_identity(run_nonce.max(1));
            (Just(identity), foreign_client_order_id(identity.te_tag))
        }),
    ) {
        prop_assume!(
            parse_client_order_id(&text).is_none_or(|(tag, _)| tag != identity.te_tag)
        );
        prop_assert_eq!(
            classify_client_order_id(&text, identity).provenance,
            Provenance::Foreign
        );

        let symbols = SymbolTable::new([(INSTRUMENT, "BTCUSDT".into())]);
        let assets = AssetDictionary::default();
        let context = DecodeContext {
            symbols: &symbols,
            assets: &assets,
            identity,
            received_ts_us: RECEIVED,
        };
        let event = decode_order_record(&resting_order(&text), ExecKind::SnapshotOrder, 0, &context)
            .expect("the record decodes")
            .expect("the symbol is tracked");
        prop_assert_eq!(event.provenance, Provenance::Foreign);

        let mut core = quoting_core();
        let mut effects = Vec::new();
        core.on_event(&event, &mut |effect| effects.push(effect))
            .expect("a foreign order asks nothing of the mirror");
        core.observe_venue_order(MirroredOrder {
            instrument: INSTRUMENT,
            client_id: event.client_id,
            side: event.side,
            price: event.price,
            qty: event.qty,
            provenance: event.provenance,
            has_sent_cancel: false,
            is_ambiguous: false,
        })
        .expect("a foreign order asks nothing of the mirror");
        prop_assert!(core.mirror().is_empty(), "mirrored: {:?}", core.mirror());

        core.begin_sweep(CancelReason::Fatal, None, &mut |effect| effects.push(effect));
        let sent: Vec<_> = effects
            .iter()
            .filter(|effect| matches!(effect, ExecEffect::Send { .. }))
            .collect();
        prop_assert!(sent.is_empty(), "the core sent {sent:?}");
    }
}
