//! The execution tables take their size from the instrument rows of a run, and a start whose order
//! table a client order id cannot address is refused before anything starts.

use engine::hot::dispatch::{HotEngine, HotEngineSetup};
use engine::hot::exec::ExecCapacity;
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::AssetId;
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::ui::UiEvent;
use engine::runtime::StartError;
use engine::runtime::config::TrackerSpec;
use engine::runtime::registry::InstrumentRow;

use crate::engine_support::{
    Idle, ONE, account_chunk, detached_setup, instrument_row, spin, ts, ui_event_ring,
};

/// Seventeen instruments over seventeen assets: sixteen bases and the quote they share, which is
/// asset 16.
fn seventeen_rows() -> Vec<InstrumentRow> {
    (0..17)
        .map(|id| InstrumentRow {
            base_asset: AssetId(id % 16),
            quote_asset: AssetId(16),
            ..instrument_row(id, TrackerSpec::default(), 8)
        })
        .collect()
}

/// FITNESS: no constant caps the instruments or the assets of a run. Each table is as large as
/// the rows of the run need, and a balance that the venue states for any asset of those rows is
/// the balance that the engine holds. A capped table drops the asset past its cap and reads it as
/// zero, and an engine that reads its own quote balance as zero refuses or misjudges each order.
///
/// The one limit left is the client order id, which holds 16 bits of slot index. An order table
/// past it would mint two orders under one id, and the engine could no longer tell its own orders
/// apart, so the start is refused and names the table.
#[test]
fn start_beyond_capacity_is_refused_and_names_table() {
    let rows: Vec<InstrumentRow> = (0..1_025)
        .map(|id| instrument_row(id, TrackerSpec::default(), 8))
        .collect();
    let refused = ExecCapacity::of(&rows);
    assert!(
        matches!(
            refused,
            Err(StartError::ExecutionCapacity {
                table: "order slots",
                needed: 65_600,
                addressable: 65_536,
            })
        ),
        "1,025 instruments need 65,600 order slots, past the 65,536 that 16 bits address: {refused:?}"
    );
    let error = refused.expect_err("the start is refused");
    assert_eq!(error.failure_class(), FailureClass::Halt(Halt::Start));
    let text = error.to_string();
    assert!(
        ["order slots", "65600", "65536"]
            .iter()
            .all(|part| text.contains(part)),
        "the refusal names the table and both numbers: {text}"
    );

    let capacity = ExecCapacity::of(&rows[..1_024]).expect("1,024 instruments are addressable");
    assert_eq!(capacity.order_slots(), 65_536);
    assert_eq!(capacity.prior_run_slots(), 16_384);
    assert_eq!(capacity.command_bank(), 65_536 + 6_144);

    let rows = seventeen_rows();
    let (ui_event_sink, mut ui_events) = ui_event_ring(256);
    let mut engine = HotEngine::new(HotEngineSetup {
        ui_event_sink,
        ..detached_setup(&rows, Box::new(Idle))
    });
    let balances = account_chunk(&[(AssetId(16), 7 * ONE)], 1_000);
    engine.dispatch(&InboundMessage::new(
        ts(1_000),
        MessageBody::Account(balances),
    ));
    engine.dispatch(&spin(1, 2_000));
    let mut stated = None;
    while let Ok(event) = ui_events.pop() {
        if let UiEvent::Balance {
            asset: AssetId(16),
            free,
            ..
        } = event
        {
            stated = Some(free);
        }
    }
    assert_eq!(
        stated,
        Some(7 * ONE),
        "the shared quote, asset 16, has its own balance row"
    );
}
