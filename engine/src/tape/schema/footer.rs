//! The footer of each file of a run: the keys that decode its dense ids and name its run. Each
//! file carries the whole footer of its own run, so a reader decodes it with no other file.

use crate::msg::labelled_enum::labelled_enum;
use crate::runtime::manifest::CONTRACT_VERSION;
use crate::tape::RunMeta;
use crate::tape::payload::TAPE_KINDS;

labelled_enum! {
    /// Each key of the footer, in the order of the contract. A key is appended, never renamed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub(in crate::tape) enum FooterKey {
        StrategyId = "strategy_id",
        TeId = "te_id",
        ExecutionMode = "execution_mode",
        FixedScale = "fixed_scale",
        FeatureDictionary = "feature_dictionary",
        InstrumentDictionary = "instrument_dictionary",
        AssetDictionary = "asset_dictionary",
        EngineVersion = "engine_version",
        ContractVersion = "contract_version",
        BootTsUs = "boot_ts_us",
        RunKind = "run_kind",
        LinkSlotNames = "link_slot_names",
        LinkTopicNames = "link_topic_names",
        SenderDictionary = "sender_dictionary",
        TapeKinds = "tape_kinds",
        FeatureKinds = "feature_kinds",
    }
    pub(in crate::tape) fn as_str;
    pub(in crate::tape) const ALL;
}

impl FooterKey {
    /// How a reader parses the value, as the contract names it.
    pub(in crate::tape) const fn form(self) -> &'static str {
        match self {
            FooterKey::StrategyId | FooterKey::TeId | FooterKey::EngineVersion => "text",
            FooterKey::ExecutionMode => "label:execution_mode",
            FooterKey::RunKind => "label:run_kind",
            FooterKey::FixedScale | FooterKey::ContractVersion | FooterKey::BootTsUs => "integer",
            FooterKey::FeatureDictionary
            | FooterKey::InstrumentDictionary
            | FooterKey::LinkSlotNames
            | FooterKey::LinkTopicNames => "json_string_array",
            FooterKey::AssetDictionary => "json_pair_array",
            FooterKey::SenderDictionary => "json_object",
            FooterKey::TapeKinds => "json_object_array",
            FooterKey::FeatureKinds => "json_label_array:feature_kind",
        }
    }

    fn value(self, meta: &RunMeta) -> String {
        match self {
            FooterKey::StrategyId => meta.strategy_id.as_str().to_owned(),
            FooterKey::TeId => meta.te_id.as_str().to_owned(),
            // `absent` keeps a missing execution block apart from `off`.
            FooterKey::ExecutionMode => meta.artifacts.footer_mode().to_owned(),
            FooterKey::FixedScale => meta.fixed_scale.to_string(),
            FooterKey::FeatureDictionary => json(&meta.feature_names),
            FooterKey::InstrumentDictionary => json(&meta.instrument_symbols),
            FooterKey::AssetDictionary => json(&meta.asset_symbols),
            FooterKey::EngineVersion => meta.engine_version.to_string(),
            FooterKey::ContractVersion => CONTRACT_VERSION.to_string(),
            FooterKey::BootTsUs => meta.run_id.to_string(),
            FooterKey::RunKind => meta.run_kind.as_str().to_owned(),
            FooterKey::LinkSlotNames => json(&meta.link_slot_names),
            FooterKey::LinkTopicNames => json(&meta.link_topic_names),
            FooterKey::FeatureKinds => {
                let kinds: Vec<&str> = meta
                    .feature_kinds
                    .iter()
                    .map(|kind| kind.as_str())
                    .collect();
                json(&kinds)
            }
            FooterKey::SenderDictionary => {
                let senders: serde_json::Map<String, serde_json::Value> = meta
                    .senders
                    .iter()
                    .map(|(hash, te_id)| (format!("0x{:016x}", hash.0), te_id.as_ref().into()))
                    .collect();
                serde_json::Value::Object(senders).to_string()
            }
            FooterKey::TapeKinds => tape_kinds_json(),
        }
    }
}

/// The footer of each file of the run of `meta`, each key in the order of the contract.
pub(in crate::tape) fn footer_metadata(meta: &RunMeta) -> Vec<(String, String)> {
    FooterKey::ALL
        .into_iter()
        .map(|key| (key.as_str().to_owned(), key.value(meta)))
        .collect()
}

fn json<T: serde::Serialize>(items: &[T]) -> String {
    serde_json::to_string(items)
        .expect("a slice of strings or of string pairs always serialises to json")
}

/// Each kind of tape row that this build writes, with the digest of its payload.
fn tape_kinds_json() -> String {
    let kinds: Vec<serde_json::Value> = TAPE_KINDS
        .iter()
        .map(|kind| {
            serde_json::json!({
                "kind": kind.id,
                "name": kind.name,
                "digest": kind.digest_text(),
            })
        })
        .collect();
    serde_json::Value::Array(kinds).to_string()
}
