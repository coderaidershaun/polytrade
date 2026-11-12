//! Manifest invariants that no amount of Rust source can express.
//!
//! Identity is two-part, so the "folder == bin == id" invariant needs enforcing rather than
//! hoping: every trading-engine `[[bin]]` must be named `<strategy-id>-<te-id>` and live at
//! `strategies/<strategy-id>/<te-id>/main.rs`. That makes bin-name uniqueness structural, keeps the
//! `--config`-less default path resolvable from the bin name alone, and stops a trading engine from
//! silently writing into a sibling's data tree.
//!
//! And no native build may link the GUI. `eframe` is declared only under the wasm32 target table,
//! so a trading engine never builds a renderer, winit or X11. Moving it into the plain
//! `[dependencies]` table — or letting any feature pull it in — puts the whole GUI stack back into
//! every deployment with every gate still green. That is the silent loss this pins.
//!
//! And the same holds for the agent's MCP server: a trading engine must never build its protocol
//! stack, so `rmcp`, `rustix`, `hyper` and `hyper-util` stay optional behind feature `mcp` and only
//! the `polysim-mcp` bin asks for it.
//!
//! And for the browser workstation's server: `hyper`, `hyper-util` and `http-body-util` stay
//! optional behind feature `web`, so a trading engine never builds an HTTP server it does not run.

use std::path::Path;

/// Trunk builds the page and its fixture with no feature flags, so a feature declared on either
/// would leave it unbuildable there.
const PAGE_TARGETS: [&str; 2] = ["polysim-ui", "dom-fixture"];

/// Targets outside `strategies/` that gate on a feature, with the exact `required-features` value
/// each must declare. Every other non-strategy target declares none.
///
/// Named one at a time on purpose. The guard below exists so that a new target cannot quietly
/// pull a heavy dependency stack into every build, and any rule shaped like "servers are exempt"
/// would hand that back.
const FEATURE_GATED_TARGETS: [(&str, &str); 2] =
    [("polysim-mcp", r#"["mcp"]"#), ("polysim-web", r#"["web"]"#)];

const WASM_TABLE: &str = r#"[target.'cfg(target_arch = "wasm32")'.dependencies]"#;

/// Parsed straight from the manifest text — pulling in a toml crate to read two tables would be a
/// new dependency earned for nothing.
#[derive(Default)]
struct TargetDeclaration {
    kind: String,
    name: String,
    path: String,
    required_features: String,
}

#[test]
fn every_trading_engine_bin_is_named_and_placed_by_its_two_part_identity() {
    let engines: Vec<TargetDeclaration> = declared_targets()
        .into_iter()
        .filter(|target| target.kind == "bin" && target.path.starts_with("strategies/"))
        .collect();
    assert!(
        !engines.is_empty(),
        "Cargo.toml declares no trading-engine [[bin]] — a strategy folder is not auto-discovered, \
         so a missing declaration means the trading engine cannot be built at all"
    );

    for engine in &engines {
        let (strategy_id, te_id) = split_identity(&engine.path);
        assert_eq!(
            engine.name,
            format!("{strategy_id}-{te_id}"),
            "bin name must be <strategy-id>-<te-id> for path {}",
            engine.path
        );
        assert_eq!(
            engine.path,
            format!("strategies/{strategy_id}/{te_id}/main.rs"),
            "bin {} must live at strategies/<strategy-id>/<te-id>/main.rs",
            engine.name
        );
        assert!(
            Path::new(&engine.path).is_file(),
            "bin {} points at {}, which does not exist",
            engine.name,
            engine.path
        );
        assert!(
            te_id.starts_with("te-"),
            "te id {te_id} must be prefixed te- so a strategy folder listing reads as its engines"
        );
    }
}

#[test]
fn the_default_build_links_no_eframe() {
    let manifest = read_manifest();
    let headers = manifest
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with('['));
    for header in headers.filter(|header| *header != WASM_TABLE) {
        assert!(
            !header.contains("eframe")
                && section_lines(&manifest, header).all(|line| value_of(line, "eframe").is_none()),
            "eframe must not sit in {header}: a native build would link the whole GUI stack again"
        );
    }
    let eframe = section_lines(&manifest, WASM_TABLE)
        .find_map(|line| value_of(line, "eframe"))
        .expect("the wasm32 table declares eframe: the browser workstation paints with it");
    assert!(
        eframe.contains(r#"version = "=0.35.0""#) && eframe.contains("default-features = false"),
        "eframe must stay pinned to one version with its default features off: {eframe}"
    );
    assert!(
        !feature_names(&manifest)
            .iter()
            .any(|feature| feature == "default" || feature == "ui"),
        "no default or ui feature may exist: one that reached eframe would re-link it everywhere"
    );

    let gated: Vec<TargetDeclaration> = declared_targets()
        .into_iter()
        .filter(|target| !target.path.starts_with("strategies/"))
        .collect();
    let tabled = PAGE_TARGETS
        .into_iter()
        .chain(FEATURE_GATED_TARGETS.map(|(name, _)| name));
    for named in tabled {
        assert!(
            gated.iter().any(|target| target.name == named),
            "{named} is named by this test but is no longer a declared target — a stale name is \
             how the next target slips through"
        );
    }

    for target in gated {
        let expected = required_features_for(&target.name);
        assert_eq!(
            target.required_features, expected,
            "{} {} must declare required-features = {expected:?}: the agent server gates on mcp, \
             the web server on web, and every other target declares none",
            target.kind, target.name
        );
    }
}

#[test]
fn the_default_build_links_no_rmcp() {
    let manifest = read_manifest();
    for crate_name in ["hyper", "hyper-util", "rmcp", "rustix"] {
        assert!(
            dependency_line(&manifest, crate_name).contains("optional = true"),
            "{crate_name} must stay optional, or every trading engine builds the agent server's \
             dependencies"
        );
    }
    assert_eq!(
        feature_definition(&manifest, "mcp"),
        r#"["dep:hyper", "dep:hyper-util", "dep:rmcp", "dep:rustix"]"#,
        "feature mcp must map to the agent server's optional dependencies alone"
    );
    assert!(
        feature_names(&manifest)
            .iter()
            .all(|feature| feature != "default"),
        "no default feature may exist: one that reached mcp would build rmcp everywhere"
    );
    let server = declared_targets()
        .into_iter()
        .find(|target| target.name == "polysim-mcp")
        .expect("Cargo.toml declares the polysim-mcp bin");
    assert_eq!(
        server.required_features, r#"["mcp"]"#,
        "the agent server is the only target allowed to need rmcp"
    );
}

#[test]
fn the_default_build_links_no_web_server() {
    let manifest = read_manifest();
    for crate_name in ["http-body-util", "hyper", "hyper-util"] {
        assert!(
            dependency_line(&manifest, crate_name).contains("optional = true"),
            "{crate_name} must stay optional, or every trading engine builds the web server's \
             dependencies"
        );
    }
    assert_eq!(
        feature_definition(&manifest, "web"),
        r#"["dep:hyper", "dep:hyper-util", "dep:http-body-util"]"#,
        "feature web must map to the web server's optional dependencies alone"
    );
    let server = declared_targets()
        .into_iter()
        .find(|target| target.name == "polysim-web")
        .expect("Cargo.toml declares the polysim-web bin");
    assert_eq!(
        server.required_features, r#"["web"]"#,
        "the web server is the only target allowed to need the web feature"
    );
}

/// A target outside the table declares no feature, so a new non-strategy target is ungated by
/// default and must be named here to carry a gate.
fn required_features_for(name: &str) -> &'static str {
    FEATURE_GATED_TARGETS
        .iter()
        .find(|(target, _)| *target == name)
        .map_or("", |(_, features)| features)
}

/// The `(strategy-id, te-id)` pair the path claims, taken from the path so name and path are checked
/// against one another rather than both against the same source.
fn split_identity(path: &str) -> (&str, &str) {
    let segments: Vec<&str> = path.split('/').collect();
    assert_eq!(
        segments.len(),
        4,
        "bin path {path} must be strategies/<strategy-id>/<te-id>/main.rs"
    );
    (segments[1], segments[2])
}

fn read_manifest() -> String {
    std::fs::read_to_string("Cargo.toml").expect("Cargo.toml is readable")
}

fn declared_targets() -> Vec<TargetDeclaration> {
    let manifest = read_manifest();
    let mut targets: Vec<TargetDeclaration> = Vec::new();
    let mut kind = String::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            // Only bin/example tables are tracked; `[[test]]` carries name/path keys too, and
            // letting them through would silently overwrite the preceding target's fields.
            let table = line.trim_matches(['[', ']']);
            kind = match table {
                "bin" | "example" => table.to_owned(),
                _ => String::new(),
            };
            if !kind.is_empty() {
                targets.push(TargetDeclaration {
                    kind: kind.clone(),
                    ..TargetDeclaration::default()
                });
            }
            continue;
        }
        let Some(target) = targets.last_mut().filter(|_| !kind.is_empty()) else {
            continue;
        };
        if let Some(value) = value_of(line, "name") {
            target.name = unquote(value);
        }
        if let Some(value) = value_of(line, "path") {
            target.path = unquote(value);
        }
        if let Some(value) = value_of(line, "required-features") {
            target.required_features = value.to_owned();
        }
    }
    targets
}

fn dependency_line(manifest: &str, crate_name: &str) -> String {
    section_lines(manifest, "[dependencies]")
        .find_map(|line| value_of(line, crate_name).map(str::to_owned))
        .unwrap_or_else(|| panic!("[dependencies] declares no {crate_name}"))
}

fn feature_definition(manifest: &str, feature: &str) -> String {
    section_lines(manifest, "[features]")
        .find_map(|line| value_of(line, feature).map(str::to_owned))
        .unwrap_or_else(|| panic!("[features] declares no {feature}"))
}

fn feature_names(manifest: &str) -> Vec<String> {
    section_lines(manifest, "[features]")
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim().to_owned()))
        .collect()
}

fn section_lines<'a>(manifest: &'a str, header: &'a str) -> impl Iterator<Item = &'a str> {
    manifest
        .lines()
        .map(str::trim)
        .skip_while(move |line| *line != header)
        .skip(1)
        .take_while(|line| !line.starts_with('['))
}

fn value_of<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    Some(
        line.strip_prefix(key)?
            .trim_start()
            .strip_prefix('=')?
            .trim(),
    )
}

fn unquote(value: &str) -> String {
    value.trim_matches('"').to_owned()
}
