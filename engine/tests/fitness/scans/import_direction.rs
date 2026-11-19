//! Imports point one way: everything may use `msg`, the hot thread reaches no async code and no
//! venue wire format, and the three venue edges never import each other. No type holds that, so
//! each rule here reads the paths that each file of the library names.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::log_lane_ownership::without_comments_and_literals;
use crate::operator::rust_sources;

/// One direction that imports must never take: no file below `from` names a path below `banned`.
struct Rule {
    from: &'static str,
    /// The fewest files that `from` can honestly hold, so a scan of a moved or empty folder fails
    /// instead of passing over nothing.
    floor: usize,
    banned: &'static [&'static str],
    why: &'static str,
}

const RULES: [Rule; 6] = [
    Rule {
        from: "msg",
        floor: 14,
        banned: &[
            "hot", "log", "venues", "link", "tape", "runtime", "operator",
        ],
        why: "every box may use the message vocabulary, so a type it took from above would make \
              the vocabulary depend on the code that sits on it",
    },
    Rule {
        from: "hot",
        floor: 80,
        banned: &[
            "venues",
            "operator",
            "tape",
            "link::actor",
            "runtime::assembly",
            "runtime::otlp",
        ],
        why: "the hot thread folds plain messages: it reaches no async code and no venue's wire \
              format",
    },
    Rule {
        from: "venues::binance",
        floor: 30,
        banned: &["venues::polymarket", "venues::sim"],
        why: "the three venue edges never import each other; what they share lives in \
              `venues::edge` and `venues::exec`",
    },
    Rule {
        from: "venues::polymarket",
        floor: 40,
        banned: &["venues::binance", "venues::sim"],
        why: "the three venue edges never import each other; what they share lives in \
              `venues::edge` and `venues::exec`",
    },
    Rule {
        from: "venues",
        floor: 140,
        banned: &["link", "operator"],
        why: "a venue edge talks to its venue and to the hot thread, never to the link between \
              engines or to the operator",
    },
    Rule {
        from: "hot::quant",
        floor: 30,
        banned: &["hot::tracker", "hot::strategy", "hot::dispatch"],
        why: "the calculators take borrowed values from the tracker and the strategy, and never \
              their state",
    },
];

/// The words of async code. A file of the hot thread names none of them.
const ASYNC_WORDS: [&str; 3] = ["tokio", "async", ".await"];

/// The crates that read or write a venue's wire format.
const WIRE_CRATES: [&str; 4] = ["serde_json", "tokio_tungstenite", "tungstenite", "reqwest"];

/// The one edge that imports another today, each file with its reason. A new file that imports
/// another edge fails, and so does a listed file that no longer does: remove its row.
const EDGE_EXCEPTIONS: [(&str, &str); 8] = [
    (
        "src/venues/sim/bringup.rs",
        "the simulator stands in for Binance Spot and reads its symbol table",
    ),
    (
        "src/venues/sim/mod.rs",
        "the simulator states the capabilities of the Binance Spot venue it stands in for",
    ),
    (
        "src/venues/sim/actor.rs",
        "the simulator decodes the Binance Spot frames of its own wire",
    ),
    (
        "src/venues/sim/wire/mod.rs",
        "the simulator writes Binance Spot client order ids",
    ),
    (
        "src/venues/sim/wire/decode.rs",
        "the simulator decodes Binance Spot requests",
    ),
    (
        "src/venues/sim/driver/account.rs",
        "the simulator decodes the Binance Spot account frames that it answers with",
    ),
    (
        "src/venues/sim/driver/handshake.rs",
        "the simulator answers the Binance Spot account handshake",
    ),
    (
        "src/venues/sim/driver/session.rs",
        "the simulator answers Binance Spot requests",
    ),
];

/// The simulator is Binance Spot in many places; each one is flagged where it sits, and a later
/// stage gives the simulator the facts of the venue it simulates instead.
const EDGE_EXCEPTION_FROM: &str = "venues::sim";
const EDGE_EXCEPTION_TO: &str = "venues::binance";

#[test]
fn imports_point_one_way() {
    let engine = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = engine.join("src");
    let files: Vec<SourceFile> = rust_sources(&source_root)
        .iter()
        .map(|path| SourceFile::read(path, &source_root))
        .collect();
    let macros = exported_macros(&files);

    let mut problems = Vec::new();
    for rule in &RULES {
        let scanned: Vec<&SourceFile> = files
            .iter()
            .filter(|file| file.is_below(rule.from))
            .collect();
        if scanned.len() < rule.floor {
            problems.push(format!(
                "{} holds {} files, fewer than its floor of {}: the scan reads the wrong folder",
                rule.from,
                scanned.len(),
                rule.floor
            ));
        }
        for file in scanned {
            for (line, path) in file.named_paths(&macros) {
                if let Some(banned) = rule.banned.iter().find(|banned| is_below(&path, banned)) {
                    problems.push(format!(
                        "{}:{line} names {path}, below {banned}: {}",
                        file.relative, rule.why
                    ));
                }
            }
        }
    }
    problems.extend(hot_names_no_async_code(&files));
    problems.extend(simulator_exceptions(&files, &macros));
    assert!(
        problems.is_empty(),
        "each line below is an import that points the wrong way. Move the type it needs into the \
         box below both, or name the import in the rule with its reason:\n{}",
        problems.join("\n")
    );
}

fn hot_names_no_async_code(files: &[SourceFile]) -> Vec<String> {
    let mut problems = Vec::new();
    for file in files.iter().filter(|file| file.is_below("hot")) {
        for (number, line) in file.code.lines().enumerate() {
            let words = ASYNC_WORDS.iter().chain(WIRE_CRATES.iter());
            for word in words.filter(|word| names_word(line, word)) {
                problems.push(format!(
                    "{}:{} names {word}: the hot thread runs no async code and reads no wire format",
                    file.relative,
                    number + 1
                ));
            }
        }
    }
    problems
}

fn simulator_exceptions(files: &[SourceFile], macros: &BTreeMap<String, String>) -> Vec<String> {
    let importing: BTreeSet<&str> = files
        .iter()
        .filter(|file| file.is_below(EDGE_EXCEPTION_FROM))
        .filter(|file| {
            file.named_paths(macros)
                .iter()
                .any(|(_, path)| is_below(path, EDGE_EXCEPTION_TO))
        })
        .map(|file| file.relative.as_str())
        .collect();
    let listed: BTreeSet<&str> = EDGE_EXCEPTIONS.iter().map(|(file, _)| *file).collect();
    let mut problems: Vec<String> = importing
        .difference(&listed)
        .map(|file| {
            format!(
                "{file} names {EDGE_EXCEPTION_TO}: the three venue edges never import each other"
            )
        })
        .collect();
    problems.extend(listed.difference(&importing).map(|file| {
        format!("{file} no longer names {EDGE_EXCEPTION_TO}: remove its row from the exceptions")
    }));
    problems
}

/// A file of the library, its module path, and its code with comments and literals blanked.
struct SourceFile {
    relative: String,
    module: Vec<String>,
    code: String,
}

impl SourceFile {
    fn read(path: &Path, source_root: &Path) -> Self {
        let source = std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let below_root = path
            .strip_prefix(source_root)
            .expect("each source sits below the source root");
        let mut module: Vec<String> = below_root
            .with_extension("")
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        if module
            .last()
            .is_some_and(|last| last == "mod" || last == "lib")
        {
            module.pop();
        }
        Self {
            relative: format!("src/{}", below_root.to_string_lossy()),
            module,
            code: without_comments_and_literals(&source),
        }
    }

    fn is_below(&self, prefix: &str) -> bool {
        is_below(&self.module.join("::"), prefix)
    }

    /// Each path of the crate that this file names, with its line, as a module path from the root.
    /// A macro that the crate exports is named at the root, so it counts as a path of the module
    /// that defines it.
    fn named_paths(&self, macros: &BTreeMap<String, String>) -> Vec<(usize, String)> {
        let mut paths = Vec::new();
        for (at, anchor, rest) in path_starts(&self.code) {
            let line = self.code[..at].matches('\n').count() + 1;
            let base: Vec<String> = match anchor {
                Anchor::Crate => Vec::new(),
                Anchor::Super(levels) => {
                    let keep = self.module.len().saturating_sub(levels);
                    self.module[..keep].to_vec()
                }
            };
            for tail in expand_paths(rest) {
                let mut full = base.clone();
                full.extend(tail);
                match full.as_slice() {
                    [only] if macros.contains_key(only) => {
                        paths.push((line, macros[only].clone()));
                    }
                    _ => paths.push((line, full.join("::"))),
                }
            }
        }
        paths
    }
}

#[derive(Debug, Clone, Copy)]
enum Anchor {
    Crate,
    Super(usize),
}

/// Each place in `code` where a path of the crate starts, `crate::`, `$crate::` or a run of
/// `super::`, with its byte offset and the text after it.
fn path_starts(code: &str) -> Vec<(usize, Anchor, &str)> {
    let mut starts = Vec::new();
    let mut search = 0;
    while let Some((offset, anchor, length)) = next_anchor(&code[search..]) {
        let at = search + offset;
        let is_word_start = !code[..at]
            .chars()
            .next_back()
            .is_some_and(|before| is_ident(before) || before == ':');
        let rest = &code[at + length..];
        if is_word_start {
            match anchor {
                Anchor::Super(_) => {
                    let mut levels = 1;
                    let mut tail = rest;
                    while let Some(next) = tail.strip_prefix("super::") {
                        levels += 1;
                        tail = next;
                    }
                    starts.push((at, Anchor::Super(levels), tail));
                }
                Anchor::Crate => starts.push((at, anchor, rest)),
            }
        }
        search = at + length;
    }
    starts
}

fn next_anchor(text: &str) -> Option<(usize, Anchor, usize)> {
    let crate_at = text.find("crate::");
    let super_at = text.find("super::");
    match (crate_at, super_at) {
        (Some(crate_at), Some(super_at)) if super_at < crate_at => {
            Some((super_at, Anchor::Super(1), "super::".len()))
        }
        (Some(crate_at), _) => Some((crate_at, Anchor::Crate, "crate::".len())),
        (None, Some(super_at)) => Some((super_at, Anchor::Super(1), "super::".len())),
        (None, None) => None,
    }
}

/// The paths that `text` spells after an anchor, with each brace group expanded:
/// `{a, b::{c, d}}` gives `a`, `b::c` and `b::d`.
fn expand_paths(text: &str) -> Vec<Vec<String>> {
    let characters: Vec<char> = text
        .chars()
        .take_while(|character| *character != ';')
        .take(MAX_PATH_CHARS)
        .collect();
    let (paths, _) = parse_tree(&characters, 0);
    paths
}

/// Longer than any `use` line of the crate. A path never runs on past a `;`.
const MAX_PATH_CHARS: usize = 4096;

fn parse_tree(characters: &[char], mut at: usize) -> (Vec<Vec<String>>, usize) {
    let mut prefix = Vec::new();
    loop {
        let start = at;
        while at < characters.len() && is_ident(characters[at]) {
            at += 1;
        }
        if at > start {
            prefix.push(characters[start..at].iter().collect::<String>());
        }
        let is_separator = characters.get(at) == Some(&':') && characters.get(at + 1) == Some(&':');
        if !is_separator {
            break;
        }
        at += 2;
        if characters.get(at) == Some(&'{') {
            let (group, after) = parse_group(characters, at + 1);
            let paths = group
                .into_iter()
                .map(|tail| prefix.iter().cloned().chain(tail).collect())
                .collect();
            return (paths, after);
        }
    }
    let paths = if prefix.is_empty() { Vec::new() } else { vec![prefix] };
    (paths, at)
}

fn parse_group(characters: &[char], mut at: usize) -> (Vec<Vec<String>>, usize) {
    let mut paths = Vec::new();
    while at < characters.len() {
        match characters[at] {
            '}' => return (paths, at + 1),
            ',' | ' ' | '\n' => at += 1,
            _ => {
                let (tree, after) = parse_tree(characters, at);
                paths.extend(tree);
                at = after.max(at + 1);
                while at < characters.len() && !matches!(characters[at], ',' | '}') {
                    at += 1;
                }
            }
        }
    }
    (paths, at)
}

fn is_ident(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

fn is_below(path: &str, prefix: &str) -> bool {
    path == prefix || path.starts_with(&format!("{prefix}::"))
}

/// Whether `line` names `word` as a whole word, not as part of a longer name.
fn names_word(line: &str, word: &str) -> bool {
    line.match_indices(word).any(|(at, _)| {
        let before = line[..at].chars().next_back();
        let after = line[at + word.len()..].chars().next();
        let starts_word = word.starts_with('.') || !before.is_some_and(is_ident);
        starts_word && !after.is_some_and(is_ident)
    })
}

/// Each macro that the crate exports, by name, with the top module of the file that defines it.
fn exported_macros(files: &[SourceFile]) -> BTreeMap<String, String> {
    let mut macros = BTreeMap::new();
    for file in files {
        let mut is_exported = false;
        for line in file.code.lines() {
            let line = line.trim();
            if line.starts_with("#[macro_export") {
                is_exported = true;
                continue;
            }
            if let Some(rest) = line.strip_prefix("macro_rules!") {
                let name: String = rest.trim().chars().take_while(|c| is_ident(*c)).collect();
                if is_exported && let Some(top) = file.module.first() {
                    macros.insert(name, top.clone());
                }
            }
            if !line.starts_with("#[") {
                is_exported = false;
            }
        }
    }
    macros
}
