//! `log::register_thread` scopes a log lane to the calling OS THREAD, and no Rust signature can
//! stop a tokio task body from calling it. Tasks share the multi-worker pool, so two landing on one
//! worker overwrite each other's tag, and a task migrating at an `.await` pushes into whichever
//! ring that worker had last registered, so a Binance REST warning could be filed under
//! `[polymarket-exec]`. One log file carries engine and strategy records
//! alike, and the tag column is the only thing distinguishing their origin, so the attribution is
//! load-bearing rather than cosmetic.
//!
//! The rule is invisible at the call site, so the threads are named here one at a time. A new call
//! site fails until someone states which OS thread it owns, and a task author is pointed at
//! `log::tag_task` instead.

use std::path::{Path, PathBuf};

use crate::operator::rust_sources;

/// This engine's complete census of thread lanes: the text lane of each OS thread it owns (the
/// process entry, the single hot thread, the metrics thread, the pinned senders' thread, and the
/// two dedicated output threads), and the hot lane of the hot thread. Everything else that runs
/// concurrently here is a tokio task. The browser workstation runs on one thread with no log
/// lane. Each entry is (call, file, tag).
const LANE_OWNERS: [(&str, &str, &str); 7] = [
    (TEXT_LANE_CALL, "src/hot/spawn.rs", "config.tag"),
    (
        TEXT_LANE_CALL,
        "src/runtime/assembly/metrics/mod.rs",
        "metrics",
    ),
    (TEXT_LANE_CALL, "src/runtime/assembly/mod.rs", "main"),
    (TEXT_LANE_CALL, "src/runtime/assembly/senders.rs", "sender"),
    (TEXT_LANE_CALL, "src/runtime/exposure/writer.rs", "exposure"),
    (TEXT_LANE_CALL, "src/tape/drain.rs", "persist"),
    (HOT_LANE_CALL, "src/hot/spawn.rs", "config.tag"),
];

const TEXT_LANE_CALL: &str = "log::register_thread(";
const HOT_LANE_CALL: &str = "log::register_hot_lane(";

/// Calls that put a lane on a thread with no tag and no drain. The engine never makes them: they
/// exist for a caller that runs its own drain.
const UNTAGGED_LANE_CALLS: [&str; 2] = ["log::open_hot_lane(", "log::open_text_lane("];

#[test]
fn every_thread_lane_belongs_to_an_owned_thread() {
    let calls = [TEXT_LANE_CALL, HOT_LANE_CALL]
        .into_iter()
        .chain(UNTAGGED_LANE_CALLS);
    let mut found: Vec<(String, String, String)> = calls
        .flat_map(|call| registration_sites(Path::new("src"), call))
        .collect();
    found.sort();
    let mut expected: Vec<(String, String, String)> = LANE_OWNERS
        .iter()
        .map(|(call, path, tag)| ((*call).to_owned(), (*path).to_owned(), (*tag).to_owned()))
        .collect();
    expected.sort();
    assert_eq!(
        found, expected,
        "the lane call sites under src/ no longer match the census of thread lanes. A tokio task \
         must instead be wrapped at its spawn site — `rt.spawn(log::tag_task(tag, body))` — \
         because a task registering by thread files its records under whichever tag its worker \
         last saw. If this genuinely is a new OS thread the engine owns, name it in LANE_OWNERS"
    );
}

fn registration_sites(dir: &Path, call: &str) -> Vec<(String, String, String)> {
    let mut sites = Vec::new();
    for path in rust_sources(dir) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
        let file = path.to_str().expect("source paths are utf-8").to_owned();
        sites.extend(
            source
                .lines()
                .filter_map(|line| tag_argument(line, call))
                .map(|tag| (call.to_owned(), file.clone(), tag)),
        );
    }
    sites
}

fn tag_argument(line: &str, call: &str) -> Option<String> {
    let (_, rest) = line.split_once(call)?;
    let (argument, _) = rest.split_once(')')?;
    Some(argument.trim_matches('"').to_owned())
}

/// The folders whose code runs on the hot thread. A text macro or a `format!` there formats and
/// allocates on the thread that folds messages, where only typed records of fixed size may go.
const HOT_FOLDERS: [&str; 2] = ["engine/src/hot", "strategies"];

/// Each word that formats text, or writes it through a log lane that formats on the caller.
const TEXT_WORDS: [&str; 9] = [
    "info!",
    "warn!",
    "error!",
    "warn_repeating!",
    "format!",
    "write!",
    "writeln!",
    "to_string",
    "{:?}",
];

/// Each place in those folders that may still format text, and why. It does not run on the hot
/// thread.
const TEXT_EXCEPTIONS: [(&str, &str, &str); 1] = [(
    "engine/src/hot/exec/reconcile.rs",
    "write!",
    "renders a reject reason of a hot record into text, which only the log drain thread does",
)];

#[test]
fn hot_path_holds_no_text_macro() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the package sits in the workspace");
    let mut found: Vec<(String, &str)> = Vec::new();
    for folder in HOT_FOLDERS {
        for path in rust_sources(&workspace.join(folder)) {
            let code = without_comments_and_literals(&read(&path));
            for word in TEXT_WORDS {
                for _ in code
                    .match_indices(word)
                    .filter(|&(at, _)| stands_alone(&code, at, word))
                {
                    found.push((relative_to(&path, workspace), word));
                }
            }
        }
    }
    let mut excepted: Vec<(String, &str)> = TEXT_EXCEPTIONS
        .iter()
        .map(|(file, word, _)| ((*file).to_owned(), *word))
        .collect();
    found.sort();
    excepted.sort();
    assert_eq!(
        found, excepted,
        "the hot thread formats text here. A hot call takes a typed record (hot_info!, hot_warn!, \
         hot_error!), and a strategy logs with note! or alert!, so the text is made on the drain \
         thread. A new exception needs its reason in TEXT_EXCEPTIONS"
    );
}

/// A word stands alone when no identifier character sits before it, nor after it when it ends in
/// one, so `hot_warn!` is not `warn!` and `to_string_lossy` is not `to_string`.
fn stands_alone(code: &str, at: usize, word: &str) -> bool {
    let is_ident = |character: char| character.is_alphanumeric() || character == '_';
    let before = code[..at].chars().next_back();
    let after = code[at + word.len()..].chars().next();
    let ends_in_ident = word.chars().next_back().is_some_and(is_ident);
    let is_glued_before = before.is_some_and(is_ident);
    let is_glued_after = ends_in_ident && after.is_some_and(is_ident);
    !(is_glued_before || is_glued_after)
}

/// The code of a Rust source with each comment, string and character literal blanked out, so the
/// words of a log template or a doc line are never read as code. Each blanked character becomes a
/// space and each line break stays, so an offset into the code is an offset into the source.
pub(super) fn without_comments_and_literals(source: &str) -> String {
    let characters: Vec<char> = source.chars().collect();
    let mut code = String::with_capacity(source.len());
    let mut index = 0;
    while index < characters.len() {
        let rest = &characters[index..];
        let blanked = match rest {
            ['/', '/', ..] => rest
                .iter()
                .take_while(|&&character| character != '\n')
                .count(),
            ['/', '*', ..] => block_comment_len(rest),
            ['r', '#' | '"', ..] if !is_ident_tail(&code) => raw_string_len(rest),
            ['"', ..] => quoted_len(rest, '"'),
            ['\'', '\\', ..] => quoted_len(rest, '\''),
            ['\'', _, '\'', ..] => 3,
            [character, ..] => {
                code.push(*character);
                index += 1;
                continue;
            }
            [] => unreachable!("the loop stops at the end of the source"),
        };
        let blanked = blanked.min(rest.len());
        code.extend(
            rest[..blanked]
                .iter()
                .map(|&character| if character == '\n' { '\n' } else { ' ' }),
        );
        index += blanked;
    }
    code
}

fn is_ident_tail(code: &str) -> bool {
    code.chars()
        .next_back()
        .is_some_and(|character| character.is_alphanumeric() || character == '_')
}

fn block_comment_len(rest: &[char]) -> usize {
    rest.windows(2)
        .skip(2)
        .position(|pair| pair == ['*', '/'])
        .map_or(rest.len(), |at| at + 4)
}

/// A literal that opens and closes with `quote`, with backslash escapes.
fn quoted_len(rest: &[char], quote: char) -> usize {
    let mut index = 1;
    while index < rest.len() {
        match rest[index] {
            '\\' => index += 2,
            character if character == quote => return index + 1,
            _ => index += 1,
        }
    }
    rest.len()
}

/// `r"..."`, `r#"..."#` and deeper: it ends at a quote followed by as many hashes as it opened with.
fn raw_string_len(rest: &[char]) -> usize {
    let hashes = rest[1..]
        .iter()
        .take_while(|&&character| character == '#')
        .count();
    let body = 2 + hashes;
    let closing: Vec<char> = std::iter::once('"')
        .chain(std::iter::repeat_n('#', hashes))
        .collect();
    rest[body..]
        .windows(closing.len())
        .position(|window| window == closing.as_slice())
        .map_or(rest.len(), |at| body + at + closing.len())
}

fn read(path: &PathBuf) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

fn relative_to(path: &Path, workspace: &Path) -> String {
    path.strip_prefix(workspace)
        .expect("each source sits below the workspace")
        .to_string_lossy()
        .into_owned()
}
