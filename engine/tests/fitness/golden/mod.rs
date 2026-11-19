//! The quotes that both shipped strategies declare at each spin, held in one golden file for each
//! strategy, so a change of strategy code that moves a quote by accident turns the suite red.

mod maker;
mod record;
mod recorder;

use engine::runtime::config::TableKind;
use record::DeclaredQuotes;

/// Renews each golden file whose lines differ, and writes its value into the header as the reason.
const BLESS_VARIABLE: &str = "POLYTRADE_BLESS_QUOTES";

/// A golden file as the build holds it, and the path that bless writes, relative to the package
/// root that the suite runs from.
struct GoldenFile {
    strategy: &'static str,
    path: &'static str,
    checked_in: &'static str,
}

const RECORDER_FILE: GoldenFile = GoldenFile {
    strategy: "binance recorder",
    path: "tests/fitness/golden/binance_recorder.txt",
    checked_in: include_str!("binance_recorder.txt"),
};

const MAKER_FILE: GoldenFile = GoldenFile {
    strategy: "polymarket maker",
    path: "tests/fitness/golden/polymarket_maker.txt",
    checked_in: include_str!("polymarket_maker.txt"),
};

/// What the check of one golden file decides.
enum Golden {
    Holds,
    Rewrite(String),
    Red(String),
}

impl GoldenFile {
    /// The header lines start with `#` and are kept on a rewrite; each other line is one quote.
    fn verdict(&self, quotes: &DeclaredQuotes, bless: Option<&str>) -> Golden {
        let (header, body): (Vec<&str>, Vec<&str>) = self
            .checked_in
            .lines()
            .partition(|line| line.starts_with('#'));
        if body == quotes.lines {
            return Golden::Holds;
        }
        match bless.map(str::trim) {
            Some(reason) if !reason.is_empty() => {
                let mut text = String::new();
                for line in header {
                    text.push_str(line);
                    text.push('\n');
                }
                text.push_str(&format!("# Renewed: {reason}\n"));
                for line in &quotes.lines {
                    text.push_str(line);
                    text.push('\n');
                }
                Golden::Rewrite(text)
            }
            Some(_) => Golden::Red(format!(
                "{}: {BLESS_VARIABLE} holds no reason, and the header of a renewed file names one",
                self.path
            )),
            None => Golden::Red(format!(
                "{}: the declared quotes of the {} differ from the golden file. {}. If the change \
                 is on purpose, rerun with {BLESS_VARIABLE}='<task>: <reason>' and commit the file",
                self.path,
                self.strategy,
                first_difference(&body, &quotes.lines)
            )),
        }
    }
}

fn first_difference(golden: &[&str], now: &[String]) -> String {
    let index = golden
        .iter()
        .zip(now)
        .position(|(golden, now)| golden != now)
        .unwrap_or(golden.len().min(now.len()));
    format!(
        "Quote line {} is `{}` in the file and `{}` now ({} lines in the file, {} now)",
        index + 1,
        golden.get(index).copied().unwrap_or("<none>"),
        now.get(index).map_or("<none>", String::as_str),
        golden.len(),
        now.len()
    )
}

/// FITNESS: the quotes that the Binance recorder and the Polymarket maker declare at each spin, over
/// the recorded venue captures and the seeded books of their own tests, equal the golden files. A
/// changed quote is silent everywhere else: it still prices, still places, and still records.
#[test]
fn declared_quotes_equal_golden_file() {
    let bless = std::env::var(BLESS_VARIABLE).ok();
    let mut problems = Vec::new();
    for (file, quotes) in [
        (RECORDER_FILE, recorder::declared_quotes()),
        (MAKER_FILE, maker::declared_quotes()),
    ] {
        println!(
            "{}: {} quotes declared, on {} lines of spin and instrument",
            file.strategy,
            quotes.quoted,
            quotes.lines.len()
        );
        assert!(
            quotes.quoted > 0,
            "the {} declared no quote on its tapes, so its golden file pins nothing: the warmup \
             must end inside the tape",
            file.strategy
        );
        match file.verdict(&quotes, bless.as_deref()) {
            Golden::Holds => {}
            Golden::Rewrite(text) => {
                std::fs::write(file.path, text).expect("the golden file is writable");
            }
            Golden::Red(why) => problems.push(why),
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// FITNESS: the recorder declares the same quotes at each spin whichever tables the config records.
/// A research table that gated a quote would change what a run trades with no error anywhere.
#[test]
fn quotes_do_not_depend_on_recorded_tables() {
    let shipped = [
        TableKind::Features,
        TableKind::LinkFrames,
        TableKind::Orders,
        TableKind::Fills,
    ];
    let reference = recorder::seeded_quotes_with(shipped.to_vec());
    assert!(
        reference.quoted > 0,
        "with the shipped tables the recorder declares no quote on tape `quotes`, so this test pins \
         nothing"
    );
    for tables in [vec![TableKind::Features], Vec::new()] {
        let quotes = recorder::seeded_quotes_with(tables.clone());
        assert_eq!(
            (
                quotes.quoted,
                first_difference_of(&reference.lines, &quotes.lines)
            ),
            (reference.quoted, None),
            "tables {tables:?} change the declared quotes of tape `quotes` against {shipped:?}"
        );
    }
}

fn first_difference_of(reference: &[String], now: &[String]) -> Option<String> {
    (reference != now).then(|| {
        let golden: Vec<&str> = reference.iter().map(String::as_str).collect();
        first_difference(&golden, now)
    })
}
