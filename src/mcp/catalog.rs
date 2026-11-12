//! What is on disk: the engines under `strategies/`, the mode each config asks for, the one-line
//! edit that changes it, and the console log each run writes.

use std::fmt::Display;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::config::{ExecutionMode, RunIdentity};

use super::error::FleetError;

const STRATEGIES_DIR: &str = "strategies";
const ENGINE_DIR_PREFIX: &str = "te-";
const CONFIG_FILE: &str = "config.yaml";
const LOGS_DIR: &str = "logs";
const MODE_KEY: &str = "mode:";
const TAIL_LINES: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct Engine {
    pub(super) identity: RunIdentity,
    pub(super) config_path: PathBuf,
}

/// Sorted by strategy id, then engine id. A folder whose name is not exactly a valid id is skipped,
/// because its bin name and config path would not exist under the lowercased id.
pub(super) fn discover(root: &Path) -> Result<Vec<Engine>, FleetError> {
    let mut engines = Vec::new();
    for strategy_dir in sorted_subdirectories(&strategies_dir(root))? {
        for engine_dir in sorted_subdirectories(&strategy_dir)? {
            engines.extend(Engine::at(&strategy_dir, &engine_dir));
        }
    }
    Ok(engines)
}

pub(super) fn strategies_dir(root: &Path) -> PathBuf {
    root.join(STRATEGIES_DIR)
}

impl Engine {
    fn at(strategy_dir: &Path, engine_dir: &Path) -> Option<Engine> {
        let strategy_name = strategy_dir.file_name()?.to_str()?;
        let engine_name = engine_dir.file_name()?.to_str()?;
        let config_path = engine_dir.join(CONFIG_FILE);
        if !engine_name.starts_with(ENGINE_DIR_PREFIX) || !config_path.is_file() {
            return None;
        }
        let identity = RunIdentity::new(strategy_name, engine_name).ok()?;
        let is_exact = identity.strategy_id.as_str() == strategy_name
            && identity.te_id.as_str() == engine_name;
        is_exact.then_some(Engine {
            identity,
            config_path,
        })
    }

    pub(super) fn probe(&self) -> ProbedConfig {
        match fs::read_to_string(&self.config_path) {
            Ok(text) => ProbedConfig::of_text(&text, &self.config_path),
            Err(error) => ProbedConfig::unreadable(&self.config_path, &error),
        }
    }

    /// A sim run gets its own file, so simulated fills never sit in the log a live run writes.
    pub(super) fn console_log_path(&self, root: &Path, mode: Option<ExecutionMode>) -> PathBuf {
        let stem = ExecutionMode::artifact_stem(mode, &self.identity);
        root.join(LOGS_DIR).join(format!("{stem}-console.log"))
    }

    /// Writes only when the edited text reads back as `mode` through the same parse the listing
    /// uses, because nothing else stands between this edit and the mode the engine boots with.
    /// Returns the mode the file read as before the edit.
    pub(super) fn rewrite_mode(
        &self,
        mode: ExecutionMode,
    ) -> Result<Option<ExecutionMode>, FleetError> {
        let path = &self.config_path;
        let text = fs::read_to_string(path).map_err(|source| FleetError::ConfigRead {
            path: path.clone(),
            source,
        })?;
        let previous = match ProbedConfig::of_text(&text, path) {
            ProbedConfig::Parsed(parsed) => parsed.mode,
            ProbedConfig::Unreadable(refusal) => return Err(refusal),
        };
        let rewritten = rewrite_mode_line(&text, mode)
            .ok_or_else(|| FleetError::NoModeLine { path: path.clone() })?;
        let read_back = ProbedConfig::of_text(&rewritten, path);
        if read_back.mode() != Some(mode) {
            return Err(FleetError::RewriteNotVerified {
                path: path.clone(),
                read_back: read_back.label(),
            });
        }
        write_by_rename(path, &rewritten)?;
        Ok(previous)
    }
}

/// What the listing shows for one config. Read leniently, so an engine's own strategy params never
/// make its mode unknown.
#[derive(Debug)]
pub(super) enum ProbedConfig {
    Parsed(ParsedConfig),
    /// Always the refusal that names the file, so the listing and every refused request show an
    /// agent one message for one broken config.
    Unreadable(FleetError),
}

/// The two config values a row reports. A running engine keeps the pair it booted with, so its row
/// describes the run and not a later edit to the file.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ParsedConfig {
    pub(super) mode: Option<ExecutionMode>,
    pub(super) link_bind: Option<String>,
}

#[derive(Deserialize)]
struct ConfigShape {
    execution: Option<ExecutionShape>,
    link: Option<LinkShape>,
}

#[derive(Deserialize)]
struct ExecutionShape {
    mode: ExecutionMode,
}

/// A string rather than an address, so a bad address is reported as written instead of hiding the
/// mode as well.
#[derive(Deserialize)]
struct LinkShape {
    bind: String,
}

impl ProbedConfig {
    fn of_text(text: &str, path: &Path) -> ProbedConfig {
        match serde_saphyr::from_str::<ConfigShape>(text) {
            Ok(shape) => ProbedConfig::Parsed(ParsedConfig {
                mode: shape.execution.map(|execution| execution.mode),
                link_bind: shape.link.map(|link| link.bind),
            }),
            Err(error) => ProbedConfig::unreadable(path, &error),
        }
    }

    fn unreadable(path: &Path, detail: &impl Display) -> ProbedConfig {
        ProbedConfig::Unreadable(FleetError::ConfigUnreadable {
            path: path.to_path_buf(),
            detail: detail.to_string().into_boxed_str(),
        })
    }

    pub(super) fn mode(&self) -> Option<ExecutionMode> {
        match self {
            ProbedConfig::Parsed(parsed) => parsed.mode,
            ProbedConfig::Unreadable(_) => None,
        }
    }

    pub(super) fn label(&self) -> &'static str {
        match self {
            ProbedConfig::Parsed(parsed) => ExecutionMode::footer_value(parsed.mode),
            ProbedConfig::Unreadable(_) => "unreadable",
        }
    }
}

/// The child's stdout and stderr share one append-only file, and `start_offset` marks where this
/// run's output begins.
#[derive(Debug)]
pub(super) struct ConsoleLog {
    pub(super) stdout: File,
    pub(super) stderr: File,
    pub(super) start_offset: u64,
}

impl ConsoleLog {
    pub(super) fn open(path: &Path) -> Result<ConsoleLog, FleetError> {
        ConsoleLog::open_for_append(path).map_err(|source| FleetError::ConsoleLog {
            path: path.to_path_buf(),
            source,
        })
    }

    fn open_for_append(path: &Path) -> io::Result<ConsoleLog> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let stdout = OpenOptions::new().create(true).append(true).open(path)?;
        let stderr = stdout.try_clone()?;
        let start_offset = stdout.metadata()?.len();
        Ok(ConsoleLog {
            stdout,
            stderr,
            start_offset,
        })
    }

    /// The last lines a run wrote, for the error that reports its early exit. A log that cannot be
    /// read says so in the text instead, because the exit itself is the error being reported.
    pub(super) fn tail(path: &Path, start_offset: u64) -> String {
        match read_from(path, start_offset) {
            Ok(bytes) => last_lines(&String::from_utf8_lossy(&bytes)),
            Err(error) => format!(
                "(the console log {} is unreadable: {error})",
                path.display()
            ),
        }
    }
}

fn read_from(path: &Path, offset: u64) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn last_lines(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(TAIL_LINES)..].join("\n")
}

fn sorted_subdirectories(dir: &Path) -> Result<Vec<PathBuf>, FleetError> {
    let read_failed = |source| FleetError::ReadDir {
        path: dir.to_path_buf(),
        source,
    };
    let mut subdirectories = Vec::new();
    for entry in fs::read_dir(dir).map_err(read_failed)? {
        let path = entry.map_err(read_failed)?.path();
        if path.is_dir() {
            subdirectories.push(path);
        }
    }
    subdirectories.sort();
    Ok(subdirectories)
}

/// A rename replaces the file in one step, so a server that dies mid-write never leaves a torn
/// config behind. The new file takes the old one's permission bits, so the edit changes nothing
/// but the mode value.
fn write_by_rename(path: &Path, text: &str) -> Result<(), FleetError> {
    let permissions = fs::metadata(path)
        .map_err(|source| FleetError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    let staging = path.with_extension(format!("yaml.tmp-{}", std::process::id()));
    // SMELL: if a step after creating the staging file fails, that file stays next to the config
    // and nothing removes it. A cleanup needs a way to report its own failure first.
    fs::write(&staging, text)
        .and_then(|()| fs::set_permissions(&staging, permissions))
        .map_err(|source| FleetError::ConfigWrite {
            path: staging.clone(),
            source,
        })?;
    fs::rename(&staging, path).map_err(|source| FleetError::ConfigWrite {
        path: path.to_path_buf(),
        source,
    })
}

/// Line-based so that every byte outside the one value token is copied unchanged: comments, blank
/// lines, CRLF endings and the rest of the mode line itself. `None` when there is no top-level
/// `execution:` block, no `mode:` line among its direct children, or no value on that line.
fn rewrite_mode_line(text: &str, mode: ExecutionMode) -> Option<String> {
    let mut rewritten = String::with_capacity(text.len() + mode.as_str().len());
    let mut is_replaced = false;
    let mut is_in_block = false;
    let mut child_indent = None;
    for line in text.split_inclusive('\n') {
        let content = line_content(line);
        if is_replaced {
            rewritten.push_str(line);
            continue;
        }
        if is_in_block && !continues_block(content) {
            is_in_block = false;
        }
        if !is_in_block {
            is_in_block = opens_execution_block(content);
            child_indent = None;
            rewritten.push_str(line);
            continue;
        }
        let trimmed = content.trim_start_matches(' ');
        if trimmed.is_empty() || trimmed.starts_with('#') {
            rewritten.push_str(line);
            continue;
        }
        let indent = content.len() - trimmed.len();
        let is_mode_line =
            *child_indent.get_or_insert(indent) == indent && trimmed.starts_with(MODE_KEY);
        if !is_mode_line {
            rewritten.push_str(line);
            continue;
        }
        rewritten.push_str(&with_value(line, indent + MODE_KEY.len(), mode)?);
        is_replaced = true;
    }
    is_replaced.then_some(rewritten)
}

fn line_content(line: &str) -> &str {
    let without_newline = line.strip_suffix('\n').unwrap_or(line);
    without_newline
        .strip_suffix('\r')
        .unwrap_or(without_newline)
}

fn opens_execution_block(content: &str) -> bool {
    content.strip_prefix("execution:").is_some_and(|rest| {
        let rest = rest.trim_start_matches(' ');
        rest.is_empty() || rest.starts_with('#')
    })
}

/// Indented lines, comments and blank lines stay inside the block. The first line that starts at
/// column zero with anything else ends it.
fn continues_block(content: &str) -> bool {
    content.starts_with([' ', '#']) || content.trim().is_empty()
}

/// Replaces the value token that starts after `value_from` (past any spaces) and runs up to
/// whitespace, a comment or the line end. `None` when that token is empty.
fn with_value(line: &str, value_from: usize, mode: ExecutionMode) -> Option<String> {
    let after_key = &line[value_from..];
    let token_start = value_from + (after_key.len() - after_key.trim_start_matches(' ').len());
    let token_length = line[token_start..]
        .find(|ch: char| ch.is_whitespace() || ch == '#')
        .unwrap_or(line.len() - token_start);
    if token_length == 0 {
        return None;
    }
    Some(format!(
        "{}{}{}",
        &line[..token_start],
        mode.as_str(),
        &line[token_start + token_length..]
    ))
}
