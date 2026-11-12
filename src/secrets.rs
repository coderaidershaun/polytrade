//! Venue-neutral credential loading: config names the ENVIRONMENT VARIABLES that hold an API key
//! pair, never the values, and the values themselves live in a type that refuses to print itself.

use std::env::VarError;
use std::fmt;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// Prints <redacted>, zeroes bytes on drop. Not Clone -> every copy needs zeroing.
pub struct Secret {
    bytes: Box<[u8]>,
}

impl Secret {
    pub fn new(value: &str) -> Self {
        Self {
            bytes: value.as_bytes().into(),
        }
    }

    /// The only way out. Named to make a leak deliberate rather than incidental at the call site.
    pub fn expose_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Duplicate so EnvFile can drop+zero after startup instead of living process-long.
    fn duplicate(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
        }
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        self.bytes.fill(0);
        // Optimizer may delete fill -> use black_box to inhibit. unsafe forbidden crate-wide.
        std::hint::black_box(&self.bytes);
    }
}

/// Which env vars hold venue API key pair. Named not positional -> prevent swap compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialVariables<'a> {
    pub api_key_env: &'a str,
    pub api_secret_env: &'a str,
}

#[derive(Debug)]
pub struct Credentials {
    api_key: Secret,
    api_secret: Secret,
}

impl Credentials {
    pub fn api_key(&self) -> &Secret {
        &self.api_key
    }

    pub fn api_secret(&self) -> &Secret {
        &self.api_secret
    }
}

/// Variables parsed from .env, consulted only when process environment is silent. Fallback layer
/// so CI/prod can inject differently; env::set_var is unsafe forbidden crate-wide.
#[derive(Debug)]
pub struct EnvFile {
    path: PathBuf,
    entries: Vec<(String, Secret)>,
}

impl EnvFile {
    /// Repo-root default; a deployment passes its own path to [`EnvFile::load`] instead.
    pub const DEFAULT_PATH: &'static str = ".env";

    /// Missing file OK; operator may export vars instead.
    pub fn load(path: &Path) -> Result<Self, SecretError> {
        // Read bytes not String: file is all secrets, String.drop recovers from core dump.
        let mut raw = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == ErrorKind::NotFound => Vec::new(),
            Err(source) => {
                return Err(SecretError::ReadFile {
                    path: path.to_path_buf(),
                    source,
                });
            }
        };
        let parsed = parse_entries(path, &raw);
        raw.fill(0);
        std::hint::black_box(&raw);

        Ok(Self {
            path: path.to_path_buf(),
            entries: parsed?,
        })
    }

    /// Process env wins; file fills unset only. Exported-but-empty doesn't shadow file.
    pub fn resolve(&self, variable: &str) -> Result<Secret, SecretError> {
        let from_process = match std::env::var(variable) {
            Ok(value) => Some(value),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => {
                return Err(SecretError::NotUnicode {
                    variable: variable.to_owned(),
                });
            }
        };
        let from_file = self
            .entries
            .iter()
            .find(|(name, _)| name == variable)
            .map(|(_, secret)| secret);

        if let Some(value) = from_process.as_deref().filter(|value| !value.is_empty()) {
            return Ok(Secret::new(value));
        }
        if let Some(secret) = from_file.filter(|secret| !secret.expose_bytes().is_empty()) {
            return Ok(secret.duplicate());
        }
        if from_process.is_some() || from_file.is_some() {
            return Err(SecretError::Empty {
                variable: variable.to_owned(),
            });
        }
        Err(SecretError::Missing {
            variable: variable.to_owned(),
            path: self.path.clone(),
        })
    }

    pub fn resolve_credentials(
        &self,
        variables: &CredentialVariables<'_>,
    ) -> Result<Credentials, SecretError> {
        Ok(Credentials {
            api_key: self.resolve(variables.api_key_env)?,
            api_secret: self.resolve(variables.api_secret_env)?,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SecretError {
    #[error("reading env file {path} failed")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// No content: line may be pasted secret, don't expose in error message.
    #[error(
        "env file {path} line {line} is malformed — expected KEY=VALUE with KEY matching [A-Za-z_][A-Za-z0-9_]*, a # comment, or a blank; `export KEY=VALUE` is not accepted"
    )]
    MalformedLine { path: PathBuf, line: usize },
    #[error(
        "credential variable {variable} is not set — export it or add {variable}=<value> to {path}"
    )]
    Missing { variable: String, path: PathBuf },
    #[error("credential variable {variable} is set but carries no value")]
    Empty { variable: String },
    #[error("credential variable {variable} holds bytes that are not utf-8")]
    NotUnicode { variable: String },
}

fn parse_entries(path: &Path, raw: &[u8]) -> Result<Vec<(String, Secret)>, SecretError> {
    let mut entries = Vec::new();
    for (index, raw_line) in raw.split(|byte| *byte == b'\n').enumerate() {
        let malformed = || SecretError::MalformedLine {
            path: path.to_path_buf(),
            line: index + 1,
        };
        let Ok(line) = str::from_utf8(raw_line) else {
            return Err(malformed());
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            return Err(malformed());
        };
        let name = name.trim();
        if !is_variable_name(name) {
            return Err(malformed());
        }
        entries.push((name.to_owned(), Secret::new(unquote(value.trim()))));
    }
    Ok(entries)
}

/// Reject names loosely -> prevent `export KEY=value` stored as `"export KEY"` reading as unset.
fn is_variable_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn unquote(value: &str) -> &str {
    let single = value
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''));
    let double = value
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'));
    single.or(double).unwrap_or(value)
}
