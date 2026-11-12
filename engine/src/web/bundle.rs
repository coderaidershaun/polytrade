//! The workstation bundle, read into memory once. A request can only name a file that was there at
//! startup, so no request path is ever joined to a folder and none can escape it. The app
//! descriptor sits in the same table, so it needs no route of its own.

use std::collections::HashMap;
use std::path::Path;

use hyper::body::Bytes;
use serde::Serialize;

use super::WebError;

const INDEX_PATH: &str = "index.html";
const DESCRIPTOR_PATH: &str = "jet-app.json";

pub(super) struct BundleFile {
    pub(super) content_type: &'static str,
    pub(super) body: Bytes,
}

pub(super) struct Bundle {
    files: HashMap<String, BundleFile>,
}

#[derive(Serialize)]
struct Descriptor<'a> {
    version: u8,
    name: &'static str,
    ui: &'static str,
    embedding: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    mcp: Option<&'a str>,
}

impl Bundle {
    pub(super) fn load(dir: &Path, mcp: Option<&str>) -> Result<Bundle, WebError> {
        let mut files = HashMap::new();
        if dir.is_dir() {
            read_tree(dir, "", &mut files)?;
        }
        if !files.contains_key(INDEX_PATH) {
            return Err(WebError::BundleMissing {
                dir: dir.to_path_buf(),
            });
        }
        files.insert(DESCRIPTOR_PATH.to_owned(), descriptor(mcp));
        Ok(Bundle { files })
    }

    /// `request_path` is a URI path, so it starts with a slash. `/` is the page.
    pub(super) fn find(&self, request_path: &str) -> Option<&BundleFile> {
        let name = request_path.strip_prefix('/')?;
        self.files
            .get(if name.is_empty() { INDEX_PATH } else { name })
    }
}

fn read_tree(
    dir: &Path,
    prefix: &str,
    files: &mut HashMap<String, BundleFile>,
) -> Result<(), WebError> {
    for entry in std::fs::read_dir(dir).map_err(read_failure(dir))? {
        let entry = entry.map_err(read_failure(dir))?;
        let path = entry.path();
        let name = format!("{prefix}{}", entry.file_name().to_string_lossy());
        let kind = entry.file_type().map_err(read_failure(&path))?;
        // SMELL: hidden files, such as the lock file Trunk leaves in the bundle, are loaded and
        // served like the page itself. Skipping names that start with a dot would keep build
        // leftovers off the wire.
        if kind.is_dir() {
            read_tree(&path, &format!("{name}/"), files)?;
        } else if kind.is_file() {
            let body = std::fs::read(&path).map_err(read_failure(&path))?;
            let content_type = content_type_of(&name);
            files.insert(
                name,
                BundleFile {
                    content_type,
                    body: Bytes::from(body),
                },
            );
        }
    }
    Ok(())
}

fn read_failure(path: &Path) -> impl FnOnce(std::io::Error) -> WebError {
    let path = path.to_path_buf();
    move |source| WebError::BundleRead { path, source }
}

fn descriptor(mcp: Option<&str>) -> BundleFile {
    let descriptor = Descriptor {
        version: 1,
        name: "Polysim",
        ui: "./",
        embedding: "relative-paths",
        mcp,
    };
    let body = serde_json::to_vec(&descriptor)
        .expect("a descriptor of text and one integer always serialises");
    BundleFile {
        content_type: "application/json",
        body: Bytes::from(body),
    }
}

fn content_type_of(name: &str) -> &'static str {
    let extension = Path::new(name)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "wasm" => "application/wasm",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
