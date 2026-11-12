//! The mcp suites share these fixtures: an engine tree on disk, shell scripts that stand in for
//! engine binaries, deadlines short enough for a test, and the checks that read a tool's reply.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rmcp::model::CallToolResult;
use serde_json::Value;

use polysim::config::RunIdentity;
use polysim::mcp::{Fleet, FleetTiming};

use crate::parquet_readback::TempDir;

pub const FAST: FleetTiming = FleetTiming {
    start_grace: Duration::from_millis(500),
    stop_deadline: Duration::from_secs(1),
};

/// A freshly written script can sit in the operating system's malware scan for tens of seconds
/// under load. Every wait on one returns as soon as it answers, so a fast machine never pays this.
pub const FRESH_SCRIPT_DEADLINE: Duration = Duration::from_secs(60);

/// Every long-running script ends on its own after about thirty seconds, so a failed assertion
/// never leaves an endless loop behind on the machine. Each one prints `ready` once its trap is
/// set, because the first run of a freshly written script can take longer than the start grace,
/// and a signal that lands before the trap kills the shell outright.
macro_rules! bounded_script {
    ($prelude:literal) => {
        concat!(
            "#!/bin/sh\n",
            $prelude,
            "echo ready\n",
            "i=0; while [ $i -lt 600 ]; do sleep 0.05; i=$((i+1)); done\n"
        )
    };
}

pub const TRAP_TERM: &str = bounded_script!("trap 'exit 0' TERM\n");
pub const IGNORE_TERM: &str = bounded_script!("trap '' TERM\n");
pub const MARKERS: &str =
    bounded_script!("echo stdout-marker\necho stderr-marker >&2\ntrap 'exit 0' TERM\n");
pub const EXITS_THREE: &str = "#!/bin/sh\necho \"config refused\" >&2\nexit 3\n";

pub const LINK_BIND: &str = "127.0.0.1:9399";
pub const MODE_OFF: &str = "execution:\n  mode: off\nlink:\n  bind: \"127.0.0.1:9399\"\n";
pub const MODE_SIM: &str = "execution:\n  mode: sim\n";
pub const NO_EXECUTION: &str = "engine:\n  spin_interval_us: 1000\n";
pub const UNPARSEABLE: &str = "execution: [mode: off\n";

pub struct EngineTree {
    dir: TempDir,
}

impl EngineTree {
    pub fn new(tag: &str) -> EngineTree {
        EngineTree {
            dir: TempDir::new(tag),
        }
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    pub fn bin_dir(&self) -> PathBuf {
        self.root().join("bin")
    }

    pub fn config(&self, strategy: &str, te: &str, text: &str) -> &EngineTree {
        let path = self.config_path(strategy, te);
        std::fs::create_dir_all(path.parent().expect("config has a folder")).expect("mkdir");
        std::fs::write(&path, text).expect("write config");
        self
    }

    pub fn script(&self, strategy: &str, te: &str, body: &str) -> &EngineTree {
        let dir = self.bin_dir();
        std::fs::create_dir_all(&dir).expect("mkdir bin");
        let path = dir.join(format!("{strategy}-{te}"));
        std::fs::write(&path, body).expect("write script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        self
    }

    pub fn engine(&self, strategy: &str, te: &str, config: &str, body: &str) -> &EngineTree {
        self.config(strategy, te, config).script(strategy, te, body)
    }

    pub fn config_path(&self, strategy: &str, te: &str) -> PathBuf {
        self.root()
            .join("strategies")
            .join(strategy)
            .join(te)
            .join("config.yaml")
    }

    pub fn console_log(&self, stem: &str) -> PathBuf {
        self.root().join("logs").join(format!("{stem}-console.log"))
    }

    /// Returns once the script has set its signal trap, and fails the test if it never does.
    pub fn wait_until_ready(&self, stem: &str) {
        let log = self.console_log(stem);
        let text = wait_for_file_containing(&log, &["ready"]);
        assert!(
            text.contains("ready"),
            "{stem} never set its trap: {text:?}"
        );
    }

    pub fn fleet(&self, timing: FleetTiming) -> Fleet {
        Fleet::new(self.root().to_path_buf(), self.bin_dir(), timing)
    }
}

pub fn identity(strategy: &str, te: &str) -> RunIdentity {
    RunIdentity::new(strategy, te).expect("a valid identity")
}

/// Polls rather than trusting the start grace, so a loaded parallel run cannot turn a slow child
/// into a false failure.
pub fn wait_for_file_containing(path: &Path, needles: &[&str]) -> String {
    let deadline = Instant::now() + FRESH_SCRIPT_DEADLINE;
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if needles.iter().all(|needle| text.contains(needle)) || Instant::now() >= deadline {
            return text;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn text_of(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|block| block.as_text())
        .map(|text| text.text.as_str())
        .expect("every reply carries a text block")
}

/// A success must carry the same JSON object as structured content and as its first text block.
pub fn reply_of(result: &CallToolResult) -> Value {
    assert_eq!(result.is_error, Some(false), "{}", text_of(result));
    let structured = result
        .structured_content
        .clone()
        .expect("a success carries structured content");
    let text: Value = serde_json::from_str(text_of(result)).expect("the text block is JSON");
    assert_eq!(
        structured, text,
        "the text block mirrors the structured content"
    );
    structured
}
