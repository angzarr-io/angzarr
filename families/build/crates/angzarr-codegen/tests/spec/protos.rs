//! The protos a scenario declares, written out and compiled: each domain
//! its own package (`buy-in` is `buyin`), each message with the options the
//! scenario gives it, compiled by protoc into the descriptor set the
//! generator reads, with angzarr's options importable.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;

use angzarr_test_dir::TestDir;

/// A component's `ComponentOptions`: its one name and the arm of its kind.
#[derive(Clone, Debug, Default)]
pub struct Component {
    /// `AGGREGATE`, `PROCESS_MANAGER`, `POLICY` or `PROJECTOR`.
    pub kind: String,
    /// `component_name`; none declares none.
    pub component_name: Option<String>,
    pub domain: Option<String>,
    pub output_domain: Option<String>,
    pub starting_types: Vec<String>,
}

/// A command's `CommandOptions`, or a fact's `FactOptions`.
#[derive(Clone, Debug, Default)]
pub struct Input {
    pub component: String,
    pub emits: Vec<String>,
    /// A command's only.
    pub refuses: Vec<String>,
    /// Declared by `(fact)`, not `(command)`.
    pub fact: bool,
    /// Declared by both options, `(command)` and `(fact)`.
    pub both: bool,
}

/// What a scenario says of a message's `(refusal_status)` option.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Status {
    /// Nothing: a message some command's `refuses` names is written with
    /// a valid status, so a scenario about something else is not refused
    /// for lacking one; any other message carries none.
    #[default]
    Unsaid,
    /// The scenario says it carries none.
    Absent,
    /// The scenario marks it (`REFUSAL_STATUS_NOT_FOUND`).
    Marked(String),
}

/// The status a message no scenario marked is written with when a
/// command's `refuses` names it.
const DEFAULT_STATUS: &str = "REFUSAL_STATUS_FAILED_PRECONDITION";

/// One message, with its options.
#[derive(Clone, Debug, Default)]
pub struct Message {
    pub component: Option<Component>,
    pub input: Option<Input>,
    /// Its `(event)` entries: the component, and the domain it names.
    pub events: Vec<(String, Option<String>)>,
    /// Its `(refusal_status)`.
    pub status: Status,
}

/// Every message a scenario declared, by full name.
#[derive(Clone, Debug, Default)]
pub struct Protos {
    pub messages: BTreeMap<String, Message>,
    /// Files of the protos' include path every package file imports (the
    /// framework's own, `io/angzarr/v1/types.proto`).
    pub imports: Vec<String>,
}

/// A domain's package: its name with no `-`.
pub fn package(domain: &str) -> String {
    domain.replace('-', "")
}

/// A type as a scenario names it, `buy-in.RecordFundsHeld`, by its full
/// proto name, `buyin.RecordFundsHeld`.
pub fn full(name: &str) -> String {
    match name.rsplit_once('.') {
        Some((domain, rest)) => format!("{}.{rest}", package(domain)),
        None => name.to_string(),
    }
}

fn list(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|i| format!("\"{i}\"")).collect();
    format!("[{}]", quoted.join(", "))
}

fn component_option(c: &Component) -> String {
    let mut out = String::new();
    if let Some(name) = &c.component_name {
        let _ = writeln!(out, "    component_name: \"{name}\"");
    }
    let mut arm = String::new();
    for (key, value) in [("domain", &c.domain), ("output_domain", &c.output_domain)] {
        if let Some(value) = value {
            let _ = write!(arm, " {key}: \"{value}\"");
        }
    }
    if !c.starting_types.is_empty() {
        let _ = write!(arm, " starting_types: {}", list(&c.starting_types));
    }
    let _ = writeln!(out, "    {}: {{{arm} }}", c.kind.to_lowercase());
    format!("  option (io.angzarr.v1.component) = {{\n{out}  }};\n")
}

fn input_option(i: &Input) -> String {
    let mut out = format!("    component: \"{}\"\n", i.component);
    if !i.emits.is_empty() {
        let _ = writeln!(out, "    emits: {}", list(&i.emits));
    }
    if !i.refuses.is_empty() {
        let _ = writeln!(out, "    refuses: {}", list(&i.refuses));
    }
    let options: &[&str] = match (i.both, i.fact) {
        (true, _) => &["command", "fact"],
        (false, true) => &["fact"],
        (false, false) => &["command"],
    };
    options
        .iter()
        .map(|option| format!("  option (io.angzarr.v1.{option}) = {{\n{out}  }};\n"))
        .collect()
}

fn event_option((component, domain): &(String, Option<String>)) -> String {
    let domain = domain
        .as_ref()
        .map(|d| format!(" domain: \"{d}\""))
        .unwrap_or_default();
    format!("  option (io.angzarr.v1.event) = {{component: \"{component}\"{domain}}};\n")
}

fn message(name: &str, m: &Message, refused: bool) -> String {
    let mut out = format!("message {name} {{\n");
    let status = match &m.status {
        Status::Marked(status) => Some(status.as_str()),
        Status::Unsaid if refused => Some(DEFAULT_STATUS),
        Status::Unsaid | Status::Absent => None,
    };
    if let Some(status) = status {
        let _ = writeln!(out, "  option (io.angzarr.v1.refusal_status) = {status};");
    }
    out += &m
        .component
        .as_ref()
        .map(component_option)
        .unwrap_or_default();
    out += &m.input.as_ref().map(input_option).unwrap_or_default();
    for event in &m.events {
        out += &event_option(event);
    }
    out + "}\n\n"
}

impl Protos {
    /// The message `name` (a full name), declared empty if it is not yet.
    pub fn message(&mut self, name: &str) -> &mut Message {
        self.messages.entry(name.to_string()).or_default()
    }

    /// Each package's file: its path and its text.
    pub fn files(&self) -> BTreeMap<String, String> {
        let mut files: BTreeMap<String, String> = BTreeMap::new();
        let imports: String = self
            .imports
            .iter()
            .map(|file| format!("import \"{file}\";\n"))
            .collect();
        let mut text = |full: &str, body: String| {
            let (package, _) = full.rsplit_once('.').expect("a full name");
            let file = files.entry(format!("{package}.proto")).or_insert_with(|| {
                format!("syntax = \"proto3\";\npackage {package};\nimport \"io/angzarr/v1/options.proto\";\n{imports}\n")
            });
            file.push_str(&body);
        };
        let refused: Vec<&String> = self
            .messages
            .values()
            .filter_map(|m| m.input.as_ref())
            .flat_map(|i| &i.refuses)
            .collect();
        for (full, m) in &self.messages {
            text(
                full,
                message(
                    full.rsplit_once('.').expect("a full name").1,
                    m,
                    refused.contains(&full),
                ),
            );
        }
        files
    }
}

/// protoc's descriptor set for `files` (path, text), with angzarr's options
/// importable: the spec's own protos first, then angzarr-proto's. They are
/// written to a [`TestDir`] of their own, removed when compiled.
pub fn compile(files: &BTreeMap<String, String>) -> Vec<u8> {
    let dir = TestDir::new(concat!(env!("CARGO_PKG_NAME"), "-spec"));
    let protos = dir.join("protos");
    std::fs::create_dir_all(&protos).expect("a scratch directory");
    for (path, text) in files {
        std::fs::write(protos.join(path), text).expect("a proto written");
    }
    let angzarr =
        angzarr_proto::protos::write_include_dir(&dir.join("angzarr")).expect("angzarr's protos");
    let spec = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../angzarr-project/proto");
    let out = dir.join("set.bin");
    let protoc = std::env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
    let status = Command::new(protoc)
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", out.display()))
        .arg(format!("-I{}", protos.display()))
        .arg(format!("-I{}", spec.display()))
        .arg(format!("-I{}", angzarr.display()))
        .args(files.keys())
        .status()
        .expect("protoc on PATH");
    assert!(status.success(), "protoc failed on {files:#?}");
    std::fs::read(&out).expect("the descriptor set")
}
