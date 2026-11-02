//! The reason keys' args, read from the compiled descriptors (decision
//! 14.60): each live value of a reasons.proto's `RouterReason`,
//! `HostReason` and `CoordinatorReason` (core's, and each transport's;
//! decision 14.64) names its args in its `(io.angzarr.v1.reason)` option,
//! which prost does not generate (it skips `extend`), so the option is
//! read off the raw `FileDescriptorSet` bytes (`build_descriptor.rs`), the
//! way `tests/conflict_units.rs` reads the conflict-unit options. Shared by
//! angzarr-core's proto crates' build scripts (`#[path]`).
//!
//! From them it writes `$OUT_DIR/reason_args.rs`, included in `v1`: an
//! `args()` per enum, an exhaustive match on its variants (so a value the
//! spec adds without a variant here fails to compile, never silently has no
//! args), and the `reason_args` module, one constant per distinct arg name.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::descriptor::{file, messages, text, varints};

/// Each key enum's value-name prefix, by enum name: the same in core's
/// reasons.proto and in each transport's (decision 14.64).
const PREFIXES: [(&str, &str); 3] = [
    ("RouterReason", "ROUTER_REASON_"),
    ("HostReason", "HOST_REASON_"),
    ("CoordinatorReason", "COORDINATOR_REASON_"),
];

/// The `(io.angzarr.v1.reason)` extension's field number on
/// `EnumValueOptions`.
const REASON_OPTION: u64 = 50109;

/// One enum value: its full name, its number, and its declared args
/// (`None` where the option is absent).
struct Value {
    name: String,
    number: u64,
    args: Option<Vec<String>>,
}

/// An `EnumValueDescriptorProto`: name 1, number 2, options 3; the
/// options' extension holds a `ReasonOptions`, whose args are field 1.
fn value(buf: &[u8]) -> Value {
    let number = varints(buf, 2).next().unwrap_or(0);
    let args = messages(buf, 3)
        .flat_map(|options| messages(options, REASON_OPTION).collect::<Vec<_>>())
        .last()
        .map(|reason| {
            messages(reason, 1)
                .map(|arg| String::from_utf8(arg.to_vec()).expect("a UTF-8 arg name"))
                .collect()
        });
    Value {
        name: text(buf, 1),
        number,
        args,
    }
}

/// Each key enum of the file `path`, by enum name, with its prefix and its
/// values, read from the encoded set.
fn read(set: &[u8], path: &str) -> Vec<(String, &'static str, Vec<Value>)> {
    messages(file(set, path), 5)
        .filter_map(|e| {
            let name = text(e, 1);
            let prefix = PREFIXES.iter().find(|(n, _)| *n == name)?.1;
            Some((name, prefix, messages(e, 2).map(value).collect()))
        })
        .collect()
}

/// prost's variant name for a value: its enum's prefix dropped, each
/// `_`-separated word capitalised.
fn variant(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix)
        .unwrap_or_else(|| panic!("{name} does not carry its enum's prefix {prefix}"))
        .split('_')
        .map(|word| {
            let lower = word.to_ascii_lowercase();
            let mut chars = lower.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_ascii_uppercase().to_string() + chars.as_str()
            })
        })
        .collect()
}

/// One enum's `impl` with its `args()`.
fn write_enum(out: &mut String, name: &str, prefix: &str, values: &[Value]) {
    writeln!(out, "impl {name} {{").unwrap();
    writeln!(
        out,
        "    /// The key's args, by name, as its `(io.angzarr.v1.reason)` option\n    \
         /// declares them in reasons.proto (decision 14.60); none for\n    \
         /// `UNSPECIFIED`, which is never answered.\n    \
         pub const fn args(self) -> &'static [&'static str] {{\n        match self {{"
    )
    .unwrap();
    for v in values {
        let args = match (&v.args, v.number) {
            (_, 0) => Vec::new(),
            (Some(args), _) => args.clone(),
            (None, _) => panic!(
                "{} declares no args: reasons.proto's live keys each carry the (reason) option",
                v.name
            ),
        };
        writeln!(
            out,
            "            Self::{} => &{:?},",
            variant(&v.name, prefix),
            args
        )
        .unwrap();
    }
    out.push_str("        }\n    }\n}\n\n");
}

/// The constants module: one `pub const` per distinct arg name.
fn write_names(out: &mut String, names: &BTreeSet<String>) {
    out.push_str(
        "/// Every arg name a reason key declares (decision 14.60), one constant\n\
         /// each, named as the arg in SCREAMING_SNAKE: the `ErrorInfo` metadata\n\
         /// keys and `Diagnostic.args` names, generated from reasons.proto.\n\
         pub mod reason_args {\n",
    );
    for name in names {
        writeln!(
            out,
            "    pub const {}: &str = {name:?};",
            name.to_ascii_uppercase()
        )
        .unwrap();
    }
    out.push_str("}\n");
}

/// `reason_args.rs` for the key enums `enums` of the reasons.proto at
/// `path` in the encoded descriptor set `set`.
pub fn generate(set: &[u8], path: &str, enums: &[&str]) -> String {
    let found = read(set, path);
    let names_found: Vec<&str> = found.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(names_found, enums, "{path}'s key enums");
    let mut out = String::new();
    let mut names = BTreeSet::new();
    for (name, prefix, values) in &found {
        write_enum(&mut out, name, prefix, values);
        names.extend(values.iter().flat_map(|v| v.args.iter().flatten().cloned()));
    }
    write_names(&mut out, &names);
    out
}

/// A package's reason keys: its `reasons.proto`'s key enums' args, written
/// to `$OUT_DIR/reason_args.rs` from the encoded descriptor set `set`, and
/// the `reasons.en-US.ftl` beside it copied to `$OUT_DIR` (decisions 14.60
/// and 14.64), for the library to include and embed.
pub fn reason_keys(
    root: &std::path::Path,
    out: &std::path::Path,
    set: &[u8],
    package: &str,
    enums: &[&str],
) {
    let catalog = root.join(package).join("reasons.en-US.ftl");
    println!("cargo:rerun-if-changed={}", catalog.display());
    let path = format!("{package}/reasons.proto");
    std::fs::write(out.join("reason_args.rs"), generate(set, &path, enums))
        .expect("write reason_args.rs");
    std::fs::copy(&catalog, out.join("reasons.en-US.ftl")).unwrap_or_else(|e| {
        panic!(
            "copy {} (an angzarr-project revision before decision 14.64?): {e}",
            catalog.display()
        )
    });
}
