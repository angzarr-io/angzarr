//! The reason keys' args and surfaces, read from the compiled descriptors
//! (decisions 14.60 and 14.104): each live value of a reasons.proto's key
//! enum (core's `AngzarrReason`, each transport's `EmbeddedReason` or
//! `DistributedReason`; one enum per layer) names its args and its surface
//! in its `(io.angzarr.v1.reason)` option, which prost does not generate (it
//! skips `extend`), so the option is read off the raw `FileDescriptorSet`
//! bytes (`build_descriptor.rs`). Shared by angzarr-core's proto crates'
//! build scripts (`#[path]`).
//!
//! From them it writes `$OUT_DIR/reason_args.rs`, included in `v1`: an
//! `args()` and a `surface()` per enum, each an exhaustive match on its
//! variants (so a value the spec adds without a variant here fails to
//! compile, never silently has no args or surface), and the `reason_args`
//! module, one constant per distinct arg name. The surface is the option's
//! `surfaces` oneof (decision 14.91): its `grpc_status`, a `google.rpc.Code`
//! named by its variant (read from the vendored code.proto in the same set),
//! or `readiness`.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::descriptor::{fields, file, messages, text, varints};

/// Each key enum's value-name prefix, by enum name: one enum per layer
/// (decision 14.104), core's `AngzarrReason` and each transport's.
const PREFIXES: [(&str, &str); 3] = [
    ("AngzarrReason", "ANGZARR_REASON_"),
    ("EmbeddedReason", "EMBEDDED_REASON_"),
    ("DistributedReason", "DISTRIBUTED_REASON_"),
];

/// Core's reasons.proto, under the proto root: the crate whose code names
/// [`ReasonSurface`] and `google.rpc.Code` from inside its own `v1`.
const CORE_REASONS: &str = "io/angzarr/v1/reasons.proto";

/// The `(io.angzarr.v1.reason)` extension's field number on
/// `EnumValueOptions` (options.proto's contiguous range, decision 14.100).
const REASON_OPTION: u64 = 50105;

/// `ReasonOptions`' fields: `args` 1, and the `surfaces` oneof's arms,
/// `grpc_status` 2 (a `google.rpc.Code`) and `readiness` 3 (decision 14.91).
const ARGS: u64 = 1;
const GRPC_STATUS: u64 = 2;
const READINESS: u64 = 3;

/// How a key surfaces, as its option declares it.
enum Surface {
    /// `grpc_status`, its `google.rpc.Code` number.
    Status(u64),
    Readiness,
}

/// One enum value: its full name, its number, its declared args and its
/// declared surface (`None` where the option, or its surface, is absent).
struct Value {
    name: String,
    number: u64,
    args: Option<Vec<String>>,
    surface: Option<Surface>,
}

/// The surface the last of the option's `surfaces` arms names, as protobuf
/// reads a oneof: the last arm on the wire wins.
fn surface(reason: &[u8]) -> Option<Surface> {
    fields(reason)
        .into_iter()
        .filter_map(|(number, _, value)| match number {
            GRPC_STATUS => Some(Surface::Status(value)),
            READINESS => Some(Surface::Readiness),
            _ => None,
        })
        .next_back()
}

/// An `EnumValueDescriptorProto`: name 1, number 2, options 3; the
/// options' extension holds a `ReasonOptions`.
fn value(buf: &[u8]) -> Value {
    let number = varints(buf, 2).next().unwrap_or(0);
    let reason = messages(buf, 3)
        .flat_map(|options| messages(options, REASON_OPTION).collect::<Vec<_>>())
        .last();
    let args = reason.map(|reason| {
        messages(reason, ARGS)
            .map(|arg| String::from_utf8(arg.to_vec()).expect("a UTF-8 arg name"))
            .collect()
    });
    Value {
        name: text(buf, 1),
        number,
        args,
        surface: reason.and_then(surface),
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

/// The paths the generated code names core's types by: from inside core's
/// own `v1`, or from a transport crate through its angzarr-proto
/// dependency.
struct Paths {
    surface: &'static str,
    code: &'static str,
}

/// The vendored `google.rpc.Code` (decision 14.104), as the set holds it
/// (every crate's set includes it, an import of core's reasons.proto).
const GOOGLE_RPC_CODE: &str = "google/rpc/code.proto";

/// prost's variant of each `google.rpc.Code` number: its value name, each
/// word capitalised (the enum's values carry no prefix).
fn code_variants(set: &[u8]) -> Vec<(u64, String)> {
    messages(file(set, GOOGLE_RPC_CODE), 5)
        .filter(|e| text(e, 1) == "Code")
        .flat_map(|e| {
            messages(e, 2)
                .map(|v| (varints(v, 2).next().unwrap_or(0), variant(&text(v, 1), "")))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The `Surface` expression of one live value.
fn surface_expr(v: &Value, paths: &Paths, codes: &[(u64, String)]) -> String {
    match &v.surface {
        Some(Surface::Status(0)) => panic!(
            "{} declares grpc_status OK, which is no key's status (decision 14.104)",
            v.name
        ),
        Some(Surface::Status(number)) => {
            let code = codes
                .iter()
                .find(|(n, _)| n == number)
                .map(|(_, name)| name)
                .unwrap_or_else(|| panic!("{} declares grpc_status {number}, no google.rpc.Code", v.name));
            format!("Some({}::GrpcStatus({}::{code}))", paths.surface, paths.code)
        }
        Some(Surface::Readiness) => format!("Some({}::Readiness)", paths.surface),
        None => panic!(
            "{} declares no surface: reasons.proto's live keys each name grpc_status or readiness (decision 14.91)",
            v.name
        ),
    }
}

/// One enum's `impl` with its `args()` and its `surface()`.
fn write_enum(
    out: &mut String,
    (name, prefix, values): (&str, &str, &[Value]),
    paths: &Paths,
    codes: &[(u64, String)],
) {
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
    out.push_str("        }\n    }\n\n");
    writeln!(
        out,
        "    /// How the key surfaces, as its `(io.angzarr.v1.reason)` option\n    \
         /// declares it in reasons.proto (decisions 14.91 and 14.104): the\n    \
         /// `google.rpc.Code` it travels with, or a readiness refusal;\n    \
         /// `None` for `UNSPECIFIED`, which is never answered.\n    \
         pub const fn surface(self) -> Option<{}> {{\n        match self {{",
        paths.surface
    )
    .unwrap();
    for v in values {
        let expr = if v.number == 0 {
            "None".to_string()
        } else {
            surface_expr(v, paths, codes)
        };
        writeln!(
            out,
            "            Self::{} => {expr},",
            variant(&v.name, prefix)
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
/// `path` in the encoded descriptor set `set`. Core's statuses are in scope
/// in core's own `v1`; a transport's code names them through its
/// angzarr-proto dependency.
pub fn generate(set: &[u8], path: &str, enums: &[&str]) -> String {
    let paths = if path == CORE_REASONS {
        Paths {
            surface: "crate::ReasonSurface",
            code: "crate::google::rpc::Code",
        }
    } else {
        Paths {
            surface: "::angzarr_proto::ReasonSurface",
            code: "::angzarr_proto::google::rpc::Code",
        }
    };
    let codes = code_variants(set);
    let found = read(set, path);
    let names_found: Vec<&str> = found.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(names_found, enums, "{path}'s key enums");
    let mut out = String::new();
    let mut names = BTreeSet::new();
    for (name, prefix, values) in &found {
        write_enum(&mut out, (name, prefix, values), &paths, &codes);
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
