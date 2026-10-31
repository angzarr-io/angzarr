//! The reason keys' args, read from the compiled descriptors (decision
//! 14.60): each live value of `RouterReason`, `HostReason` and
//! `CoordinatorReason` names its args in its `(io.angzarr.v1.reason)`
//! option, which prost does not generate (it skips `extend`), so the
//! option is read off the raw `FileDescriptorSet` bytes, the way
//! `tests/conflict_units.rs` reads the conflict-unit options.
//!
//! From them it writes `$OUT_DIR/reason_args.rs`, included in `v1`: an
//! `args()` per enum, an exhaustive match on its variants (so a value the
//! spec adds without a variant here fails to compile, never silently has no
//! args), and the `reason_args` module, one constant per distinct arg name.

use std::collections::BTreeSet;
use std::fmt::Write as _;

/// The file the enums are in, as the descriptor set names it.
const REASONS_PROTO: &str = "io/angzarr/v1/reasons.proto";

/// The enums whose values are keys, and each one's value-name prefix.
const ENUMS: [(&str, &str); 3] = [
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

/// Each wire field: its number, its bytes if length-delimited, and its
/// value if a varint. A compiled descriptor holds no fixed-width field (an
/// uninterpreted option's double never survives compilation), so one is
/// refused rather than skipped unread.
fn fields(mut buf: &[u8]) -> Vec<(u64, Option<&[u8]>, u64)> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        let key = varint(&mut buf);
        match key & 7 {
            0 => {
                let v = varint(&mut buf);
                out.push((key >> 3, None, v));
            }
            2 => {
                let len = usize::try_from(varint(&mut buf)).expect("a length");
                let (body, rest) = buf.split_at(len);
                out.push((key >> 3, Some(body), 0));
                buf = rest;
            }
            wire => panic!("descriptor wire type {wire}: not a varint or length-delimited field"),
        }
    }
    out
}

fn varint(buf: &mut &[u8]) -> u64 {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = buf[0];
        *buf = &buf[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
    }
    panic!("a varint longer than 64 bits in the descriptor set")
}

/// The length-delimited fields of `buf` numbered `number`.
fn messages(buf: &[u8], number: u64) -> impl Iterator<Item = &[u8]> {
    fields(buf)
        .into_iter()
        .filter(move |(n, _, _)| *n == number)
        .filter_map(|(_, body, _)| body)
}

fn text(buf: &[u8], number: u64) -> String {
    messages(buf, number)
        .last()
        .map(|b| String::from_utf8(b.to_vec()).expect("a UTF-8 name"))
        .unwrap_or_default()
}

/// An `EnumValueDescriptorProto`: name 1, number 2, options 3; the
/// options' extension holds a `ReasonOptions`, whose args are field 1.
fn value(buf: &[u8]) -> Value {
    let number = fields(buf)
        .into_iter()
        .find(|(n, _, _)| *n == 2)
        .map_or(0, |(_, _, v)| v);
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

/// Each key enum's values, by enum name, read from the encoded set.
fn read(set: &[u8]) -> Vec<(String, Vec<Value>)> {
    let file = messages(set, 1)
        .find(|file| text(file, 1) == REASONS_PROTO)
        .unwrap_or_else(|| panic!("{REASONS_PROTO} is not in the descriptor set"));
    messages(file, 5)
        .map(|e| (text(e, 1), messages(e, 2).map(value).collect()))
        .filter(|(name, _)| ENUMS.iter().any(|(n, _)| n == name))
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

/// `reason_args.rs` for the encoded descriptor set `set`.
pub fn generate(set: &[u8]) -> String {
    let enums = read(set);
    assert_eq!(
        enums.len(),
        ENUMS.len(),
        "{REASONS_PROTO} lacks one of {ENUMS:?}"
    );
    let mut out = String::new();
    let mut names = BTreeSet::new();
    for (name, values) in &enums {
        let prefix = ENUMS.iter().find(|(n, _)| n == name).expect("listed").1;
        write_enum(&mut out, name, prefix, values);
        names.extend(values.iter().flat_map(|v| v.args.iter().flatten().cloned()));
    }
    write_names(&mut out, &names);
    out
}
