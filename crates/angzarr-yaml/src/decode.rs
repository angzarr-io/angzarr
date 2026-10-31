//! A strict serde decode over [`Value`]: one Rust type is a document's
//! schema, shared by its reader and by any linter.
//!
//! - **Scalars are text,** parsed only when the target type asks: `port: 80`
//!   is the text `80` until a `u16` field reads it, and a `String` field
//!   takes `no`, `1.10` or `0x1F` as written. Booleans are `true` and
//!   `false`; integers are decimal (`-` for a signed type); floats are Rust's
//!   decimal forms plus YAML's `.inf`, `-.inf` and `.nan`. An empty value is
//!   null: an `Option` reads it as `None`, `()` as itself, and nothing else
//!   takes it.
//! - **Unknown keys are refused,** whether or not the type says
//!   `#[serde(deny_unknown_fields)]` (strict for all, owner ruling
//!   2026-10-31: there is no keep-unknown mode). With it, the error lists the
//!   keys the mapping could have had.
//! - **Enums** are a scalar for a unit variant, or a mapping of one key, the
//!   variant, to its content.
//! - **Untyped targets** (an untagged enum, `#[serde(flatten)]`, serde's own
//!   buffering, `deserialize_any`) see every scalar as its text and null as
//!   unit: nothing is guessed, so such a target's numbers are strings.
//! - **Every failure is a [`DecodeError`]:** a code, the key path
//!   ([`DecodeError::key_path`], `component[1].kind`), what was expected
//!   ([`Expected`]) and what was found, and never English. A type's own
//!   refusal (`de::Error::custom`) keeps its text verbatim as the arg
//!   `ext_err_txt` (the platform's rule for a third party's text, 2026-10-29).

use std::cell::Cell;
use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic};
use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};

use crate::Value;

/// Decode `value` into `T`, strictly.
pub fn from_value<'de, T: de::Deserialize<'de>>(value: &'de Value) -> Result<T, DecodeError> {
    let ctx = Ctx {
        asked: Cell::new(None),
    };
    let de = Node {
        at: At::Value(value),
        path: Vec::new(),
        ctx: &ctx,
    };
    let path = de.path.clone();
    T::deserialize(de).map_err(|f| f.finish(&path, At::Value(value), &ctx))
}

/// One step of a key path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathSeg {
    Key(String),
    Item(usize),
}

/// What the type asked for where the decode failed: a token, never prose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expected {
    Bool,
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Char,
    String,
    Bytes,
    /// An empty value: `()`, a unit struct or a unit variant's content.
    Null,
    Sequence,
    Mapping,
    Enum,
    /// Anything: an untyped target.
    Any,
}

impl Expected {
    /// The token the error's `expected` arg carries.
    pub fn token(self) -> &'static str {
        match self {
            Expected::Bool => "bool",
            Expected::I8 => "i8",
            Expected::I16 => "i16",
            Expected::I32 => "i32",
            Expected::I64 => "i64",
            Expected::I128 => "i128",
            Expected::U8 => "u8",
            Expected::U16 => "u16",
            Expected::U32 => "u32",
            Expected::U64 => "u64",
            Expected::U128 => "u128",
            Expected::F32 => "f32",
            Expected::F64 => "f64",
            Expected::Char => "char",
            Expected::String => "string",
            Expected::Bytes => "bytes",
            Expected::Null => "null",
            Expected::Sequence => "sequence",
            Expected::Mapping => "mapping",
            Expected::Enum => "enum",
            Expected::Any => "any",
        }
    }
}

/// What kind of node was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Null,
    Scalar,
    Sequence,
    Mapping,
}

impl Kind {
    /// The token the error's `found` arg carries.
    pub fn token(self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Scalar => "scalar",
            Kind::Sequence => "sequence",
            Kind::Mapping => "mapping",
        }
    }
}

/// A value that does not decode, and where.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeError {
    /// From the root; empty for the root itself.
    pub path: Vec<PathSeg>,
    pub problem: DecodeProblem,
}

/// Why a value does not decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeProblem {
    /// A node of the wrong kind: a sequence where a string goes, say.
    Type { expected: Expected, found: Kind },
    /// A scalar whose text is not what the type asks: `8o` for a `u16`.
    Scalar { expected: Expected, text: String },
    /// A key the mapping's type does not have; `known` lists those it has,
    /// when the type declares them (`deny_unknown_fields`), else is empty.
    UnknownKey {
        key: String,
        known: Vec<&'static str>,
    },
    /// A key the type needs and the mapping lacks; the path ends with it.
    MissingKey { key: String },
    /// An enum variant the type does not have.
    UnknownVariant {
        variant: String,
        known: Vec<&'static str>,
    },
    /// A tuple of the wrong length.
    Length { expected: usize, found: usize },
    /// The type's own refusal, its text kept verbatim; `found` is the
    /// scalar's text or the node's kind.
    Invalid {
        expected: Expected,
        found: String,
        text: String,
    },
}

impl DecodeError {
    /// The path written out: keys dotted, items bracketed (`a[1].b`); empty
    /// for the root.
    pub fn key_path(&self) -> String {
        let mut out = String::new();
        for seg in &self.path {
            match seg {
                PathSeg::Key(k) => {
                    if !out.is_empty() {
                        out.push('.');
                    }
                    out.push_str(k);
                }
                PathSeg::Item(i) => out.push_str(&format!("[{i}]")),
            }
        }
        out
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for DecodeError {}

impl Diagnostic for DecodeError {
    fn code(&self) -> &str {
        match self.problem {
            DecodeProblem::Type { .. } => "angzarr-yaml-decode-type",
            DecodeProblem::Scalar { .. } => "angzarr-yaml-decode-scalar",
            DecodeProblem::UnknownKey { .. } => "angzarr-yaml-decode-unknown-key",
            DecodeProblem::MissingKey { .. } => "angzarr-yaml-decode-missing-key",
            DecodeProblem::UnknownVariant { .. } => "angzarr-yaml-decode-unknown-variant",
            DecodeProblem::Length { .. } => "angzarr-yaml-decode-length",
            DecodeProblem::Invalid { .. } => "angzarr-yaml-decode-invalid",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let (expected, found): (Arg, Arg) = match &self.problem {
            DecodeProblem::Type { expected, found } => {
                (expected.token().into(), found.token().into())
            }
            DecodeProblem::Scalar { expected, text } => {
                (expected.token().into(), text.as_str().into())
            }
            DecodeProblem::UnknownKey { key, known } => {
                (known.join(",").into(), key.as_str().into())
            }
            DecodeProblem::MissingKey { key } => (key.as_str().into(), "absent".into()),
            DecodeProblem::UnknownVariant { variant, known } => {
                (known.join(",").into(), variant.as_str().into())
            }
            DecodeProblem::Length { expected, found } => ((*expected).into(), (*found).into()),
            DecodeProblem::Invalid {
                expected, found, ..
            } => (expected.token().into(), found.as_str().into()),
        };
        let mut args = vec![
            ("key_path", Arg::from(self.key_path())),
            ("expected", expected),
            ("found", found),
        ];
        if let DecodeProblem::Invalid { text, .. } = &self.problem {
            args.push(("ext_err_txt", text.as_str().into()));
        }
        args
    }
}

// ------------------------------------------------------------ the decoder

/// What the decode shares: the last thing a type asked for, and of which
/// node, so a refusal raised after the node's own call (a type that reads a
/// `u32`, then refuses it) still names what was expected.
struct Ctx {
    asked: Cell<Option<(usize, Expected)>>,
}

/// The node being decoded: a value, or a mapping key (a scalar).
#[derive(Clone, Copy)]
enum At<'de> {
    Value(&'de Value),
    Key(&'de str),
}

impl<'de> At<'de> {
    fn id(self) -> usize {
        match self {
            At::Value(v) => v as *const Value as usize,
            At::Key(k) => k.as_ptr() as usize,
        }
    }

    fn text(self) -> Option<&'de str> {
        match self {
            At::Value(Value::Scalar(s)) => Some(s),
            At::Key(k) => Some(k),
            At::Value(_) => None,
        }
    }

    fn kind(self) -> Kind {
        match self {
            At::Value(Value::Null) => Kind::Null,
            At::Value(Value::Scalar(_)) | At::Key(_) => Kind::Scalar,
            At::Value(Value::Seq(_)) => Kind::Sequence,
            At::Value(Value::Map(_)) => Kind::Mapping,
        }
    }

    /// The `found` of a refusal: the scalar's text, or the node's kind.
    fn found(self) -> String {
        match self.text() {
            Some(t) => t.to_string(),
            None => self.kind().token().to_string(),
        }
    }
}

/// The error serde's traits pass around: a [`DecodeError`]'s problem, the
/// path once a node has claimed it, and what serde's own calls left for the
/// node to fill in.
#[derive(Debug)]
struct Failure {
    path: Option<Vec<PathSeg>>,
    problem: Pending,
}

#[derive(Debug)]
enum Pending {
    Done(DecodeProblem),
    /// `invalid_type`, `invalid_value`, `invalid_length`: judged against the
    /// node, not serde's English.
    Unexpected,
    /// `custom`: the type's own text.
    Custom(String),
    /// `missing_field`, from the mapping's own decode.
    Missing(&'static str),
}

/// What a type that wraps a failure in its own (`custom(format!(..))`) shows:
/// the code and args as far as they are known, or the type's own text.
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = self.path.clone().unwrap_or_default();
        match &self.problem {
            Pending::Done(problem) => DecodeError {
                path,
                problem: problem.clone(),
            }
            .fmt(f),
            Pending::Custom(text) => f.write_str(text),
            Pending::Unexpected => f.write_str("angzarr-yaml-decode-type"),
            Pending::Missing(key) => DecodeError {
                path,
                problem: DecodeProblem::MissingKey {
                    key: key.to_string(),
                },
            }
            .fmt(f),
        }
    }
}

impl std::error::Error for Failure {}

impl de::Error for Failure {
    fn custom<T: fmt::Display>(msg: T) -> Failure {
        Failure::pending(Pending::Custom(msg.to_string()))
    }

    fn invalid_type(_: de::Unexpected<'_>, _: &dyn de::Expected) -> Failure {
        Failure::pending(Pending::Unexpected)
    }

    fn invalid_value(_: de::Unexpected<'_>, _: &dyn de::Expected) -> Failure {
        Failure::pending(Pending::Unexpected)
    }

    fn invalid_length(_: usize, _: &dyn de::Expected) -> Failure {
        Failure::pending(Pending::Unexpected)
    }

    fn unknown_variant(variant: &str, expected: &'static [&'static str]) -> Failure {
        Failure::pending(Pending::Done(DecodeProblem::UnknownVariant {
            variant: variant.to_string(),
            known: expected.to_vec(),
        }))
    }

    fn unknown_field(field: &str, expected: &'static [&'static str]) -> Failure {
        Failure::pending(Pending::Done(DecodeProblem::UnknownKey {
            key: field.to_string(),
            known: expected.to_vec(),
        }))
    }

    fn missing_field(field: &'static str) -> Failure {
        Failure::pending(Pending::Missing(field))
    }
}

impl Failure {
    fn pending(problem: Pending) -> Failure {
        Failure {
            path: None,
            problem,
        }
    }

    /// Claim an unclaimed failure for the node at `path`, which was asked
    /// for `expected`.
    fn claim(mut self, path: &[PathSeg], at: At<'_>, expected: Expected) -> Failure {
        if self.path.is_some() {
            return self;
        }
        let mut path = path.to_vec();
        let problem = match self.problem {
            Pending::Done(problem) => problem,
            Pending::Unexpected => match at.text() {
                Some(text) => DecodeProblem::Scalar {
                    expected,
                    text: text.to_string(),
                },
                None => DecodeProblem::Type {
                    expected,
                    found: at.kind(),
                },
            },
            Pending::Custom(text) => DecodeProblem::Invalid {
                expected,
                found: at.found(),
                text,
            },
            Pending::Missing(key) => {
                path.push(PathSeg::Key(key.to_string()));
                DecodeProblem::MissingKey {
                    key: key.to_string(),
                }
            }
        };
        self.path = Some(path);
        self.problem = Pending::Done(problem);
        self
    }

    /// Claim it for a node whose own call has returned: what it was asked
    /// is the context's record, if that was this node.
    fn claim_after(self, path: &[PathSeg], at: At<'_>, ctx: &Ctx) -> Failure {
        let expected = match ctx.asked.get() {
            Some((id, expected)) if id == at.id() => expected,
            _ => Expected::Any,
        };
        self.claim(path, at, expected)
    }

    fn finish(self, path: &[PathSeg], at: At<'_>, ctx: &Ctx) -> DecodeError {
        let claimed = self.claim_after(path, at, ctx);
        match claimed.problem {
            Pending::Done(problem) => DecodeError {
                path: claimed.path.unwrap_or_default(),
                problem,
            },
            _ => unreachable!("a claimed failure is done"),
        }
    }
}

/// The deserializer for one node.
struct Node<'a, 'de> {
    at: At<'de>,
    path: Vec<PathSeg>,
    ctx: &'a Ctx,
}

impl<'a, 'de> Node<'a, 'de> {
    fn child(&self, at: At<'de>, seg: PathSeg) -> Node<'a, 'de> {
        let mut path = self.path.clone();
        path.push(seg);
        Node {
            at,
            path,
            ctx: self.ctx,
        }
    }

    fn fail(&self, problem: DecodeProblem) -> Failure {
        Failure {
            path: Some(self.path.clone()),
            problem: Pending::Done(problem),
        }
    }

    fn wrong(&self, expected: Expected) -> Failure {
        self.fail(DecodeProblem::Type {
            expected,
            found: self.at.kind(),
        })
    }

    /// Record what this node is asked for, then run `f`, claiming what it
    /// leaves unclaimed.
    fn asked<T>(
        self,
        expected: Expected,
        f: impl FnOnce(&Node<'a, 'de>) -> Result<T, Failure>,
    ) -> Result<T, Failure> {
        self.ctx.asked.set(Some((self.at.id(), expected)));
        f(&self).map_err(|e| e.claim(&self.path, self.at, expected))
    }

    /// The scalar's text parsed by `parse`, or a refusal naming `expected`.
    fn scalar<T>(
        &self,
        expected: Expected,
        parse: impl FnOnce(&str) -> Option<T>,
    ) -> Result<T, Failure> {
        let Some(text) = self.at.text() else {
            return Err(self.wrong(expected));
        };
        parse(text).ok_or_else(|| {
            self.fail(DecodeProblem::Scalar {
                expected,
                text: text.to_string(),
            })
        })
    }
}

/// Rust's integer text, decimal only: `+` and `_` and every other radix
/// refused, as YAML 1.2's core schema reads an int (`-` for a signed type).
fn int<T: std::str::FromStr>(text: &str) -> Option<T> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// A float: Rust's decimal forms, and YAML's `.inf`, `-.inf`, `+.inf` and
/// `.nan`; Rust's own `inf` and `NaN` spellings are refused.
fn float(text: &str) -> Option<f64> {
    match text {
        ".inf" | "+.inf" => Some(f64::INFINITY),
        "-.inf" => Some(f64::NEG_INFINITY),
        ".nan" => Some(f64::NAN),
        _ if text
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E')) =>
        {
            text.parse().ok()
        }
        _ => None,
    }
}

macro_rules! ints {
    ($($method:ident $visit:ident $t:ty => $e:ident),* $(,)?) => {
        $(fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
            self.asked(Expected::$e, |n| visitor.$visit(n.scalar(Expected::$e, int::<$t>)?))
        })*
    };
}

impl<'a, 'de> Deserializer<'de> for Node<'a, 'de> {
    type Error = Failure;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Any, |n| match n.at {
            At::Key(k) => visitor.visit_borrowed_str(k),
            At::Value(Value::Null) => visitor.visit_unit(),
            At::Value(Value::Scalar(s)) => visitor.visit_borrowed_str(s),
            At::Value(Value::Seq(items)) => visitor.visit_seq(Items::new(n, items)),
            At::Value(Value::Map(entries)) => visitor.visit_map(Entries::new(n, entries)),
        })
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Bool, |n| {
            visitor.visit_bool(n.scalar(Expected::Bool, |t| match t {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })?)
        })
    }

    ints! {
        deserialize_i8 visit_i8 i8 => I8,
        deserialize_i16 visit_i16 i16 => I16,
        deserialize_i32 visit_i32 i32 => I32,
        deserialize_i64 visit_i64 i64 => I64,
        deserialize_i128 visit_i128 i128 => I128,
        deserialize_u8 visit_u8 u8 => U8,
        deserialize_u16 visit_u16 u16 => U16,
        deserialize_u32 visit_u32 u32 => U32,
        deserialize_u64 visit_u64 u64 => U64,
        deserialize_u128 visit_u128 u128 => U128,
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::F32, |n| {
            visitor.visit_f32(n.scalar(Expected::F32, |t| float(t).map(|f| f as f32))?)
        })
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::F64, |n| {
            visitor.visit_f64(n.scalar(Expected::F64, float)?)
        })
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Char, |n| {
            visitor.visit_char(n.scalar(Expected::Char, |t| {
                let mut chars = t.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) => Some(c),
                    _ => None,
                }
            })?)
        })
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::String, |n| match n.at.text() {
            Some(t) => visitor.visit_borrowed_str(t),
            None => Err(n.wrong(Expected::String)),
        })
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Bytes, |n| match n.at.text() {
            Some(t) => visitor.visit_borrowed_bytes(t.as_bytes()),
            None => Err(n.wrong(Expected::Bytes)),
        })
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        match self.at {
            At::Value(Value::Null) => self.asked(Expected::Null, |_| visitor.visit_none()),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Null, |n| match n.at {
            At::Value(Value::Null) => visitor.visit_unit(),
            _ => Err(n.wrong(Expected::Null)),
        })
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Failure> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Failure> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Sequence, |n| match n.at {
            At::Value(Value::Seq(items)) => visitor.visit_seq(Items::new(n, items)),
            _ => Err(n.wrong(Expected::Sequence)),
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Failure> {
        self.asked(Expected::Sequence, |n| match n.at {
            At::Value(Value::Seq(items)) if items.len() == len => {
                visitor.visit_seq(Items::new(n, items))
            }
            At::Value(Value::Seq(items)) => Err(n.fail(DecodeProblem::Length {
                expected: len,
                found: items.len(),
            })),
            _ => Err(n.wrong(Expected::Sequence)),
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Failure> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.asked(Expected::Mapping, |n| match n.at {
            At::Value(Value::Map(entries)) => visitor.visit_map(Entries::new(n, entries)),
            _ => Err(n.wrong(Expected::Mapping)),
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Failure> {
        self.asked(Expected::Mapping, |n| match n.at {
            At::Value(Value::Map(entries)) => visitor.visit_map(Entries::new(n, entries)),
            _ => Err(n.wrong(Expected::Mapping)),
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Failure> {
        self.asked(Expected::Enum, |n| match n.at {
            At::Key(k) => visitor.visit_enum(Variant {
                node: n.child_same(),
                name: k,
                content: None,
            }),
            At::Value(Value::Scalar(s)) => visitor.visit_enum(Variant {
                node: n.child_same(),
                name: s,
                content: None,
            }),
            At::Value(Value::Map(entries)) if entries.len() == 1 => {
                let (k, v) = &entries[0];
                visitor.visit_enum(Variant {
                    node: n.child_same(),
                    name: k,
                    content: Some(v),
                })
            }
            _ => Err(n.wrong(Expected::Enum)),
        })
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Failure> {
        self.deserialize_str(visitor)
    }

    /// A value ignored is a key the type does not have: refused, whatever
    /// the type says (strict for all).
    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Failure> {
        let key = match self.path.last() {
            Some(PathSeg::Key(k)) => k.clone(),
            Some(PathSeg::Item(i)) => i.to_string(),
            None => String::new(),
        };
        Err(self.fail(DecodeProblem::UnknownKey {
            key,
            known: Vec::new(),
        }))
    }
}

impl<'a, 'de> Node<'a, 'de> {
    fn child_same(&self) -> Node<'a, 'de> {
        Node {
            at: self.at,
            path: self.path.clone(),
            ctx: self.ctx,
        }
    }
}

/// A sequence's items, each at its index.
struct Items<'a, 'de> {
    node: Node<'a, 'de>,
    items: std::iter::Enumerate<std::slice::Iter<'de, Value>>,
}

impl<'a, 'de> Items<'a, 'de> {
    fn new(node: &Node<'a, 'de>, items: &'de [Value]) -> Items<'a, 'de> {
        Items {
            node: node.child_same(),
            items: items.iter().enumerate(),
        }
    }
}

impl<'a, 'de> SeqAccess<'de> for Items<'a, 'de> {
    type Error = Failure;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Failure> {
        let Some((i, item)) = self.items.next() else {
            return Ok(None);
        };
        let child = self.node.child(At::Value(item), PathSeg::Item(i));
        let (path, at, ctx) = (child.path.clone(), child.at, child.ctx);
        seed.deserialize(child)
            .map(Some)
            .map_err(|e| e.claim_after(&path, at, ctx))
    }
}

/// A mapping's entries: each key at its own path, then its value.
struct Entries<'a, 'de> {
    node: Node<'a, 'de>,
    entries: std::slice::Iter<'de, (String, Value)>,
    value: Option<(&'de str, &'de Value)>,
}

impl<'a, 'de> Entries<'a, 'de> {
    fn new(node: &Node<'a, 'de>, entries: &'de [(String, Value)]) -> Entries<'a, 'de> {
        Entries {
            node: node.child_same(),
            entries: entries.iter(),
            value: None,
        }
    }
}

impl<'a, 'de> MapAccess<'de> for Entries<'a, 'de> {
    type Error = Failure;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Failure> {
        let Some((k, v)) = self.entries.next() else {
            return Ok(None);
        };
        self.value = Some((k, v));
        let child = self.node.child(At::Key(k), PathSeg::Key(k.clone()));
        let (path, at, ctx) = (child.path.clone(), child.at, child.ctx);
        seed.deserialize(child)
            .map(Some)
            .map_err(|e| e.claim_after(&path, at, ctx))
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, Failure> {
        let (k, v) = self
            .value
            .take()
            .expect("serde asks for a key before its value");
        let child = self.node.child(At::Value(v), PathSeg::Key(k.to_string()));
        let (path, at, ctx) = (child.path.clone(), child.at, child.ctx);
        seed.deserialize(child)
            .map_err(|e| e.claim_after(&path, at, ctx))
    }
}

/// An enum: its variant's name, and its content when written as a mapping of
/// one key.
struct Variant<'a, 'de> {
    node: Node<'a, 'de>,
    name: &'de str,
    content: Option<&'de Value>,
}

impl<'a, 'de> EnumAccess<'de> for Variant<'a, 'de> {
    type Error = Failure;
    type Variant = Content<'a, 'de>;

    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Content<'a, 'de>), Failure> {
        let name = de::value::BorrowedStrDeserializer::<'de, Failure>::new(self.name);
        let path = self.node.path.clone();
        let variant = seed
            .deserialize(name)
            .map_err(|e| e.claim(&path, At::Key(self.name), Expected::Enum))?;
        let content = match self.content {
            Some(v) => Some(
                self.node
                    .child(At::Value(v), PathSeg::Key(self.name.to_string())),
            ),
            None => None,
        };
        Ok((
            variant,
            Content {
                node: self.node,
                content,
            },
        ))
    }
}

/// A variant's content: none for the scalar form.
struct Content<'a, 'de> {
    /// The enum's own node, where a missing content is reported.
    node: Node<'a, 'de>,
    content: Option<Node<'a, 'de>>,
}

impl<'a, 'de> Content<'a, 'de> {
    fn needs(self, expected: Expected) -> Result<Node<'a, 'de>, Failure> {
        match self.content {
            Some(node) => Ok(node),
            None => Err(self.node.wrong(expected)),
        }
    }
}

impl<'a, 'de> VariantAccess<'de> for Content<'a, 'de> {
    type Error = Failure;

    fn unit_variant(self) -> Result<(), Failure> {
        match self.content {
            None => Ok(()),
            Some(node) => de::Deserialize::deserialize(node),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Failure> {
        let node = self.needs(Expected::Mapping)?;
        let (path, at, ctx) = (node.path.clone(), node.at, node.ctx);
        seed.deserialize(node)
            .map_err(|e| e.claim_after(&path, at, ctx))
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Failure> {
        self.needs(Expected::Mapping)?
            .deserialize_tuple(len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Failure> {
        self.needs(Expected::Mapping)?
            .deserialize_struct("", fields, visitor)
    }
}

/// One value of every decode error, for the catalog parity test.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let path = vec![
        PathSeg::Key("component".into()),
        PathSeg::Item(1),
        PathSeg::Key("kind".into()),
    ];
    [
        DecodeProblem::Type {
            expected: Expected::String,
            found: Kind::Sequence,
        },
        DecodeProblem::Scalar {
            expected: Expected::U16,
            text: "8o".into(),
        },
        DecodeProblem::UnknownKey {
            key: "colour".into(),
            known: vec!["name", "kind"],
        },
        DecodeProblem::MissingKey { key: "kind".into() },
        DecodeProblem::UnknownVariant {
            variant: "slow".into(),
            known: vec!["fast", "careful"],
        },
        DecodeProblem::Length {
            expected: 2,
            found: 3,
        },
        DecodeProblem::Invalid {
            expected: Expected::U32,
            found: "3".into(),
            text: "odd".into(),
        },
    ]
    .into_iter()
    .map(|problem| {
        Box::new(DecodeError {
            path: path.clone(),
            problem,
        }) as Box<dyn Diagnostic>
    })
    .collect()
}

#[cfg(test)]
#[path = "decode.test.rs"]
mod tests;
