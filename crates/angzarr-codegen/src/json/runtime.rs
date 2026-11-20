// The proto3 JSON runtime every generated `angzarr-json.rs` opens with:
// a JSON value whose objects keep their order, the trait each wire type
// implements, the registry an `Any` is expanded through, and the
// well-known types' text. It names only `serde`, `prost`, `prost_types` and
// `angzarr_proto`, which a wire crate depends on.

/// A JSON value. An object keeps its keys in the order they were written
/// (or read), so a message's JSON is the same bytes every time.
#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

/// A JSON number, as it was written or read.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Number {
    I64(i64),
    U64(u64),
    F64(f64),
    /// A `float` field's value, written by its own shortest text.
    F32(f32),
}

/// Why JSON is not the message asked for, or a message cannot be written.
/// `at` is the path to the value, from the message's own type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonError {
    /// A value of the wrong kind, or text the field's type does not read.
    Invalid { at: String, expected: &'static str },
    /// A key the message does not have.
    UnknownField { at: String, key: String },
    /// A key given twice, or two arms of one oneof.
    Duplicate { at: String, key: String },
    /// An enum name the enum does not have.
    UnknownEnum { at: String, name: String },
    /// An `Any` of a type the registry does not know.
    UnknownType { type_url: String },
    /// A value the canonical mapping cannot write: a UUID not 16 bytes, a
    /// timestamp or duration out of the proto3 range.
    Unwritable { at: String, expected: &'static str },
}

impl ::std::fmt::Display for JsonError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            JsonError::Invalid { at, expected } => {
                write!(f, "json-invalid at={at} expected={expected}")
            }
            JsonError::UnknownField { at, key } => {
                write!(f, "json-unknown-field at={at} key={key}")
            }
            JsonError::Duplicate { at, key } => write!(f, "json-duplicate at={at} key={key}"),
            JsonError::UnknownEnum { at, name } => {
                write!(f, "json-unknown-enum at={at} name={name}")
            }
            JsonError::UnknownType { type_url } => {
                write!(f, "json-unknown-type type_url={type_url}")
            }
            JsonError::Unwritable { at, expected } => {
                write!(f, "json-unwritable at={at} expected={expected}")
            }
        }
    }
}

impl ::std::error::Error for JsonError {}

fn invalid(at: &str, expected: &'static str) -> JsonError {
    JsonError::Invalid {
        at: at.to_string(),
        expected,
    }
}

/// A wire type's proto3 JSON: its object's fields, written and read in
/// place (what a flattened field or a tagged oneof's arm writes into its
/// parent), and the whole value.
pub trait JsonMessage: Sized + Default {
    /// The message's full proto name, for paths in errors.
    const NAME: &'static str;

    /// The message's fields, in order, into `out`; its own tag is
    /// [`JsonMessage::to_json`]'s.
    fn write_fields(
        &self,
        out: &mut Vec<(String, Json)>,
        registry: &Registry,
    ) -> Result<(), JsonError>;

    /// The message read from `fields`, each key it reads taken.
    fn read_fields(fields: &mut Fields, registry: &Registry) -> Result<Self, JsonError>;

    /// The key and value its object opens with, when it is written on its
    /// own (`json_tag_key`, `json_tag`).
    fn tag() -> Option<(&'static str, &'static str)> {
        None
    }

    fn to_json(&self, registry: &Registry) -> Result<Json, JsonError> {
        let mut out = Vec::new();
        if let Some((key, tag)) = Self::tag() {
            out.push((key.to_string(), Json::String(tag.to_string())));
        }
        self.write_fields(&mut out, registry)?;
        Ok(Json::Object(out))
    }

    fn from_json(json: &Json, registry: &Registry) -> Result<Self, JsonError> {
        let mut fields = Fields::of(json, Self::NAME)?;
        if let Some((key, tag)) = Self::tag()
            && let Some(found) = fields.take(&[key])?
            && found != &Json::String(tag.to_string())
        {
            return Err(invalid(&fields.at(key), tag));
        }
        let message = Self::read_fields(&mut fields, registry)?;
        fields.finish()?;
        Ok(message)
    }

    /// Whether its JSON is an object: an `Any` of it holds its fields
    /// beside `@type`, else its value under `value`.
    fn is_object() -> bool {
        true
    }
}

/// An object's keys being read: each taken once, and any left over at the
/// end refused.
pub struct Fields<'a> {
    at: &'static str,
    entries: Vec<(&'a str, Option<&'a Json>)>,
}

impl<'a> Fields<'a> {
    /// The keys of `json`, an object of the message `at`; a key given
    /// twice refused.
    pub fn of(json: &'a Json, at: &'static str) -> Result<Fields<'a>, JsonError> {
        let Json::Object(entries) = json else {
            return Err(invalid(at, "object"));
        };
        let mut seen: Vec<&str> = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            if seen.contains(&key.as_str()) {
                return Err(JsonError::Duplicate {
                    at: at.to_string(),
                    key: key.clone(),
                });
            }
            seen.push(key);
        }
        Ok(Fields {
            at,
            entries: entries.iter().map(|(k, v)| (k.as_str(), Some(v))).collect(),
        })
    }

    /// The path of `key` here.
    pub fn at(&self, key: &str) -> String {
        format!("{}.{key}", self.at)
    }

    /// The value under the first of `keys` present, taken, all of them
    /// taken; two of them present refused; `null` read as absent.
    pub fn take(&mut self, keys: &[&str]) -> Result<Option<&'a Json>, JsonError> {
        let mut found: Option<(&str, &'a Json)> = None;
        for (key, value) in self.entries.iter_mut() {
            if keys.contains(key)
                && let Some(v) = value.take()
            {
                if let Some((first, _)) = found {
                    return Err(JsonError::Duplicate {
                        at: self.at.to_string(),
                        key: format!("{first}/{key}"),
                    });
                }
                found = Some((key, v));
            }
        }
        Ok(found.map(|(_, v)| v).filter(|v| **v != Json::Null))
    }

    /// How many keys have been taken: a flattened field read is present
    /// when it took any.
    pub fn taken(&self) -> usize {
        self.entries.iter().filter(|(_, v)| v.is_none()).count()
    }

    /// Every key taken, or the first one left refused.
    pub fn finish(self) -> Result<(), JsonError> {
        match self.entries.iter().find(|(_, v)| v.is_some()) {
            Some((key, _)) => Err(JsonError::UnknownField {
                at: self.at.to_string(),
                key: key.to_string(),
            }),
            None => Ok(()),
        }
    }
}

// region values

/// `value` as the field holding it takes it: itself, or boxed where prost
/// boxes a recursive field.
pub fn own<T, F: From<T>>(value: T) -> F {
    F::from(value)
}

/// Whether a field without presence holds its default, which the
/// canonical mapping leaves out.
pub fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// Whether a oneof's local is set yet.
pub fn is_set<T>(value: &Option<T>) -> bool {
    value.is_some()
}

/// A 32-bit integer's JSON: a number.
pub fn int(value: impl Into<i64>) -> Json {
    Json::Number(Number::I64(value.into()))
}

/// An unsigned 32-bit integer's JSON: a number.
pub fn uint(value: impl Into<u64>) -> Json {
    Json::Number(Number::U64(value.into()))
}

/// A 64-bit integer's JSON: its decimal text, as the canonical mapping
/// writes it (JSON readers lose precision past 2^53).
pub fn long(value: impl ToString) -> Json {
    Json::String(value.to_string())
}

/// A double's JSON: a number, or `"NaN"`, `"Infinity"`, `"-Infinity"`.
pub fn double(value: f64) -> Json {
    match value {
        v if v.is_nan() => Json::String("NaN".to_string()),
        f64::INFINITY => Json::String("Infinity".to_string()),
        f64::NEG_INFINITY => Json::String("-Infinity".to_string()),
        v => Json::Number(Number::F64(v)),
    }
}

/// A float's JSON, as a double's.
pub fn float(value: f32) -> Json {
    match value {
        v if v.is_nan() => Json::String("NaN".to_string()),
        f32::INFINITY => Json::String("Infinity".to_string()),
        f32::NEG_INFINITY => Json::String("-Infinity".to_string()),
        v => Json::Number(Number::F32(v)),
    }
}

pub fn boolean(value: bool) -> Json {
    Json::Bool(value)
}

pub fn string(value: &str) -> Json {
    Json::String(value.to_string())
}

/// Bytes' JSON: standard base64, padded.
pub fn bytes(value: &[u8]) -> Json {
    Json::String(base64_encode(value))
}

/// An integer read: a number, or its decimal text (or exponent text of
/// an integral value), in `T`'s range.
pub fn read_int<T: TryFrom<i128>>(json: &Json, at: &str) -> Result<T, JsonError> {
    let wide: i128 = match json {
        Json::Number(Number::I64(v)) => i128::from(*v),
        Json::Number(Number::U64(v)) => i128::from(*v),
        Json::Number(Number::F64(v)) => integral(*v).ok_or_else(|| invalid(at, "integer"))?,
        Json::Number(Number::F32(v)) => {
            integral(f64::from(*v)).ok_or_else(|| invalid(at, "integer"))?
        }
        Json::String(s) => match s.parse::<i128>() {
            Ok(v) => v,
            Err(_) => s
                .parse::<f64>()
                .ok()
                .and_then(integral)
                .ok_or_else(|| invalid(at, "integer"))?,
        },
        _ => return Err(invalid(at, "integer")),
    };
    T::try_from(wide).map_err(|_| invalid(at, "integer in range"))
}

fn integral(v: f64) -> Option<i128> {
    (v.is_finite() && v.fract() == 0.0 && v.abs() < 1.8e19).then_some(v as i128)
}

/// A double read: a number, its text, or `"NaN"`, `"Infinity"`,
/// `"-Infinity"`.
pub fn read_double(json: &Json, at: &str) -> Result<f64, JsonError> {
    match json {
        Json::Number(Number::I64(v)) => Ok(*v as f64),
        Json::Number(Number::U64(v)) => Ok(*v as f64),
        Json::Number(Number::F64(v)) => Ok(*v),
        Json::Number(Number::F32(v)) => Ok(f64::from(*v)),
        Json::String(s) => match s.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            text => text.parse().map_err(|_| invalid(at, "number")),
        },
        _ => Err(invalid(at, "number")),
    }
}

/// A float read, as a double, narrowed.
pub fn read_float(json: &Json, at: &str) -> Result<f32, JsonError> {
    match json {
        Json::Number(Number::F32(v)) => Ok(*v),
        Json::String(s) => match s.as_str() {
            "NaN" => Ok(f32::NAN),
            "Infinity" => Ok(f32::INFINITY),
            "-Infinity" => Ok(f32::NEG_INFINITY),
            text => text.parse().map_err(|_| invalid(at, "number")),
        },
        other => read_double(other, at).map(|v| v as f32),
    }
}

pub fn read_bool(json: &Json, at: &str) -> Result<bool, JsonError> {
    match json {
        Json::Bool(v) => Ok(*v),
        _ => Err(invalid(at, "boolean")),
    }
}

pub fn read_string(json: &Json, at: &str) -> Result<String, JsonError> {
    match json {
        Json::String(v) => Ok(v.clone()),
        _ => Err(invalid(at, "string")),
    }
}

/// Bytes read: base64, standard or URL-safe, padded or not.
pub fn read_bytes(json: &Json, at: &str) -> Result<Vec<u8>, JsonError> {
    match json {
        Json::String(v) => base64_decode(v).ok_or_else(|| invalid(at, "base64")),
        _ => Err(invalid(at, "base64")),
    }
}

/// A list's JSON: an array of each value's.
pub fn list<T>(
    values: &[T],
    mut each: impl FnMut(&T) -> Result<Json, JsonError>,
) -> Result<Json, JsonError> {
    values
        .iter()
        .map(&mut each)
        .collect::<Result<_, _>>()
        .map(Json::Array)
}

/// A list read from an array, each value by `each`.
pub fn read_list<T>(
    json: &Json,
    at: &str,
    mut each: impl FnMut(&Json) -> Result<T, JsonError>,
) -> Result<Vec<T>, JsonError> {
    match json {
        Json::Array(values) => values.iter().map(&mut each).collect(),
        _ => Err(invalid(at, "array")),
    }
}

/// A map's JSON: an object of each entry, keyed by the key's text, the
/// keys sorted as text whatever the key type, so the bytes never depend
/// on a hash.
pub fn map<'a, K: ToString + 'a, V: 'a>(
    entries: impl IntoIterator<Item = (&'a K, &'a V)>,
    mut each: impl FnMut(&V) -> Result<Json, JsonError>,
) -> Result<Json, JsonError> {
    let mut out: Vec<(String, Json)> = entries
        .into_iter()
        .map(|(k, v)| Ok((k.to_string(), each(v)?)))
        .collect::<Result<_, JsonError>>()?;
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Json::Object(out))
}

/// A map read from an object, each key parsed from its text.
pub fn read_map<K: ::std::str::FromStr + Ord, V>(
    json: &Json,
    at: &str,
    mut each: impl FnMut(&Json) -> Result<V, JsonError>,
) -> Result<::std::collections::BTreeMap<K, V>, JsonError> {
    let Json::Object(entries) = json else {
        return Err(invalid(at, "object"));
    };
    let mut out = ::std::collections::BTreeMap::new();
    for (key, value) in entries {
        let k: K = key.parse().map_err(|_| invalid(at, "map key"))?;
        if out.insert(k, each(value)?).is_some() {
            return Err(JsonError::Duplicate {
                at: at.to_string(),
                key: key.clone(),
            });
        }
    }
    Ok(out)
}

/// An enum's names: each number, the name it is written as, and every name
/// it is read from.
pub struct EnumNames {
    pub values: &'static [(i32, &'static str, &'static [&'static str])],
}

impl EnumNames {
    /// `number`'s JSON: its name, or the number itself when the enum has no
    /// value of it.
    pub fn write(&self, number: i32) -> Json {
        match self.values.iter().find(|(n, _, _)| *n == number) {
            Some((_, name, _)) => Json::String((*name).to_string()),
            None => int(number),
        }
    }

    /// A number read: by any of its names, or as a number.
    pub fn read(&self, json: &Json, at: &str) -> Result<i32, JsonError> {
        match json {
            Json::String(name) => self
                .values
                .iter()
                .find(|(_, _, names)| names.contains(&name.as_str()))
                .map(|(n, _, _)| *n)
                .ok_or_else(|| JsonError::UnknownEnum {
                    at: at.to_string(),
                    name: name.clone(),
                }),
            other => read_int(other, at),
        }
    }
}

// endregion values

// region well-known types

const SECONDS_MIN: i64 = -62_135_596_800;
const SECONDS_MAX: i64 = 253_402_300_799;
const DURATION_MAX: i64 = 315_576_000_000;

/// Days since 1970-01-01 as a civil date (proleptic Gregorian).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// A civil date as days since 1970-01-01.
fn days(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year.rem_euclid(400);
    let month = i64::from(month);
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Nanoseconds as a fraction: none, or 3, 6 or 9 digits, as the canonical
/// mapping writes them.
fn fraction(nanos: u32) -> String {
    match nanos {
        0 => String::new(),
        n if n % 1_000_000 == 0 => format!(".{:03}", n / 1_000_000),
        n if n % 1_000 == 0 => format!(".{:06}", n / 1_000),
        n => format!(".{n:09}"),
    }
}

/// A fraction's digits as nanoseconds: one to nine digits.
fn read_fraction(digits: &str) -> Option<i32> {
    if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    format!("{digits:0<9}").parse().ok()
}

/// A timestamp's JSON: RFC 3339 in UTC, `Z`.
pub fn timestamp(value: &::prost_types::Timestamp, at: &str) -> Result<Json, JsonError> {
    if !(SECONDS_MIN..=SECONDS_MAX).contains(&value.seconds)
        || !(0..1_000_000_000).contains(&value.nanos)
    {
        return Err(JsonError::Unwritable {
            at: at.to_string(),
            expected: "a timestamp from 0001 to 9999",
        });
    }
    let (year, month, day) = civil(value.seconds.div_euclid(86_400));
    let secs = value.seconds.rem_euclid(86_400);
    Ok(Json::String(format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}{}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        fraction(value.nanos as u32)
    )))
}

/// A timestamp read: RFC 3339, `Z` or an offset.
pub fn read_timestamp(json: &Json, at: &str) -> Result<::prost_types::Timestamp, JsonError> {
    let text = read_string(json, at)?;
    parse_timestamp(&text).ok_or_else(|| invalid(at, "RFC 3339 timestamp"))
}

fn parse_timestamp(text: &str) -> Option<::prost_types::Timestamp> {
    let b = text.as_bytes();
    let num = |from: usize, to: usize| -> Option<i64> {
        let s = text.get(from..to)?;
        s.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| s.parse().ok())?
    };
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let (year, month, day) = (num(0, 4)?, num(5, 7)? as u32, num(8, 10)? as u32);
    let (hour, minute, second) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&month) || day == 0 || day > 31 || hour > 23 || minute > 59 || second > 59
    {
        return None;
    }
    let rest = &text[19..];
    let (nanos, zone) = match rest.strip_prefix('.') {
        Some(frac) => {
            let end = frac
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(frac.len());
            (read_fraction(&frac[..end])?, &frac[end..])
        }
        None => (0, rest),
    };
    let offset = match zone {
        "Z" | "z" => 0,
        z if z.len() == 6 && z.as_bytes()[3] == b':' => {
            let sign = match z.as_bytes()[0] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let part = |r: ::std::ops::Range<usize>| -> Option<i64> {
                let s = z.get(r)?;
                s.bytes()
                    .all(|c| c.is_ascii_digit())
                    .then(|| s.parse().ok())?
            };
            let (h, m) = (part(1..3)?, part(4..6)?);
            if h > 23 || m > 59 {
                return None;
            }
            sign * (h * 3600 + m * 60)
        }
        _ => return None,
    };
    let date = days(year, month, day);
    if civil(date) != (year, month, day) {
        return None;
    }
    let seconds = date * 86_400 + hour * 3600 + minute * 60 + second - offset;
    (SECONDS_MIN..=SECONDS_MAX)
        .contains(&seconds)
        .then_some(::prost_types::Timestamp { seconds, nanos })
}

/// A duration's JSON: its seconds, a fraction of 0, 3, 6 or 9 digits,
/// and `s`.
pub fn duration(value: &::prost_types::Duration, at: &str) -> Result<Json, JsonError> {
    let (s, n) = (value.seconds, value.nanos);
    if s.abs() > DURATION_MAX || n.abs() >= 1_000_000_000 || (s > 0 && n < 0) || (s < 0 && n > 0) {
        return Err(JsonError::Unwritable {
            at: at.to_string(),
            expected: "a duration within 10000 years, its parts of one sign",
        });
    }
    let sign = if s < 0 || n < 0 { "-" } else { "" };
    Ok(Json::String(format!(
        "{sign}{}{}s",
        s.unsigned_abs(),
        fraction(n.unsigned_abs())
    )))
}

/// A duration read: seconds, an optional fraction, `s`.
pub fn read_duration(json: &Json, at: &str) -> Result<::prost_types::Duration, JsonError> {
    let text = read_string(json, at)?;
    parse_duration(&text).ok_or_else(|| invalid(at, "duration"))
}

fn parse_duration(text: &str) -> Option<::prost_types::Duration> {
    let body = text.strip_suffix('s')?;
    let (negative, body) = match body.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, body),
    };
    let (whole, frac) = match body.split_once('.') {
        Some((w, f)) => (w, read_fraction(f)?),
        None => (body, 0),
    };
    if whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let seconds: i64 = whole.parse().ok()?;
    if seconds > DURATION_MAX {
        return None;
    }
    Some(if negative {
        ::prost_types::Duration {
            seconds: -seconds,
            nanos: -frac,
        }
    } else {
        ::prost_types::Duration {
            seconds,
            nanos: frac,
        }
    })
}

/// A UUID's JSON: its canonical hyphenated text; one not 16 bytes cannot
/// be written.
pub fn uuid(value: &::angzarr_proto::v1::Uuid, at: &str) -> Result<Json, JsonError> {
    if !value.is_well_formed() {
        return Err(JsonError::Unwritable {
            at: at.to_string(),
            expected: "a UUID of 16 bytes",
        });
    }
    Ok(Json::String(value.to_string()))
}

/// A UUID read: the hyphenated text, either case.
pub fn read_uuid(json: &Json, at: &str) -> Result<::angzarr_proto::v1::Uuid, JsonError> {
    read_string(json, at)?
        .parse()
        .map_err(|_| invalid(at, "hyphenated UUID"))
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(BASE64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let (mut acc, mut bits) = (0u32, 0u32);
    let body = text.trim_end_matches('=');
    if text.len() - body.len() > 2 {
        return None;
    }
    for c in body.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    (bits < 6 && acc == 0).then_some(out)
}

impl JsonMessage for ::prost_types::Timestamp {
    const NAME: &'static str = "google.protobuf.Timestamp";

    fn write_fields(&self, _: &mut Vec<(String, Json)>, _: &Registry) -> Result<(), JsonError> {
        Err(invalid(Self::NAME, "no flattening of a well-known type"))
    }

    fn read_fields(fields: &mut Fields, _: &Registry) -> Result<Self, JsonError> {
        Err(invalid(fields.at, "no flattening of a well-known type"))
    }

    fn to_json(&self, _: &Registry) -> Result<Json, JsonError> {
        timestamp(self, Self::NAME)
    }

    fn from_json(json: &Json, _: &Registry) -> Result<Self, JsonError> {
        read_timestamp(json, Self::NAME)
    }

    fn is_object() -> bool {
        false
    }
}

impl JsonMessage for ::prost_types::Duration {
    const NAME: &'static str = "google.protobuf.Duration";

    fn write_fields(&self, _: &mut Vec<(String, Json)>, _: &Registry) -> Result<(), JsonError> {
        Err(invalid(Self::NAME, "no flattening of a well-known type"))
    }

    fn read_fields(fields: &mut Fields, _: &Registry) -> Result<Self, JsonError> {
        Err(invalid(fields.at, "no flattening of a well-known type"))
    }

    fn to_json(&self, _: &Registry) -> Result<Json, JsonError> {
        duration(self, Self::NAME)
    }

    fn from_json(json: &Json, _: &Registry) -> Result<Self, JsonError> {
        read_duration(json, Self::NAME)
    }

    fn is_object() -> bool {
        false
    }
}

impl JsonMessage for ::angzarr_proto::v1::Uuid {
    const NAME: &'static str = "io.angzarr.v1.UUID";

    fn write_fields(&self, _: &mut Vec<(String, Json)>, _: &Registry) -> Result<(), JsonError> {
        Err(invalid(Self::NAME, "no flattening of a UUID"))
    }

    fn read_fields(fields: &mut Fields, _: &Registry) -> Result<Self, JsonError> {
        Err(invalid(fields.at, "no flattening of a UUID"))
    }

    fn to_json(&self, _: &Registry) -> Result<Json, JsonError> {
        uuid(self, Self::NAME)
    }

    fn from_json(json: &Json, _: &Registry) -> Result<Self, JsonError> {
        read_uuid(json, Self::NAME)
    }

    fn is_object() -> bool {
        false
    }
}

// endregion well-known types

// region Any

/// How one type's `Any` is written and read: its value's bytes to JSON,
/// and back.
#[derive(Clone, Copy)]
struct AnyType {
    write: fn(&[u8], &Registry) -> Result<Json, JsonError>,
    read: fn(&Json, &Registry) -> Result<Vec<u8>, JsonError>,
    object: bool,
}

/// The types an `Any` is expanded through, by type URL: what a caller
/// that knows its types passes, since the JSON names a type the store does
/// not know. [`Registry::new`] knows the well-known types.
#[derive(Clone)]
pub struct Registry {
    types: ::std::collections::BTreeMap<String, AnyType>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry::new()
    }
}

fn any_write<T: JsonMessage + ::prost::Message>(
    bytes: &[u8],
    registry: &Registry,
) -> Result<Json, JsonError> {
    let message = T::decode(bytes).map_err(|_| invalid(T::NAME, "the type's protobuf bytes"))?;
    message.to_json(registry)
}

fn any_read<T: JsonMessage + ::prost::Message>(
    json: &Json,
    registry: &Registry,
) -> Result<Vec<u8>, JsonError> {
    Ok(T::from_json(json, registry)?.encode_to_vec())
}

impl Registry {
    /// A registry of the well-known types the runtime writes:
    /// `Timestamp`, `Duration`.
    pub fn new() -> Self {
        Registry {
            types: ::std::collections::BTreeMap::new(),
        }
        .with::<::prost_types::Timestamp>()
        .with::<::prost_types::Duration>()
    }

    /// This registry, knowing `T` too.
    pub fn with<T: JsonMessage + ::prost::Message + ::prost::Name>(mut self) -> Self {
        self.types.insert(
            T::type_url(),
            AnyType {
                write: any_write::<T>,
                read: any_read::<T>,
                object: T::is_object(),
            },
        );
        self
    }

    fn get(&self, type_url: &str) -> Result<&AnyType, JsonError> {
        self.types
            .get(type_url)
            .ok_or_else(|| JsonError::UnknownType {
                type_url: type_url.to_string(),
            })
    }
}

impl JsonMessage for ::prost_types::Any {
    const NAME: &'static str = "google.protobuf.Any";

    fn write_fields(
        &self,
        out: &mut Vec<(String, Json)>,
        registry: &Registry,
    ) -> Result<(), JsonError> {
        let found = registry.get(&self.type_url)?;
        out.push(("@type".to_string(), Json::String(self.type_url.clone())));
        match (found.write)(&self.value, registry)? {
            Json::Object(fields) if found.object => out.extend(fields),
            value => out.push(("value".to_string(), value)),
        }
        Ok(())
    }

    fn read_fields(fields: &mut Fields, registry: &Registry) -> Result<Self, JsonError> {
        let at = fields.at("@type");
        let Some(type_url) = fields.take(&["@type"])? else {
            return Err(invalid(&at, "@type"));
        };
        let type_url = read_string(type_url, &at)?;
        let found = *registry.get(&type_url)?;
        let value = if found.object {
            let rest: Vec<(String, Json)> = fields
                .entries
                .iter_mut()
                .filter_map(|(k, v)| v.take().map(|v| (k.to_string(), v.clone())))
                .collect();
            (found.read)(&Json::Object(rest), registry)?
        } else {
            let at = fields.at("value");
            let value = fields
                .take(&["value"])?
                .ok_or_else(|| invalid(&at, "value"))?;
            (found.read)(value, registry)?
        };
        Ok(::prost_types::Any { type_url, value })
    }
}

// endregion Any

// region serde

impl ::serde::Serialize for Json {
    fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use ::serde::ser::{SerializeMap, SerializeSeq};
        match self {
            Json::Null => serializer.serialize_unit(),
            Json::Bool(v) => serializer.serialize_bool(*v),
            Json::Number(Number::I64(v)) => serializer.serialize_i64(*v),
            Json::Number(Number::U64(v)) => serializer.serialize_u64(*v),
            Json::Number(Number::F64(v)) => serializer.serialize_f64(*v),
            Json::Number(Number::F32(v)) => serializer.serialize_f32(*v),
            Json::String(v) => serializer.serialize_str(v),
            Json::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(value)?;
                }
                seq.end()
            }
            Json::Object(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

struct JsonVisitor;

impl<'de> ::serde::de::Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_none<E>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_some<D: ::serde::Deserializer<'de>>(self, d: D) -> Result<Json, D::Error> {
        ::serde::Deserialize::deserialize(d)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Json, E> {
        Ok(Json::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Json, E> {
        Ok(Json::Number(Number::I64(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Json, E> {
        Ok(Json::Number(Number::U64(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Json, E> {
        Ok(Json::Number(Number::F64(v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Json, E> {
        Ok(Json::String(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<Json, E> {
        Ok(Json::String(v))
    }

    fn visit_seq<A: ::serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element()? {
            values.push(value);
        }
        Ok(Json::Array(values))
    }

    fn visit_map<A: ::serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut entries = Vec::new();
        while let Some((key, value)) = map.next_entry::<String, Json>()? {
            entries.push((key, value));
        }
        Ok(Json::Object(entries))
    }
}

impl<'de> ::serde::Deserialize<'de> for Json {
    fn deserialize<D: ::serde::Deserializer<'de>>(d: D) -> Result<Json, D::Error> {
        d.deserialize_any(JsonVisitor)
    }
}

/// `message` with the registry its `Any`s are expanded through, as
/// `Serialize`: what a caller knowing its types writes.
pub struct Registered<'a, T>(pub &'a T, pub &'a Registry);

impl<T: JsonMessage> ::serde::Serialize for Registered<'_, T> {
    fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0
            .to_json(self.1)
            .map_err(::serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

/// `message` as `Serialize`, through a registry of the well-known types
/// alone: an `Any` of any other type is refused.
pub fn serialize<T: JsonMessage, S: ::serde::Serializer>(
    message: &T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    ::serde::Serialize::serialize(&Registered(message, &Registry::new()), serializer)
}

/// A `T` from a deserializer, through a registry of the well-known types
/// alone.
pub fn deserialize<'de, T: JsonMessage, D: ::serde::Deserializer<'de>>(
    d: D,
) -> Result<T, D::Error> {
    let json: Json = ::serde::Deserialize::deserialize(d)?;
    T::from_json(&json, &Registry::new()).map_err(::serde::de::Error::custom)
}

// endregion serde
