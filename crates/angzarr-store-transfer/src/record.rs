//! The JSONL format's records: one per line, each one object with one key,
//! its kind. The proto messages inside are proto3 JSON values (the crate's
//! documentation has the format); the envelopes are these serde structs,
//! which refuse a key they do not know.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// One line of the file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Record {
    /// The first line: what the file is and what wrote it.
    Header(Header),
    /// One of a stream's own pages.
    Page(PageRecord),
    /// A stream's snapshot.
    Snapshot(SnapshotRecord),
    /// A handler's position on a stream, above 0.
    Position(PositionRecord),
    /// A dead letter about a stream.
    DeadLetter(DeadLetterRecord),
    /// The last line: the counts, as the store reported them while
    /// exporting.
    End(End),
}

/// The first line.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Header {
    /// [`FORMAT`](crate::FORMAT).
    pub format: String,
    /// [`FORMAT_VERSION`](crate::FORMAT_VERSION).
    pub format_version: u32,
    /// The proto generation the JSON is shaped by
    /// ([`GENERATION`](crate::GENERATION) of the build that wrote it).
    pub generation: String,
    /// The backend the rows came from, as its package names it.
    pub store: String,
    /// The versions of the store's ledger, in order.
    pub ledger: Vec<u64>,
}

/// A page and the stream it is on (`Cover`, `EventPage`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRecord {
    pub cover: Value,
    pub page: Value,
}

/// A snapshot and its stream (`Cover`, `Snapshot`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRecord {
    pub cover: Value,
    pub snapshot: Value,
}

/// A handler's next sequence on a stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionRecord {
    pub cover: Value,
    pub handler: String,
    pub next: Count,
}

/// A dead letter (`AngzarrDeadLetter`), its stream and where on it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadLetterRecord {
    pub cover: Value,
    pub at: At,
    pub letter: Value,
}

/// Where on its stream a dead letter is: `{"sequence": "N"}` or
/// `{"commandId": UUID}` (a `UUID` in proto3 JSON).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum At {
    Sequence(Count),
    CommandId(Value),
}

/// The last line: how many records lie between the header and it, and
/// each stream's counts, in the file's order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct End {
    pub records: Count,
    pub streams: Vec<StreamCount>,
}

/// One stream's counts: its own pages, its head (the sequence after its
/// last own page, 0 with none), its snapshot's sequence if it has one,
/// its positions and its dead letters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamCount {
    pub cover: Value,
    pub pages: Count,
    pub head: Count,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Count>,
    pub positions: Count,
    pub dead_letters: Count,
}

/// One line of a re-fold digest: a stream folded, its head, and its
/// state (an `Any`, expanded). Not a record of the export's file: a
/// digest is a file of its own.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigestLine {
    pub cover: Value,
    pub head: Count,
    pub state: Value,
}

/// A `uint64`, written as proto3 JSON writes one: a decimal string.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Count(pub u64);

impl Serialize for Count {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Count {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Count, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map(Count).map_err(|_| {
            serde::de::Error::invalid_value(serde::de::Unexpected::Str(&text), &"a decimal uint64")
        })
    }
}
