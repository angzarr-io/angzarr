//! The export: a store's [`Export`] rows, checked and written as JSONL.

use std::io::Write;

use angzarr_ports::angzarr_proto::v1::{AngzarrDeadLetter, Cover, EventPage, Snapshot, Uuid};
use angzarr_ports::{
    AppliedMigration, DeadLetterAt, Export, ExportRow, Migration, Migrations, StreamRef,
};
use prost::{Message, Name};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value};

use crate::error::{ExportError, RowAt};
use crate::record::{
    At, Count, DeadLetterRecord, DigestLine, End, Header, PageRecord, PositionRecord, Record,
    SnapshotRecord, StreamCount,
};
use crate::{FORMAT, FORMAT_VERSION, GENERATION};

/// A pool of angzarr's descriptors (`angzarr_proto::FILE_DESCRIPTOR_SET`)
/// and the app's `FileDescriptorSet`s, encoded: what an export expands
/// payloads through. A file in two sets is taken once. Refused
/// ([`ExportError::DescriptorSet`]): a set that does not decode or does
/// not join the pool, by its index (0 is angzarr's own; the app's from 1).
pub fn descriptor_pool(app_sets: &[&[u8]]) -> Result<DescriptorPool, ExportError> {
    let mut pool = DescriptorPool::new();
    let sets = std::iter::once(angzarr_ports::angzarr_proto::FILE_DESCRIPTOR_SET)
        .chain(app_sets.iter().copied());
    for (index, set) in (0u64..).zip(sets) {
        pool.decode_file_descriptor_set(set)
            .map_err(|_| ExportError::DescriptorSet { index })?;
    }
    Ok(pool)
}

/// Versions, as a refusal names them: `1,2,3`.
fn listed(versions: impl Iterator<Item = u64>) -> String {
    versions
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// That `ledger` is exactly `build`'s migration set: every version of it,
/// each a success with the build's checksum, and nothing more. Answers
/// its versions, in order; refuses ([`ExportError::LedgerNotTheBuilds`])
/// a store older or newer than the build, dirty, or changed. For a caller
/// that judges a store's ledger before opening it (an open migrates).
pub fn check_ledger(
    build: &[Migration],
    ledger: &[AppliedMigration],
) -> Result<Vec<u64>, ExportError> {
    let same = build.len() == ledger.len()
        && build
            .iter()
            .zip(ledger)
            .all(|(m, row)| m.version == row.version && m.checksum == row.checksum && row.success);
    if !same {
        return Err(ExportError::LedgerNotTheBuilds {
            applied: listed(ledger.iter().map(|r| r.version)),
            build: listed(build.iter().map(|m| m.version)),
        });
    }
    Ok(ledger.iter().map(|r| r.version).collect())
}

/// Export `store` to `out`: the header, every row of its [`Export`] as a
/// record, then the end. Every row is checked first, and nothing is
/// written unless all pass ([`ExportError`]). Answers the end record
/// written.
pub async fn export<S, W>(store: &S, pool: &DescriptorPool, out: &mut W) -> Result<End, ExportError>
where
    S: Export + Migrations,
    W: Write,
{
    let ledger = check_ledger(&store.migrations(), &store.applied().await?)?;
    let rows = store.export().await?;
    let pending = rows
        .iter()
        .filter(|row| matches!(row, ExportRow::Outbox(_)))
        .count();
    if pending > 0 {
        return Err(ExportError::PendingOutbox {
            entries: pending as u64,
        });
    }
    let writer = Writer::new(pool)?;
    let header = Record::Header(Header {
        format: FORMAT.to_string(),
        format_version: FORMAT_VERSION,
        generation: GENERATION.to_string(),
        store: store.backend().to_string(),
        ledger,
    });
    let mut records = vec![header];
    let mut counts = Counts::default();
    for row in &rows {
        records.push(writer.record(row)?);
        counts.add(row, &writer)?;
    }
    let end = End {
        records: Count(rows.len() as u64),
        streams: counts.streams,
    };
    records.push(Record::End(end.clone()));
    write_lines(out, &records)?;
    Ok(end)
}

/// `records`, one per line, then a flush.
fn write_lines<W: Write, R: serde::Serialize>(
    out: &mut W,
    records: &[R],
) -> Result<(), ExportError> {
    let failed = |e: std::io::Error| ExportError::Write {
        kind: format!("{:?}", e.kind()),
    };
    for record in records {
        let line = serde_json::to_string(record).map_err(|e| ExportError::Write {
            kind: format!("{:?}", e.classify()),
        })?;
        out.write_all(line.as_bytes()).map_err(failed)?;
        out.write_all(b"\n").map_err(failed)?;
    }
    out.flush().map_err(failed)
}

/// `domain:edition:root`, the edition empty for main: how a refusal names
/// a stream.
fn stream_text(cover: &Cover) -> String {
    match StreamRef::try_from(cover) {
        Ok(s) => format!("{}:{}:{}", s.domain, s.edition.unwrap_or(""), s.root),
        Err(_) => format!(
            "{}:{}:",
            cover.domain,
            cover.edition.as_deref().unwrap_or("")
        ),
    }
}

/// Where a dead letter is, as a refusal names it.
fn at_text(at: &DeadLetterAt) -> String {
    match at {
        DeadLetterAt::Sequence(n) => n.to_string(),
        DeadLetterAt::Command(id) => id.to_string(),
    }
}

/// Which row `row` is, for a refusal.
fn row_at(row: &ExportRow) -> RowAt {
    let (kind, stream, place) = match row {
        ExportRow::Page {
            stream, sequence, ..
        } => ("page", stream, sequence.to_string()),
        ExportRow::Snapshot { stream, .. } => ("snapshot", stream, String::new()),
        ExportRow::Position {
            stream, handler, ..
        } => ("position", stream, handler.clone()),
        ExportRow::DeadLetter { stream, at, .. } => ("dead-letter", stream, at_text(at)),
        ExportRow::Outbox(entry) => {
            return RowAt {
                kind: "outbox",
                stream: String::new(),
                place: entry.id.to_string(),
            }
        }
    };
    RowAt {
        kind,
        stream: stream_text(stream),
        place,
    }
}

/// The type URL of `google.protobuf.Any`'s message, and its fields.
const ANY: &str = "google.protobuf.Any";

/// Writes rows as records through one pool: the framework messages'
/// descriptors, looked up once.
struct Writer<'p> {
    pool: &'p DescriptorPool,
    cover: MessageDescriptor,
    page: MessageDescriptor,
    snapshot: MessageDescriptor,
    letter: MessageDescriptor,
    uuid: MessageDescriptor,
    any: MessageDescriptor,
}

impl<'p> Writer<'p> {
    fn new(pool: &'p DescriptorPool) -> Result<Writer<'p>, ExportError> {
        let get = |name: String| {
            pool.get_message_by_name(&name)
                .ok_or(ExportError::MissingDescriptor { message: name })
        };
        Ok(Writer {
            pool,
            cover: get(Cover::full_name())?,
            page: get(EventPage::full_name())?,
            snapshot: get(Snapshot::full_name())?,
            letter: get(AngzarrDeadLetter::full_name())?,
            uuid: get(Uuid::full_name())?,
            any: get(ANY.to_string())?,
        })
    }

    /// `row` as its record, checked.
    fn record(&self, row: &ExportRow) -> Result<Record, ExportError> {
        let at = || row_at(row);
        Ok(match row {
            ExportRow::Page { stream, stored, .. } => Record::Page(PageRecord {
                cover: self.cover(stream, &at)?,
                page: self.stored::<EventPage>(&self.page, stored, &at)?,
            }),
            ExportRow::Snapshot { stream, stored } => Record::Snapshot(SnapshotRecord {
                cover: self.cover(stream, &at)?,
                snapshot: self.stored::<Snapshot>(&self.snapshot, stored, &at)?,
            }),
            ExportRow::Position {
                stream,
                handler,
                next,
            } => Record::Position(PositionRecord {
                cover: self.cover(stream, &at)?,
                handler: handler.clone(),
                next: Count(*next),
            }),
            ExportRow::DeadLetter {
                stream,
                at: place,
                stored,
                ..
            } => Record::DeadLetter(DeadLetterRecord {
                cover: self.cover(stream, &at)?,
                at: match place {
                    DeadLetterAt::Sequence(n) => At::Sequence(Count(*n)),
                    DeadLetterAt::Command(id) => {
                        let id: Uuid = (*id).into();
                        At::CommandId(self.json(&self.uuid, &id.encode_to_vec(), &at)?)
                    }
                },
                letter: self.stored::<AngzarrDeadLetter>(&self.letter, stored, &at)?,
            }),
            ExportRow::Outbox(_) => {
                unreachable!("an export with a pending outbox is refused first")
            }
        })
    }

    /// A cover, as proto3 JSON.
    fn cover(
        &self,
        cover: &Cover,
        at: &dyn Fn() -> RowAt,
    ) -> Result<serde_json::Value, ExportError> {
        self.json(&self.cover, &cover.encode_to_vec(), at)
    }

    /// A stored row of message `M`: decoded by the build's own type, it
    /// must re-encode to its bytes; then decoded through the pool, every
    /// field known at every depth, written as proto3 JSON.
    fn stored<M: Message + Default>(
        &self,
        descriptor: &MessageDescriptor,
        stored: &[u8],
        at: &dyn Fn() -> RowAt,
    ) -> Result<serde_json::Value, ExportError> {
        let message = descriptor.full_name().to_string();
        let decoded = M::decode(stored).map_err(|_| ExportError::Undecodable {
            row: at(),
            message: message.clone(),
        })?;
        if decoded.encode_to_vec() != stored {
            return Err(ExportError::ReencodingDiffers { row: at(), message });
        }
        self.json(descriptor, stored, at)
    }

    /// `bytes`, a message of `descriptor`, as proto3 JSON, refused if any
    /// field at any depth is unknown or any `Any` unresolved.
    fn json(
        &self,
        descriptor: &MessageDescriptor,
        bytes: &[u8],
        at: &dyn Fn() -> RowAt,
    ) -> Result<serde_json::Value, ExportError> {
        let dynamic = DynamicMessage::decode(descriptor.clone(), bytes).map_err(|_| {
            ExportError::Undecodable {
                row: at(),
                message: descriptor.full_name().to_string(),
            }
        })?;
        self.known(&dynamic, &mut Vec::new(), at)?;
        let value = serde_json::to_value(&dynamic).map_err(|_| ExportError::Unwritable {
            row: at(),
            message: descriptor.full_name().to_string(),
        })?;
        Ok(sorted(value))
    }

    /// That `message` and everything in it is known: no unknown field, and
    /// every `Any` of a type the pool names, its value known in turn.
    /// `path` is the field names from the row's message down.
    fn known(
        &self,
        message: &DynamicMessage,
        path: &mut Vec<String>,
        at: &dyn Fn() -> RowAt,
    ) -> Result<(), ExportError> {
        if message.unknown_fields().next().is_some() {
            return Err(ExportError::UnknownFields {
                row: at(),
                message: message.descriptor().full_name().to_string(),
                path: path.join("."),
            });
        }
        if message.descriptor().full_name() == ANY {
            return self.known_any(message, path, at);
        }
        for (field, value) in message.fields() {
            path.push(field.name().to_string());
            self.known_value(value, path, at)?;
            path.pop();
        }
        Ok(())
    }

    /// A field's value: a message, a list of them or a map of them is
    /// known as [`Writer::known`] knows one.
    fn known_value(
        &self,
        value: &Value,
        path: &mut Vec<String>,
        at: &dyn Fn() -> RowAt,
    ) -> Result<(), ExportError> {
        match value {
            Value::Message(m) => self.known(m, path, at),
            Value::List(items) => items
                .iter()
                .try_for_each(|item| self.known_value(item, path, at)),
            Value::Map(entries) => entries
                .values()
                .try_for_each(|item| self.known_value(item, path, at)),
            _ => Ok(()),
        }
    }

    /// An `Any`: its type named by the pool, its value decoded and known.
    fn known_any(
        &self,
        any: &DynamicMessage,
        path: &mut Vec<String>,
        at: &dyn Fn() -> RowAt,
    ) -> Result<(), ExportError> {
        let type_url = any
            .get_field_by_name("type_url")
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        let value = any
            .get_field_by_name("value")
            .and_then(|v| v.as_bytes().cloned())
            .unwrap_or_default();
        let name = type_url.rsplit('/').next().unwrap_or_default();
        let unresolved = || ExportError::UnresolvedType {
            row: at(),
            type_url: type_url.clone(),
        };
        let descriptor = self.pool.get_message_by_name(name).ok_or_else(unresolved)?;
        let inner = DynamicMessage::decode(descriptor.clone(), value.as_ref()).map_err(|_| {
            ExportError::Undecodable {
                row: at(),
                message: descriptor.full_name().to_string(),
            }
        })?;
        self.known(&inner, path, at)
    }
}

/// `value` with every object's keys in sorted order, whatever map
/// serde_json was built with: one store, one file.
fn sorted(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<(String, serde_json::Value)> = object.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            serde_json::Value::Object(entries.into_iter().map(|(k, v)| (k, sorted(v))).collect())
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(sorted).collect())
        }
        other => other,
    }
}

/// Each stream's counts, built as its rows go by: rows come one stream at
/// a time, so a stream's counts are the last ones while its rows last.
#[derive(Default)]
struct Counts {
    streams: Vec<StreamCount>,
    last: Option<Cover>,
}

impl Counts {
    fn add(&mut self, row: &ExportRow, writer: &Writer<'_>) -> Result<(), ExportError> {
        let stream = match row {
            ExportRow::Page { stream, .. }
            | ExportRow::Snapshot { stream, .. }
            | ExportRow::Position { stream, .. }
            | ExportRow::DeadLetter { stream, .. } => stream,
            ExportRow::Outbox(_) => return Ok(()),
        };
        if self.last.as_ref() != Some(stream) {
            self.streams.push(StreamCount {
                cover: writer.cover(stream, &|| row_at(row))?,
                pages: Count(0),
                head: Count(0),
                snapshot: None,
                positions: Count(0),
                dead_letters: Count(0),
            });
            self.last = Some(stream.clone());
        }
        let count = self.streams.last_mut().expect("pushed above");
        match row {
            ExportRow::Page { sequence, .. } => {
                count.pages.0 += 1;
                count.head = Count(sequence + 1);
            }
            ExportRow::Snapshot { stored, .. } => {
                let sequence = Snapshot::decode(stored.as_slice()).map_or(0, |s| s.sequence);
                count.snapshot = Some(Count(sequence));
            }
            ExportRow::Position { .. } => count.positions.0 += 1,
            ExportRow::DeadLetter { .. } => count.dead_letters.0 += 1,
            ExportRow::Outbox(_) => {}
        }
        Ok(())
    }
}

/// One stream folded by an app's build: its cover, its head and its state
/// (its snapshot form, packed), what a re-fold digest writes (the store
/// export/import plan, section 5.3).
#[derive(Clone, Debug, PartialEq)]
pub struct Digested {
    pub cover: Cover,
    pub head: u64,
    pub state: prost_types::Any,
}

/// Write the re-fold digest of `streams`, one line per stream in the
/// order given: `{"cover": Cover, "head": "h", "state": Any}`, the state
/// expanded through `pool`, every object's keys sorted. Two builds whose
/// folds agree write the same bytes. Refused, as an export refuses a row:
/// a state whose type the pool does not name, or holding an unknown field.
pub fn write_digest<W: Write>(
    streams: &[Digested],
    pool: &DescriptorPool,
    out: &mut W,
) -> Result<(), ExportError> {
    let writer = Writer::new(pool)?;
    let mut lines = Vec::new();
    for stream in streams {
        let at = || RowAt {
            kind: "state",
            stream: stream_text(&stream.cover),
            place: String::new(),
        };
        lines.push(DigestLine {
            cover: writer.cover(&stream.cover, &at)?,
            head: Count(stream.head),
            state: writer.json(&writer.any, &stream.state.encode_to_vec(), &at)?,
        });
    }
    write_lines(out, &lines)
}
