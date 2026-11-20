//! The journal's entries and the operations they record: what each store
//! operation is journaled as, and how a journaled entry is applied again.
//!
//! An entry carries the arguments of the operation, never its result: replay
//! calls the in-memory store's own method with them, against the same state
//! the operation first met, so it answers the same.

use angzarr_ports::angzarr_proto::v1::{AngzarrDeadLetter, EventPage, Snapshot};
use angzarr_ports::memory::MemoryStore;
use angzarr_ports::{
    Append, DeadLetter, DeadLetterAt, DeadLetterKey, DeadLetterStore, Editions, EventStore, Outbox,
    OutboxEntry, PositionKey, Purge, SnapshotStore, StoreError, StreamRef, Uuid, Write,
};
use prost::Message;
use prost_types::Any;

use crate::ready::ready;
use crate::schema::{self, entry::Op};

/// What an accepted operation appends.
pub(crate) enum Plan {
    /// Nothing: the operation changes nothing (a write that only checked).
    Skip,
    /// This entry, one line.
    Line(schema::Entry),
    /// The operation cannot be written as an entry (a dead letter about a
    /// stream that is no stream); the store refuses it, so it is run only
    /// to say how.
    Unencodable,
}

/// Why a journaled entry cannot be applied again.
#[derive(Debug)]
pub(crate) enum Replay {
    /// The entry is not one a store writes: a field it needs is missing or
    /// malformed. Names the field.
    Malformed(&'static str),
    /// The in-memory store refused the recorded operation.
    Refused(StoreError),
    /// The in-memory store did not complete at once.
    Suspended,
}

fn line(op: Op) -> Plan {
    Plan::Line(schema::Entry { op: Some(op) })
}

// ------------------------------------------------------------------ recording

fn stream(stream: StreamRef<'_>) -> schema::Stream {
    schema::Stream {
        domain: stream.domain.to_string(),
        edition: stream.edition.map(str::to_string),
        root: stream.root.to_string(),
    }
}

fn place(at: DeadLetterAt) -> schema::Place {
    let at = match at {
        DeadLetterAt::Sequence(sequence) => schema::place::At::Sequence(sequence),
        DeadLetterAt::Command(id) => schema::place::At::Command(id.to_string()),
    };
    schema::Place { at: Some(at) }
}

/// The dead letter as an entry holds it; `None` for one about a stream
/// that is no stream.
fn dead_letter(letter: &DeadLetter) -> Option<schema::DeadLetter> {
    let about = StreamRef::try_from(&letter.stream).ok()?;
    Some(schema::DeadLetter {
        stream: Some(stream(about)),
        at: Some(place(letter.at)),
        letter: letter.letter.encode_to_vec(),
    })
}

/// A write: everything it changes, together, as one entry.
pub(crate) fn write(write: &Write<'_>) -> Plan {
    let lands_pages = write.events.is_some_and(|events| !events.pages.is_empty());
    if !lands_pages
        && write.positions.is_empty()
        && write.outbox.is_empty()
        && write.dead_letters.is_empty()
        && write.cleared.is_empty()
    {
        return Plan::Skip;
    }
    let Some(dead_letters) = write
        .dead_letters
        .iter()
        .map(dead_letter)
        .collect::<Option<Vec<_>>>()
    else {
        return Plan::Unencodable;
    };
    line(Op::Write(schema::Write {
        events: write.events.map(|events| schema::Append {
            stream: Some(stream(events.stream)),
            expected: events.expected,
            pages: events.pages.iter().map(Message::encode_to_vec).collect(),
        }),
        positions: write
            .positions
            .iter()
            .map(|(key, next)| schema::Position {
                handler: key.handler.to_string(),
                stream: Some(stream(key.stream)),
                next: *next,
            })
            .collect(),
        outbox: write
            .outbox
            .iter()
            .map(|entry| schema::OutboxEntry {
                id: entry.id.to_string(),
                origin: entry.origin.clone(),
                message: entry.message.encode_to_vec(),
                edition: entry.edition.clone(),
            })
            .collect(),
        dead_letters,
        cleared: write.cleared.iter().map(Uuid::to_string).collect(),
    }))
}

pub(crate) fn snapshot(at: StreamRef<'_>, snapshot: &Snapshot) -> Plan {
    line(Op::SnapshotWritten(schema::SnapshotWritten {
        stream: Some(stream(at)),
        snapshot: snapshot.encode_to_vec(),
    }))
}

pub(crate) fn purge(at: StreamRef<'_>, expected: u64) -> Plan {
    line(Op::Purged(schema::Purged {
        stream: Some(stream(at)),
        expected,
    }))
}

pub(crate) fn remove_edition_stream(at: StreamRef<'_>) -> Plan {
    line(Op::EditionStreamRemoved(schema::EditionStreamRemoved {
        stream: Some(stream(at)),
    }))
}

pub(crate) fn clear(ids: &[Uuid]) -> Plan {
    line(Op::OutboxCleared(schema::OutboxCleared {
        ids: ids.iter().map(Uuid::to_string).collect(),
    }))
}

pub(crate) fn drain(at: StreamRef<'_>) -> Plan {
    line(Op::DeadLettersDrained(schema::DeadLettersDrained {
        stream: Some(stream(at)),
    }))
}

pub(crate) fn remove_dead_letters(keys: &[DeadLetterKey<'_>]) -> Plan {
    line(Op::DeadLettersRemoved(schema::DeadLettersRemoved {
        keys: keys
            .iter()
            .map(|key| schema::DeadLetterKey {
                stream: Some(stream(key.stream)),
                at: Some(place(key.at)),
                component: key.component.to_string(),
            })
            .collect(),
    }))
}

// -------------------------------------------------------------------- replay

fn uuid(text: &str, field: &'static str) -> Result<Uuid, Replay> {
    Uuid::parse_str(text).map_err(|_| Replay::Malformed(field))
}

fn stream_ref<'a>(
    stream: &'a Option<schema::Stream>,
    field: &'static str,
) -> Result<StreamRef<'a>, Replay> {
    let stream = stream.as_ref().ok_or(Replay::Malformed(field))?;
    if stream.edition.as_deref() == Some("") {
        return Err(Replay::Malformed(field));
    }
    Ok(StreamRef {
        domain: &stream.domain,
        edition: stream.edition.as_deref(),
        root: uuid(&stream.root, field)?,
    })
}

fn dead_letter_at(at: &Option<schema::Place>, field: &'static str) -> Result<DeadLetterAt, Replay> {
    match at.as_ref().and_then(|place| place.at.as_ref()) {
        Some(schema::place::At::Sequence(sequence)) => Ok(DeadLetterAt::Sequence(*sequence)),
        Some(schema::place::At::Command(id)) => Ok(DeadLetterAt::Command(uuid(id, field)?)),
        None => Err(Replay::Malformed(field)),
    }
}

/// What the in-memory store answered, or why replay stops.
fn answered<T>(answer: Option<Result<T, StoreError>>) -> Result<T, Replay> {
    answer.ok_or(Replay::Suspended)?.map_err(Replay::Refused)
}

/// Apply `entry` to `inner` as its operation was first applied.
pub(crate) fn replay(inner: &MemoryStore, entry: schema::Entry) -> Result<(), Replay> {
    match entry.op.ok_or(Replay::Malformed("op"))? {
        Op::Header(_) => Err(Replay::Malformed("header")),
        Op::Write(recorded) => replay_write(inner, &recorded),
        Op::SnapshotWritten(recorded) => {
            let at = stream_ref(&recorded.stream, "snapshot_written.stream")?;
            let snapshot = Snapshot::decode(recorded.snapshot.as_slice())
                .map_err(|_| Replay::Malformed("snapshot_written.snapshot"))?;
            answered(ready(inner.write_snapshot(at, &snapshot)))
        }
        Op::Purged(recorded) => {
            let at = stream_ref(&recorded.stream, "purged.stream")?;
            answered(ready(inner.purge(at, recorded.expected))).map(drop)
        }
        Op::EditionStreamRemoved(recorded) => {
            let at = stream_ref(&recorded.stream, "edition_stream_removed.stream")?;
            answered(ready(inner.remove_edition_stream(at))).map(drop)
        }
        Op::OutboxCleared(recorded) => {
            let ids = recorded
                .ids
                .iter()
                .map(|id| uuid(id, "outbox_cleared.ids"))
                .collect::<Result<Vec<_>, _>>()?;
            answered(ready(inner.clear(&ids))).map(drop)
        }
        Op::DeadLettersDrained(recorded) => {
            let at = stream_ref(&recorded.stream, "dead_letters_drained.stream")?;
            answered(ready(inner.drain_dead_letters(at))).map(drop)
        }
        Op::DeadLettersRemoved(recorded) => replay_remove_dead_letters(inner, &recorded),
    }
}

fn replay_remove_dead_letters(
    inner: &MemoryStore,
    recorded: &schema::DeadLettersRemoved,
) -> Result<(), Replay> {
    let keys = recorded
        .keys
        .iter()
        .map(|key| {
            Ok(DeadLetterKey {
                stream: stream_ref(&key.stream, "dead_letters_removed.keys.stream")?,
                at: dead_letter_at(&key.at, "dead_letters_removed.keys.at")?,
                component: &key.component,
            })
        })
        .collect::<Result<Vec<_>, Replay>>()?;
    answered(ready(inner.remove_dead_letters(&keys))).map(drop)
}

fn replay_write(inner: &MemoryStore, recorded: &schema::Write) -> Result<(), Replay> {
    let pages = match &recorded.events {
        Some(events) => events
            .pages
            .iter()
            .map(|page| EventPage::decode(page.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Replay::Malformed("write.events.pages"))?,
        None => Vec::new(),
    };
    let events = match &recorded.events {
        Some(events) => Some(Append {
            stream: stream_ref(&events.stream, "write.events.stream")?,
            expected: events.expected,
            pages: &pages,
        }),
        None => None,
    };
    let positions = recorded
        .positions
        .iter()
        .map(|position| {
            let key = PositionKey {
                handler: &position.handler,
                stream: stream_ref(&position.stream, "write.positions.stream")?,
            };
            Ok((key, position.next))
        })
        .collect::<Result<Vec<_>, Replay>>()?;
    let outbox = recorded
        .outbox
        .iter()
        .map(|entry| {
            Ok(OutboxEntry {
                id: uuid(&entry.id, "write.outbox.id")?,
                origin: entry.origin.clone(),
                message: Any::decode(entry.message.as_slice())
                    .map_err(|_| Replay::Malformed("write.outbox.message"))?,
                edition: entry.edition.clone(),
            })
        })
        .collect::<Result<Vec<_>, Replay>>()?;
    let dead_letters = recorded
        .dead_letters
        .iter()
        .map(|letter| {
            Ok(DeadLetter {
                stream: stream_ref(&letter.stream, "write.dead_letters.stream")?.to_cover(),
                at: dead_letter_at(&letter.at, "write.dead_letters.at")?,
                letter: AngzarrDeadLetter::decode(letter.letter.as_slice())
                    .map_err(|_| Replay::Malformed("write.dead_letters.letter"))?,
            })
        })
        .collect::<Result<Vec<_>, Replay>>()?;
    let cleared = recorded
        .cleared
        .iter()
        .map(|id| uuid(id, "write.cleared"))
        .collect::<Result<Vec<_>, Replay>>()?;
    let write = Write {
        events,
        positions: &positions,
        outbox: &outbox,
        dead_letters: &dead_letters,
        cleared: &cleared,
    };
    answered(ready(inner.write(write))).map(drop)
}

#[cfg(test)]
#[path = "entry.test.rs"]
mod tests;
