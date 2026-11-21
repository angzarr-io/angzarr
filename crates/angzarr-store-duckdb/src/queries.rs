//! The reads, as an extension of DuckDB's connection: every one runs on the
//! connection it is called on, so the store's own connection and a read
//! unit's (a transaction held open) answer the same way.

use angzarr_ports::angzarr_proto::v1::{Cover, EventPage, Snapshot};
use angzarr_ports::{
    DeadLetter, DeadLetterKey, Head, PositionKey, Slice, StoreError, StreamRef, Uuid,
};
use duckdb::types::ToSql;
use duckdb::{Connection, Row};
use prost_types::Timestamp;

use crate::error::unavailable;
use crate::{rows, statements};

/// A stream as its rows key it: domain, the edition column (`''` for
/// main) and the root's bytes.
#[derive(Clone, Debug)]
pub(crate) struct Key {
    pub(crate) domain: String,
    pub(crate) edition: String,
    pub(crate) root: Vec<u8>,
}

impl From<StreamRef<'_>> for Key {
    fn from(stream: StreamRef<'_>) -> Key {
        Key {
            domain: stream.domain.to_string(),
            edition: stream.edition.unwrap_or_default().to_string(),
            root: stream.root.as_bytes().to_vec(),
        }
    }
}

impl Key {
    /// The same stream on the main timeline.
    fn in_main(&self) -> Key {
        Key {
            edition: String::new(),
            ..self.clone()
        }
    }

    /// The statement's first three parameters.
    pub(crate) fn params(&self) -> [&dyn ToSql; 3] {
        [&self.domain, &self.edition, &self.root]
    }

    /// The first three parameters, then `rest`.
    pub(crate) fn with<'a>(&'a self, rest: &[&'a dyn ToSql]) -> Vec<&'a dyn ToSql> {
        let mut all: Vec<&dyn ToSql> = self.params().to_vec();
        all.extend_from_slice(rest);
        all
    }
}

/// A stream's own extent: where its own pages begin, and where they end.
#[derive(Clone, Debug)]
pub(crate) struct Extent {
    pub(crate) first_sequence: u64,
    pub(crate) head: Head,
}

/// What narrows a read of a stream's pages beyond the range.
#[derive(Clone, Copy)]
pub(crate) enum Narrow<'a> {
    /// The range alone.
    Range,
    /// The pages a command caused.
    Command(&'a [u8]),
    /// The pages of a correlation.
    Correlation(&'a [u8]),
    /// The pages recorded at or before an instant.
    Until(i64),
}

impl Narrow<'_> {
    fn sql(&self) -> &'static str {
        match self {
            Narrow::Range => statements::PAGES,
            Narrow::Command(_) => statements::OF_COMMAND,
            Narrow::Correlation(_) => statements::OF_CORRELATION,
            Narrow::Until(_) => statements::UNTIL,
        }
    }
}

/// A row's first column as a stream's edition, then its root.
fn stored_key(row: &Row<'_>) -> duckdb::Result<(String, Vec<u8>)> {
    Ok((row.get(0)?, row.get(1)?))
}

/// A head from a stream's extent columns.
fn head_of(next: i64, seconds: Option<i64>, nanos: Option<i32>) -> Head {
    Head {
        next_sequence: next.unsigned_abs(),
        last_recorded_at: seconds.map(|seconds| Timestamp {
            seconds,
            nanos: nanos.unwrap_or_default(),
        }),
    }
}

/// The reads. Implemented for DuckDB's [`Connection`]; a transaction
/// reaches them through its deref.
pub(crate) trait Queries {
    /// Every row of `sql` under `params`, each made by `make`.
    fn rows<T>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        make: impl FnMut(&Row<'_>) -> duckdb::Result<T>,
    ) -> Result<Vec<T>, StoreError>;

    /// The statement run for what it changes: how many rows.
    fn run(&self, sql: &str, params: &[&dyn ToSql]) -> Result<usize, StoreError>;

    /// The first row of `sql`, if there is one.
    fn first<T>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        make: impl FnMut(&Row<'_>) -> duckdb::Result<T>,
    ) -> Result<Option<T>, StoreError> {
        Ok(self.rows(sql, params, make)?.into_iter().next())
    }

    /// The stream's own extent, `None` while it has no pages of its own.
    fn extent(&self, key: &Key) -> Result<Option<Extent>, StoreError> {
        self.first(statements::EXTENT, &key.params(), |row| {
            let first: i64 = row.get(0)?;
            let next: i64 = row.get(1)?;
            Ok(Extent {
                first_sequence: first.unsigned_abs(),
                head: head_of(next, row.get(2)?, row.get(3)?),
            })
        })
    }

    /// The stream's head as read in its edition: its own extent if it has
    /// pages of its own (main's below its fork, then its own), else main's.
    fn head(&self, stream: StreamRef<'_>) -> Result<Head, StoreError> {
        let key = Key::from(stream);
        if let Some(own) = self.extent(&key)? {
            return Ok(own.head);
        }
        let main = self.extent(&key.in_main())?;
        Ok(main.map(|extent| extent.head).unwrap_or_default())
    }

    /// The pages `from <= sequence < to` of the stream as read in its
    /// edition, narrowed: main's below its fork, then its own.
    fn view(
        &self,
        stream: StreamRef<'_>,
        range: (u64, Option<u64>),
        narrow: Narrow<'_>,
    ) -> Result<Vec<EventPage>, StoreError> {
        let key = Key::from(stream);
        let from = statements::sequence_column(range.0);
        let to = range.1.map_or(i64::MAX, statements::sequence_column);
        let mut pages = Vec::new();
        if stream.edition.is_some() {
            let fork = self.extent(&key)?.map_or(i64::MAX, |own| {
                statements::sequence_column(own.first_sequence)
            });
            let cover = stream.to_cover();
            pages = self.own_pages(&cover, &key.in_main(), (from, to.min(fork)), narrow)?;
        }
        let cover = stream.to_cover();
        pages.extend(self.own_pages(&cover, &key, (from, to), narrow)?);
        Ok(pages)
    }

    /// A stream's own pages in a range, narrowed.
    fn own_pages(
        &self,
        cover: &Cover,
        key: &Key,
        (from, to): (i64, i64),
        narrow: Narrow<'_>,
    ) -> Result<Vec<EventPage>, StoreError> {
        let extra = match &narrow {
            Narrow::Range => None,
            Narrow::Command(id) | Narrow::Correlation(id) => Some(id as &dyn ToSql),
            Narrow::Until(by) => Some(by as &dyn ToSql),
        };
        let mut params = key.with(&[&from, &to]);
        params.extend(extra);
        let stored = self.rows(narrow.sql(), &params, |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        stored
            .into_iter()
            .map(|(sequence, bytes)| rows::page(cover, sequence, &bytes))
            .collect()
    }

    /// The pages `from <= sequence < to`, with the stream's head.
    fn read(&self, stream: StreamRef<'_>, from: u64, to: Option<u64>) -> Result<Slice, StoreError> {
        let pages = self.view(stream, (from, to), Narrow::Range)?;
        let head = self.head(stream)?;
        Ok(Slice {
            next_sequence: head.next_sequence,
            last_recorded_at: head.last_recorded_at,
            pages,
        })
    }

    /// The pages the command caused.
    fn find_command(&self, stream: StreamRef<'_>, id: Uuid) -> Result<Vec<EventPage>, StoreError> {
        self.view(stream, (0, None), Narrow::Command(id.as_bytes()))
    }

    /// `domain`'s streams holding pages of their own, main first.
    fn covers(&self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        let found = self.rows(statements::COVERS, &[&domain], stored_key)?;
        found
            .into_iter()
            .map(|(edition, root)| rows::cover(domain, edition, &root))
            .collect()
    }

    /// The stream's own snapshot.
    fn snapshot(&self, stream: StreamRef<'_>) -> Result<Option<Snapshot>, StoreError> {
        let key = Key::from(stream);
        let stored = self.first(statements::SNAPSHOT, &key.params(), |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        stored
            .map(|bytes| rows::snapshot(&stream.to_cover(), &bytes))
            .transpose()
    }

    /// A handler's next sequence on a stream: 0 until put.
    fn position(&self, key: PositionKey<'_>) -> Result<u64, StoreError> {
        let stream = Key::from(key.stream);
        let params = stream.with(&[&key.handler]);
        let next = self.first(statements::POSITION, &params, |row| row.get::<_, i64>(0))?;
        Ok(next.unwrap_or_default().unsigned_abs())
    }

    /// Every letter about `stream`, in key order.
    fn letters_of(&self, stream: StreamRef<'_>) -> Result<Vec<DeadLetter>, StoreError> {
        let key = Key::from(stream);
        self.letters(stream, statements::DEAD_LETTERS, &key.params())
    }

    /// `(at_kind, at, letter)` rows of `sql` about `stream`, decoded.
    fn letters(
        &self,
        stream: StreamRef<'_>,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Vec<DeadLetter>, StoreError> {
        let cover = stream.to_cover();
        let stored = self.rows(sql, params, |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        stored
            .iter()
            .map(|(kind, at, letter)| rows::dead_letter(&cover, *kind, at, letter))
            .collect()
    }

    /// The letter of one key, if kept.
    fn letter(&self, key: DeadLetterKey<'_>) -> Result<Option<DeadLetter>, StoreError> {
        let stream = Key::from(key.stream);
        let (kind, at) = rows::place(key.at);
        let params = stream.with(&[&kind, &at, &key.component]);
        let found = self.letters(key.stream, statements::DEAD_LETTER, &params)?;
        Ok(found.into_iter().next())
    }

    /// Whether any stream is in the edition: a fork of it exists.
    fn edition_known(&self, name: &str) -> Result<bool, StoreError> {
        let known = self.first(statements::EDITION_KNOWN, &[&name], |row| {
            row.get::<_, bool>(0)
        })?;
        Ok(known.unwrap_or(false))
    }
}

impl Queries for Connection {
    fn rows<T>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
        make: impl FnMut(&Row<'_>) -> duckdb::Result<T>,
    ) -> Result<Vec<T>, StoreError> {
        let mut statement = self.prepare_cached(sql).map_err(unavailable)?;
        let found = statement.query_map(params, make).map_err(unavailable)?;
        found
            .collect::<duckdb::Result<Vec<T>>>()
            .map_err(unavailable)
    }

    fn run(&self, sql: &str, params: &[&dyn ToSql]) -> Result<usize, StoreError> {
        let mut statement = self.prepare_cached(sql).map_err(unavailable)?;
        statement.execute(params).map_err(unavailable)
    }
}

#[cfg(test)]
#[path = "queries.test.rs"]
mod tests;
