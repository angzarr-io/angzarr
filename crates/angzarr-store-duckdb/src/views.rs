//! The reads that look across or behind the base ones: a read unit that
//! keeps its moment, the reads by time and by correlation, the listings of
//! streams and positions, and the export.

use std::collections::BTreeMap;

use angzarr_ports::angzarr_proto::v1::{Cover, EventBook, Snapshot};
use angzarr_ports::{
    Export, ExportRow, ListStreams, Processed, ReadByCorrelation, ReadStore, ReadUnit,
    ReadUntilTime, Slice, StoreError, StreamRef, Uuid,
};
use duckdb::Connection;
use prost_types::Timestamp;

use crate::error::unavailable;
use crate::ports::pending;
use crate::queries::{Narrow, Queries};
use crate::writes::in_transaction;
use crate::{rows, statements, DuckStore};

/// A read-only unit: a DuckDB transaction on a connection of its own,
/// seeing the database as it began. The store goes on serving writes while
/// it is open. Dropped, it ends, having written nothing.
pub struct DuckRead {
    conn: Connection,
}

impl ReadStore for DuckStore {
    type Reads<'s> = DuckRead;

    /// A connection of the store's database, in a transaction whose moment
    /// the first read fixes (it reads the store's own table).
    async fn begin_read(&self) -> Result<DuckRead, StoreError> {
        let conn = self.with(|conn| conn.try_clone().map_err(unavailable))?;
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(unavailable)?;
        conn.edition_known("")?;
        Ok(DuckRead { conn })
    }
}

impl DuckRead {
    /// Refuse a stream in an edition no fork of which exists.
    fn known(&self, stream: StreamRef<'_>) -> Result<(), StoreError> {
        match stream.edition {
            Some(edition) if !self.conn.edition_known(edition)? => {
                Err(StoreError::unknown_edition(stream))
            }
            _ => Ok(()),
        }
    }
}

impl ReadUnit for DuckRead {
    async fn covers(&mut self, domain: &str) -> Result<Vec<Cover>, StoreError> {
        Queries::covers(&self.conn, domain)
    }

    async fn read(
        &mut self,
        stream: StreamRef<'_>,
        from: u64,
        to: Option<u64>,
    ) -> Result<Slice, StoreError> {
        self.known(stream)?;
        self.conn.read(stream, from, to)
    }

    async fn read_snapshot(
        &mut self,
        stream: StreamRef<'_>,
    ) -> Result<Option<Snapshot>, StoreError> {
        self.known(stream)?;
        self.conn.snapshot(stream)
    }
}

impl ReadUntilTime for DuckStore {
    /// The prefix recorded at or before `until`, and the stream's head.
    async fn read_until_time(
        &self,
        stream: StreamRef<'_>,
        until: &Timestamp,
    ) -> Result<Slice, StoreError> {
        let by = statements::instant_column(until);
        self.with(|conn| {
            let pages = conn.view(stream, (0, None), Narrow::Until(by))?;
            let head = Queries::head(conn, stream)?;
            Ok(Slice {
                next_sequence: head.next_sequence,
                last_recorded_at: head.last_recorded_at,
                pages,
            })
        })
    }
}

impl ReadByCorrelation for DuckStore {
    /// Every stream holding an event of the correlation in main or in the
    /// edition, each read as `read` reads it in the edition: those with
    /// pages of it, one book each.
    async fn read_by_correlation(
        &self,
        domain: Option<&str>,
        edition: Option<&str>,
        correlation_id: Uuid,
    ) -> Result<Vec<EventBook>, StoreError> {
        let id = correlation_id.as_bytes().to_vec();
        let in_edition = edition.unwrap_or_default().to_string();
        let domain = domain.map(str::to_string);
        self.with(|conn| {
            let candidates = conn.rows(
                statements::CORRELATED_STREAMS,
                &[&id, &in_edition, &domain],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )?;
            let mut books = Vec::new();
            for (stream_domain, root) in candidates {
                let stream = StreamRef {
                    domain: &stream_domain,
                    edition,
                    root: rows::stream_root(&root)?,
                };
                let pages = conn.view(stream, (0, None), Narrow::Correlation(&id))?;
                if pages.is_empty() {
                    continue;
                }
                let head = Queries::head(conn, stream)?;
                books.push(EventBook {
                    cover: Some(stream.to_cover()),
                    pages,
                    snapshot: None,
                    next_sequence: head.next_sequence,
                });
            }
            Ok(books)
        })
    }
}

impl ListStreams for DuckStore {
    async fn list_streams(&self) -> Result<Vec<Cover>, StoreError> {
        let stored = self.with(|conn| {
            conn.rows(statements::ALL_STREAMS, &[], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
        })?;
        stored
            .into_iter()
            .map(|(domain, edition, root)| rows::cover(&domain, edition, &root))
            .collect()
    }
}

impl Processed for DuckStore {
    async fn processed(&self, handler: &str) -> Result<Vec<(Cover, u64)>, StoreError> {
        let stored = self.with(|conn| {
            conn.rows(statements::HANDLER_POSITIONS, &[&handler], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
        })?;
        stored
            .into_iter()
            .map(|(domain, edition, root, next)| {
                Ok((rows::cover(&domain, edition, &root)?, next.unsigned_abs()))
            })
            .collect()
    }
}

/// A stream as its rows key it: domain, the edition column (`''` for
/// main) and the root's bytes. Its order is `StreamRef`'s.
type StoredKey = (String, String, Vec<u8>);

/// Each stream's export rows, gathered table by table: a stream's pages
/// first, then its snapshot, its positions and its letters, as the tables
/// are read in that order.
#[derive(Default)]
struct Gathered(BTreeMap<StoredKey, Vec<ExportRow>>);

impl Gathered {
    /// Add one row for each stored row of `sql`: its stream from the first
    /// three columns and the rest made by `make`.
    fn add(
        &mut self,
        conn: &Connection,
        sql: &str,
        make: impl Fn(Cover, &duckdb::Row<'_>) -> Result<ExportRow, StoreError>,
    ) -> Result<(), StoreError> {
        let mut statement = conn.prepare_cached(sql).map_err(unavailable)?;
        let mut stored = statement.query([]).map_err(unavailable)?;
        while let Some(row) = stored.next().map_err(unavailable)? {
            let key: StoredKey = (
                row.get(0).map_err(unavailable)?,
                row.get(1).map_err(unavailable)?,
                row.get(2).map_err(unavailable)?,
            );
            let cover = rows::cover(&key.0, key.1.clone(), &key.2)?;
            let exported = make(cover, row)?;
            self.0.entry(key).or_default().push(exported);
        }
        Ok(())
    }

    /// Every table's rows.
    fn gather(conn: &Connection) -> Result<Gathered, StoreError> {
        let mut gathered = Gathered::default();
        gathered.add(conn, statements::EXPORT_PAGES, |stream, row| {
            let sequence: i64 = row.get(3).map_err(unavailable)?;
            Ok(ExportRow::Page {
                stream,
                sequence: sequence.unsigned_abs(),
                stored: row.get(4).map_err(unavailable)?,
            })
        })?;
        gathered.add(conn, statements::EXPORT_SNAPSHOTS, |stream, row| {
            Ok(ExportRow::Snapshot {
                stream,
                stored: row.get(3).map_err(unavailable)?,
            })
        })?;
        gathered.add(conn, statements::EXPORT_POSITIONS, |stream, row| {
            let next: i64 = row.get(4).map_err(unavailable)?;
            Ok(ExportRow::Position {
                stream,
                handler: row.get(3).map_err(unavailable)?,
                next: next.unsigned_abs(),
            })
        })?;
        gathered.add(conn, statements::EXPORT_DEAD_LETTERS, |stream, row| {
            let kind: i64 = row.get(3).map_err(unavailable)?;
            let at: Vec<u8> = row.get(4).map_err(unavailable)?;
            Ok(ExportRow::DeadLetter {
                at: rows::letter_place(&stream, kind, &at)?,
                stream,
                component: row.get(5).map_err(unavailable)?,
                stored: row.get(6).map_err(unavailable)?,
            })
        })?;
        Ok(gathered)
    }
}

impl Export for DuckStore {
    fn backend(&self) -> &str {
        env!("CARGO_PKG_NAME")
    }

    /// Every table's rows in one transaction, gathered by stream in the
    /// key's order, then the pending outbox.
    async fn export(&self) -> Result<Vec<ExportRow>, StoreError> {
        self.with(|conn| {
            in_transaction(conn, |tx| {
                let gathered = Gathered::gather(tx)?;
                let outbox = pending(tx, i64::MAX)?;
                let mut rows: Vec<ExportRow> = gathered.0.into_values().flatten().collect();
                rows.extend(outbox.into_iter().map(ExportRow::Outbox));
                Ok(rows)
            })
        })
    }
}

#[cfg(test)]
#[path = "views.test.rs"]
mod tests;
