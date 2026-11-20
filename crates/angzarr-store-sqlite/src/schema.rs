//! The tables and columns, as sea-query identifiers: every statement names
//! them through these, never as strings. Each backend's migrations create
//! the tables (`migrations/<backend>/`).

use sea_query::Iden;

#[derive(Iden)]
pub(crate) enum Events {
    Table,
    Domain,
    Edition,
    Root,
    Sequence,
    CommandId,
    Page,
    CorrelationId,
    RecordedAt,
}

#[derive(Iden)]
pub(crate) enum Snapshots {
    Table,
    Domain,
    Edition,
    Root,
    Snapshot,
}
