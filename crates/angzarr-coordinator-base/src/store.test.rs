use angzarr_ports::{Arg, Cause, StoreError};
use angzarr_proto::v1::{Cover, Uuid};

use super::*;
use crate::Code;

const ROOT: &str = "07070707-0707-0707-0707-070707070707";

fn cover(edition: Option<&str>) -> Cover {
    Cover {
        domain: "account".to_string(),
        root: Some(Uuid::from_bytes([7; 16])),
        edition: edition.map(str::to_string),
    }
}

fn io() -> Cause {
    Cause::new("angzarr-store-sqlite-io", [("errno", Arg::Uint(5))])
}

type Row = (StoreError, &'static str, Code, Vec<(&'static str, String)>);

fn row<const N: usize>(
    error: StoreError,
    key: &'static str,
    code: Code,
    args: [(&'static str, &str); N],
) -> Row {
    let args = args.into_iter().map(|(n, v)| (n, v.to_string())).collect();
    (error, key, code, args)
}

/// One row per `StoreError` variant: its key, its status and its args.
fn table() -> Vec<Row> {
    let mut rows = stream_rows();
    rows.extend(edition_and_migration_rows());
    rows
}

/// The rows of the append, read and open-store failures.
fn stream_rows() -> Vec<Row> {
    let stream = format!("account/{ROOT}");
    let in_edition = format!("account/{ROOT}@what-if");
    vec![
        row(
            StoreError::conflict(cover(None), 1, 2),
            "angzarr-ports-store-conflict",
            Code::Aborted,
            [("stream", &stream), ("expected", "1"), ("actual", "2")],
        ),
        row(
            StoreError::discontinuous(cover(None), 2, 3),
            "angzarr-ports-store-discontinuous",
            Code::Internal,
            [("stream", &stream), ("expected", "2"), ("found", "3")],
        ),
        row(
            StoreError::malformed_stream(cover(None)),
            "angzarr-ports-store-malformed-stream",
            Code::InvalidArgument,
            [("stream", &stream)],
        ),
        row(
            StoreError::Unavailable(io()),
            "ANGZARR_REASON_STORE_UNAVAILABLE",
            Code::Unavailable,
            [("cause", "angzarr-store-sqlite-io"), ("cause.errno", "5")],
        ),
        row(
            StoreError::unreadable(cover(None), 4, io()),
            "angzarr-ports-store-unreadable",
            Code::DataLoss,
            [
                ("stream", &stream),
                ("sequence", "4"),
                ("cause", "angzarr-store-sqlite-io"),
                ("cause.errno", "5"),
            ],
        ),
        row(
            StoreError::dead_letter_unreadable(cover(None), io()),
            "angzarr-ports-store-dead-letter-unreadable",
            Code::DataLoss,
            [
                ("stream", &stream),
                ("cause", "angzarr-store-sqlite-io"),
                ("cause.errno", "5"),
            ],
        ),
        row(
            StoreError::unknown_edition(cover(Some("what-if"))),
            "angzarr-ports-store-unknown-edition",
            Code::NotFound,
            [("stream", &in_edition)],
        ),
        row(
            StoreError::EditionNameRequired,
            "angzarr-ports-edition-name-required",
            Code::InvalidArgument,
            [],
        ),
        row(
            StoreError::EditionNotFound {
                name: "what-if".to_string(),
            },
            "angzarr-ports-edition-not-found",
            Code::NotFound,
            [("name", "what-if")],
        ),
    ]
}

/// The rows after the reads' and the writes': editions and migrations.
fn edition_and_migration_rows() -> Vec<Row> {
    let stream = format!("account/{ROOT}");
    let in_edition = format!("account/{ROOT}@what-if");
    vec![
        // The spec's key: `stream` is the stream, `edition` its edition.
        row(
            StoreError::edition_not_forked(cover(Some("what-if"))),
            "ANGZARR_REASON_EDITION_NOT_FORKED",
            Code::FailedPrecondition,
            [("stream", &stream), ("edition", "what-if")],
        ),
        row(
            StoreError::edition_already_forked(cover(Some("what-if"))),
            "angzarr-ports-store-edition-already-forked",
            Code::AlreadyExists,
            [("stream", &in_edition)],
        ),
        row(
            StoreError::NewerThanBuild {
                applied: 6,
                build: 5,
            },
            "angzarr-ports-store-newer-than-build",
            Code::FailedPrecondition,
            [("applied", "6"), ("build", "5")],
        ),
        row(
            StoreError::MigrationFailed { version: 3 },
            "angzarr-ports-store-migration-failed",
            Code::Internal,
            [("version", "3")],
        ),
        row(
            StoreError::MigrationChanged { version: 2 },
            "angzarr-ports-store-migration-changed",
            Code::Internal,
            [("version", "2")],
        ),
        row(
            StoreError::MigrationMissing { version: 2 },
            "angzarr-ports-store-migration-missing",
            Code::Internal,
            [("version", "2")],
        ),
        row(
            StoreError::MigrationUnknown { version: 7 },
            "angzarr-ports-store-migration-unknown",
            Code::InvalidArgument,
            [("version", "7")],
        ),
        row(
            StoreError::MigrationIrreversible { version: 4 },
            "angzarr-ports-store-migration-irreversible",
            Code::InvalidArgument,
            [("version", "4")],
        ),
        row(
            StoreError::BackupExists {
                target: "state.db.bak".to_string(),
            },
            "angzarr-ports-store-backup-exists",
            Code::FailedPrecondition,
            [("target", "state.db.bak")],
        ),
    ]
}

/// Every store error is a keyed Coded: the store failing to answer and an
/// unforked edition under the spec's keys, the rest under the ports' own
/// codes, each under its status, its args flattened.
#[test]
fn store_errors_convert_to_keyed_coded() {
    for (error, key, code, args) in table() {
        let coded = Coded::from(&error);
        assert_eq!(coded.key, key, "{error}");
        assert_eq!(coded.code, code, "{key}");
        let expected = args
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect();
        assert_eq!(coded.args, expected, "{key}");
        // By value and as a Refusal, the same.
        assert_eq!(Coded::from(error.clone()), coded, "{key}");
        assert_eq!(error.coded(), coded, "{key}");
    }
}

/// The table has a row for every variant the ports have: a variant added
/// there without a row here fails this, not a client at run time.
#[test]
fn the_table_covers_every_store_error_variant() {
    let rows = table();
    assert_eq!(rows.len(), angzarr_ports::diagnostic_samples().len());
    let codes: std::collections::BTreeSet<_> = rows.iter().map(|(e, ..)| e.code()).collect();
    assert_eq!(codes.len(), rows.len(), "one row per variant");
}

/// A key that is not a spec key is the store's own kebab-case code; a
/// spec key is a live `AngzarrReason` verbatim.
#[test]
fn a_spec_key_is_a_live_reason_and_the_rest_are_the_ports_codes() {
    for (error, key, ..) in table() {
        let spec = angzarr_proto::v1::AngzarrReason::from_str_name(key);
        match spec {
            Some(_) => assert!(key.starts_with("ANGZARR_REASON_"), "{key}"),
            None => assert_eq!(key, error.code()),
        }
    }
}
