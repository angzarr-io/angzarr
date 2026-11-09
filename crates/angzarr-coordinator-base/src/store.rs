//! A [`StoreError`] as a keyed [`Coded`].
//!
//! Two of the store's failures are a spec key, the status its option
//! declares: the store failing to answer is `ANGZARR_REASON_STORE_UNAVAILABLE`
//! (its `cause` the adapter's own code and args), a write to an edition
//! stream with no fork `ANGZARR_REASON_EDITION_NOT_FORKED`. Every other
//! stays under its own `angzarr-ports-*` code, the args the error gives,
//! under the status the ports' docs give it: a conflict ABORTED (re-read
//! and run again), a malformed stream or an edition operation naming no
//! edition INVALID_ARGUMENT, an edition that does not exist NOT_FOUND, one
//! forked twice ALREADY_EXISTS, a stored event or dead letter that cannot be
//! read DATA_LOSS, a discontinuous append and a store a migration left dirty
//! INTERNAL, a store a newer build migrated or a backup that would
//! overwrite FAILED_PRECONDITION, a migration the build cannot run to
//! INVALID_ARGUMENT.

use angzarr_ports::StoreError;
use angzarr_proto::v1::{reason_args, AngzarrReason, Cover};

use crate::{flat_args, Code, Coded, Refusal};

/// A stream as the spec's keys name it: `domain/root`, the edition apart.
fn stream_text(stream: &Cover) -> String {
    let root = stream
        .root
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    format!("{}/{root}", stream.domain)
}

/// The status of a store error that is not a spec key. What is not listed
/// is a bug or damage on the store's side (INTERNAL): a discontinuous
/// append, a store a migration left dirty, changed or short of a version.
fn status(error: &StoreError) -> Code {
    match error {
        StoreError::Conflict { .. } => Code::Aborted,
        StoreError::MalformedStream { .. }
        | StoreError::EditionNameRequired
        | StoreError::MigrationUnknown { .. }
        | StoreError::MigrationIrreversible { .. } => Code::InvalidArgument,
        StoreError::UnknownEdition { .. } | StoreError::EditionNotFound { .. } => Code::NotFound,
        StoreError::EditionAlreadyForked { .. } => Code::AlreadyExists,
        StoreError::Unreadable { .. } | StoreError::DeadLetterUnreadable { .. } => Code::DataLoss,
        StoreError::NewerThanBuild { .. } | StoreError::BackupExists { .. } => {
            Code::FailedPrecondition
        }
        _ => Code::Internal,
    }
}

impl From<&StoreError> for Coded {
    fn from(error: &StoreError) -> Coded {
        match error {
            StoreError::Unavailable(_) => {
                Coded::spec(AngzarrReason::StoreUnavailable, flat_args(error))
            }
            StoreError::EditionNotForked { stream } => Coded::spec(
                AngzarrReason::EditionNotForked,
                [
                    (reason_args::STREAM.to_string(), stream_text(stream)),
                    (
                        reason_args::EDITION.to_string(),
                        stream.edition.clone().unwrap_or_default(),
                    ),
                ],
            ),
            other => Coded::from_diagnostic(status(other), other),
        }
    }
}

impl From<StoreError> for Coded {
    fn from(error: StoreError) -> Coded {
        Coded::from(&error)
    }
}

impl Refusal for StoreError {
    fn coded(&self) -> Coded {
        Coded::from(self)
    }
}

#[cfg(test)]
#[path = "store.test.rs"]
mod tests;
