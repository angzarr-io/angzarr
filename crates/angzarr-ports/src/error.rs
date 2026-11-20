//! One error for every port, with stable codes.
//!
//! Every error is an [`angzarr_diagnostic::Diagnostic`]: a stable kebab-case
//! [`code`](StoreError::code) starting with this crate's name and named
//! [`args`](StoreError::args) that carry data. An error is a KEY and an
//! ARGUMENT MAP, never rendered English (decision 14.37): text is rendered
//! only where it is displayed, from a Fluent catalog keyed by the code (this
//! crate's en-US one is [`EN_US`](crate::EN_US)), and the `Display` (what a
//! log shows) is [`angzarr_diagnostic::display`]'s: the code followed by its
//! args, ` name=value` each, a nested [`Cause`] as ` name=<its code>` then
//! ` name.<arg>=value` for each of its args. A runtime forwards a wrapped
//! `StoreError` as a nested diagnostic, its code and args unchanged; core
//! maps the codes to gRPC statuses, the code as the reason and the args,
//! flattened the same way, as the `ErrorInfo` metadata.
//!
//! [`Arg`] and [`Cause`] are this crate's own, `Clone` and `Eq` so the error
//! is: the inherent [`StoreError::args`] gives them, and the `Diagnostic`
//! impls convert ([`Arg`] into `angzarr_diagnostic::Arg`, a [`Cause`] into a
//! nested diagnostic).

use std::fmt;

use angzarr_diagnostic::code;
use angzarr_proto::v1::Cover;

/// Why a store refused or failed. A refusal it is sure of and a failure to
/// answer are different variants: a [`Conflict`](StoreError::Conflict)
/// means "re-read and run again"; [`Unavailable`](StoreError::Unavailable)
/// means "the store could not say".
///
/// Every stream a variant names is boxed (owner ruling, 2026-10-28), so the
/// error stays small (pinned in `tests/error.rs`); build one with the
/// constructors ([`StoreError::conflict`], ...) and read its stream with
/// [`StoreError::stream`], and the box never shows.
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum StoreError {
    /// Someone else appended first: the stream is at `actual`, not
    /// `expected`. Nothing was appended. Core's `SequenceConflict`.
    Conflict {
        stream: Box<Cover>,
        expected: u64,
        actual: u64,
    },
    /// The pages do not continue the stream: sequence `found` where
    /// `expected` was due. A caller bug; nothing was appended.
    Discontinuous {
        stream: Box<Cover>,
        expected: u64,
        found: u64,
    },
    /// A wire `Cover` that is no stream: no root, a root not 16 bytes, or an
    /// edition set to the empty string (INVALID_ARGUMENT).
    MalformedStream { stream: Box<Cover> },
    /// The store failed: I/O, a timeout, a dropped connection, a schema this
    /// build cannot read. A write may or may not have landed.
    Unavailable(Cause),
    /// The store holds event `sequence` of `stream` but cannot give it back:
    /// bytes that do not decode as an `EventPage`, a row with metadata
    /// missing. Upcasting is never the store's: an unknown payload type is
    /// not this.
    Unreadable {
        stream: Box<Cover>,
        sequence: u64,
        reason: Cause,
    },
    /// The store holds a dead letter about `stream` but cannot give it
    /// back: bytes that do not decode as an `AngzarrDeadLetter`, or a place
    /// that is neither a sequence nor a command id.
    DeadLetterUnreadable { stream: Box<Cover>, reason: Cause },
    /// A read unit was given a stream in an edition that does not exist (no
    /// fork event of it anywhere; 14.32: NOT_FOUND). Nothing was read.
    UnknownEdition { stream: Box<Cover> },
    /// A write to an edition stream with no fork in that edition, other
    /// than its fork: every stream of an edition is forked explicitly, by
    /// ForkStream, and nothing propagates (decision 14.107: the not-forked
    /// rule, FAILED_PRECONDITION `ANGZARR_REASON_EDITION_NOT_FORKED` at the
    /// coordinator). Nothing was written.
    EditionNotForked { stream: Box<Cover> },
    /// A fork of a stream already forked in its edition (14.32:
    /// ALREADY_EXISTS), or of main, which is never forked into. Nothing
    /// was written.
    EditionAlreadyForked { stream: Box<Cover> },
    /// An edition operation with no edition: main is unnamed, and is
    /// neither forked into nor deleted (INVALID_ARGUMENT).
    EditionNameRequired,
    /// The edition does not exist: no fork event of it remains (NOT_FOUND).
    /// A step's position on a stream of it with no rows is refused so:
    /// work in flight after a deletion leaves no row of the edition behind,
    /// and only a fork brings the name back (decision 14.107).
    EditionNotFound { name: String },
    /// A store a newer build migrated: its ledger's highest version,
    /// `applied`, is beyond the build's highest, `build` (a
    /// [`MigrationStatus::Newer`](crate::MigrationStatus::Newer) in its
    /// info). Refused at open and by a run, untouched: run a build at least
    /// as new.
    NewerThanBuild { applied: u64, build: u64 },
    /// A store a migration failed in part way: `version`'s row in its
    /// ledger is not a success, so the store is dirty. Refused at open and
    /// by a run, untouched: an operator repairs it (or restores a backup).
    MigrationFailed { version: u64 },
    /// An applied migration, `version`, whose checksum is not the build's:
    /// changed since it was applied. Refused at open and by a run.
    MigrationChanged { version: u64 },
    /// An applied migration, `version`, the build does not have, below the
    /// build's highest. Refused at open and by a run.
    MigrationMissing { version: u64 },
    /// A run or undo to `version`, which is neither 0 nor a version of the
    /// build. Nothing was changed.
    MigrationUnknown { version: u64 },
    /// An undo past `version`, an applied migration with no down to undo
    /// it (the highest such). Nothing was changed.
    MigrationIrreversible { version: u64 },
    /// A backup refused because something is already at its target, named
    /// as the adapter names it: a backup never overwrites. Nothing was
    /// written.
    BackupExists { target: String },
}

impl From<crate::stream::StreamRef<'_>> for Cover {
    fn from(stream: crate::stream::StreamRef<'_>) -> Cover {
        stream.to_cover()
    }
}

impl StoreError {
    pub fn conflict(stream: impl Into<Cover>, expected: u64, actual: u64) -> StoreError {
        StoreError::Conflict {
            stream: Box::new(stream.into()),
            expected,
            actual,
        }
    }

    pub fn discontinuous(stream: impl Into<Cover>, expected: u64, found: u64) -> StoreError {
        StoreError::Discontinuous {
            stream: Box::new(stream.into()),
            expected,
            found,
        }
    }

    pub fn malformed_stream(stream: impl Into<Cover>) -> StoreError {
        StoreError::MalformedStream {
            stream: Box::new(stream.into()),
        }
    }

    pub fn unreadable(stream: impl Into<Cover>, sequence: u64, reason: Cause) -> StoreError {
        StoreError::Unreadable {
            stream: Box::new(stream.into()),
            sequence,
            reason,
        }
    }

    pub fn dead_letter_unreadable(stream: impl Into<Cover>, reason: Cause) -> StoreError {
        StoreError::DeadLetterUnreadable {
            stream: Box::new(stream.into()),
            reason,
        }
    }

    pub fn unknown_edition(stream: impl Into<Cover>) -> StoreError {
        StoreError::UnknownEdition {
            stream: Box::new(stream.into()),
        }
    }

    pub fn edition_not_forked(stream: impl Into<Cover>) -> StoreError {
        StoreError::EditionNotForked {
            stream: Box::new(stream.into()),
        }
    }

    pub fn edition_already_forked(stream: impl Into<Cover>) -> StoreError {
        StoreError::EditionAlreadyForked {
            stream: Box::new(stream.into()),
        }
    }

    /// The stream the error is about, if it names one.
    pub fn stream(&self) -> Option<&Cover> {
        match self {
            StoreError::Conflict { stream, .. }
            | StoreError::Discontinuous { stream, .. }
            | StoreError::MalformedStream { stream }
            | StoreError::Unreadable { stream, .. }
            | StoreError::DeadLetterUnreadable { stream, .. }
            | StoreError::UnknownEdition { stream }
            | StoreError::EditionNotForked { stream }
            | StoreError::EditionAlreadyForked { stream } => Some(stream),
            StoreError::Unavailable(_)
            | StoreError::EditionNameRequired
            | StoreError::EditionNotFound { .. }
            | StoreError::NewerThanBuild { .. }
            | StoreError::MigrationFailed { .. }
            | StoreError::MigrationChanged { .. }
            | StoreError::MigrationMissing { .. }
            | StoreError::MigrationUnknown { .. }
            | StoreError::MigrationIrreversible { .. }
            | StoreError::BackupExists { .. } => None,
        }
    }
}

/// What a caller does about a [`StoreError`]: angzarr's one recovery,
/// angzarr-diagnostic's. A store error is never
/// [`Business`](Recovery::Business): a conflict or the store unavailable is
/// `Retry`, a malformed request or one naming what does not exist
/// `Invalid`, a discontinuous append or an unreadable event `Bug`.
pub use angzarr_diagnostic::Recovery;

/// An adapter's own failure, captured as data so [`StoreError`] stays
/// `Clone` and `Eq`: the adapter's stable code (its own crate's prefix,
/// `annals-store-sql-io`) and its args, by name. Never its own English
/// (decision 14.37): an adapter maps what its driver reports (an errno, a
/// driver's own code) to args, and the driver's own text, a third-party
/// library's, travels verbatim as the arg `ext_err_txt` (owner's ruling,
/// 2026-10-29, late).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Cause {
    pub code: &'static str,
    pub args: Vec<(&'static str, Arg)>,
}

impl Cause {
    pub fn new(code: &'static str, args: impl IntoIterator<Item = (&'static str, Arg)>) -> Cause {
        Cause {
            code,
            args: args.into_iter().collect(),
        }
    }
}

/// One named argument of an error's message: data, never English.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Arg {
    /// A sequence, a length, a count.
    Uint(u64),
    /// A name, a stream's text form, an id.
    Str(String),
    /// A wrapped cause, rendered in its own right.
    Cause(Cause),
}

/// A stream's text form, as its arg: `domain/root`, or
/// `domain/root@edition` (annals' form).
fn stream_text(stream: &Cover) -> String {
    let root = stream
        .root
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    match &stream.edition {
        Some(edition) => format!("{}/{root}@{edition}", stream.domain),
        None => format!("{}/{root}", stream.domain),
    }
}

fn stream_arg(stream: &Cover) -> Arg {
    Arg::Str(stream_text(stream))
}

impl StoreError {
    /// The message id: kebab-case, starting `angzarr-ports-`, stable, never
    /// reused.
    pub fn code(&self) -> &'static str {
        match self {
            StoreError::Conflict { .. } => code!("store-conflict"),
            StoreError::Discontinuous { .. } => code!("store-discontinuous"),
            StoreError::MalformedStream { .. } => code!("store-malformed-stream"),
            StoreError::Unavailable(_) => code!("store-unavailable"),
            StoreError::Unreadable { .. } => code!("store-unreadable"),
            StoreError::DeadLetterUnreadable { .. } => code!("store-dead-letter-unreadable"),
            StoreError::UnknownEdition { .. } => code!("store-unknown-edition"),
            StoreError::EditionNameRequired => code!("edition-name-required"),
            StoreError::EditionNotFound { .. } => code!("edition-not-found"),
            StoreError::EditionNotForked { .. } => code!("store-edition-not-forked"),
            StoreError::EditionAlreadyForked { .. } => code!("store-edition-already-forked"),
            StoreError::NewerThanBuild { .. } => code!("store-newer-than-build"),
            StoreError::MigrationFailed { .. } => code!("store-migration-failed"),
            StoreError::MigrationChanged { .. } => code!("store-migration-changed"),
            StoreError::MigrationMissing { .. } => code!("store-migration-missing"),
            StoreError::MigrationUnknown { .. } => code!("store-migration-unknown"),
            StoreError::MigrationIrreversible { .. } => {
                code!("store-migration-irreversible")
            }
            StoreError::BackupExists { .. } => code!("store-backup-exists"),
        }
    }

    /// The message's arguments, by name: the same names every time for a
    /// given code.
    pub fn args(&self) -> Vec<(&'static str, Arg)> {
        match self {
            StoreError::Conflict {
                stream,
                expected,
                actual,
            } => vec![
                ("stream", stream_arg(stream)),
                ("expected", Arg::Uint(*expected)),
                ("actual", Arg::Uint(*actual)),
            ],
            StoreError::Discontinuous {
                stream,
                expected,
                found,
            } => vec![
                ("stream", stream_arg(stream)),
                ("expected", Arg::Uint(*expected)),
                ("found", Arg::Uint(*found)),
            ],
            StoreError::MalformedStream { stream } | StoreError::UnknownEdition { stream } => {
                vec![("stream", stream_arg(stream))]
            }
            StoreError::Unavailable(cause) => vec![("cause", Arg::Cause(cause.clone()))],
            StoreError::Unreadable {
                stream,
                sequence,
                reason,
            } => vec![
                ("stream", stream_arg(stream)),
                ("sequence", Arg::Uint(*sequence)),
                ("cause", Arg::Cause(reason.clone())),
            ],
            StoreError::DeadLetterUnreadable { stream, reason } => vec![
                ("stream", stream_arg(stream)),
                ("cause", Arg::Cause(reason.clone())),
            ],
            StoreError::EditionNameRequired => Vec::new(),
            StoreError::EditionNotFound { name } => vec![("name", Arg::Str(name.clone()))],
            StoreError::EditionNotForked { stream }
            | StoreError::EditionAlreadyForked { stream } => {
                vec![("stream", stream_arg(stream))]
            }
            StoreError::NewerThanBuild { applied, build } => vec![
                ("applied", Arg::Uint(*applied)),
                ("build", Arg::Uint(*build)),
            ],
            StoreError::MigrationFailed { version }
            | StoreError::MigrationChanged { version }
            | StoreError::MigrationMissing { version }
            | StoreError::MigrationUnknown { version }
            | StoreError::MigrationIrreversible { version } => {
                vec![("version", Arg::Uint(*version))]
            }
            StoreError::BackupExists { target } => vec![("target", Arg::Str(target.clone()))],
        }
    }

    /// What the caller does about it.
    pub fn recovery(&self) -> Recovery {
        match self {
            StoreError::Conflict { .. } | StoreError::Unavailable(_) => Recovery::Retry,
            StoreError::Discontinuous { .. }
            | StoreError::Unreadable { .. }
            | StoreError::DeadLetterUnreadable { .. } => Recovery::Bug,
            StoreError::MalformedStream { .. }
            | StoreError::UnknownEdition { .. }
            | StoreError::EditionNameRequired
            | StoreError::EditionNotFound { .. } => Recovery::Invalid,
            StoreError::EditionNotForked { .. } | StoreError::EditionAlreadyForked { .. } => {
                Recovery::Invalid
            }
            StoreError::NewerThanBuild { .. }
            | StoreError::BackupExists { .. }
            | StoreError::MigrationUnknown { .. }
            | StoreError::MigrationIrreversible { .. } => Recovery::Invalid,
            StoreError::MigrationFailed { .. }
            | StoreError::MigrationChanged { .. }
            | StoreError::MigrationMissing { .. } => Recovery::Bug,
        }
    }
}

/// The code, then its args (decision 14.37): the key and the map, never
/// English. Text is rendered only where it is displayed, from a Fluent
/// catalog keyed by the code ([`EN_US`](crate::EN_US)).
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl From<Arg> for angzarr_diagnostic::Arg {
    fn from(arg: Arg) -> angzarr_diagnostic::Arg {
        match arg {
            Arg::Uint(n) => angzarr_diagnostic::Arg::Uint(n),
            Arg::Str(s) => angzarr_diagnostic::Arg::Str(s),
            Arg::Cause(cause) => angzarr_diagnostic::Arg::Nested(Box::new(cause)),
        }
    }
}

fn diagnostic_args(args: Vec<(&'static str, Arg)>) -> Vec<(&'static str, angzarr_diagnostic::Arg)> {
    args.into_iter()
        .map(|(name, arg)| (name, arg.into()))
        .collect()
}

impl angzarr_diagnostic::Diagnostic for StoreError {
    fn code(&self) -> &str {
        StoreError::code(self)
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        diagnostic_args(StoreError::args(self))
    }
}

/// The adapter's code, then its args: never English.
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

/// An adapter's cause is a diagnostic of the adapter's own: its code, its
/// args.
impl angzarr_diagnostic::Diagnostic for Cause {
    fn code(&self) -> &str {
        self.code
    }

    fn args(&self) -> Vec<(&str, angzarr_diagnostic::Arg)> {
        diagnostic_args(self.args.clone())
    }
}

impl std::error::Error for StoreError {}

/// One of every variant, each once: [`crate::diagnostic_samples`].
pub(crate) fn samples() -> Vec<StoreError> {
    let stream = || Cover {
        domain: "account".to_string(),
        root: Some(angzarr_proto::v1::Uuid::from_bytes([7; 16])),
        edition: None,
    };
    let io = || Cause::new("angzarr-store-sqlite-io", [("errno", Arg::Uint(5))]);
    vec![
        StoreError::conflict(stream(), 1, 2),
        StoreError::discontinuous(stream(), 2, 3),
        StoreError::malformed_stream(stream()),
        StoreError::Unavailable(io()),
        StoreError::unreadable(stream(), 4, io()),
        StoreError::dead_letter_unreadable(stream(), io()),
        StoreError::unknown_edition(stream()),
        StoreError::EditionNameRequired,
        StoreError::EditionNotFound {
            name: "what-if".to_string(),
        },
        StoreError::edition_not_forked(stream()),
        StoreError::edition_already_forked(stream()),
        StoreError::NewerThanBuild {
            applied: 6,
            build: 5,
        },
        StoreError::MigrationFailed { version: 3 },
        StoreError::MigrationChanged { version: 2 },
        StoreError::MigrationMissing { version: 2 },
        StoreError::MigrationUnknown { version: 7 },
        StoreError::MigrationIrreversible { version: 4 },
        StoreError::BackupExists {
            target: "state.db.pre-3-20261031T000000Z".to_string(),
        },
    ]
}
