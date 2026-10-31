//! Versioned documents: a root mapping stamped `schema_version: N`, read
//! through a chain of registered migrations up to the version the code reads.
//!
//! - **The stamp.** Every versioned document is a mapping with a root stamp
//!   key, [`DEFAULT_KEY`] (`schema_version`) unless the kind names its own
//!   ([`Migrations::key`]), whose value is a non-negative integer (decimal
//!   digits) that only ever increases: a format generation, never semver. A
//!   mapping with no stamp is version 0, so files written before versioning
//!   load through the whole chain. A stamp that is not such an integer is
//!   refused ([`LoadProblem::BadVersion`]).
//! - **A legacy key.** A kind whose files were stamped under another key
//!   declares it ([`Migrations::legacy_key`]): before the stamp is read, a
//!   pre-step renames that key, in its place, to the kind's key, so the old
//!   files load and an upgrade rewrites them under the new key. A document
//!   holding both is refused ([`LoadProblem::LegacyAndStamp`]).
//! - **The chain.** [`Migrations::new`] names the document kind and the
//!   current version; [`Migrations::step`] registers the step from `N` to
//!   `N + 1`. [`Migrations::build`] checks the chain and gives a [`Loader`]:
//!   no duplicate steps, no step at or past the current version, and no gap
//!   between the oldest step and the current version. The oldest supported
//!   version is the lowest registered step's (the current version with no
//!   steps). Dropping support for old files is deleting their oldest steps.
//! - **A step** edits the tree in place and leaves the stamp to the loader,
//!   which writes `schema_version: N + 1` after the step returns. The helpers here
//!   ([`rename_key`], [`move_key`], [`set_default`], [`remove_key`],
//!   [`map_seq`]) cover the usual edits.
//! - **Loading** is string in, tree out: parse, read the version, run the
//!   steps from it to the current version, hand the tree to the caller's
//!   decoder. It never writes anything back; [`Loader::upgrade`] gives the
//!   upgraded text for a tool that means to rewrite a file. No I/O, no clock:
//!   the same input gives the same tree and the same bytes.
//! - **Fixtures.** [`Loader::check_fixtures`] proves old files still load:
//!   each fixture, at any supported version, must migrate to its expected
//!   current-version tree.
//! - **Errors** are data: [`LoadError`], [`ChainError`] and [`MigrationError`]
//!   are [`Diagnostic`]s. A step or decoder that fails for the caller's own
//!   reason returns the caller's own diagnostic, kept with its code as a
//!   [`Report`]: [`MigrationError::game`] (or `Report::new(..).into()`) in a
//!   step, and any `E: Diagnostic` from [`Loader::load_as`]'s decoder.

use std::fmt;

use angzarr_diagnostic::{Arg, Diagnostic, Report};

use crate::{parse, write, Error, Value};

/// The stamp key every kind uses unless it names its own: a root
/// `schema_version`, as Go ctxloom's `schemaver.Key` (owner ruling,
/// 2026-10-31: one key, a monotonically increasing unsigned integer).
pub const DEFAULT_KEY: &str = "schema_version";

/// A migration step: edits a document at version `N` into version `N + 1`.
/// The loader stamps the new version; the step need not.
pub type Step = fn(&mut Value) -> Result<(), MigrationError>;

/// Why a migration step could not migrate a document: one of the helpers'
/// failures, or the caller's own error. Key paths (`from`, `to`) are written
/// dotted, `stats.hp`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationError {
    /// [`rename_key`] on a value that is not a mapping.
    RenameNotAMapping { from: String },
    /// [`rename_key`] to a key the mapping already has.
    RenameTargetExists { from: String, to: String },
    /// [`move_key`] with an empty key path.
    MoveEmptyPath { from: String, to: String },
    /// [`move_key`] from or into a value that is not a mapping.
    MoveNotAMapping { from: String, to: String },
    /// [`move_key`] through `key`, which is not a mapping.
    MoveThroughNotAMapping {
        from: String,
        to: String,
        key: String,
    },
    /// [`move_key`] onto `key`, which is already there.
    MoveTargetExists {
        from: String,
        to: String,
        key: String,
    },
    /// [`set_default`] on a value that is not a mapping.
    DefaultNotAMapping { key: String },
    /// [`remove_key`] on a value that is not a mapping.
    RemoveNotAMapping { key: String },
    /// [`map_seq`] on a value that is not a mapping.
    MapNotAMapping { key: String },
    /// [`map_seq`] over `key`, which is not a sequence.
    MapNotASequence { key: String },
    /// [`map_seq`]'s function failed on item `index` of `key`.
    Item {
        key: String,
        index: usize,
        error: Box<MigrationError>,
    },
    /// The step left the root something other than a mapping.
    RootNotAMapping,
    /// The caller's own error, with the caller's own code: see
    /// [`MigrationError::game`].
    Game(Report),
}

impl MigrationError {
    /// A caller's own error, keeping its code and args.
    pub fn game(error: &dyn Diagnostic) -> MigrationError {
        MigrationError::Game(Report::of(error))
    }
}

impl From<Report> for MigrationError {
    fn from(report: Report) -> MigrationError {
        MigrationError::Game(report)
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for MigrationError {}

impl Diagnostic for MigrationError {
    fn code(&self) -> &str {
        match self {
            MigrationError::RenameNotAMapping { .. } => "angzarr-yaml-rename-not-a-mapping",
            MigrationError::RenameTargetExists { .. } => "angzarr-yaml-rename-target-exists",
            MigrationError::MoveEmptyPath { .. } => "angzarr-yaml-move-empty-path",
            MigrationError::MoveNotAMapping { .. } => "angzarr-yaml-move-not-a-mapping",
            MigrationError::MoveThroughNotAMapping { .. } => {
                "angzarr-yaml-move-through-not-a-mapping"
            }
            MigrationError::MoveTargetExists { .. } => "angzarr-yaml-move-target-exists",
            MigrationError::DefaultNotAMapping { .. } => "angzarr-yaml-default-not-a-mapping",
            MigrationError::RemoveNotAMapping { .. } => "angzarr-yaml-remove-not-a-mapping",
            MigrationError::MapNotAMapping { .. } => "angzarr-yaml-map-not-a-mapping",
            MigrationError::MapNotASequence { .. } => "angzarr-yaml-map-not-a-sequence",
            MigrationError::Item { .. } => "angzarr-yaml-map-item",
            MigrationError::RootNotAMapping => "angzarr-yaml-step-root-not-a-mapping",
            MigrationError::Game(report) => report.code(),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let s = |s: &String| Arg::from(s.as_str());
        match self {
            MigrationError::RenameNotAMapping { from } => vec![("from", s(from))],
            MigrationError::RenameTargetExists { from, to } => {
                vec![("from", s(from)), ("to", s(to))]
            }
            MigrationError::MoveEmptyPath { from, to }
            | MigrationError::MoveNotAMapping { from, to } => {
                vec![("from", s(from)), ("to", s(to))]
            }
            MigrationError::MoveThroughNotAMapping { from, to, key }
            | MigrationError::MoveTargetExists { from, to, key } => {
                vec![("from", s(from)), ("to", s(to)), ("key", s(key))]
            }
            MigrationError::DefaultNotAMapping { key }
            | MigrationError::RemoveNotAMapping { key }
            | MigrationError::MapNotAMapping { key }
            | MigrationError::MapNotASequence { key } => vec![("key", s(key))],
            MigrationError::Item { key, index, error } => {
                vec![
                    ("key", s(key)),
                    ("index", (*index).into()),
                    ("error", Arg::nested(&**error)),
                ]
            }
            MigrationError::RootNotAMapping => vec![],
            MigrationError::Game(report) => report.args(),
        }
    }
}

// ---------------------------------------------------------------- registry

/// A document kind's migration steps, not yet checked. [`Migrations::build`]
/// checks them.
#[derive(Clone, Debug)]
pub struct Migrations {
    kind: String,
    current: u32,
    key: String,
    legacy: Option<String>,
    steps: Vec<(u32, Step)>,
}

/// A chain of steps that cannot load documents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainError {
    /// The document kind.
    pub kind: String,
    pub problem: ChainProblem,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainProblem {
    /// Two steps from the same version.
    Duplicate { from: u32 },
    /// A step from a version at or past the current one.
    BeyondCurrent { from: u32, current: u32 },
    /// No step from this version, though older versions have one.
    Gap { from: u32 },
}

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for ChainError {}

impl Diagnostic for ChainError {
    fn code(&self) -> &str {
        match self.problem {
            ChainProblem::Duplicate { .. } => "angzarr-yaml-chain-duplicate",
            ChainProblem::BeyondCurrent { .. } => "angzarr-yaml-chain-beyond-current",
            ChainProblem::Gap { .. } => "angzarr-yaml-chain-gap",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let kind = ("kind", Arg::from(self.kind.as_str()));
        match self.problem {
            ChainProblem::Duplicate { from } => vec![kind, ("from", from.into())],
            ChainProblem::BeyondCurrent { from, current } => {
                vec![kind, ("from", from.into()), ("current", current.into())]
            }
            ChainProblem::Gap { from } => vec![
                kind,
                ("from", from.into()),
                ("to", (u64::from(from) + 1).into()),
            ],
        }
    }
}

impl Migrations {
    /// No steps yet, for documents of `kind` (named in every error) whose
    /// current version is `current`.
    /// Its stamp key is [`DEFAULT_KEY`].
    pub fn new(kind: &str, current: u32) -> Migrations {
        Migrations {
            kind: kind.to_string(),
            current,
            key: DEFAULT_KEY.to_string(),
            legacy: None,
            steps: Vec::new(),
        }
    }

    /// Stamp this kind under `key` instead of [`DEFAULT_KEY`].
    pub fn key(mut self, key: &str) -> Migrations {
        self.key = key.to_string();
        self
    }

    /// Read a stamp written under `legacy` as this kind's key: a pre-step,
    /// before the stamp is read, renames `legacy` to the key in its place.
    /// For a kind moving off an old stamp key (bosun's and the cli's
    /// `version`). The same as the key, it is no legacy key.
    pub fn legacy_key(mut self, legacy: &str) -> Migrations {
        self.legacy = Some(legacy.to_string());
        self
    }

    /// Register the step from version `from` to `from + 1`.
    pub fn step(mut self, from: u32, step: Step) -> Migrations {
        self.steps.push((from, step));
        self
    }

    /// Check the chain. Not a panic at [`Migrations::step`], because a gap
    /// shows only once every step is in, and a typed error lets a test say
    /// which chain is wrong; a caller calls this once, where it defines its
    /// steps, and expects it to succeed.
    pub fn build(self) -> Result<Loader, ChainError> {
        let Migrations {
            kind,
            current,
            key,
            legacy,
            mut steps,
        } = self;
        let legacy = legacy.filter(|l| *l != key);
        let fail = |problem| {
            Err(ChainError {
                kind: kind.clone(),
                problem,
            })
        };
        steps.sort_by_key(|(from, _)| *from);
        for pair in steps.windows(2) {
            if pair[0].0 == pair[1].0 {
                return fail(ChainProblem::Duplicate { from: pair[0].0 });
            }
        }
        if let Some(&(from, _)) = steps.iter().find(|(from, _)| *from >= current) {
            return fail(ChainProblem::BeyondCurrent { from, current });
        }
        let oldest = steps.first().map_or(current, |(from, _)| *from);
        for (i, (from, _)) in steps.iter().enumerate() {
            let want = oldest + i as u32;
            if *from != want {
                return fail(ChainProblem::Gap { from: want });
            }
        }
        if oldest + steps.len() as u32 != current {
            return fail(ChainProblem::Gap {
                from: oldest + steps.len() as u32,
            });
        }
        Ok(Loader {
            kind,
            current,
            key,
            legacy,
            oldest,
            steps: steps.into_iter().map(|(_, s)| s).collect(),
        })
    }
}

// ---------------------------------------------------------------- loading

/// Loads one kind of versioned document, migrating it to the current
/// version. Made by [`Migrations::build`], so its chain is whole: every
/// version from [`Loader::oldest`] up to [`Loader::current`] has its step.
#[derive(Clone, Debug)]
pub struct Loader {
    kind: String,
    current: u32,
    key: String,
    legacy: Option<String>,
    oldest: u32,
    /// The step from `oldest + i` at index `i`.
    steps: Vec<Step>,
}

/// A document that does not load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadError {
    /// The document kind.
    pub kind: String,
    /// Where the document came from, if the caller said.
    pub source: Option<String>,
    pub problem: LoadProblem,
}

/// Why a document does not load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadProblem {
    /// Not YAML this crate reads.
    Yaml(Error),
    /// The root is not a mapping.
    NotAMapping,
    /// The stamp, under `key` (the kind's, or its legacy key), is not a
    /// non-negative integer.
    BadVersion { key: String, problem: BadVersion },
    /// Both the kind's key and the legacy key it replaces are set: which
    /// one stamps the document is not for the loader to guess.
    LegacyAndStamp { key: String, legacy: String },
    /// Written by a newer build than this one.
    TooNew { found: u32, current: u32 },
    /// Older than the oldest version this build migrates.
    TooOld { found: u32, oldest: u32 },
    /// The step from `from` to `from + 1` failed.
    Step {
        from: u32,
        error: Box<MigrationError>,
    },
    /// The current-version tree did not decode: the decoder's own error,
    /// with the caller's own code. See [`Loader::load_as`].
    Decode(Report),
}

/// What a stamp that is not a non-negative integer is instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BadVersion {
    /// The key with no value.
    Empty,
    Sequence,
    Mapping,
    /// A scalar that is not decimal digits, as read.
    NotAnInteger {
        text: String,
    },
    /// Decimal digits too large for a version, as read.
    TooLarge {
        text: String,
    },
}

impl fmt::Display for LoadProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl Diagnostic for LoadProblem {
    fn code(&self) -> &str {
        match self {
            LoadProblem::Yaml(e) => e.code(),
            LoadProblem::NotAMapping => "angzarr-yaml-root-not-a-mapping",
            LoadProblem::BadVersion { problem, .. } => match problem {
                BadVersion::Empty => "angzarr-yaml-version-empty",
                BadVersion::Sequence => "angzarr-yaml-version-sequence",
                BadVersion::Mapping => "angzarr-yaml-version-mapping",
                BadVersion::NotAnInteger { .. } => "angzarr-yaml-version-not-an-integer",
                BadVersion::TooLarge { .. } => "angzarr-yaml-version-too-large",
            },
            LoadProblem::LegacyAndStamp { .. } => "angzarr-yaml-legacy-and-stamp",
            LoadProblem::TooNew { .. } => "angzarr-yaml-too-new",
            LoadProblem::TooOld { .. } => "angzarr-yaml-too-old",
            LoadProblem::Step { .. } => "angzarr-yaml-step-failed",
            LoadProblem::Decode(e) => e.code(),
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        match self {
            LoadProblem::Yaml(e) => e.args(),
            LoadProblem::NotAMapping => vec![],
            LoadProblem::BadVersion { key, problem } => {
                let mut args = vec![("key", Arg::from(key.as_str()))];
                if let BadVersion::NotAnInteger { text } | BadVersion::TooLarge { text } = problem {
                    args.push(("text", text.as_str().into()));
                }
                args
            }
            LoadProblem::LegacyAndStamp { key, legacy } => vec![
                ("key", key.as_str().into()),
                ("legacy", legacy.as_str().into()),
            ],
            LoadProblem::TooNew { found, current } => {
                vec![("found", (*found).into()), ("current", (*current).into())]
            }
            LoadProblem::TooOld { found, oldest } => {
                vec![("found", (*found).into()), ("oldest", (*oldest).into())]
            }
            LoadProblem::Step { from, error } => vec![
                ("from", (*from).into()),
                ("to", (u64::from(*from) + 1).into()),
                ("error", Arg::nested(&**error)),
            ],
            LoadProblem::Decode(e) => e.args(),
        }
    }
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        angzarr_diagnostic::display(self, f)
    }
}

impl std::error::Error for LoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.problem {
            LoadProblem::Yaml(e) => Some(e),
            LoadProblem::Step { error, .. } => Some(&**error),
            LoadProblem::Decode(e) => Some(e),
            _ => None,
        }
    }
}

impl Diagnostic for LoadError {
    fn code(&self) -> &str {
        match self.source {
            None => "angzarr-yaml-load-failed",
            Some(_) => "angzarr-yaml-load-failed-at",
        }
    }

    fn args(&self) -> Vec<(&str, Arg)> {
        let mut args = vec![("kind", Arg::from(self.kind.as_str()))];
        if let Some(source) = &self.source {
            args.push(("source", source.as_str().into()));
        }
        args.push(("problem", Arg::nested(&self.problem)));
        args
    }
}

/// A document's stamp under [`DEFAULT_KEY`]; a mapping without one is
/// version 0. [`Loader::version_of`] reads a kind's own key.
pub fn version_of(doc: &Value) -> Result<u32, LoadProblem> {
    version_at(doc, DEFAULT_KEY)
}

/// A document's stamp under `key`; a mapping without one is version 0.
pub fn version_at(doc: &Value, key: &str) -> Result<u32, LoadProblem> {
    if doc.as_map().is_none() {
        return Err(LoadProblem::NotAMapping);
    }
    let bad = |problem| {
        Err(LoadProblem::BadVersion {
            key: key.to_string(),
            problem,
        })
    };
    let found = match doc.get(key) {
        None => return Ok(0),
        Some(Value::Scalar(s)) => s,
        Some(Value::Null) => return bad(BadVersion::Empty),
        Some(Value::Seq(_)) => return bad(BadVersion::Sequence),
        Some(Value::Map(_)) => return bad(BadVersion::Mapping),
    };
    if found.is_empty() || !found.bytes().all(|b| b.is_ascii_digit()) {
        return bad(BadVersion::NotAnInteger {
            text: found.clone(),
        });
    }
    found.parse().or_else(|_| {
        bad(BadVersion::TooLarge {
            text: found.clone(),
        })
    })
}

impl Loader {
    /// The document kind errors name.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// The version documents migrate to.
    pub fn current(&self) -> u32 {
        self.current
    }

    /// The oldest version this loader migrates.
    pub fn oldest(&self) -> u32 {
        self.oldest
    }

    /// The key documents are stamped under.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The old stamp key a pre-step renames to [`Loader::key`], if the kind
    /// has one.
    pub fn legacy_key(&self) -> Option<&str> {
        self.legacy.as_deref()
    }

    /// The legacy key, if `doc` is stamped under it rather than the kind's
    /// key; refused if under both.
    fn legacy_stamp(&self, doc: &Value) -> Result<Option<&str>, LoadProblem> {
        let Some(legacy) = self.legacy.as_deref() else {
            return Ok(None);
        };
        match (doc.get(&self.key), doc.get(legacy)) {
            (Some(_), Some(_)) => Err(LoadProblem::LegacyAndStamp {
                key: self.key.clone(),
                legacy: legacy.to_string(),
            }),
            (None, Some(_)) => Ok(Some(legacy)),
            _ => Ok(None),
        }
    }

    /// `doc`'s stamp, read as this kind reads it: under its key, or under
    /// its legacy key when only that is set. A mapping with neither is
    /// version 0.
    pub fn version_of(&self, doc: &Value) -> Result<u32, LoadProblem> {
        if doc.as_map().is_none() {
            return Err(LoadProblem::NotAMapping);
        }
        let key = self.legacy_stamp(doc)?.unwrap_or(&self.key);
        version_at(doc, key)
    }

    /// Whether `doc` needs no upgrade on disk: stamped under the kind's own
    /// key at the current version.
    fn is_current(&self, doc: &Value) -> bool {
        matches!(self.legacy_stamp(doc), Ok(None)) && version_at(doc, &self.key) == Ok(self.current)
    }

    fn fail<T>(&self, source: Option<&str>, problem: LoadProblem) -> Result<T, LoadError> {
        Err(LoadError {
            kind: self.kind.clone(),
            source: source.map(str::to_string),
            problem,
        })
    }

    /// Parse `text` and migrate it to the current version. `source` (a path,
    /// say) is only named in errors.
    pub fn load(&self, source: Option<&str>, text: &str) -> Result<Value, LoadError> {
        match parse(text) {
            Ok(doc) => self.migrate(source, doc),
            Err(e) => self.fail(source, LoadProblem::Yaml(e)),
        }
    }

    /// Migrate an already-parsed document to the current version. A
    /// current-version document comes back untouched.
    pub fn migrate(&self, source: Option<&str>, mut doc: Value) -> Result<Value, LoadError> {
        let found = match self.version_of(&doc) {
            Ok(v) => v,
            Err(problem) => return self.fail(source, problem),
        };
        if found > self.current {
            return self.fail(
                source,
                LoadProblem::TooNew {
                    found,
                    current: self.current,
                },
            );
        }
        if found < self.oldest {
            return self.fail(
                source,
                LoadProblem::TooOld {
                    found,
                    oldest: self.oldest,
                },
            );
        }
        // The pre-step: a legacy stamp takes the kind's key, in its place.
        if let Ok(Some(legacy)) = self.legacy_stamp(&doc) {
            if let Value::Map(entries) = &mut doc {
                for (k, _) in entries.iter_mut().filter(|(k, _)| k == legacy) {
                    *k = self.key.clone();
                }
            }
        }
        if let Value::Map(entries) = &mut doc {
            if found < self.current && !entries.iter().any(|(k, _)| *k == self.key) {
                entries.insert(0, (self.key.clone(), Value::Scalar("0".to_string())));
            }
        }
        for from in found..self.current {
            let step = self.steps[(from - self.oldest) as usize];
            let outcome = match step(&mut doc) {
                Ok(()) if doc.as_map().is_none() => Err(MigrationError::RootNotAMapping),
                outcome => outcome,
            };
            if let Err(error) = outcome {
                return self.fail(
                    source,
                    LoadProblem::Step {
                        from,
                        error: Box::new(error),
                    },
                );
            }
            *doc.entry(&self.key) = Value::Scalar((from + 1).to_string());
        }
        Ok(doc)
    }

    /// Load `text` and decode the current-version tree with `decode`. Its
    /// error, a caller's [`Diagnostic`] with the caller's own code, is reported as
    /// [`LoadProblem::Decode`].
    pub fn load_as<T, E: Diagnostic>(
        &self,
        source: Option<&str>,
        text: &str,
        decode: impl FnOnce(&Value) -> Result<T, E>,
    ) -> Result<T, LoadError> {
        let doc = self.load(source, text)?;
        decode(&doc).or_else(|e| self.fail(source, LoadProblem::Decode(Report::of(&e))))
    }

    /// The text of `text` migrated to the current version, written with
    /// [`crate::write`] at `flow_depth`, or None if it is already current.
    /// For tools that upgrade files on purpose: the written text has no
    /// comments, so a current file is left as it is.
    pub fn upgrade(
        &self,
        source: Option<&str>,
        text: &str,
        flow_depth: usize,
    ) -> Result<Option<String>, LoadError> {
        let doc = match parse(text) {
            Ok(doc) => doc,
            Err(e) => return self.fail(source, LoadProblem::Yaml(e)),
        };
        if self.is_current(&doc) {
            return Ok(None);
        }
        Ok(Some(write(&self.migrate(source, doc)?, flow_depth)))
    }

    /// [`Loader::upgrade`], keeping the text: the migration is written back
    /// through [`crate::edit::Document::update_to`], so only the entries it
    /// changed are rewritten and comments elsewhere stay. When an edit can't
    /// be made in place, the whole tree is written as [`Loader::upgrade`]
    /// would, and [`Upgraded::comments_kept`] is false. None if `text` is
    /// already current.
    pub fn upgrade_keeping(
        &self,
        source: Option<&str>,
        text: &str,
        flow_depth: usize,
    ) -> Result<Option<Upgraded>, LoadError> {
        let mut doc = match crate::edit::Document::parse(text) {
            Ok(doc) => doc,
            Err(e) => return self.fail(source, LoadProblem::Yaml(e)),
        };
        if self.is_current(doc.value()) {
            return Ok(None);
        }
        let migrated = self.migrate(source, doc.value().clone())?;
        // The legacy stamp is renamed where it stands, so the update has
        // only its value left to change.
        if let Ok(Some(legacy)) = self.legacy_stamp(doc.value()) {
            let renamed = doc.rename_key(&[], legacy, &self.key);
            debug_assert!(renamed.is_ok(), "{renamed:?}");
        }
        Ok(Some(match doc.update_to(&migrated, flow_depth) {
            Ok(()) => Upgraded {
                text: doc.into_string(),
                comments_kept: true,
            },
            Err(_) => Upgraded {
                text: write(&migrated, flow_depth),
                comments_kept: false,
            },
        }))
    }

    /// Check that every fixture loads to its expected tree. Every failure is
    /// listed, one line each.
    pub fn check_fixtures(&self, fixtures: &[Fixture<'_>]) -> Result<(), Vec<String>> {
        let mut failures = Vec::new();
        if fixtures.is_empty() {
            failures.push(format!("{}: no fixtures", self.kind));
        }
        for fx in fixtures {
            let got = match self.load(Some(fx.name), fx.input) {
                Ok(doc) => doc,
                Err(e) => {
                    failures.push(e.to_string());
                    continue;
                }
            };
            let want = match fx.expected {
                Some(text) => match parse(text) {
                    Ok(doc) => doc,
                    Err(e) => {
                        failures.push(format!("{}: expected output: {e}", fx.name));
                        continue;
                    }
                },
                None => match parse(fx.input) {
                    Ok(doc) => doc,
                    Err(e) => {
                        failures.push(format!("{}: {e}", fx.name));
                        continue;
                    }
                },
            };
            match version_at(&want, &self.key) {
                Ok(v) if v == self.current => {}
                Ok(v) if fx.expected.is_none() => failures.push(format!(
                    "{}: at version {v}, not the current {}: give it its expected current-version output",
                    fx.name, self.current
                )),
                _ => failures.push(format!(
                    "{}: the expected output must be at the current version, {}",
                    fx.name, self.current
                )),
            }
            if got != want {
                failures.push(format!(
                    "{}: migrated to\n{}which is not the expected\n{}",
                    fx.name,
                    write(&got, 2),
                    write(&want, 2)
                ));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }

    /// [`Loader::check_fixtures`], panicking with every failure: for tests.
    #[track_caller]
    pub fn assert_fixtures(&self, fixtures: &[Fixture<'_>]) {
        if let Err(failures) = self.check_fixtures(fixtures) {
            panic!(
                "{} fixtures do not migrate:\n{}",
                self.kind,
                failures.join("\n")
            );
        }
    }
}

/// A document's text migrated to the current version, from
/// [`Loader::upgrade_keeping`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upgraded {
    /// The migrated text.
    pub text: String,
    /// Whether it was edited in place, keeping every comment outside the
    /// entries the migration changed; false when it was written whole.
    pub comments_kept: bool,
}

/// A document at some supported version, and what it must migrate to.
#[derive(Clone, Copy, Debug)]
pub struct Fixture<'a> {
    /// Named in failures: its path, say.
    pub name: &'a str,
    /// The document as stored.
    pub input: &'a str,
    /// The current-version document it must migrate to. None for a document
    /// already at the current version, which must load unchanged; when the
    /// version moves on, such a fixture fails until it is given its expected
    /// output.
    pub expected: Option<&'a str>,
}

// ---------------------------------------------------------------- helpers

/// Rename `from` to `to` in `map`, keeping its place. Nothing to do if `from`
/// is missing; an error if `to` is already there.
pub fn rename_key(map: &mut Value, from: &str, to: &str) -> Result<(), MigrationError> {
    let Value::Map(entries) = map else {
        return Err(MigrationError::RenameNotAMapping {
            from: from.to_string(),
        });
    };
    if from == to || !entries.iter().any(|(k, _)| k == from) {
        return Ok(());
    }
    if entries.iter().any(|(k, _)| k == to) {
        return Err(MigrationError::RenameTargetExists {
            from: from.to_string(),
            to: to.to_string(),
        });
    }
    for (k, _) in entries.iter_mut() {
        if k == from {
            *k = to.to_string();
        }
    }
    Ok(())
}

/// Move the value at key path `from` to key path `to`, both from `root`.
/// Missing mappings on the way to `to` are made (a null counts as an empty
/// mapping). Nothing to do if `from` is missing; an error if `to` is already
/// there or a key on either path is not a mapping, and then `root` is left
/// as it was.
pub fn move_key(root: &mut Value, from: &[&str], to: &[&str]) -> Result<(), MigrationError> {
    let before = root.clone();
    let moved = try_move(root, from, to);
    if moved.is_err() {
        *root = before;
    }
    moved
}

fn try_move(root: &mut Value, from: &[&str], to: &[&str]) -> Result<(), MigrationError> {
    let paths = || (from.join("."), to.join("."));
    let not_a_mapping = || {
        let (from, to) = paths();
        MigrationError::MoveNotAMapping { from, to }
    };
    let through = |key: &str| {
        let (from, to) = paths();
        MigrationError::MoveThroughNotAMapping {
            from,
            to,
            key: key.to_string(),
        }
    };
    let (Some((from_key, from_parents)), Some((to_key, to_parents))) =
        (from.split_last(), to.split_last())
    else {
        let (from, to) = paths();
        return Err(MigrationError::MoveEmptyPath { from, to });
    };
    let mut parent = &mut *root;
    for key in from_parents {
        match parent.get_mut(key) {
            Some(next @ Value::Map(_)) => parent = next,
            Some(Value::Null) | None => return Ok(()),
            Some(_) => return Err(through(key)),
        }
    }
    if parent.as_map().is_none() {
        return Err(not_a_mapping());
    }
    let Some(value) = parent.remove(from_key) else {
        return Ok(());
    };
    let mut parent = &mut *root;
    for key in to_parents {
        if parent.as_map().is_none() {
            return Err(through(key));
        }
        let next = parent.entry(key);
        if *next == Value::Null {
            *next = Value::Map(Vec::new());
        }
        parent = next;
    }
    if parent.as_map().is_none() {
        return Err(not_a_mapping());
    }
    if parent.get(to_key).is_some() {
        let (from, to) = paths();
        return Err(MigrationError::MoveTargetExists {
            from,
            to,
            key: to_key.to_string(),
        });
    }
    *parent.entry(to_key) = value;
    Ok(())
}

/// Set `key` in `map` to `value` if it is missing or null.
pub fn set_default(map: &mut Value, key: &str, value: Value) -> Result<(), MigrationError> {
    if map.as_map().is_none() {
        return Err(MigrationError::DefaultNotAMapping {
            key: key.to_string(),
        });
    }
    let slot = map.entry(key);
    if *slot == Value::Null {
        *slot = value;
    }
    Ok(())
}

/// Take `key` out of `map`, giving its value if it was there.
pub fn remove_key(map: &mut Value, key: &str) -> Result<Option<Value>, MigrationError> {
    if map.as_map().is_none() {
        return Err(MigrationError::RemoveNotAMapping {
            key: key.to_string(),
        });
    }
    Ok(map.remove(key))
}

/// Run `f` over each item of the sequence at `key` in `map`. Nothing to do if
/// `key` is missing or null; an error names the failing item's index.
pub fn map_seq(
    map: &mut Value,
    key: &str,
    mut f: impl FnMut(&mut Value) -> Result<(), MigrationError>,
) -> Result<(), MigrationError> {
    if map.as_map().is_none() {
        return Err(MigrationError::MapNotAMapping {
            key: key.to_string(),
        });
    }
    match map.get_mut(key) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Seq(items)) => {
            for (index, item) in items.iter_mut().enumerate() {
                f(item).map_err(|e| MigrationError::Item {
                    key: key.to_string(),
                    index,
                    error: Box::new(e),
                })?;
            }
            Ok(())
        }
        Some(_) => Err(MigrationError::MapNotASequence {
            key: key.to_string(),
        }),
    }
}

/// One value of every error this module defines, each variant once; see
/// [`crate::diagnostic_samples`]. A caller's own code ([`MigrationError::Game`],
/// [`LoadProblem::Decode`]) is the caller's to sample.
pub(crate) fn diagnostic_samples() -> Vec<Box<dyn Diagnostic>> {
    let s = |s: &str| s.to_string();
    let migration = vec![
        MigrationError::RenameNotAMapping { from: s("hp") },
        MigrationError::RenameTargetExists {
            from: s("hp"),
            to: s("health"),
        },
        MigrationError::MoveEmptyPath {
            from: s(""),
            to: s("stats.hp"),
        },
        MigrationError::MoveNotAMapping {
            from: s("hp"),
            to: s("stats.hp"),
        },
        MigrationError::MoveThroughNotAMapping {
            from: s("hp"),
            to: s("stats.hp"),
            key: s("stats"),
        },
        MigrationError::MoveTargetExists {
            from: s("hp"),
            to: s("stats.hp"),
            key: s("hp"),
        },
        MigrationError::DefaultNotAMapping { key: s("speed") },
        MigrationError::RemoveNotAMapping { key: s("legacy") },
        MigrationError::MapNotAMapping { key: s("units") },
        MigrationError::MapNotASequence { key: s("units") },
        MigrationError::Item {
            key: s("units"),
            index: 2,
            error: Box::new(MigrationError::DefaultNotAMapping { key: s("hp") }),
        },
        MigrationError::RootNotAMapping,
    ];
    let yaml = Error {
        line: 2,
        problem: crate::ParseProblem::TabIndent,
    };
    let problems = vec![
        LoadProblem::NotAMapping,
        LoadProblem::BadVersion {
            key: s(DEFAULT_KEY),
            problem: BadVersion::Empty,
        },
        LoadProblem::BadVersion {
            key: s(DEFAULT_KEY),
            problem: BadVersion::Sequence,
        },
        LoadProblem::BadVersion {
            key: s(DEFAULT_KEY),
            problem: BadVersion::Mapping,
        },
        LoadProblem::BadVersion {
            key: s(DEFAULT_KEY),
            problem: BadVersion::NotAnInteger { text: s("two") },
        },
        LoadProblem::BadVersion {
            key: s(DEFAULT_KEY),
            problem: BadVersion::TooLarge {
                text: s("99999999999"),
            },
        },
        LoadProblem::LegacyAndStamp {
            key: s(DEFAULT_KEY),
            legacy: s("version"),
        },
        LoadProblem::TooNew {
            found: 4,
            current: 3,
        },
        LoadProblem::TooOld {
            found: 0,
            oldest: 1,
        },
        LoadProblem::Step {
            from: 1,
            error: Box::new(MigrationError::RootNotAMapping),
        },
    ];
    let load = |source: Option<&str>, problem| LoadError {
        kind: s("unit"),
        source: source.map(s),
        problem,
    };
    let chain = |problem| ChainError {
        kind: s("unit"),
        problem,
    };
    let mut out: Vec<Box<dyn Diagnostic>> = Vec::new();
    out.extend(migration.into_iter().map(|e| Box::new(e) as _));
    out.extend(problems.into_iter().map(|e| Box::new(e) as _));
    out.push(Box::new(load(None, LoadProblem::Yaml(yaml.clone()))));
    out.push(Box::new(load(
        Some("content/units.yaml"),
        LoadProblem::Yaml(yaml),
    )));
    out.push(Box::new(chain(ChainProblem::Duplicate { from: 1 })));
    out.push(Box::new(chain(ChainProblem::BeyondCurrent {
        from: 3,
        current: 3,
    })));
    out.push(Box::new(chain(ChainProblem::Gap { from: 1 })));
    out
}

#[cfg(test)]
#[path = "versioned.test.rs"]
mod tests;
