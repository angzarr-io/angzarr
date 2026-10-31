//! A store's schema migrations and its backups: opt-in, store-agnostic,
//! after the common shape of SQL migrators (sqlx's `Migrator`, refinery,
//! diesel, golang-migrate, Flyway), so an adapter maps onto its own
//! migrator almost one to one, and nothing here names one.
//!
//! - **A build has a versioned migration set** ([`Migration`]: a version,
//!   a description, a checksum), in version order.
//! - **A store keeps a ledger** of what was applied to it
//!   ([`AppliedMigration`]: the same three, whether it succeeded, when it
//!   was installed, how long it ran).
//! - **[`Migrations::info`]** lays one over the other, a status per
//!   version ([`MigrationStatus`]), as `flyway info` and `sqlx migrate
//!   info` do; **[`Migrations::validate`]** refuses what a run would
//!   refuse; **[`Migrations::run`]** applies every pending migration to the
//!   latest, under the store's own lock against concurrent migrators.
//! - **A store is migrated when it is opened,** for reading or for writing
//!   (the owner, 2026-10-31): opening is the adapter's, and an adapter's
//!   every open runs [`Migrations::run`] before it serves. A store newer
//!   than the build, or one a migration failed in or was changed in, is
//!   refused at open, untouched.
//! - **No undo, and no run to a target version.** Reversible migrations
//!   are not common to the model (refinery has none, Flyway's undo is not
//!   in its free edition, and sqlx's run goes only to the latest), and
//!   this crate's adapters write none.
//! - **[`Backup`] is its own capability:** no migrator above takes one
//!   (Flyway and golang-migrate leave it to the operator).
//!
//! A level is a migration's version. Nothing here is ordered or positioned
//! store-wide: a migration changes the schema the streams are kept in,
//! never a stream's events or their order.

use std::future::Future;
use std::time::Duration;

use prost_types::Timestamp;

use crate::error::StoreError;
use crate::send::{MaybeSend, MaybeSync};

/// One migration of the build's set.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Migration {
    /// Its version: unique in the set, the set in its order.
    pub version: u64,
    /// What it does, in its author's words (a name, not a message).
    pub description: String,
    /// A digest of what it runs: an applied migration whose checksum
    /// differs from the build's was changed since it was applied.
    pub checksum: Vec<u8>,
}

/// One row of a store's ledger: a migration applied to it.
#[derive(Clone, PartialEq, Debug)]
pub struct AppliedMigration {
    pub version: u64,
    pub description: String,
    pub checksum: Vec<u8>,
    /// Whether it finished. One that did not left the store dirty: a run
    /// and an open refuse it ([`StoreError::MigrationFailed`]).
    pub success: bool,
    /// When it was installed.
    pub installed_on: Timestamp,
    /// How long it ran.
    pub execution_time: Duration,
}

/// How one version stands, the build's set laid over the store's ledger.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MigrationStatus {
    /// In both, succeeded, and unchanged.
    Applied,
    /// In the build, not yet applied.
    Pending,
    /// In the ledger as not finished: the store is dirty.
    Failed,
    /// Applied, but its checksum is not the build's: changed since.
    Changed,
    /// Applied, unknown to the build, and below the build's highest: a
    /// migration the build no longer has.
    Missing,
    /// Applied, unknown to the build, and beyond the build's highest: a
    /// newer build migrated the store (Flyway's "future").
    Newer,
}

/// One version's line of [`Migrations::info`].
#[derive(Clone, PartialEq, Debug)]
pub struct MigrationInfo {
    pub version: u64,
    /// The build's description, or the ledger's for a version the build
    /// does not have.
    pub description: String,
    pub status: MigrationStatus,
    /// Its row in the ledger, if it has one.
    pub applied: Option<AppliedMigration>,
}

/// `build`'s set laid over `ledger`: every version in either, in version
/// order, with how it stands. What [`Migrations::info`] answers, and what
/// an adapter's open may judge by.
pub fn info(build: &[Migration], ledger: &[AppliedMigration]) -> Vec<MigrationInfo> {
    let _ = (build, ledger);
    todo!("red: info awaits the owner's review")
}

/// What a run refuses, judged from `build`'s set and `ledger`: dirty
/// ([`StoreError::MigrationFailed`], the lowest such version), then newer
/// than the build ([`StoreError::NewerThanBuild`], the ledger's highest and
/// the build's), then changed ([`StoreError::MigrationChanged`]), then
/// missing ([`StoreError::MigrationMissing`]), each the lowest version so
/// standing. Pending migrations are no refusal.
pub fn validate(build: &[Migration], ledger: &[AppliedMigration]) -> Result<(), StoreError> {
    let _ = (build, ledger);
    todo!("red: validate awaits the owner's review")
}

/// Opt-in: a store's schema migrations. A backend with no schema has an
/// empty set and an empty ledger, and every run applies nothing.
pub trait Migrations: MaybeSend + MaybeSync {
    /// The build's migrations, in version order.
    fn migrations(&self) -> Vec<Migration>;

    /// The store's ledger, in version order. A read: nothing is applied.
    fn applied(
        &self,
    ) -> impl Future<Output = Result<Vec<AppliedMigration>, StoreError>> + MaybeSend;

    /// Every version in the build's set or the ledger, in version order,
    /// with how it stands. A read.
    fn info(&self) -> impl Future<Output = Result<Vec<MigrationInfo>, StoreError>> + MaybeSend {
        async move { Ok(info(&self.migrations(), &self.applied().await?)) }
    }

    /// What a run refuses, without running: a store dirty
    /// ([`StoreError::MigrationFailed`]), newer than the build
    /// ([`StoreError::NewerThanBuild`]), with a migration changed
    /// ([`StoreError::MigrationChanged`]) or one the build no longer has
    /// ([`StoreError::MigrationMissing`]), in that order of precedence.
    /// Pending migrations are no refusal. A read.
    fn validate(&self) -> impl Future<Output = Result<(), StoreError>> + MaybeSend {
        async move { validate(&self.migrations(), &self.applied().await?) }
    }

    /// Apply every pending migration, in version order, under the store's
    /// lock against concurrent migrators, after validating as
    /// [`validate`](Migrations::validate) does (a refusal applies
    /// nothing). Answers the rows it added to the ledger: none for a
    /// current store, and none for a migrator that found another had just
    /// applied them. Every stream's events are kept.
    fn run(&self) -> impl Future<Output = Result<Vec<AppliedMigration>, StoreError>> + MaybeSend;
}

/// Opt-in: a copy of the store as one moment of it, ledger and all,
/// migrating nothing.
pub trait Backup: MaybeSend + MaybeSync {
    /// Where a backup is written, named as the adapter names a store's
    /// place (a path, a URL, a slot in memory).
    type Target: MaybeSync + ?Sized;

    /// Copy the store, as it stands at one moment, to `target`. The store
    /// is left as it is. Something already at `target` is never
    /// overwritten: the backup is refused ([`StoreError::BackupExists`])
    /// and `target` left as it was.
    fn backup(
        &self,
        target: &Self::Target,
    ) -> impl Future<Output = Result<(), StoreError>> + MaybeSend;
}
