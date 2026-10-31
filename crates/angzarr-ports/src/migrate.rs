//! A store's schema migrations and its backups: opt-in, store-agnostic,
//! after the common shape of SQL migrators (sqlx's `Migrator`, refinery,
//! diesel, golang-migrate, Flyway), so an adapter maps onto its own
//! migrator almost one to one, and nothing here names one.
//!
//! - **A build has a versioned migration set** ([`Migration`]: a version,
//!   a description, a checksum, and whether it has a down to undo it), in
//!   version order.
//! - **A store keeps a ledger** of what was applied to it
//!   ([`AppliedMigration`]: the same three, whether it succeeded, when it
//!   was installed, how long it ran).
//! - **[`Migrations::info`]** lays one over the other, a status per
//!   version ([`MigrationStatus`]), as `flyway info` and `sqlx migrate
//!   info` do; **[`Migrations::validate`]** refuses what a run would
//!   refuse; **[`Migrations::run`]** applies every pending migration to the
//!   latest, and [`Migrations::run_to`] those up to a target, under the
//!   store's own lock against concurrent migrators; **[`Migrations::undo_to`]**
//!   reverts the applied ones above a target, highest first, through their
//!   downs (sqlx's `undo`, golang-migrate's `down`, diesel's revert).
//! - **A store is migrated when it is opened,** for reading or for writing
//!   (the owner, 2026-10-31): opening is the adapter's, and an adapter's
//!   every open runs [`Migrations::run`], to the latest, before it serves.
//!   A store newer than the build, or one a migration failed in or was
//!   changed in, is refused at open, untouched.
//! - **A backend with [`Backup`] backs a store up before changing its
//!   schema** (the owner, 2026-10-31): an open with migrations pending, and
//!   any undo that reverts something, first back the store up beside it,
//!   as it stood; for a file `state.db`, to
//!   `state.db.pre-<applied level>-<UTC time>`. An open with nothing
//!   pending, and one refused, take none.
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
    /// Whether it has a down that undoes it. An undo past one that has
    /// none is refused ([`StoreError::MigrationIrreversible`]).
    pub reversible: bool,
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
    let top = latest(build);
    let mut versions: Vec<u64> = build
        .iter()
        .map(|m| m.version)
        .chain(ledger.iter().map(|r| r.version))
        .collect();
    versions.sort_unstable();
    versions.dedup();
    versions
        .into_iter()
        .map(|version| {
            let known = build.iter().find(|m| m.version == version);
            let row = ledger.iter().find(|r| r.version == version);
            let status = match (known, row) {
                (_, Some(r)) if !r.success => MigrationStatus::Failed,
                (Some(m), Some(r)) if m.checksum != r.checksum => MigrationStatus::Changed,
                (Some(_), Some(_)) => MigrationStatus::Applied,
                (Some(_), None) => MigrationStatus::Pending,
                (None, _) if version > top => MigrationStatus::Newer,
                (None, _) => MigrationStatus::Missing,
            };
            let description = known
                .map(|m| m.description.clone())
                .or_else(|| row.map(|r| r.description.clone()))
                .unwrap_or_default();
            MigrationInfo {
                version,
                description,
                status,
                applied: row.cloned(),
            }
        })
        .collect()
}

/// What a run refuses, judged from `build`'s set and `ledger`: dirty
/// ([`StoreError::MigrationFailed`], the lowest such version), then newer
/// than the build ([`StoreError::NewerThanBuild`], the ledger's highest and
/// the build's), then changed ([`StoreError::MigrationChanged`]), then
/// missing ([`StoreError::MigrationMissing`]), each the lowest version so
/// standing. Pending migrations are no refusal.
pub fn validate(build: &[Migration], ledger: &[AppliedMigration]) -> Result<(), StoreError> {
    let lines = info(build, ledger);
    let lowest = |status| {
        lines
            .iter()
            .find(|line| line.status == status)
            .map(|line| line.version)
    };
    if let Some(version) = lowest(MigrationStatus::Failed) {
        return Err(StoreError::MigrationFailed { version });
    }
    if lowest(MigrationStatus::Newer).is_some() {
        return Err(StoreError::NewerThanBuild {
            applied: ledger.iter().map(|r| r.version).max().unwrap_or(0),
            build: latest(build),
        });
    }
    if let Some(version) = lowest(MigrationStatus::Changed) {
        return Err(StoreError::MigrationChanged { version });
    }
    if let Some(version) = lowest(MigrationStatus::Missing) {
        return Err(StoreError::MigrationMissing { version });
    }
    Ok(())
}

/// That `target` is 0 or a version of `build`.
fn known_target(build: &[Migration], target: u64) -> Result<(), StoreError> {
    if target == 0 || build.iter().any(|m| m.version == target) {
        Ok(())
    } else {
        Err(StoreError::MigrationUnknown { version: target })
    }
}

/// The versions a run to `target` applies, in order: every one pending at
/// or below it, after [`validate`]. `target` is 0 or a version of the
/// build ([`StoreError::MigrationUnknown`] otherwise).
pub fn plan_run(
    build: &[Migration],
    ledger: &[AppliedMigration],
    target: u64,
) -> Result<Vec<u64>, StoreError> {
    validate(build, ledger)?;
    known_target(build, target)?;
    let mut planned: Vec<u64> = build
        .iter()
        .map(|m| m.version)
        .filter(|v| *v <= target && !ledger.iter().any(|r| r.version == *v))
        .collect();
    planned.sort_unstable();
    Ok(planned)
}

/// The versions an undo to `target` reverts, highest first: every one
/// applied above it, after [`validate`]. `target` is 0 or a version of the
/// build ([`StoreError::MigrationUnknown`] otherwise); one of them with no
/// down refuses the whole undo, naming the highest such
/// ([`StoreError::MigrationIrreversible`]).
pub fn plan_undo(
    build: &[Migration],
    ledger: &[AppliedMigration],
    target: u64,
) -> Result<Vec<u64>, StoreError> {
    validate(build, ledger)?;
    known_target(build, target)?;
    let mut planned: Vec<u64> = ledger
        .iter()
        .map(|r| r.version)
        .filter(|v| *v > target)
        .collect();
    planned.sort_unstable_by(|a, b| b.cmp(a));
    let irreversible = planned
        .iter()
        .copied()
        .find(|v| build.iter().any(|m| m.version == *v && !m.reversible));
    match irreversible {
        Some(version) => Err(StoreError::MigrationIrreversible { version }),
        None => Ok(planned),
    }
}

/// The highest version of the build's set, 0 for none: where a run goes.
pub fn latest(build: &[Migration]) -> u64 {
    build.iter().map(|m| m.version).max().unwrap_or(0)
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

    /// Apply every pending migration: [`run_to`](Migrations::run_to) the
    /// build's latest. What every open does.
    fn run(&self) -> impl Future<Output = Result<Vec<AppliedMigration>, StoreError>> + MaybeSend {
        async move { self.run_to(latest(&self.migrations())).await }
    }

    /// Apply every pending migration at or below `target`, in version
    /// order, under the store's lock against concurrent migrators, as
    /// [`plan_run`] plans it (a refusal applies nothing). Answers the rows
    /// it added to the ledger: none for a store already there, and none for
    /// a migrator that found another had just applied them. Every stream's
    /// events are kept.
    fn run_to(
        &self,
        target: u64,
    ) -> impl Future<Output = Result<Vec<AppliedMigration>, StoreError>> + MaybeSend;

    /// Revert every applied migration above `target`, highest first,
    /// through its down, under the same lock, as [`plan_undo`] plans it (a
    /// refusal changes nothing), dropping their rows from the ledger.
    /// Answers the rows it dropped, highest first. A backend with
    /// [`Backup`] backs the store up first when it reverts anything.
    fn undo_to(
        &self,
        target: u64,
    ) -> impl Future<Output = Result<Vec<AppliedMigration>, StoreError>> + MaybeSend;
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
