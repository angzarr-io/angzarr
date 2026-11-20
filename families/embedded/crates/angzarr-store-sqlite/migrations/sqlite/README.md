# SQLite migrations

Every open of a store, to write (`SqliteStore::open`) or to read
(`SqliteStore::open_existing`), runs what is pending, each migration applied by
sqlx's `Migrate::apply` inside one `BEGIN IMMEDIATE` transaction, so stores
opening a file at once take turns. A migration therefore cannot opt out of a
transaction (`-- no-transaction`) or change `PRAGMA journal_mode` (the store
keeps every file in WAL mode itself): neither can run inside a transaction.
`every_migration_runs_inside_the_open_transaction` in `src/sqlite.test.rs`
checks both.

Before anything is applied to a store holding something, it is backed up beside
itself, `state.db.pre-<applied level>-<UTC time>` (`VACUUM INTO`), and so it is
before an undo. The store is angzarr-ports' `Migrations` and `Backup`
(`src/migrations.rs`): `run_to` and `undo_to` a version go through sqlx's
`apply` and `revert`.

None of these has a down, so no undo past any of them goes through
(`angzarr-ports-store-migration-irreversible`). A migration given a down is
`<version>_<name>.up.sql` with its `<version>_<name>.down.sql`.

An applied migration is never edited: sqlx records its checksum, and a store
refuses a database whose applied migrations differ from its own
(`angzarr-ports-store-migration-changed`), one a migration failed in
(`-migration-failed`), and one a newer build migrated (`-newer-than-build`).
