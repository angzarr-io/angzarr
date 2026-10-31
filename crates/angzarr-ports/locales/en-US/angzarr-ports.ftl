# Messages for angzarr-ports' store errors. Each id is a StoreError code; the
# variables are its args. $stream names a stream as domain/root, with
# @edition outside the main timeline; $cause is the adapter's own error,
# rendered by its own message, from the adapter's catalog. A runtime that
# forwards these errors under their own codes renders them from this
# catalog, which the crate exports as angzarr_ports::EN_US.

angzarr-ports-store-conflict =
    stream { $stream } holds { $actual ->
        [one] one event
       *[other] { $actual } events
    }, not the { $expected } expected: someone else appended first
angzarr-ports-store-discontinuous = an append to stream { $stream } gives sequence { $found } where { $expected } was due
angzarr-ports-store-malformed-stream = { $stream } is no stream: it needs a 16-byte root and, if it names an edition, a non-empty one
angzarr-ports-store-malformed-creation = the creation of stream { $stream } at sequence { $sequence } names { $parent_sequence } as its parent sequence: the two must agree
angzarr-ports-store-unavailable = the store is unavailable: { $cause }
angzarr-ports-store-unreadable = event { $sequence } of stream { $stream } cannot be read: { $cause }
angzarr-ports-store-unknown-edition = stream { $stream } names an edition that does not exist
angzarr-ports-edition-name-required = an edition operation needs an edition: main is never forked into or deleted
angzarr-ports-store-edition-not-forked = stream { $stream } is not yet created in its edition: only its creation may be written first
angzarr-ports-store-edition-already-forked = stream { $stream } is already created in its edition, or is on main, which is never forked into
angzarr-ports-store-edition-not-deleted = stream { $stream } cannot be removed: its last event is not its deletion
angzarr-ports-store-edition-not-propagated = stream { $stream } cannot be removed yet: its deletion has not been delivered onward
angzarr-ports-edition-not-found = edition { $name } does not exist: no stream is forked in it
angzarr-ports-store-newer-than-build = the store was migrated by a newer build, to level { $applied }; this build knows up to { $build }, so it neither reads nor migrates it
angzarr-ports-store-migration-failed = migration { $version } failed part way in this store, which is left as it is: repair it, or restore a backup
angzarr-ports-store-migration-changed = migration { $version } was changed since it was applied to this store
angzarr-ports-store-migration-missing = migration { $version } was applied to this store, but this build no longer has it
angzarr-ports-store-migration-unknown = { $version } is not a migration of this build
angzarr-ports-store-migration-irreversible = migration { $version } has no down, so the store cannot be migrated back past it; nothing was changed
angzarr-ports-store-backup-exists = no backup was written: something is already at { $target }
