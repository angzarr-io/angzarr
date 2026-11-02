# Messages for angzarr-ports' store errors. Each id is a StoreError code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args. $stream names a
# stream as domain/root, with @edition outside the main timeline; $cause is
# the adapter's own error, rendered by its own message, from the adapter's
# catalog. A runtime that forwards these errors under their own codes
# renders them from this catalog, which the crate exports as
# angzarr_ports::EN_US.

store-conflict =
    stream { $stream } holds { $actual ->
        [one] one event
       *[other] { $actual } events
    }, not the { $expected } expected: someone else appended first
store-discontinuous = an append to stream { $stream } gives sequence { $found } where { $expected } was due
store-malformed-stream = { $stream } is no stream: it needs a 16-byte root and, if it names an edition, a non-empty one
store-malformed-creation = the creation of stream { $stream } at sequence { $sequence } names { $parent_sequence } as its parent sequence: the two must agree
store-unavailable = the store is unavailable: { $cause }
store-unreadable = event { $sequence } of stream { $stream } cannot be read: { $cause }
store-dead-letter-unreadable = a dead letter about stream { $stream } cannot be read: { $cause }
store-unknown-edition = stream { $stream } names an edition that does not exist
edition-name-required = an edition operation needs an edition: main is never forked into or deleted
store-edition-not-forked = stream { $stream } is not yet created in its edition: only its creation may be written first
store-edition-already-forked = stream { $stream } is already created in its edition, or is on main, which is never forked into
store-edition-not-deleted = stream { $stream } cannot be removed: its last event is not its deletion
store-edition-not-propagated = stream { $stream } cannot be removed yet: its deletion has not been delivered onward
edition-not-found = edition { $name } does not exist: no stream is forked in it
store-newer-than-build = the store was migrated by a newer build, to level { $applied }; this build knows up to { $build }, so it neither reads nor migrates it
store-migration-failed = migration { $version } failed part way in this store, which is left as it is: repair it, or restore a backup
store-migration-changed = migration { $version } was changed since it was applied to this store
store-migration-missing = migration { $version } was applied to this store, but this build no longer has it
store-migration-unknown = { $version } is not a migration of this build
store-migration-irreversible = migration { $version } has no down, so the store cannot be migrated back past it; nothing was changed
store-backup-exists = no backup was written: something is already at { $target }
