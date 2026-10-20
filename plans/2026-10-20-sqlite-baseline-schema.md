# SQLite baseline schema for the latest proto

**Status:** implemented 2026-10-20 (owner ruling: a fresh baseline; old
databases are wiped). Migration: `migrations/sqlite/0001_baseline.sql`.

## Tables

- `events`: one row per event. `event_data` holds the whole `EventPage`
  (payload, sequence, `EventMeta`); the other columns are what reads select
  by: `domain`, `edition` (NULL = main), `root` (16-byte UUID), `sequence`,
  `recorded_at` (fixed-width RFC 3339, so text order is time order),
  `command_id` (16 bytes; NULL for a PM process event) and
  `correlation_id` (16 bytes).
  - `uq_events_stream_sequence` (stream, sequence), unique: the optimistic
    append.
  - `idx_events_command` (stream, command_id), not unique: deduplication is
    an index lookup (CI-1).
  - `idx_events_correlation` (correlation_id, domain): correlation queries.
- `snapshots`: the newest snapshot per stream (unique per stream; the default
  policy keeps only that one): `sequence` (the last event folded in),
  `state_type_url` and `rules_version` (what selection checks, decision
  14.1), and `state_value` (the state's bytes, offered unchanged).

Every id is a 16-byte blob (UU-3). No column exists without a reader: the
cascade, deferred-source, `ext` and external-id columns, and the positions
and editions tables (no reader on this branch), are gone.

## Old databases

Startup refuses a database created by the earlier schema (an `events`
table with `created_at`, or applied migrations past the baseline) with
`Old storage schema: ...`, naming the file and the fix (delete it), and
leaves the file untouched. Old events cannot be decoded by this build, so
nothing is migrated.

## Not yet

- A `(stream, triggering_event)` index for PM process-event dedup, and an
  editions table, arrive with the PM coordinator and the edition registry.
