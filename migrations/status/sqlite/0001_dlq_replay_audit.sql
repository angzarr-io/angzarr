-- dlq_replay_audit: durable record of every DLQ replay attempt.
--
-- Plan reference: P1.4 in plans/virtual-spinning-flute.md.
--
-- Design notes:
--   * `dlq_id` is NOT unique — the same dead letter can be replayed
--     multiple times (after fixes / upstream changes / retries). Each
--     replay leaves its own row; the UI inspects history to warn an
--     operator before a re-replay of an already-successful entry.
--   * `replay_mode` and `outcome` are stored as TEXT discriminators
--     (matches the existing dlq_entries.rejection_type pattern) so
--     ad-hoc SQL queries stay human-readable.
--   * `replayed_at` is RFC-3339 (driven by the handler) so the same
--     parse path works for SQLite + Postgres; we don't rely on
--     SQLite's `datetime('now')` default.
--   * No FK to dlq_entries.id — the operator may delete the original
--     row after a successful replay, but the audit trail must
--     survive.

CREATE TABLE IF NOT EXISTS dlq_replay_audit (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    dlq_id                  INTEGER NOT NULL,
    replayed_at             TEXT    NOT NULL,
    replay_mode             TEXT    NOT NULL,  -- 'as_is' | 'fresh_sequence'
    new_correlation_id      TEXT    NOT NULL,
    original_correlation_id TEXT,
    outcome                 TEXT    NOT NULL,  -- 'success' | 'failure'
    result_message          TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_replay_audit_dlq_id
    ON dlq_replay_audit(dlq_id);

CREATE INDEX IF NOT EXISTS idx_dlq_replay_audit_replayed_at
    ON dlq_replay_audit(replayed_at);
