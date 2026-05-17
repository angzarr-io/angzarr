-- dlq_replay_audit: durable record of every DLQ replay attempt.
-- Postgres variant. See migrations/status/sqlite/0001_dlq_replay_audit.sql
-- for design rationale.

CREATE TABLE IF NOT EXISTS dlq_replay_audit (
    id                      BIGSERIAL PRIMARY KEY,
    dlq_id                  BIGINT    NOT NULL,
    replayed_at             TEXT      NOT NULL,  -- RFC-3339, stamped by the handler
    replay_mode             TEXT      NOT NULL,  -- 'as_is' | 'fresh_sequence'
    new_correlation_id      TEXT      NOT NULL,
    original_correlation_id TEXT,
    outcome                 TEXT      NOT NULL,  -- 'success' | 'failure'
    result_message          TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_replay_audit_dlq_id
    ON dlq_replay_audit(dlq_id);

CREATE INDEX IF NOT EXISTS idx_dlq_replay_audit_replayed_at
    ON dlq_replay_audit(replayed_at);
