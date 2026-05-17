-- Add `idempotency_key` to dlq_replay_audit (H-31).
-- Postgres variant. See migrations/status/sqlite/0002_idempotency_key.sql
-- for the design rationale.

ALTER TABLE dlq_replay_audit
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT '';

-- Backfill so the UNIQUE index below has nothing to reject. gen_random_uuid
-- requires pgcrypto in older Postgres versions; md5(random()::text) is
-- universally available and gives us a per-row salt.
UPDATE dlq_replay_audit
SET idempotency_key = 'legacy-' || id || '-' || md5(random()::text)
WHERE idempotency_key = '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_replay_audit_idempotency_key
    ON dlq_replay_audit(idempotency_key);
