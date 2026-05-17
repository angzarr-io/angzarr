-- Add `idempotency_key` to dlq_replay_audit (H-31).
--
-- Plan reference: P1.4 follow-up — the two-phase replay protocol
-- INSERTs a pending audit row BEFORE publishing and relies on the
-- UNIQUE constraint here to fence out a concurrent replica that picked
-- up the same operator click. The `outcome` column gains a new
-- 'pending' value (no schema enforcement; the discriminator stays
-- TEXT).
--
-- `DEFAULT ''` keeps the column non-NULL across existing rows; new
-- rows always carry a concrete key. The UNIQUE index lives on
-- `idempotency_key` alone so the two-phase protocol races against ALL
-- in-flight replays, not just those targeting the same dlq_id (a
-- single operator's "replay everything in this batch" click should
-- still serialize per-attempt).
--
-- Existing rows backfill to a synthetic key derived from
-- `id`+`dlq_id`+`replayed_at` so the UNIQUE constraint can be enforced
-- without rejecting historical data. The hex(randomblob(16)) salt
-- guarantees uniqueness across rows that happened to be inserted at
-- the same RFC-3339 second.

ALTER TABLE dlq_replay_audit
    ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';

-- Backfill: every pre-existing row gets a synthetic key so the UNIQUE
-- index below has nothing to reject.
UPDATE dlq_replay_audit
SET idempotency_key = 'legacy-' || id || '-' || lower(hex(randomblob(8)))
WHERE idempotency_key = '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_replay_audit_idempotency_key
    ON dlq_replay_audit(idempotency_key);
