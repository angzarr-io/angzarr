-- C-15: harden `delete_edition_events` to reject the FULL main-timeline
-- sentinel set, not just NULL/empty.
--
-- Migration 0007 introduced the proc and rejected `p_edition IS NULL` and
-- `p_edition = ''`. But the codebase documents TWO main-timeline sentinels
-- (`""` and `"angzarr"`, per `is_main_timeline` in `src/storage/helpers/`).
-- A caller passing `"angzarr"` bypassed the guard and the proc silently
-- deleted zero rows (because migration 0007 had already normalized those
-- rows to NULL). That's a confusing no-op AND a contract violation: the
-- documented "main timeline ('angzarr' or empty edition) protection" must
-- raise for BOTH forms.
--
-- This migration replaces the proc with the broader sentinel check. Idempotent
-- (`CREATE OR REPLACE`) — re-runnable; no data touched.
--
-- The Rust event_store.rs adds a parallel client-side guard for the same
-- reason (defense in depth + faster, dialect-free error message).

CREATE OR REPLACE FUNCTION delete_edition_events(
    p_edition TEXT,
    p_domain TEXT
) RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    -- Main-timeline sentinels: NULL, empty, or the literal "angzarr".
    -- All three are equivalent representations of the protected timeline.
    IF p_edition IS NULL OR p_edition = '' OR p_edition = 'angzarr' THEN
        RAISE EXCEPTION 'Cannot delete main timeline events (edition=%, sentinel-protected)', COALESCE(p_edition, '<NULL>');
    END IF;
    DELETE FROM events WHERE edition = p_edition AND domain = p_domain;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
