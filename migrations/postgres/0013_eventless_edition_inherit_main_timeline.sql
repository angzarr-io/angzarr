-- Finding #12 (LOCKED: inherit main timeline): fix the eventless-edition
-- divergence math in the composite-read stored procedures.
--
-- Prior versions (migrations 0002/0007) computed the divergence point as
-- `COALESCE(p_explicit_divergence, MIN(edition.sequence), 0)`. For a named
-- edition with NO events of its own and no explicit divergence, `MIN()` is
-- NULL, so the point collapsed to the literal integer `0`; the main-timeline
-- filter `e.sequence < 0` was then never true and the procedure returned ZERO
-- rows. Every other backend (SQLite, ImmuDB, mock) treats "no edition events,
-- no explicit divergence" as "not diverged yet — inherit the whole main
-- timeline". This migration makes Postgres match.
--
-- The fix drops the `, 0` fallback so the divergence point is NULL in the
-- eventless case, and rewrites the main-timeline filter to treat a NULL
-- divergence point as "no upper bound" (inherit all main-timeline events):
--   `(divergence IS NULL OR e.sequence < divergence)`.
--
-- NOTE: as of the finding #28 consolidation, `PostgresEventStore`'s read path
-- no longer calls these procedures — composite reads run through the shared
-- Rust divergence/merge logic (`storage::sql::event_store`). This migration
-- keeps the in-database procedures correct as defense-in-depth for any direct
-- SQL caller, so the two paths agree on the inherit-main-timeline contract.

CREATE OR REPLACE FUNCTION get_edition_events(
    p_domain TEXT,
    p_edition TEXT,
    p_root TEXT,
    p_explicit_divergence INT DEFAULT NULL
) RETURNS TABLE (
    domain TEXT,
    edition TEXT,
    root TEXT,
    sequence INT,
    created_at TEXT,
    event_data BYTEA,
    correlation_id TEXT
) AS $$
BEGIN
    IF p_edition IS NULL OR p_edition = '' THEN
        RETURN QUERY
            SELECT e.domain, e.edition, e.root, e.sequence, e.created_at, e.event_data, e.correlation_id
            FROM events e
            WHERE e.domain = p_domain AND e.edition IS NULL AND e.root = p_root
            ORDER BY e.sequence ASC;
        RETURN;
    END IF;

    RETURN QUERY
    WITH edition_events AS (
        SELECT e.domain, e.edition, e.root, e.sequence, e.created_at, e.event_data, e.correlation_id
        FROM events e
        WHERE e.domain = p_domain AND e.edition = p_edition AND e.root = p_root
    ),
    divergence AS (
        -- #12: NO `, 0` fallback. NULL here means "eventless edition, no
        -- explicit divergence" → inherit the entire main timeline below.
        SELECT COALESCE(p_explicit_divergence, MIN(ee.sequence)) as seq
        FROM edition_events ee
    )
    SELECT e.domain, e.edition, e.root, e.sequence, e.created_at, e.event_data, e.correlation_id
    FROM events e
    WHERE e.domain = p_domain AND e.edition IS NULL AND e.root = p_root
      AND ((SELECT d.seq FROM divergence d) IS NULL
           OR e.sequence < (SELECT d.seq FROM divergence d))
    UNION ALL
    SELECT ee.domain, ee.edition, ee.root, ee.sequence, ee.created_at, ee.event_data, ee.correlation_id
    FROM edition_events ee
    ORDER BY sequence ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_edition_events_from(
    p_domain TEXT,
    p_edition TEXT,
    p_root TEXT,
    p_from_seq INT,
    p_explicit_divergence INT DEFAULT NULL
) RETURNS TABLE (
    domain TEXT,
    edition TEXT,
    root TEXT,
    sequence INT,
    created_at TEXT,
    event_data BYTEA,
    correlation_id TEXT
) AS $$
BEGIN
    IF p_edition IS NULL OR p_edition = '' THEN
        RETURN QUERY
            SELECT e.domain, e.edition, e.root, e.sequence, e.created_at, e.event_data, e.correlation_id
            FROM events e
            WHERE e.domain = p_domain AND e.edition IS NULL AND e.root = p_root
              AND e.sequence >= p_from_seq
            ORDER BY e.sequence ASC;
        RETURN;
    END IF;

    RETURN QUERY
    WITH edition_events AS (
        SELECT e.domain, e.edition, e.root, e.sequence, e.created_at, e.event_data, e.correlation_id
        FROM events e
        WHERE e.domain = p_domain AND e.edition = p_edition AND e.root = p_root
    ),
    divergence AS (
        -- #12: NULL (not 0) for the eventless-edition case.
        SELECT COALESCE(p_explicit_divergence, MIN(ee.sequence)) as seq
        FROM edition_events ee
    )
    SELECT e.domain, e.edition, e.root, e.sequence, e.created_at, e.event_data, e.correlation_id
    FROM events e
    WHERE e.domain = p_domain AND e.edition IS NULL AND e.root = p_root
      AND ((SELECT d.seq FROM divergence d) IS NULL
           OR e.sequence < (SELECT d.seq FROM divergence d))
      AND e.sequence >= p_from_seq
    UNION ALL
    SELECT ee.domain, ee.edition, ee.root, ee.sequence, ee.created_at, ee.event_data, ee.correlation_id
    FROM edition_events ee
    WHERE ee.sequence >= p_from_seq
    ORDER BY sequence ASC;
END;
$$ LANGUAGE plpgsql;
