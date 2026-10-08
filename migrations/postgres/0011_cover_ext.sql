-- Persist the parent-aggregate routing cover (`Cover.ext`, a packed
-- google.protobuf.Any) so it survives a storage round-trip. Before this
-- column the framework stamped `ext` onto every emitted book but no backend
-- stored it, so `get_by_correlation` reconstructed `ext = None` — silently
-- dropping the parent-routing metadata.
--
-- Nullable BYTEA: only writes that carry a parent cover populate it; all pages
-- of a write share the same value (mirrors the `correlation_id` per-row model).
ALTER TABLE events ADD COLUMN ext BYTEA;
