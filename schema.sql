-- KV service spike schema (v1).
--
-- Apply once by hand against a PlanetScale Postgres dev branch before
-- running any of the harnesses. This file is the source of truth for
-- the spike's table layout. When the KV service ships in Phase 1, this
-- schema is the seed for the first real migration in the main repo.
--
-- Design notes:
--
--   * BIGINT unix-millis timestamps instead of TIMESTAMPTZ. Same 8 bytes
--     on disk, no timezone conversion on the wire, trivial to serialize
--     from Go.
--   * metadata is nullable BYTEA. Cloudflare Workers KV-style opaque
--     blob, capped at ~1 KB by the application layer.
--   * expires_at NULL means no expiry. The partial index lets the
--     sweeper scan only rows that can expire.
--   * Primary key order (workspace_id, namespace, key) matches the
--     read path: every query is scoped to a (workspace_id, namespace)
--     tuple.

CREATE TABLE IF NOT EXISTS kv_entries (
  workspace_id TEXT   NOT NULL,
  namespace    TEXT   NOT NULL,
  key          TEXT   NOT NULL,
  value        BYTEA  NOT NULL,
  metadata     BYTEA,
  expires_at   BIGINT,
  updated_at   BIGINT NOT NULL,
  PRIMARY KEY (workspace_id, namespace, key)
);

CREATE INDEX IF NOT EXISTS kv_entries_expires_at_idx
  ON kv_entries (workspace_id, namespace, expires_at)
  WHERE expires_at IS NOT NULL;

-- Publication used by the logical-replication invalidation subscriber.
-- Create this only on the primary. Safe to re-run.
--
--   CREATE PUBLICATION kv_invalidate_pub FOR TABLE kv_entries;
--
-- Subscribers create a replication slot per node, e.g.:
--
--   SELECT pg_create_logical_replication_slot('kv_node_<id>', 'pgoutput');

-- LISTEN/NOTIFY invalidation transport. The trigger fires on every
-- committed change to kv_entries and emits a payload the subscribers
-- parse back into (workspace_id, namespace, key).
--
-- The subscriber side issues LISTEN kv_invalidate. NOTIFY payloads are
-- limited to 8000 bytes by Postgres; the 3-part key is well under that
-- limit for realistic inputs.

CREATE OR REPLACE FUNCTION kv_entries_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  target RECORD;
BEGIN
  IF (TG_OP = 'DELETE') THEN
    target := OLD;
  ELSE
    target := NEW;
  END IF;
  PERFORM pg_notify(
    'kv_invalidate',
    target.workspace_id || '|' || target.namespace || '|' || target.key
  );
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS kv_entries_notify_trg ON kv_entries;
CREATE TRIGGER kv_entries_notify_trg
  AFTER INSERT OR UPDATE OR DELETE ON kv_entries
  FOR EACH ROW EXECUTE FUNCTION kv_entries_notify();
