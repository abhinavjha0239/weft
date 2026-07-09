-- The event log: the spine every subsystem consumes
-- (design: ADR-003, as amended by red-team findings F-1/F-4/F-11).
--
-- Constraints this schema enforces:
--   1. txid gates consumption (F-1): identity ids are assigned at insert but
--      commit in arbitrary order; a consumer that reads past an uncommitted
--      lower id loses that event forever. Readers must bound scans by the
--      oldest in-flight transaction (see read rule below).
--   2. payload indirection (F-4): payload holds structural deltas plus
--      REFERENCES to content revisions (+ content_hash) — never the only copy
--      of user content. Erasure/retention purge deletes the referenced
--      revisions without touching log ordering or hash-chain validity.
--   3. Partitioned by id range: retention/compaction (ADR-003 E6) is a
--      partition drop, not a DELETE. id correlates with time, so ranges map
--      to time windows. A maintenance job pre-creates partitions; the DEFAULT
--      partition guarantees inserts never fail if the job lags.

CREATE SEQUENCE event_log_id_seq;

CREATE TABLE event_log (
    id            BIGINT NOT NULL DEFAULT nextval('event_log_id_seq'),
    txid          XID8 NOT NULL DEFAULT pg_current_xact_id(),
    org_id        BIGINT NOT NULL,
    -- NULL for org-scoped entities: DMs, org-level channels (CC-OQ1).
    workspace_id  BIGINT,
    actor_kind    SMALLINT NOT NULL, -- 1 human · 2 agent · 3 automation · 4 importer · 5 system
    actor_id      BIGINT,
    entity_type   SMALLINT NOT NULL,
    entity_id     BIGINT NOT NULL,
    verb          TEXT NOT NULL,
    payload       JSONB NOT NULL,
    hint          JSONB,
    -- Domain time; imports backdate this while recorded_at keeps ingest time
    -- (ADR-003 E3 — the import keystone).
    occurred_at   TIMESTAMPTZ NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Provenance, ALWAYS stamped by the receiver, never trusted from the
    -- wire (F-11).
    origin        JSONB,
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE TABLE event_log_p1 PARTITION OF event_log
    FOR VALUES FROM (1) TO (10000000);
CREATE TABLE event_log_default PARTITION OF event_log DEFAULT;

CREATE INDEX event_log_org_consume_idx ON event_log (org_id, id);

-- Consumer read rule (F-1). Every consumer scan must be bounded:
--
--   SELECT * FROM event_log
--   WHERE org_id = $1
--     AND id > $2                                        -- cursor
--     AND txid < pg_snapshot_xmin(pg_current_snapshot()) -- commit-order gate
--   ORDER BY id
--   LIMIT $3;
--
-- Wake-up is LISTEN/NOTIFY with polling as fallback (ADR-003 amendment).

CREATE TABLE event_consumer_cursor (
    consumer   TEXT NOT NULL,
    org_id     BIGINT NOT NULL,
    last_id    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, org_id)
);
