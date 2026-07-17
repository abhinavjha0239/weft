-- S2 (ADR-006 / red-team F-16): closure versioning + the async rebuild queue.
-- The scale-tier upgrade the perms.go RebuildClosure scale note reserved: a
-- full-org closure rebuild inside the writer's transaction is O(org group
-- graph) — correct at the first tier, but a mega-org group edit becomes a
-- multi-second churn holding locks on the request path. The scale tier splits
-- maintenance in two: hot-path group edits patch the closure INCREMENTALLY
-- (bounded by nesting depth, never membership), and the rare full recompute
-- (bulk import, big restructures) runs ASYNC behind a version fence — the
-- rebuild fills a NEW version, then atomically flips the org's
-- current-version pointer. Readers pin WHERE version = current, so they never
-- see a half-built closure and never block on a rebuild; the hot Require
-- check stays ONE indexed lookup against this exact table.

-- Every closure row carries the version it belongs to. Existing rows become
-- version 0, matching the backfilled pointer below, so a deploy over live
-- data changes nothing until the first rebuild flips to version 1.
ALTER TABLE user_group_closure ADD COLUMN version BIGINT NOT NULL DEFAULT 0;
-- Old and new versions coexist while a rebuild is in flight, so version joins
-- the identity. The (group, user, version) order keeps the hot Require probe
-- a single PK lookup and serves HoldersAt's per-group scan.
ALTER TABLE user_group_closure DROP CONSTRAINT user_group_closure_pkey;
ALTER TABLE user_group_closure ADD PRIMARY KEY (group_id, user_id, version);

-- The per-org fence: which closure version readers see. Every closure WRITER
-- (delta maintenance, full rebuild, rebuild enqueue) row-locks this row
-- first — the org closure lock — which linearizes group-graph writes per org
-- (rare, admin-scale) while readers never lock it: an in-flight rebuild
-- degrades to reads of the old version, never to blocking.
CREATE TABLE closure_current_version (
    org_id  BIGINT PRIMARY KEY REFERENCES org (id),
    version BIGINT NOT NULL DEFAULT 0
);
-- Orgs that predate the fence read the version their existing rows carry.
INSERT INTO closure_current_version (org_id, version) SELECT id, 0 FROM org;

-- The async full-rebuild queue (bulk import, big restructures). Enqueue and
-- the requester's group writes commit atomically; a worker claims a row FOR
-- UPDATE SKIP LOCKED (multi-node safe), rebuilds into a fresh version, and
-- flips the pointer in the SAME transaction as the status update — so a
-- crash re-runs the job (at-least-once) and the rebuild itself is idempotent
-- (each attempt mints a fresh version; the flip is the only visible effect).
-- status: 1 pending · 2 done · 3 failed (error records why).
CREATE TABLE closure_rebuild_job (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    status       SMALLINT NOT NULL DEFAULT 1,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    error        TEXT NOT NULL DEFAULT ''
);
-- One pending job per org: a second enqueue coalesces into it (the org
-- closure lock orders enqueue against an in-flight rebuild, so coalescing
-- never loses writes). Doubles as the claim scan's index.
CREATE UNIQUE INDEX closure_rebuild_job_pending_key
    ON closure_rebuild_job (org_id) WHERE status = 1;
