-- Idle-org skipping for the two hourly reconcile sweeps (post-#114/#117
-- audit). Both swept EVERY built channel / EVERY counter row in EVERY org
-- each window, including orgs with no activity since the previous pass.
-- docs/SCHEMA.md's scale contract is explicit that per-org work must be
-- schedulable at runtime — "idle orgs cost zero" — and at the 100k-org
-- target an hourly full pass over all membership of every built channel is
-- the dominant background cost of the whole system.
--
-- This marker is what makes a skip SAFE. Per (sweep, org) it records the
-- org's event-log high-water mark as of a pass that VERIFIED the org and
-- found nothing to repair. A sweep skips an org only while its current
-- high-water mark still equals that number — "nothing has happened since
-- the last pass that completed cleanly". Two consequences are the whole
-- design:
--
--   * A pass that repaired anything, or that hit an error on any item, does
--     NOT settle. So an org that goes idle WHILE CARRYING DRIFT still gets
--     its repairing pass, and then another verifying pass, before it can
--     ever be skipped. Divergence is never the thing that gets skipped.
--   * A pass does not settle while the maintenance consumer is behind the
--     high-water mark: the increments and membership patches that both
--     caches depend on ride that consumer, so verifying ahead of it would
--     freeze a legitimately-stale cache as "clean".
--
-- The activity signal costs the hot path NOTHING and adds no write
-- anywhere: max(event_log.id) per org rides the existing
-- event_log_org_consume_idx (org_id, id), and the consumer position is a PK
-- read of event_consumer_cursor. A per-message (or per-mark-read) touch to
-- make idleness observable would trade an hourly cost for a per-message
-- cost and defeat the entire point.
--
-- settled_at is the LEASE, not decoration. The activity signal sees only
-- drift whose cause APPENDS AN EVENT, and some writes append none: a channel
-- level change, a thread follow toggle, an alert-word edit (deliverability),
-- and the concurrent first-ever mark-read window (unread counters). Drift
-- entering an ALREADY-settled org through one of those would otherwise wait
-- for that org's next activity — which, for an org that goes quiet, means
-- forever. So a marker older than eventlog.SettleTTL (a day) stops
-- suppressing work and the org is swept regardless of activity. What this
-- table buys is therefore statable in TIME, not in window counts:
--
--     an idle org costs nothing for up to SettleTTL, and EVERY org is fully
--     verified at least once per SettleTTL no matter what it did.
--
-- No free signal for those writes exists (channel_member carries no
-- updated_at, alert_word no timestamp at all), and touching a shared per-org
-- row on every settings write would buy an hour of latency with permanent
-- write contention — the trade the scale contract rejects. Recorded cost of
-- the expiry: orgs that settle in the same window expire in the same window,
-- so a fleet deployed at once pays one old-style full pass per SettleTTL as a
-- cohort; spreading expiry with a per-org offset is the one-line upgrade if
-- that ever matters.
--
-- Cell-safe: one row per (sweep, org), org-pinned like everything else. No
-- cross-org state is introduced.
CREATE TABLE sweep_org_state (
    sweep            TEXT   NOT NULL,
    org_id           BIGINT NOT NULL REFERENCES org (id),
    -- NULL = never verified clean, so the sweep always runs this org.
    settled_event_id BIGINT,
    settled_at       TIMESTAMPTZ,
    PRIMARY KEY (sweep, org_id)
);

-- Both sweeps become per-org loops, so each needs an org-pinned entry point
-- into the set it walks; without these, skipping idle orgs would trade one
-- full scan for one full scan PER ORG.

-- Partial: only BUILT channels are ever reconciled, so this carries one
-- entry per built channel, and channel writes (create/archive/first build)
-- are rare enough to pay for it.
CREATE INDEX channel_org_built_idx ON channel (org_id)
    WHERE deliverability_built_at IS NOT NULL;

-- org_id is IMMUTABLE on a counter row, so this index does NOT cost the HOT
-- counter updates that 0023's fillfactor=85 exists to preserve (HOT is lost
-- only when an INDEXED column changes). Only the INSERT of a row — once per
-- (user, container) lifetime, not per message — pays for it.
CREATE INDEX container_unread_counter_org_idx
    ON container_unread_counter (org_id);
