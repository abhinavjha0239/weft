-- S4 (P-40): the logical-decoding consumer feed.
--
-- The xmin gate the consumers use today (0001:49-58) is DATABASE-GLOBAL: one
-- long write transaction ANYWHERE holds pg_snapshot_xmin down, so delivery
-- stalls for every org and every consumer. Worse than latency, it is a
-- CORRECTNESS hazard: txid is stamped at a transaction's FIRST write while the
-- event id is stamped at APPEND, so a transaction with the LOWER txid can hold
-- the HIGHER id. When it commits, the gate admits its id and the cursor
-- advances past a lower id whose transaction is still in flight — and that
-- event is then below the cursor forever (F-2 loss, undetectable). Logical
-- decoding removes both: WAL order IS commit order, so no gate is needed.
--
-- This migration is the SCHEMA half only. Creating the replication slot and
-- the publication is a deploy-time OPERATOR step (docs/ARCHITECTURE.md §6
-- runbook), NOT a migration: a slot is per-cell physical state that retains
-- WAL, it must survive a schema reset, and creating one implicitly on every
-- `migrate` would silently start pinning WAL on installs that never asked for
-- the feed. Cite: ADR-003 F-1/E2, internal/eventlog/consumer.go scale contract.

-- The LSN cursor. Nullable BY DESIGN: it is written only by the logical
-- driver, so a cursor that has only ever run under the default xmin poller
-- keeps last_id alone and NULL here (and a NULL lsn means "replay this
-- consumer from the slot's confirmed position" — replay = reset the cursor,
-- ADR-003 E2). last_id stays authoritative for the xmin driver and stays the
-- client-facing `seq`; the LSN is the logical driver's internal position.
ALTER TABLE event_consumer_cursor ADD COLUMN lsn PG_LSN;

COMMENT ON COLUMN event_consumer_cursor.lsn IS
    'S4 logical-feed cursor: the commit LSN through which this consumer has '
    'durably acked. NULL under the xmin poller (last_id is the position there).';

-- NOTIFY coalescing per (transaction, org) — the S4 fold-in.
--
-- Append used to issue its own `SELECT pg_notify(...)` statement per event, so
-- a transaction writing N events paid N extra round trips (and N entries into
-- the backend's pending-notify list, which is deduplicated only at commit).
-- This function collapses that to ONE pg_notify per (transaction, org): a
-- transaction-local GUC remembers the orgs this transaction already signalled,
-- and SET LOCAL semantics discard it at commit or rollback, so the next
-- transaction starts clean.
--
-- It lives in SQL rather than Go because the check and the mark must be
-- strictly ordered — expression-level evaluation order is not guaranteed —
-- and because folding it into Append's RETURNING clause removes the extra
-- round trip entirely. It returns whether it actually signalled, which is what
-- makes the coalescing observable (and testable) instead of a silent claim.
--
-- With the logical feed the payload is a WAKE HINT only: the reader is pushed
-- by the WAL, and NOTIFY just tells a poller-driven dispatcher which org to
-- visit. It stays the ONLY wake mechanism for the default xmin driver.
CREATE FUNCTION event_log_wake(p_channel TEXT, p_org BIGINT) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE
    -- A placeholder GUC reads back as '' (not NULL) once the session has
    -- touched it, so normalise both to the empty-list sentinel ','.
    seen TEXT := coalesce(nullif(current_setting('eventlog.notified', true), ''), ',');
    tag  TEXT := p_org::text || ',';
BEGIN
    IF position(',' || tag IN seen) > 0 THEN
        RETURN false;
    END IF;
    PERFORM set_config('eventlog.notified', seen || tag, true);
    PERFORM pg_notify(p_channel, p_org::text);
    RETURN true;
END;
$$;
