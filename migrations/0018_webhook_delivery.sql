-- P-24: outbound HTTP steps + delivery health (ADR-014 AU-4).
-- An http_request step ENQUEUES a row here inside the run's own tx (atomic
-- with the run) — it never dials. The delivery lane drains the queue later, so
-- a slow or hostile endpoint can never stall the org's event cursor. Every
-- send rides the P-15 SSRF-guarded egress client; a guard rejection is a
-- terminal delivery failure (the destination will never become allowed).
CREATE TABLE webhook_delivery (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    automation_id BIGINT NOT NULL REFERENCES automation (id),
    run_id        BIGINT NOT NULL REFERENCES automation_run (id),
    url           TEXT NOT NULL,
    payload       JSONB NOT NULL,
    -- 1 pending · 2 delivered · 3 failed (terminal)
    status        SMALLINT NOT NULL DEFAULT 1,
    attempts      INT NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INT,
    last_error       TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ
);
-- The delivery lane's claim predicate verbatim: only pending rows whose fire
-- time has arrived are ever due, so the partial index keeps the scan to those.
CREATE INDEX webhook_delivery_due_idx ON webhook_delivery (next_attempt_at) WHERE status = 1;
-- The per-rule deliveries dashboard read (newest first), mirroring
-- automation_run_list_idx.
CREATE INDEX webhook_delivery_rule_idx ON webhook_delivery (automation_id, id DESC);

-- Consecutive terminal delivery failures — an O(1) health counter (AU-4
-- alert-before-auto-disable). Reset to 0 on any delivered row and on a
-- self-serve re-enable; alerts fire at 5 and 15, auto-disable at 20.
ALTER TABLE automation ADD COLUMN delivery_failures INT NOT NULL DEFAULT 0;
