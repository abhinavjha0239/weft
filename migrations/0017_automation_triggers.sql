-- P-23: schedule, inbound-webhook, and slash trigger kinds for automations.
-- The trigger KIND lives inside the JSONB definition (absent = "event", so
-- every rule stored before P-23 stays valid); these two columns hold the
-- derived state the runner needs OUTSIDE the document.
ALTER TABLE automation
    ADD COLUMN schedule_next_at TIMESTAMPTZ,  -- next fire for a schedule rule
    ADD COLUMN webhook_token    TEXT;         -- capability token for a webhook rule

-- The scheduler lane's claim predicate, verbatim: only enabled, live schedule
-- rules with a computed fire time are ever due, so the partial index keeps the
-- scan to exactly those rows.
CREATE INDEX automation_schedule_due_idx ON automation (schedule_next_at)
    WHERE schedule_next_at IS NOT NULL AND enabled AND deleted_at IS NULL;
