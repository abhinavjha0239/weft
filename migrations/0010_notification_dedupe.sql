-- The notification materializer consumes the event log at-least-once (a
-- crash between insert and cursor-ack replays the batch); this key makes
-- re-processing a no-op. One row per (user, reason, entity) is also the
-- product rule — a second mention in the SAME message must not double-badge.
CREATE UNIQUE INDEX notification_dedupe_key
    ON notification (user_id, kind, entity_type, entity_id);
