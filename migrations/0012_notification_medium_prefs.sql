-- N-1 step 4 (ADR-011): per-user medium routing. Zero rows = defaults
-- (email on for dm+mention, off for the rest); the in-app row is ALWAYS
-- written — the badge accrues even when delivery is suppressed (N-4).
-- medium: 1 in_app (structural, not toggleable in v1) · 2 email · 3 push
-- (reserved).
CREATE TABLE notification_medium_pref (
    user_id BIGINT NOT NULL REFERENCES user_account (id),
    kind    SMALLINT NOT NULL, -- notification.kind (1 dm … 5 channel_activity)
    medium  SMALLINT NOT NULL,
    enabled BOOLEAN NOT NULL,
    PRIMARY KEY (user_id, kind, medium)
);

-- The offline-email watermark: a notification is emailed at most once.
ALTER TABLE notification ADD COLUMN emailed_at TIMESTAMPTZ;

-- The email worker's scan: unseen, unemailed, old enough.
CREATE INDEX notification_email_due_idx
    ON notification (created_at)
    WHERE seen_at IS NULL AND emailed_at IS NULL;
