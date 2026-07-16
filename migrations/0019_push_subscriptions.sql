-- P-21: the push medium (ADR-011 N-1 step 4, Web Push RFC 8030/8291/8292).
-- A push_subscription is a browser-registered endpoint URL plus the ECDH
-- public key (p256dh, a 65-byte uncompressed P-256 point) and auth secret
-- (16 bytes) the user agent handed us at registration. The push lane encrypts
-- a who/where payload per subscription and POSTs it through the SSRF-guarded
-- egress client — the endpoint is an attacker-influenceable URL, so it rides
-- the same guard as link previews and outbound webhooks. The medium plumbing
-- (notification_medium_pref medium 3 = push) has existed since 0012; this
-- wakes it. channel_member.notif_push (0003) stays dormant — kind-level prefs
-- only in v1.
CREATE TABLE push_subscription (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    endpoint   TEXT NOT NULL,
    p256dh     BYTEA NOT NULL,
    auth       BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ok_at TIMESTAMPTZ,
    UNIQUE (user_id, endpoint)
);

-- The push watermark: a notification is pushed at most once (the email trade —
-- a crash between mark and send loses a push, never duplicates one; the in-app
-- row stays the system of record).
ALTER TABLE notification ADD COLUMN pushed_at TIMESTAMPTZ;

-- The push worker's scan: unseen, unpushed. Mirrors 0012's email due-index;
-- push has no age delay (it is the immediacy medium), so the lane omits the
-- created_at floor the email lane applies, but the index shape is identical.
CREATE INDEX notification_push_due_idx
    ON notification (created_at)
    WHERE seen_at IS NULL AND pushed_at IS NULL;
