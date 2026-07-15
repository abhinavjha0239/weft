-- P-35: password reset via emailed single-use token. DB rows, NOT a stateless
-- MAC — the two invariants a MAC cannot carry both need server state:
--   * single-use: the confirm claims the row (used_at) so a replayed token
--     fails, and
--   * revoke-on-change: a password change (auth.ChangePassword) or a completed
--     reset DELETEs the user's outstanding rows, voiding in-flight mail.
-- The token is shown once (in the reset email); only its sha256 hash is stored
-- (the auth_session.token_hash precedent). Rows are short-lived (1h TTL) and
-- reclaimed on the next reset/change for the user — no janitor needed at v1.
CREATE TABLE password_reset (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ
);

-- Both by-user scans ride this index: the request-time throttle probe (count of
-- unused unexpired tokens) and the confirm/change-time cleanup (DELETE the
-- user's other outstanding rows).
CREATE INDEX password_reset_user_idx ON password_reset (user_id);
