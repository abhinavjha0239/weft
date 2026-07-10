-- Invite links (ADR-006 P-5 + the missing onboarding lane): a capability
-- token (stored hashed, shown once) that provisions a member or guest.
-- Guests carry their enumerated channels ON the invite — membership scope
-- is decided by the inviter, structurally below admin (role ceiling).
CREATE TABLE invite (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      BIGINT NOT NULL REFERENCES org (id),
    token_hash  TEXT NOT NULL UNIQUE,
    email       TEXT,     -- optional pin: only this address may accept
    -- 40 member · 50 guest — never below (P-5 role ceiling).
    role        SMALLINT NOT NULL DEFAULT 40 CHECK (role IN (40, 50)),
    channel_ids BIGINT[] NOT NULL DEFAULT '{}',
    created_by  BIGINT NOT NULL REFERENCES user_account (id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    max_uses    INT NOT NULL DEFAULT 1,
    used_count  INT NOT NULL DEFAULT 0,
    revoked_at  TIMESTAMPTZ
);
CREATE INDEX invite_org_idx ON invite (org_id) WHERE revoked_at IS NULL;
