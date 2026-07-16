-- P-30: OIDC login. Three tables carry the whole flow, each rooted in server
-- state the founding "invite is the authorization" model needs:
--   * auth_provider — per-org IdP config (manage_org CRUD). client_secret is
--     write-only at the API (GET returns has_secret only); enabling requires a
--     live discovery probe so a typo'd issuer can never strand logins.
--   * external_identity — the durable (provider, subject) → user_account link.
--     A first login links by a VERIFIED email to exactly one live human (the
--     user_account_email_key unique index makes "exactly one" structural);
--     every later login rides the link. NO JIT: an unknown identity is refused,
--     never provisioned.
--   * oidc_flow — the in-flight authorization-code state, single-use like
--     password_reset: only sha256(state) is stored, `used_at IS NULL` is the
--     replay guard (claimed in one tx), the PKCE verifier + nonce bind the
--     callback to its start, and rows past the 10-min TTL are dead (a sweep
--     lane is a recorded gap — like password_reset, v1 keeps no janitor).
CREATE TABLE auth_provider (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    name          TEXT NOT NULL, -- url-safe slug, unique per org
    issuer        TEXT NOT NULL,
    client_id     TEXT NOT NULL,
    client_secret TEXT NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
);

CREATE TABLE external_identity (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    user_id       BIGINT NOT NULL REFERENCES user_account (id),
    provider_id   BIGINT NOT NULL REFERENCES auth_provider (id),
    subject       TEXT NOT NULL,
    email_at_link TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider_id, subject)
);

CREATE TABLE oidc_flow (
    state_hash    TEXT PRIMARY KEY,
    provider_id   BIGINT NOT NULL REFERENCES auth_provider (id),
    pkce_verifier TEXT NOT NULL,
    nonce         TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_at       TIMESTAMPTZ
);
