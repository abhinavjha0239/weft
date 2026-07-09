-- Identity, org hierarchy, groups, permissions, grants.
-- Design: ADR-005 (Org→Workspace), ADR-006 ((verb,scope)→group, profiles),
-- CC-5 (principal model), CC-6 (grant ∩ permission), F-16a (group closure).
--
-- Conventions used across all migrations (rationale: docs/SCHEMA.md):
--   * BIGINT identity keys; no UUIDs on hot paths.
--   * SMALLINT type codes; the value registry lives in Go (internal/enum).
--   * Provenance = typed columns (origin_system/origin_id/origin_meta) with a
--     partial unique index per ADR-001 D5 — idempotent import upserts.
--   * TIMESTAMPTZ everywhere; soft-lifecycle via *_at nullable columns.
--   * settings JSONB = the policy-knob long tail (ADR-001 D7); anything on a
--     hot query path gets a real column instead.

CREATE TABLE org (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          TEXT NOT NULL,
    slug          TEXT NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
    settings      JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at TIMESTAMPTZ
);

CREATE TABLE workspace (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id          BIGINT NOT NULL REFERENCES org (id),
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL CHECK (slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
    -- 1 hidden · 2 join-on-request · 3 open (ADR-005 H3)
    discoverability SMALLINT NOT NULL DEFAULT 1,
    settings        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at     TIMESTAMPTZ,
    UNIQUE (org_id, slug)
);

CREATE TABLE user_account (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id         BIGINT NOT NULL REFERENCES org (id),
    -- 1 human · 2 agent (covers bots/apps/automation identities, CC-5)
    -- 3 imported_placeholder (ADR-001 D4) · 4 remote (ADR-004 F3)
    kind           SMALLINT NOT NULL DEFAULT 1,
    email          TEXT,
    full_name      TEXT NOT NULL,
    -- Role preset pointer (ADR-006 P-2): 10 owner · 20 admin · 30 moderator
    -- · 40 member · 50 guest. Real permissions resolve through groups.
    role           SMALLINT NOT NULL DEFAULT 40,
    owner_user_id  BIGINT REFERENCES user_account (id), -- agents' owner (AU-6)
    remote_instance TEXT,                               -- kind=remote home (F3)
    avatar_file_id BIGINT,                              -- FK added in 0006
    settings       JSONB NOT NULL DEFAULT '{}',
    origin_system  TEXT,
    origin_id      TEXT,
    origin_meta    JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at TIMESTAMPTZ,
    CHECK (kind <> 4 OR remote_instance IS NOT NULL)
);
CREATE UNIQUE INDEX user_account_email_key
    ON user_account (org_id, lower(email)) WHERE email IS NOT NULL;
CREATE UNIQUE INDEX user_account_origin_key
    ON user_account (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;

CREATE TABLE membership (
    user_id      BIGINT NOT NULL REFERENCES user_account (id),
    workspace_id BIGINT NOT NULL REFERENCES workspace (id),
    role         SMALLINT NOT NULL DEFAULT 40, -- per-workspace role (H2/H4)
    joined_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, workspace_id)
);
CREATE INDEX membership_workspace_idx ON membership (workspace_id);

CREATE TABLE user_group (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    workspace_id BIGINT REFERENCES workspace (id), -- NULL = org-scoped group
    name         TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    is_system    BOOLEAN NOT NULL DEFAULT false,   -- role-preset groups (P-2)
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at TIMESTAMPTZ,
    UNIQUE (org_id, name)
);

CREATE TABLE user_group_member (
    group_id BIGINT NOT NULL REFERENCES user_group (id),
    user_id  BIGINT NOT NULL REFERENCES user_account (id),
    PRIMARY KEY (group_id, user_id)
);
CREATE INDEX user_group_member_user_idx ON user_group_member (user_id);

CREATE TABLE user_group_subgroup (
    group_id    BIGINT NOT NULL REFERENCES user_group (id),
    subgroup_id BIGINT NOT NULL REFERENCES user_group (id),
    PRIMARY KEY (group_id, subgroup_id),
    CHECK (group_id <> subgroup_id)
);

-- F-16a: flattened transitive closure of nested membership, maintained by the
-- domain layer on any group edit. Permission check = one indexed lookup, never
-- a recursive CTE on the hot path.
CREATE TABLE user_group_closure (
    group_id BIGINT NOT NULL REFERENCES user_group (id),
    user_id  BIGINT NOT NULL REFERENCES user_account (id),
    PRIMARY KEY (group_id, user_id)
);
CREATE INDEX user_group_closure_user_idx ON user_group_closure (user_id);

-- ADR-006 P-1: every permission is (verb, scope) → group.
-- scope_type: 1 org · 2 workspace · 3 channel · 4 space · 5 item.
CREATE TABLE permission_assignment (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    verb       TEXT NOT NULL,
    scope_type SMALLINT NOT NULL,
    scope_id   BIGINT NOT NULL,
    group_id   BIGINT NOT NULL REFERENCES user_group (id),
    UNIQUE (org_id, verb, scope_type, scope_id)
);
CREATE INDEX permission_assignment_scope_idx
    ON permission_assignment (scope_type, scope_id);

-- ADR-006 P-3: named reusable verb→group bundles (Jira schemes, generalized).
CREATE TABLE permission_profile (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    name       TEXT NOT NULL,
    is_shared  BOOLEAN NOT NULL DEFAULT true, -- false = team-managed local
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
);

CREATE TABLE permission_profile_entry (
    profile_id BIGINT NOT NULL REFERENCES permission_profile (id),
    verb       TEXT NOT NULL,
    group_id   BIGINT NOT NULL REFERENCES user_group (id),
    PRIMARY KEY (profile_id, verb)
);

-- CC-6 / ADR-014 AU-8: ONE grant primitive for apps, automations, and agents.
-- A grant only narrows what the principal's groups allow — never extends.
CREATE TABLE capability_grant (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    principal_id  BIGINT NOT NULL REFERENCES user_account (id),
    name          TEXT NOT NULL,
    scopes        JSONB NOT NULL, -- {verbs:[...], channels:[...], spaces:[...]}
    token_hash    TEXT UNIQUE,    -- write-only-webhook tier = a preset (AU-8)
    created_by    BIGINT NOT NULL REFERENCES user_account (id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);
CREATE INDEX capability_grant_principal_idx
    ON capability_grant (principal_id) WHERE revoked_at IS NULL;

CREATE TABLE user_credential (
    user_id       BIGINT PRIMARY KEY REFERENCES user_account (id),
    password_hash TEXT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE auth_session (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    token_hash TEXT NOT NULL UNIQUE,
    ip         TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
CREATE INDEX auth_session_user_idx ON auth_session (user_id) WHERE revoked_at IS NULL;
