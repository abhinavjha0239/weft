-- Files, presence/notifications, automations/agents, compliance.
-- Design: ADR-012 (one File, references, monotone flags per F-12), ADR-011
-- (presence≠status, VIP/DND, schemes), ADR-014 (Automation/Run/grants, F-13),
-- ADR-013 (retention, legal hold, export — compliance_officer-gated per F-9).

CREATE TABLE file (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    -- 1 uploaded (managed blob) · 2 remote (external reference; delete never
    -- touches origin) — ADR-012 F-3.
    kind         SMALLINT NOT NULL DEFAULT 1,
    name         TEXT NOT NULL,
    mime         TEXT NOT NULL DEFAULT 'application/octet-stream',
    size_bytes   BIGINT NOT NULL DEFAULT 0,
    sha256       BYTEA,          -- per-Org dedup (OQ-F10)
    storage_key  TEXT,           -- backend-relative; NULL for kind=remote
    remote_url   TEXT,
    -- F-12: ONLY the monotone visibility tiers are cached as flags; all
    -- viewer-dependent access is evaluated per (viewer, reference) at query
    -- time. Access URLs are expiring signed URLs.
    is_org_public BOOLEAN NOT NULL DEFAULT false,
    is_web_public BOOLEAN NOT NULL DEFAULT false,
    -- 0 pending · 1 clean · 2 quarantined (F-7 upload pipeline hook)
    scan_status  SMALLINT NOT NULL DEFAULT 0,
    uploader_id  BIGINT REFERENCES user_account (id),
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at   TIMESTAMPTZ,
    CHECK (kind <> 1 OR storage_key IS NOT NULL),
    CHECK (kind <> 2 OR remote_url IS NOT NULL)
);
CREATE INDEX file_dedup_idx ON file (org_id, sha256) WHERE sha256 IS NOT NULL;
CREATE UNIQUE INDEX file_origin_key
    ON file (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;

-- ADR-012 F-1: upload once, reference N times. GC when references hit zero
-- AND retention elapsed AND no legal hold.
-- entity_type: 1 message · 2 work_item · 3 thread · 4 avatar · 5 emoji · 6 export.
CREATE TABLE file_reference (
    file_id     BIGINT NOT NULL REFERENCES file (id),
    entity_type SMALLINT NOT NULL,
    entity_id   BIGINT NOT NULL,
    created_by  BIGINT REFERENCES user_account (id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (file_id, entity_type, entity_id)
);
CREATE INDEX file_reference_entity_idx ON file_reference (entity_type, entity_id);

ALTER TABLE user_account
    ADD CONSTRAINT user_account_avatar_fk FOREIGN KEY (avatar_file_id) REFERENCES file (id);
ALTER TABLE custom_emoji
    ADD CONSTRAINT custom_emoji_file_fk FOREIGN KEY (file_id) REFERENCES file (id);

-- Presence is ephemeral and rebuildable from connections: UNLOGGED — never in
-- the event log (ADR-002 P5), never replicated, cheap heartbeat writes.
CREATE UNLOGGED TABLE presence (
    user_id        BIGINT PRIMARY KEY,
    -- 1 active · 2 idle · 3 offline (derived; invisible mode masks at read)
    status         SMALLINT NOT NULL DEFAULT 3,
    last_active_at TIMESTAMPTZ,
    last_client    SMALLINT
);

-- UserStatus is a SEPARATE durable object (ADR-011 N-3, the verified trap).
CREATE TABLE user_status (
    user_id    BIGINT PRIMARY KEY REFERENCES user_account (id),
    emoji      TEXT,
    status_text TEXT NOT NULL DEFAULT '',
    -- 1 manual · 2 in-call · 3 focus · 4 ooo · 5 after-hours (auto kinds)
    kind       SMALLINT NOT NULL DEFAULT 1,
    expires_at TIMESTAMPTZ
);

CREATE TABLE dnd_setting (
    user_id      BIGINT PRIMARY KEY REFERENCES user_account (id),
    schedule     JSONB NOT NULL DEFAULT '{}', -- per-day quiet hours (N-1 step 5)
    snoozed_until TIMESTAMPTZ
);

-- N-2: VIP list pierces DND. CC-3: an agent may appear here only via explicit
-- per-user opt-in — agents never pierce DND by default.
CREATE TABLE priority_contact (
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    contact_id BIGINT NOT NULL REFERENCES user_account (id),
    PRIMARY KEY (user_id, contact_id)
);

-- N-2: DM sender's one "notify anyway" breakthrough per recipient per day.
CREATE TABLE dm_breakthrough (
    sender_id    BIGINT NOT NULL REFERENCES user_account (id),
    recipient_id BIGINT NOT NULL REFERENCES user_account (id),
    used_on      DATE NOT NULL,
    PRIMARY KEY (sender_id, recipient_id, used_on)
);

-- In-app bell feed; badge accrues even when delivery is suppressed (N-4).
CREATE TABLE notification (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      BIGINT NOT NULL REFERENCES org (id),
    user_id     BIGINT NOT NULL REFERENCES user_account (id),
    -- reason class from N-1 step 1 (dm/mention/followed/keyword/item-event/...)
    kind        SMALLINT NOT NULL,
    entity_type SMALLINT NOT NULL,
    entity_id   BIGINT NOT NULL,
    actor_id    BIGINT REFERENCES user_account (id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    seen_at     TIMESTAMPTZ,
    read_at     TIMESTAMPTZ
);
CREATE INDEX notification_unseen_idx
    ON notification (user_id, id DESC) WHERE seen_at IS NULL;

-- N-5: event × recipient-resolver matrix, resolved at send time. v1 ships the
-- fixed team-managed scheme; the editable matrix arrives per MILESTONES.md.
CREATE TABLE notification_scheme (
    space_id BIGINT PRIMARY KEY REFERENCES space (id),
    matrix   JSONB NOT NULL
);

-- ADR-014 AU-2: owned by the SCOPE, not a user; definition is the canonical
-- automations-as-code document (AU-3).
CREATE TABLE automation (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    -- 1 org · 2 workspace · 3 channel · 4 space (narrow scope = uncounted)
    scope_type SMALLINT NOT NULL,
    scope_id   BIGINT NOT NULL,
    name       TEXT NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT false,
    definition JSONB NOT NULL, -- trigger → conditions → steps
    version    INT NOT NULL DEFAULT 1,
    -- F-13: NULL = runs as the scope's automation identity. A human actor
    -- requires that human's consent; ANY definition edit clears consent
    -- (enforced in the domain layer: edit bumps version, nulls consent_at).
    actor_user_id    BIGINT REFERENCES user_account (id),
    actor_consent_at TIMESTAMPTZ,
    -- Loop guard is opt-in (CC-decision: Jira's flag, inverted to safe).
    allow_rule_trigger BOOLEAN NOT NULL DEFAULT false,
    budget     JSONB NOT NULL DEFAULT '{}', -- token/cost caps (AU-5)
    created_by BIGINT NOT NULL REFERENCES user_account (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);
CREATE INDEX automation_scope_idx
    ON automation (scope_type, scope_id) WHERE deleted_at IS NULL;

CREATE TABLE automation_run (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    automation_id BIGINT NOT NULL REFERENCES automation (id),
    -- AU-4 idempotency: one run per (automation, triggering event) — retries
    -- can never double-fire. No FK: the event row may live in a dropped
    -- partition long after the run record is kept for audit.
    trigger_event_id BIGINT,
    -- 1 running · 2 success · 3 no-action · 4 partial-error · 5 failed ·
    -- 6 throttled · 7 aborted · 8 awaiting-approval
    status        SMALLINT NOT NULL DEFAULT 1,
    -- Step traces; purgeable via the F-4 scrub cascade. Visibility = viewer's
    -- ACL ∩ each step's touched scopes (F-13) — enforced at the read layer.
    steps         JSONB NOT NULL DEFAULT '[]',
    tokens_used   INT NOT NULL DEFAULT 0,
    cost_microcents BIGINT NOT NULL DEFAULT 0,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE UNIQUE INDEX automation_run_idempotency_key
    ON automation_run (automation_id, trigger_event_id) WHERE trigger_event_id IS NOT NULL;
CREATE INDEX automation_run_list_idx ON automation_run (automation_id, id DESC);

-- ADR-014 AU-6: an agent is a Member (user_account.kind=2); this row is its
-- config. Trust rung is granted per scope in scope_rungs, earned not toggled.
CREATE TABLE agent_config (
    user_id       BIGINT PRIMARY KEY REFERENCES user_account (id),
    model         TEXT NOT NULL DEFAULT '',   -- via the BYO model gateway
    system_prompt TEXT NOT NULL DEFAULT '',
    memory_scope  JSONB NOT NULL DEFAULT '{}',
    budget        JSONB NOT NULL DEFAULT '{}',
    -- {"<scope_type>:<scope_id>": rung} — rung 4 requires admin opt-in (AU-6)
    scope_rungs   JSONB NOT NULL DEFAULT '{}'
);

-- ADR-013 AD-3: three-level retention = rows at org/workspace/channel-or-space
-- scope; the effective policy is the nearest scope. -1 = forever.
CREATE TABLE retention_policy (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    scope_type SMALLINT NOT NULL, -- 1 org · 2 workspace · 3 channel · 4 space · 5 dm
    scope_id   BIGINT NOT NULL,
    duration_days INT NOT NULL,
    keep_edits BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (org_id, scope_type, scope_id)
);

-- ADR-013 AD-4: first-class hold; freezes matching content against retention
-- AND deletion AND log compaction (CC-2). Creation/release require the
-- compliance_officer verb (F-9) and are audited events.
CREATE TABLE legal_hold (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id            BIGINT NOT NULL REFERENCES org (id),
    name              TEXT NOT NULL,
    custodian_user_id BIGINT REFERENCES user_account (id), -- primary scoping
    channel_id        BIGINT REFERENCES channel (id),
    space_id          BIGINT REFERENCES space (id),
    query             JSONB,
    created_by        BIGINT NOT NULL REFERENCES user_account (id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_by       BIGINT REFERENCES user_account (id),
    released_at       TIMESTAMPTZ
);
CREATE INDEX legal_hold_active_idx ON legal_hold (org_id) WHERE released_at IS NULL;

-- ADR-013 AD-5: scoped export jobs (API-first; UI per MILESTONES.md).
CREATE TABLE export_job (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    requested_by BIGINT NOT NULL REFERENCES user_account (id),
    scope        JSONB NOT NULL, -- users/channels/spaces/date-range/query
    -- 1 pending · 2 running · 3 done · 4 failed
    status       SMALLINT NOT NULL DEFAULT 1,
    result_file_id BIGINT REFERENCES file (id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
