-- Containers: channels, DM spaces, work Spaces, sharing, per-user channel state.
-- Design: ADR-008 (taxonomy, lifecycle, aliases), ADR-004/005 (SharedChannel),
-- F-16b (join-date column), F-22 (alias reservation), OQ-C8 (0..N binding).

CREATE TABLE channel_folder (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    workspace_id BIGINT REFERENCES workspace (id),
    name         TEXT NOT NULL,
    position     INT NOT NULL DEFAULT 0
);

CREATE TABLE channel (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    workspace_id  BIGINT REFERENCES workspace (id), -- NULL = org-level channel (H3)
    name          TEXT NOT NULL,
    -- 1 text · 2 forum · 3 voice · 4 announcement (ADR-008 C-1)
    kind          SMALLINT NOT NULL DEFAULT 1,
    -- 1 public · 2 private · 3 web_public · 4 shared (C-2)
    visibility    SMALLINT NOT NULL DEFAULT 1,
    -- 1 shared · 2 protected (C-2; enforcement via channel_member.history_from)
    history_mode  SMALLINT NOT NULL DEFAULT 1,
    description   TEXT NOT NULL DEFAULT '',
    topic         TEXT NOT NULL DEFAULT '',
    creator_id    BIGINT REFERENCES user_account (id),
    profile_id    BIGINT REFERENCES permission_profile (id),
    folder_id     BIGINT REFERENCES channel_folder (id),
    root_thread_id BIGINT, -- the channel-root thread (F-15); FK added in 0004
    settings      JSONB NOT NULL DEFAULT '{}', -- topics_policy etc. (M-2)
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at   TIMESTAMPTZ
);
-- Names unique among live channels, per workspace (org-level pool = 0).
CREATE UNIQUE INDEX channel_name_key
    ON channel (org_id, COALESCE(workspace_id, 0), lower(name))
    WHERE archived_at IS NULL;
CREATE UNIQUE INDEX channel_origin_key
    ON channel (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;

-- F-22: renamed-away names stay reserved so old links never break; release is
-- an explicit, audited admin action (row delete).
CREATE TABLE channel_name_alias (
    org_id       BIGINT NOT NULL REFERENCES org (id),
    workspace_id BIGINT,
    name         TEXT NOT NULL,
    channel_id   BIGINT NOT NULL REFERENCES channel (id),
    renamed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, name)
);

CREATE TABLE dm_space (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id     BIGINT NOT NULL REFERENCES org (id),
    -- 1 one-to-one · 2 group · 3 self (ADR-007 M-7)
    kind       SMALLINT NOT NULL,
    -- Canonical sorted participant-id key; makes 1:1/self spaces unique and
    -- group-DM lookup O(1) without scanning participants.
    dm_key     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, dm_key)
);

CREATE TABLE dm_participant (
    dm_space_id BIGINT NOT NULL REFERENCES dm_space (id),
    user_id     BIGINT NOT NULL REFERENCES user_account (id),
    PRIMARY KEY (dm_space_id, user_id)
);
CREATE INDEX dm_participant_user_idx ON dm_participant (user_id);

CREATE TABLE space (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    workspace_id  BIGINT NOT NULL REFERENCES workspace (id),
    key           TEXT NOT NULL CHECK (key ~ '^[A-Z][A-Z0-9]{0,9}$'),
    name          TEXT NOT NULL,
    lead_user_id  BIGINT REFERENCES user_account (id),
    profile_id    BIGINT REFERENCES permission_profile (id),
    status_set_id BIGINT, -- FK added in 0005
    -- 1 story-points · 2 time (ADR-009 W-5; per-View override = OQ-W1)
    estimation_mode SMALLINT NOT NULL DEFAULT 1,
    settings      JSONB NOT NULL DEFAULT '{}',
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    archived_at   TIMESTAMPTZ,
    trashed_at    TIMESTAMPTZ, -- soft-delete with restore window (C-3)
    UNIQUE (org_id, key)      -- keys never reused (C-3)
);

CREATE TABLE space_key_alias (
    org_id   BIGINT NOT NULL REFERENCES org (id),
    key      TEXT NOT NULL,
    space_id BIGINT NOT NULL REFERENCES space (id),
    PRIMARY KEY (org_id, key)
);

-- OQ-C8: a Space may bind 0..N discussion channels.
CREATE TABLE channel_space_binding (
    channel_id BIGINT NOT NULL REFERENCES channel (id),
    space_id   BIGINT NOT NULL REFERENCES space (id),
    PRIMARY KEY (channel_id, space_id)
);

-- ADR-004/005: one entity, two trust scopes (intra-org grant / cross-instance
-- projection). peer_kind: 1 intra-org workspace · 2 same-instance org (F-3:
-- in-process T2 projection) · 3 remote instance.
CREATE TABLE shared_channel (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_id    BIGINT NOT NULL REFERENCES channel (id),
    host_org_id   BIGINT NOT NULL REFERENCES org (id),
    peer_kind     SMALLINT NOT NULL,
    peer_workspace_id BIGINT REFERENCES workspace (id),
    peer_org_id   BIGINT REFERENCES org (id),
    peer_instance TEXT,
    -- join policy, file sharing, membership sync, governing retention/hold
    -- authority (F-3: host governs by default, declared at share time).
    policy        JSONB NOT NULL DEFAULT '{}',
    created_by    BIGINT NOT NULL REFERENCES user_account (id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at    TIMESTAMPTZ
);
CREATE INDEX shared_channel_channel_idx ON shared_channel (channel_id);

CREATE TABLE default_channel (
    workspace_id BIGINT NOT NULL REFERENCES workspace (id),
    channel_id   BIGINT NOT NULL REFERENCES channel (id),
    bundle       TEXT, -- DefaultChannelGroup name (C-3), NULL = always
    PRIMARY KEY (workspace_id, channel_id)
);

-- Per-user per-channel state (ADR-008 C-4). HOT on the notification fan-out
-- path (ADR-011 N-1 step 2/3) — tri-states are real columns, not JSONB.
-- Row survives unsubscribe (unsubscribed_at) so history_from is never lost.
CREATE TABLE channel_member (
    channel_id    BIGINT NOT NULL REFERENCES channel (id),
    user_id       BIGINT NOT NULL REFERENCES user_account (id),
    joined_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- F-16b: protected-history floor; NULL = full history visible.
    history_from  TIMESTAMPTZ,
    -- 0 inherit · 1 all · 2 mentions · 3 nothing (N-1 step 2)
    level         SMALLINT NOT NULL DEFAULT 0,
    -- Mute is a SEPARATE flag, not a level (N-1 step 3, verified Slack).
    muted         BOOLEAN NOT NULL DEFAULT false,
    pinned        BOOLEAN NOT NULL DEFAULT false,
    color         TEXT,
    -- Per-medium tri-states: -1 off · 0 inherit · 1 on (C-4 quartet).
    notif_desktop SMALLINT NOT NULL DEFAULT 0,
    notif_push    SMALLINT NOT NULL DEFAULT 0,
    notif_email   SMALLINT NOT NULL DEFAULT 0,
    notif_sound   SMALLINT NOT NULL DEFAULT 0,
    wildcard_mentions_notify SMALLINT NOT NULL DEFAULT 0,
    unsubscribed_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, user_id)
) WITH (fillfactor = 90);
CREATE INDEX channel_member_user_idx
    ON channel_member (user_id) WHERE unsubscribed_at IS NULL;

CREATE TABLE sidebar_section (
    id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id  BIGINT NOT NULL REFERENCES user_account (id),
    name     TEXT NOT NULL,
    position INT NOT NULL DEFAULT 0
);

CREATE TABLE sidebar_section_channel (
    section_id BIGINT NOT NULL REFERENCES sidebar_section (id),
    channel_id BIGINT NOT NULL REFERENCES channel (id),
    position   INT NOT NULL DEFAULT 0,
    PRIMARY KEY (section_id, channel_id)
);
