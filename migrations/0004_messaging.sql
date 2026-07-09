-- Messaging: threads, messages, revisions, reactions, per-user message state.
-- Design: ADR-007 (AST, thread model, flags), ADR-001 D1/D2 (one Thread
-- primitive), F-5 (single governing container), F-7 (watermark + sparse
-- exceptions), F-8 (delete = revision-append), F-15 (channel-root semantics).

CREATE TABLE thread (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    -- F-5: exactly ONE governing container. Channel threads (incl. DM threads
    -- via dm_space) or Space threads (WorkItem discussions created directly).
    channel_id   BIGINT REFERENCES channel (id),
    dm_space_id  BIGINT REFERENCES dm_space (id),
    space_id     BIGINT REFERENCES space (id),
    -- 1 normal · 2 channel_root (F-15: roots can't be resolved/moved/titled/
    -- followed; excluded from lists/counts; no denormalized counters).
    kind         SMALLINT NOT NULL DEFAULT 1,
    title        TEXT, -- Zulip topic = titled thread (M-2); NULL = untitled
    root_message_id BIGINT,
    resolved_at  TIMESTAMPTZ,
    resolved_by  BIGINT REFERENCES user_account (id),
    -- Denormalized list-ordering state; NOT maintained for kind=2 (F-15).
    last_activity_at TIMESTAMPTZ,
    message_count INT NOT NULL DEFAULT 0,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (num_nonnulls(channel_id, dm_space_id, space_id) = 1)
);
CREATE INDEX thread_channel_activity_idx
    ON thread (channel_id, last_activity_at DESC) WHERE kind = 1;
CREATE UNIQUE INDEX thread_origin_key
    ON thread (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;

ALTER TABLE channel
    ADD CONSTRAINT channel_root_thread_fk
    FOREIGN KEY (root_thread_id) REFERENCES thread (id);

CREATE TABLE message (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      BIGINT NOT NULL REFERENCES org (id),
    thread_id   BIGINT NOT NULL REFERENCES thread (id),
    -- Denormalized from thread for fetch/search locality; thread is truth.
    channel_id  BIGINT,
    dm_space_id BIGINT,
    author_id   BIGINT NOT NULL REFERENCES user_account (id),
    -- ADR-007 M-1: raw source + canonical AST + cached render.
    source         TEXT NOT NULL,
    ast            JSONB NOT NULL,
    rendered       TEXT NOT NULL,
    render_version SMALLINT NOT NULL,
    search_tsv     TSVECTOR, -- maintained by the writer (ADR-010 S-3)
    -- Denormalized has-flags for index-cheap search predicates (S-3).
    has_attachment BOOLEAN NOT NULL DEFAULT false,
    has_link       BOOLEAN NOT NULL DEFAULT false,
    has_image      BOOLEAN NOT NULL DEFAULT false,
    forwarded_from_message_id BIGINT,
    edited_at   TIMESTAMPTZ,
    -- F-8 tombstone: live fields cleared on delete; prior content lives in the
    -- final message_revision row (survives log compaction, purgeable by scrub).
    deleted_at  TIMESTAMPTZ,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    -- Domain time: imports backdate (a 2019 Slack message sorts as 2019).
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX message_thread_idx ON message (thread_id, id);
CREATE INDEX message_channel_idx ON message (channel_id, id) WHERE channel_id IS NOT NULL;
CREATE INDEX message_dm_idx ON message (dm_space_id, id) WHERE dm_space_id IS NOT NULL;
CREATE INDEX message_search_idx ON message USING gin (search_tsv);
CREATE UNIQUE INDEX message_origin_key
    ON message (org_id, origin_system, origin_id) WHERE origin_system IS NOT NULL;

-- Unified edit history (M-3) + deletion capture (F-8).
-- kind: 1 content · 2 title/topic · 3 move · 4 delete.
CREATE TABLE message_revision (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id  BIGINT NOT NULL REFERENCES message (id),
    revision_no SMALLINT NOT NULL,
    kind        SMALLINT NOT NULL,
    prev_source TEXT,
    prev_ast    JSONB,
    prev_thread_id BIGINT,
    edited_by   BIGINT NOT NULL REFERENCES user_account (id),
    edited_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id, revision_no)
);

CREATE TABLE reaction (
    message_id BIGINT NOT NULL REFERENCES message (id),
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    -- unicode emoji or custom-emoji name; kind: 1 emoji · 2 vote (Jira import)
    emoji      TEXT NOT NULL,
    kind       SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, user_id, emoji)
);

CREATE TABLE custom_emoji (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id    BIGINT NOT NULL REFERENCES org (id),
    name      TEXT NOT NULL,
    file_id   BIGINT, -- FK added in 0006
    author_id BIGINT REFERENCES user_account (id),
    deactivated_at TIMESTAMPTZ,
    UNIQUE (org_id, name)
);

CREATE TABLE pin (
    channel_id BIGINT NOT NULL REFERENCES channel (id),
    message_id BIGINT NOT NULL REFERENCES message (id),
    pinned_by  BIGINT NOT NULL REFERENCES user_account (id),
    pinned_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, message_id)
);

-- kind: 1 saved-for-later · 2 star (M-6). Message-scoped in M1; work items
-- reference their thread's messages so one shape covers both.
CREATE TABLE saved_item (
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    message_id BIGINT NOT NULL REFERENCES message (id),
    kind       SMALLINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, message_id)
);

CREATE TABLE draft (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES user_account (id),
    channel_id  BIGINT REFERENCES channel (id),
    thread_id   BIGINT REFERENCES thread (id),
    dm_space_id BIGINT REFERENCES dm_space (id),
    source      TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX draft_user_idx ON draft (user_id);

CREATE TABLE scheduled_message (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id        BIGINT NOT NULL REFERENCES org (id),
    author_id     BIGINT NOT NULL REFERENCES user_account (id),
    channel_id    BIGINT REFERENCES channel (id),
    thread_id     BIGINT REFERENCES thread (id),
    dm_space_id   BIGINT REFERENCES dm_space (id),
    source        TEXT NOT NULL,
    scheduled_for TIMESTAMPTZ NOT NULL,
    sent_message_id BIGINT REFERENCES message (id),
    failed_reason TEXT
);
CREATE INDEX scheduled_message_due_idx
    ON scheduled_message (scheduled_for) WHERE sent_message_id IS NULL AND failed_reason IS NULL;

CREATE TABLE reminder (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    message_id BIGINT REFERENCES message (id),
    note       TEXT NOT NULL DEFAULT '',
    remind_at  TIMESTAMPTZ NOT NULL,
    fired_at   TIMESTAMPTZ
);
CREATE INDEX reminder_due_idx ON reminder (remind_at) WHERE fired_at IS NULL;

-- Per-thread follow/mute state (C-4; UserTopic 4-state — 0 rows = inherit).
-- state: 1 followed · 2 muted · 3 unmuted (un-mutes inside a muted channel).
CREATE TABLE thread_subscription (
    thread_id  BIGINT NOT NULL REFERENCES thread (id),
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    state      SMALLINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (thread_id, user_id)
);
CREATE INDEX thread_subscription_user_idx ON thread_subscription (user_id, state);

-- F-7: the read-state hybrid. A watermark row per (user, thread) — O(threads),
-- never O(messages). The channel-root thread covers the flat-channel case.
CREATE TABLE thread_read_watermark (
    user_id  BIGINT NOT NULL REFERENCES user_account (id),
    thread_id BIGINT NOT NULL REFERENCES thread (id),
    last_read_message_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, thread_id)
) WITH (fillfactor = 85);

-- F-7: sparse EXCEPTIONS only — never a dense per-(user,message) table
-- (Zulip's UserMessage is their documented scaling ceiling).
-- flag: 1 starred-legacy · 2 mentioned · 3 marked-unread · 4 alert-word.
CREATE TABLE message_user_flag (
    user_id    BIGINT NOT NULL REFERENCES user_account (id),
    message_id BIGINT NOT NULL REFERENCES message (id),
    flag       SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, message_id, flag)
);
CREATE INDEX message_user_flag_message_idx ON message_user_flag (message_id);

CREATE TABLE alert_word (
    user_id BIGINT NOT NULL REFERENCES user_account (id),
    word    TEXT NOT NULL,
    PRIMARY KEY (user_id, word)
);
