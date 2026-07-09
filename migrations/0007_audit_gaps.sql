-- Gaps found by the post-merge schema audit against ADR-001's entity list and
-- ADR-007 M-6: channel bookmarks, link rules (linkifiers/playgrounds), and
-- message reporting. Plus the WorkItem-description decision (no DDL): an
-- item's description IS the root message of its thread — one content system,
-- so descriptions get AST/revisions/search for free, and Jira import maps
-- description → root message, comments → replies (see docs/SCHEMA.md).

-- Per-channel pinned links (Slack bookmarks; distinct from personal saved_item).
CREATE TABLE channel_bookmark (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES channel (id),
    title      TEXT NOT NULL,
    url        TEXT NOT NULL,
    emoji      TEXT,
    position   INT NOT NULL DEFAULT 0,
    created_by BIGINT NOT NULL REFERENCES user_account (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX channel_bookmark_channel_idx ON channel_bookmark (channel_id);

-- ADR-001: Zulip linkifiers + code playgrounds → one org-level LinkRule
-- registry. kind: 1 linkifier (pattern → URL template) · 2 playground
-- (language → open-in URL template).
CREATE TABLE link_rule (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id       BIGINT NOT NULL REFERENCES org (id),
    kind         SMALLINT NOT NULL,
    pattern      TEXT NOT NULL, -- regex (kind 1) or language name (kind 2)
    url_template TEXT NOT NULL,
    position     INT NOT NULL DEFAULT 0,
    origin_system TEXT, origin_id TEXT, origin_meta JSONB,
    UNIQUE (org_id, kind, pattern)
);

-- ADR-007 M-6: report a message to moderators (v1).
-- status: 1 open · 2 resolved · 3 dismissed.
CREATE TABLE message_report (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    org_id      BIGINT NOT NULL REFERENCES org (id),
    message_id  BIGINT NOT NULL REFERENCES message (id),
    reporter_id BIGINT NOT NULL REFERENCES user_account (id),
    reason      TEXT NOT NULL DEFAULT '',
    status      SMALLINT NOT NULL DEFAULT 1,
    resolved_by BIGINT REFERENCES user_account (id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX message_report_open_idx
    ON message_report (org_id, id DESC) WHERE status = 1;
